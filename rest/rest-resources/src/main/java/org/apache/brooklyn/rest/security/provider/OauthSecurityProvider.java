/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.rest.security.provider;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.ServletException;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.rest.filter.BrooklynSecurityProviderFilterHelper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configurable OAuth redirect security provider
 * 
 *  Redirects all inbound requests to an oath web server unless a session token is specified. */
public class OauthSecurityProvider implements SecurityProvider {

    public static final Logger LOG = LoggerFactory.getLogger(OauthSecurityProvider.class);

    private static final String OAUTH_ACCESS_TOKEN_SESSION_KEY = "org.apache.brooklyn.security.oauth.access_token";
    private static final String OAUTH_ACCESS_TOKEN_EXPIRY_UTC_KEY = "org.apache.brooklyn.security.oauth.access_token_expiry_utc";
    
    private static final String OAUTH_AUTH_CODE_PARAMETER_FROM_USER = "code";
    private static final String OAUTH_AUTH_CODE_PARAMETER_FOR_SERVER = OAUTH_AUTH_CODE_PARAMETER_FROM_USER;
    
    // TODO parameterise
    
    // tempting to use getJettyRequest().getRequestURL().toString();
    // but some oauth providers require this to be declared
    private String callbackUri = "http://localhost.io:8081/";
    private String accessTokenResponseKey = "access_token";
    private String audience = "audience";
    private Duration validity = Duration.hours(1);
    
    // google test data
    private String uriGetToken = "https://accounts.google.com/o/oauth2/token";
    private String uriAuthorize = "https://accounts.google.com/o/oauth2/auth";
    private String uriTokenInfo = "https://www.googleapis.com/oauth2/v1/tokeninfo";
    private String clientId = "789182012565-burd24h3bc0im74g2qemi7lnihvfqd02.apps.googleusercontent.com";
    private String clientSecret = "X00v-LfU34U4SfsHqPKMWfQl";
    
    // github test data
//    private String uriGetToken = "https://github.com/login/oauth/authorize";
//    private String uriAuthorize = "https://github.com/login/oauth/authorize";
//    private String uriTokenInfo = "https://github.com/login/oauth/access_token";
//    private String clientId = "7f76b9970d8ac15b30b0";
//    private String clientSecret = "9e15f8dd651f0b1896a3a582f17fa82f049fc910";
    
    @Override
    public boolean isAuthenticated(HttpSession session) {
        LOG.info("isAuthenticated 1 "+getJettyRequest().getRequestURI()+" "+session+" ... "+this);
        Object token = session.getAttribute(OAUTH_ACCESS_TOKEN_SESSION_KEY);
        // TODO is it valid?
        return token!=null;
    }

    @Override
    public boolean authenticate(HttpSession session, String user, String password) throws SecurityProviderDeniedAuthentication {
        LOG.info("authenticate "+session+" "+user);
        
        if (isAuthenticated(session)) {
            return true;
        }
        
        Request request = getJettyRequest();
        // Redirection from the authenticator server
        String code = request.getParameter(OAUTH_AUTH_CODE_PARAMETER_FROM_USER);
        // Getting token, if exists, from the current session
        String token = (String) request.getSession().getAttribute(OAUTH_ACCESS_TOKEN_SESSION_KEY);
        
        try {
            if (Strings.isNonBlank(code)) {
                return retrieveTokenForAuthCodeFromOauthServer(session, code);
            } else if (Strings.isNonBlank(token)) {
                // they have a token but no auth code and not or no longer authenticated; 
                // we need to check that the token is still valid
                return validateTokenAgainstOauthServer(token);
            } else {
                // no token or code; the user needs to log in
                return redirectUserToOauthLoginUi();
            }
        } catch (SecurityProviderDeniedAuthentication e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("Error performing OAuth: "+e, e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public boolean logout(HttpSession session) {
        LOG.info("logout");
        session.removeAttribute(OAUTH_ACCESS_TOKEN_SESSION_KEY);
        session.removeAttribute(OAUTH_ACCESS_TOKEN_EXPIRY_UTC_KEY);
        return true;
    }
    
    @Override
    public boolean requiresUserPass() {
        return false;
    }

    private boolean retrieveTokenForAuthCodeFromOauthServer(HttpSession session, String code) throws ClientProtocolException, IOException, ServletException, SecurityProviderDeniedAuthentication {
        // get the access token by post to Google
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(OAUTH_AUTH_CODE_PARAMETER_FOR_SERVER, code);
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        params.put("redirect_uri", callbackUri);
        params.put("grant_type", "authorization_code");

        String body = post(uriGetToken, params);

        Map<?,?> jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (Map<?,?>) Yamls.parseAll(body).iterator().next();
            LOG.info("Parsed '"+body+"' as "+jsonObject);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.info("Unable to parse: '"+body+"'");
            // throw new RuntimeException("Unable to parse json " + body);
            return redirectUserToOauthLoginUi();
        }

        // TODO validate
        
        // Put token in session
        String accessToken = (String) jsonObject.get(accessTokenResponseKey);
        session.setAttribute(OAUTH_ACCESS_TOKEN_SESSION_KEY, accessToken);
        
        // TODO record code?
//        request.getSession().setAttribute(SESSION_KEY_CODE, code);

        // TODO is it valid?
        LOG.debug("Got token/code "+accessToken+"/"+code+" from "+jsonObject);
        // eg Got token/code 
        // ya29.GluHBtzZ-R-CaoWMlso6KB6cq3DrbmwX6B3kjMmzWqzU-vO76WjKuNS3Ktog7vt9CJnxSZ63NmqO4p5bg20wl0-M14yO1LuoXNV5JX3qHDmXl2rl-z1LbCPEYJ-o
        //    /  4/yADFJRSRCxLgZFcpD_KU2jQiCXBGNHTsw0eGZqZ2t6IJJh2O1oWBnBDx4eWl4ZLCRAFJx3QjPYtl7LF9zj_DNlA 
        // from {
        //   access_token=ya29.GluHBtzZ-R-CaoWMlso6KB6cq3DrbmwX6B3kjMmzWqzU-vO76WjKuNS3Ktog7vt9CJnxSZ63NmqO4p5bg20wl0-M14yO1LuoXNV5JX3qHDmXl2rl-z1LbCPEYJ-o, 
        //   expires_in=3600, 
        //   refresh_token=1/b2Xk2rCVqKFsbz_xePv1tctvihnLoyo0YHsw4YQWK8M, 
        //   scope=https://www.googleapis.com/auth/userinfo.email https://www.googleapis.com/auth/plus.me, 
        //   token_type=Bearer, 
        //   id_token=eyJhbGciOiJSUzI1NiIsImtpZCI6Ijc5NzhhOTEzNDcyNjFhMjkxYmQ3MWRjYWI0YTQ2NGJlN2QyNzk2NjYiLCJ0eXAiOiJKV1QifQ.eyJpc3MiOiJhY2NvdW50cy5nb29nbGUuY29tIiwiYXpwIjoiNzg5MTgyMDEyNTY1LWJ1cmQyNGgzYmMwaW03NGcycWVtaTdsbmlodmZxZDAyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwiYXVkIjoiNzg5MTgyMDEyNTY1LWJ1cmQyNGgzYmMwaW03NGcycWVtaTdsbmlodmZxZDAyLmFwcHMuZ29vZ2xldXNlcmNvbnRlbnQuY29tIiwic3ViIjoiMTA2MDQyNTE3MjU2MTcxNzYyMTU0IiwiaGQiOiJjbG91ZHNvZnRjb3JwLmNvbSIsImVtYWlsIjoiYWxleC5oZW5ldmVsZEBjbG91ZHNvZnRjb3JwLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjp0cnVlLCJhdF9oYXNoIjoiQXpsdHo3YnR2Wk81eTZiMXMtVzRxdyIsImlhdCI6MTU0NjYyNDE2MiwiZXhwIjoxNTQ2NjI3NzYyfQ.E0NWILU7EEHL3GsveVFW1F91sml9iRceWpfVVc9blSKqafAcNwRl08JKT1FXUgfOUvgoYYj6IDIxT4L59-3CObNHS7RtbDJmIk0eWf_h8OFFGTTtd6P2-FTtM-6HVLKkMcKvJHHB07APsqeQj4o3zWY4G3f0QIX6bb424PwxwcDGS6gO8aA9cX2vVyr90h8FgtR9qnbYQxaSrcQNmEmPYHPZiOMzFoxR5WpXhtmPAFc4sMVFjrvQEf8s3GSr6ciMdC7BtKhfBII8s9iYV4LJCRQjxvsCzZ_PfWAZmExNaNOVltfoo5uGVmDPvzCUbSIoPmpj4jpPJVJ0fQtl7E6Tlg}
        // TODO get the user's ID : https://stackoverflow.com/questions/24442668/google-oauth-api-to-get-users-email-address
        String user = accessToken;  // wrong, see above
        session.setAttribute(BrooklynSecurityProviderFilterHelper.AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
        
        return true;
    }

    private boolean validateTokenAgainstOauthServer(String token) throws ClientProtocolException, IOException {
        // TODO support validation, and run periodically
        
//        HashMap<String, String> params = new HashMap<String, String>();
//        params.put(OAUTH_ACCESS_TOKEN_KEY, token);
//
//        String body = post(uriTokenInfo, params);
//        
//        Map<?,?> jsonObject = null;
//        // get the access token from json and request info from Google
//        try {
//            jsonObject = (Map<?,?>) Yamls.parseAll(body).iterator().next();
//            LOG.info("Parsed '"+body+"' as "+jsonObject);
//        } catch (Exception e) {
//            Exceptions.propagateIfFatal(e);
//            LOG.info("Unable to parse: '"+body+"'");
//            throw new RuntimeException("Unable to parse json " + body, e);
//        }
//
//        if (!clientId.equals(jsonObject.get(audience))) {
//            LOG.warn("Oauth not meant for this client ("+clientId+"), redirecting user to login again: "+jsonObject);
//            return redirectUserToOauthLoginUi();
//        }
//        
//        // TODO
//        // if (isTokenExpiredOrNearlySo(...) { ... }
        
        return true;
    }

    // TODO these http methods need tidying
    
    // makes a GET request to url and returns body as a string
    public String get(String url) throws ClientProtocolException, IOException {
        return execute(new HttpGet(url));
    }
    
    // makes a POST request to url with form parameters and returns body as a
    // string
    public String post(String url, Map<String, String> formParameters) throws ClientProtocolException, IOException {
        HttpPost request = new HttpPost(url);

        List<NameValuePair> nvps = new ArrayList<NameValuePair>();
        for (String key : formParameters.keySet()) {
            nvps.add(new BasicNameValuePair(key, formParameters.get(key)));
        }
        request.setEntity(new UrlEncodedFormEntity(nvps));

        return execute(request);
    }
    
    // makes request and checks response code for 200
    private String execute(HttpRequestBase request) throws ClientProtocolException, IOException {
        // TODO tidy
        HttpClient httpClient = new DefaultHttpClient();
        HttpResponse response = httpClient.execute(request);

        HttpEntity entity = response.getEntity();
        String body = EntityUtils.toString(entity);

        if (response.getStatusLine().getStatusCode() != 200) {
            throw new RuntimeException(
                    "Expected 200 but got " + response.getStatusLine().getStatusCode() + ", with body " + body);
        }

        return body;
    }

    private boolean redirectUserToOauthLoginUi() throws IOException, SecurityProviderDeniedAuthentication {
        String state=Identifiers.makeRandomId(12); //should be stored in session
        StringBuilder oauthUrl = new StringBuilder().append(uriAuthorize)
                .append("?response_type=").append("code")
                .append("&client_id=").append(clientId) // the client id from the api console registration
                .append("&redirect_uri=").append(callbackUri) // the servlet that github redirects to after
                // authorization
//                .append("&scope=").append("user public_repo")
                .append("&scope=openid%20email") // scope is the api permissions we
                .append("&state=").append(state)
                .append("&access_type=offline") // here we are asking to access to user's data while they are not
                // signed in
                .append("&approval_prompt=force"); // this requires them to verify which account to use, if they are
        // already signed in

        // just for look inside
//        Collection<String> originalHeaders = response.getHeaderNames();

        throw new SecurityProviderDeniedAuthentication(
            Response.status(Status.FOUND).header(HttpHeader.LOCATION.asString(), oauthUrl.toString()).build());
//        response.addHeader("Origin", "http://localhost.io:8081");
//        response.addHeader("Access-Control-Allow-Origin", "*");
//        response.addHeader("Access-Control-Request-Method", "GET, POST");
//        response.addHeader("Access-Control-Request-Headers", "origin, x-requested-with");
    }

    private Request getJettyRequest() {
        return Optional.ofNullable(HttpConnection.getCurrentConnection())
                .map(HttpConnection::getHttpChannel)
                .map(HttpChannel::getRequest)
                .orElse(null);
    }

}
