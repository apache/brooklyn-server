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
import javax.ws.rs.container.ContainerRequestContext;

import org.apache.brooklyn.rest.filter.BrooklynSecurityProviderFilter.SimpleSecurityContext;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
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
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Configurable OAuth redirect security provider
 * 
 *  Redirects all inbound requests to an oath web server unless a session token is specified. */
public class OauthSecurityProvider implements SecurityProvider, SecurityProvider.PostAuthenticator {

    public static final Logger LOG = LoggerFactory.getLogger(OauthSecurityProvider.class);

    // TODO replace with LOG
    private static final Logger logger = LoggerFactory.getLogger(OauthSecurityProvider.class);
    private static final String SESSION_KEY_ACCESS_TOKEN = "access_token";
    private static final String SESSION_KEY_CODE = "code";
    private static final String FAKE_TOKEN_FOR_DEBUG = "fake_token";
//    public static final String PARAM_URI_TOKEN_INFO = "uriTokenInfo";
//    public static final String PARAM_URI_GETTOKEN = "uriGetToken";
//    public static final String PARAM_URI_LOGIN_REDIRECT = "uriLoginRedirect";
//    public static final String PARAM_CLIENT_ID = "clientId";
//    public static final String PARAM_CLIENT_SECRET = "clientSecret";
//    public static final String PARAM_CALLBACK_URI = "callbackUri";
//    public static final String PARAM_AUDIENCE = "audience";

    // tempting to use getJettyRequest().getRequestURL().toString();
    // but some oauth providers require this to be declared
    private String callbackUri = "http://localhost.io:8081/";
    
    private String uriGetToken = "https://accounts.google.com/o/oauth2/token";
    private String uriAuthorize = "https://accounts.google.com/o/oauth2/auth";
    private String uriTokenInfo = "https://www.googleapis.com/oauth2/v1/tokeninfo";
    
    // or github:
//    private String uriGetToken = "https://github.com/login/oauth/authorize";
//    private String uriAuthorize = "https://github.com/login/oauth/authorize";
//    private String uriTokenInfo = "https://github.com/login/oauth/access_token";
    
//    private String apiURLBase = "https://api.github.com/";

//    private String uriTokenRedirect = "/";
    // google
    // TODO parameterise
    private String clientId = "789182012565-burd24h3bc0im74g2qemi7lnihvfqd02.apps.googleusercontent.com";
    private String clientSecret = "X00v-LfU34U4SfsHqPKMWfQl";
    // github
//    private String clientId = "7f76b9970d8ac15b30b0";
//    private String clientSecret = "9e15f8dd651f0b1896a3a582f17fa82f049fc910";
    
    // TODO
    private String audience = "audience";

//    private static final String OAUTH2_TOKEN = "org.apache.activemq.jaas.oauth2.token";
//    private static final String OAUTH2_ROLE = "org.apache.activemq.jaas.oauth2.role";
//    private static final String OAUTH2_URL = "org.apache.activemq.jaas.oauth2.oauth2url";
//    private Set<Principal> principals = new HashSet<>();
//    private Subject subject;
//    private CallbackHandler callbackHandler;
//    private boolean debug;
//    private String roleName = "webconsole";
//    private String oauth2URL = uriTokenInfo;
//    private boolean loginSucceeded;
//    private String userName;
//    private boolean commitSuccess;
    
    @Override
    public boolean isAuthenticated(HttpSession session) {
        LOG.info("isAuthenticated 1 "+session+" ... "+this);
        Object token = session.getAttribute(SESSION_KEY_ACCESS_TOKEN);
        // TODO is it valid?
        return token!=null;
    }

    @Override
    public boolean authenticate(HttpSession session, String user, String password) {
        LOG.info("authenticate "+session+" "+user);
        
        if (isAuthenticated(session)) {
            return true;
        }
        
        Request request = getJettyRequest();
        // Redirection from the authenticator server
        String code = request.getParameter(SESSION_KEY_CODE);
        // Getting token, if exists, from the current session
        String token = (String) request.getSession().getAttribute(SESSION_KEY_ACCESS_TOKEN);
        
        try {
            if (Strings.isNonBlank(code)) {
                return getToken();
            } else if (Strings.isEmpty(token)) {
                return redirectLogin();
            } else {
                return validateToken(token);
            }
        } catch (Exception e) {
            LOG.warn("Error performing OAuth: "+e, e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public boolean logout(HttpSession session) {
        LOG.info("logout");
        session.removeAttribute(SESSION_KEY_ACCESS_TOKEN);
        return true;
    }
    
    @Override
    public boolean requiresUserPass() {
        return false;
    }

    private boolean getToken() throws ClientProtocolException, IOException, ServletException {
        Request request = getJettyRequest();
        String code = request.getParameter(SESSION_KEY_CODE);

        // get the access token by post to Google
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_CODE, code);
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        params.put("redirect_uri", callbackUri);
        params.put("grant_type", "authorization_code");

        String body = post(uriGetToken, params);

        Map<?,?> jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (Map<?,?>) Yamls.parseAll(body).iterator().next();
            logger.info("Parsed '"+body+"' as "+jsonObject);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            logger.info("Unable to parse: '"+body+"'");
            // throw new RuntimeException("Unable to parse json " + body);
            return redirectLogin();
        }

        // Left token and code in session
        String accessToken = (String) jsonObject.get(SESSION_KEY_ACCESS_TOKEN);
        request.getSession().setAttribute(SESSION_KEY_ACCESS_TOKEN, accessToken);
        request.getSession().setAttribute(SESSION_KEY_CODE, code);

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
        // TODO how do we get the user ID back?
        return true;
    }
    
    @Override
    public void postAuthenticate(ContainerRequestContext requestContext) {
        Request request = getJettyRequest();
        String token = (String) request.getSession().getAttribute(SESSION_KEY_ACCESS_TOKEN);
        LOG.info("TOKEN post authed = "+token);
        String user = token; // TODO not right - see above
        requestContext.setSecurityContext(new SimpleSecurityContext(user, (role) -> false, request.isSecure(), "brooklyn-oauth"));
    }

    private boolean validateToken(String token) throws ClientProtocolException, IOException {
        // TODO for debug
        if(token.equals(FAKE_TOKEN_FOR_DEBUG)){
            return true;
        }
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_ACCESS_TOKEN, token);

        String body = post(uriTokenInfo, params);
        // System.out.println(body);
        Map<?,?> jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (Map<?,?>) Yamls.parseAll(body).iterator().next();
            logger.info("Parsed '"+body+"' as "+jsonObject);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            logger.info("Unable to parse: '"+body+"'");
            throw new RuntimeException("Unable to parse json " + body, e);
        }

        // TODO what's this for?
        if (!clientId.equals(jsonObject.get(audience))) {
            return redirectLogin();
        }
        // if (isTokenExpiredOrNearlySo(...) { ... }
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

    private boolean redirectLogin() throws IOException {
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

        Response response = getJettyResponse();
        response.reset();
//        response.addHeader("Origin", "http://localhost.io:8081");
//        response.addHeader("Access-Control-Allow-Origin", "*");
////        response.addHeader("Access-Control-Request-Method", "GET, POST");
////        response.addHeader("Access-Control-Request-Headers", "origin, x-requested-with");
        LOG.info("OAUTH url redirect is: "+oauthUrl.toString());
        
        response.sendRedirect(oauthUrl.toString());

        return false;
    }

    private Request getJettyRequest() {
        return Optional.ofNullable(HttpConnection.getCurrentConnection())
                .map(HttpConnection::getHttpChannel)
                .map(HttpChannel::getRequest)
                .orElse(null);
    }

    private Response getJettyResponse() {
        return Optional.ofNullable(HttpConnection.getCurrentConnection())
                .map(HttpConnection::getHttpChannel)
                .map(HttpChannel::getResponse)
                .orElse(null);
    }
}
