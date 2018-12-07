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
package org.apache.brooklyn.rest.security.jaas;

import net.minidev.json.JSONObject;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.ContentType;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.eclipse.jetty.server.HttpChannel;
import org.eclipse.jetty.server.HttpConnection;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.Response;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.util.*;
import java.io.IOException;
import java.security.Principal;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.servlet.ServletException;


public class GoogleOauthLoginModule implements LoginModule {
    private static final Logger logger = LoggerFactory.getLogger(BrooklynLoginModule.class);
    private static final String SESSION_KEY_ACCESS_TOKEN = "access_token";
    private static final String SESSION_KEY_CODE = "code";
//    public static final String PARAM_URI_TOKEN_INFO = "uriTokenInfo";
//    public static final String PARAM_URI_GETTOKEN = "uriGetToken";
//    public static final String PARAM_URI_LOGIN_REDIRECT = "uriLoginRedirect";
//    public static final String PARAM_CLIENT_ID = "clientId";
//    public static final String PARAM_CLIENT_SECRET = "clientSecret";
//    public static final String PARAM_CALLBACK_URI = "callbackUri";
//    public static final String PARAM_AUDIENCE = "audience";

    private String authoriseURL = "https://github.com/login/oauth/authorize";
    private String tokenURL = "https://github.com/login/oauth/access_token";
    private String apiURLBase = "https://api.github.com/";
    private String uriTokenRedirect = "/";
    private String clientId = "7f76b9970d8ac15b30b0";
    private String clientSecret = "9e15f8dd651f0b1896a3a582f17fa82f049fc910";
    private String callbackUri = "http://localhost.io:8081/";
    private String audience = "audience";

    private static final String OAUTH2_TOKEN = "org.apache.activemq.jaas.oauth2.token";
    private static final String OAUTH2_ROLE = "org.apache.activemq.jaas.oauth2.role";
    private static final String OAUTH2_URL = "org.apache.activemq.jaas.oauth2.oauth2url";
    private Set<Principal> principals = new HashSet<>();
    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean debug;
    private String roleName = "webconsole";
    private String oauth2URL = tokenURL;
    private boolean loginSucceeded;
    private String userName;
    private boolean commitSuccess;

    private final Request request=getJettyRequest();
    private final Response response=getJettyResponse();

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;

        loginSucceeded = false;
        commitSuccess = false;

        debug = !"false".equalsIgnoreCase((String) options.get("debug"));

        if (debug) {
            logger.debug(">>>>>>>>>>>>>>>>Initialized debug=" + debug + " guestGroup=" + roleName + " url="
                    + oauth2URL);
        }

    }

    @Override
    public boolean login() throws LoginException {
        loginSucceeded = true;

        Callback[] callbacks = new Callback[1];
        callbacks[0] = new NameCallback("User name");

        try {
            callbackHandler.handle(callbacks);
        } catch (IOException | UnsupportedCallbackException e) {
            throw (LoginException) new LoginException().initCause(e);
        }

        userName = ((NameCallback) callbacks[0]).getName();

        if (null == userName) {
            loginSucceeded = false;
        }

        String newUrl = oauth2URL + userName;
        logger.debug("THis is the URL: " + newUrl);

        boolean eligible=false;

        // Redirection from the authenticator server
        String code = request.getParameter(SESSION_KEY_CODE);

        // Getting token, if exists, from the current session
        String token = (String) request.getSession().getAttribute(SESSION_KEY_ACCESS_TOKEN);

        try {
            if (code != null && !"".equals(code)) { // in brooklyn, have
                // Strings.isNonBlank(code)
            eligible = getToken();
            } else if (token == null || "".equals(token)) { // isBlank
                    eligible = redirectLogin();
            } else {
                eligible = validateToken(token);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ServletException e) {
            e.printStackTrace();
        }

        if (eligible) {
            principals.add(new BrooklynLoginModule.UserPrincipal(userName));
            principals.add(new BrooklynLoginModule.RolePrincipal(roleName));
        } else {
            loginSucceeded = false;
        }

        if (debug) {
            logger.debug("Token login " + loginSucceeded);
        }
        return loginSucceeded;
    }

    @Override
    public boolean commit() throws LoginException {
        if (loginSucceeded) {
            if (subject.isReadOnly()) {
                throw new LoginException("Can't commit read-only subject");
            }
            subject.getPrincipals().addAll(principals);
        }

        commitSuccess = true;
        return loginSucceeded;
    }

    @Override
    public boolean abort() throws LoginException {
        if (loginSucceeded && commitSuccess) {
            removePrincipal();
        }
        clear();
        return loginSucceeded;
    }

    @Override
    public boolean logout() throws LoginException {
        if (request != null) {
//            logger.info("REST logging {} out",providerSession.getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE));
//            provider.logout(req.getSession());
//            req.getSession().removeAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE);
        } else {
            logger.error("Request object not available for logout");
        }

        removePrincipal();
        clear();
        return true;
    }
    private boolean getToken()
            throws ClientProtocolException, IOException, ServletException {
        String code = request.getParameter(SESSION_KEY_CODE);

        // get the access token by post to Google
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_CODE, code);
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        params.put("redirect_uri", callbackUri);
        params.put("grant_type", "authorization_code");

        String body = post(authoriseURL, params);

        JSONObject jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (JSONObject) new JSONParser().parse(body);
        } catch (ParseException e) {
            // throw new RuntimeException("Unable to parse json " + body);
            return redirectLogin();
        }

        // Left token and code in session
        String accessToken = (String) jsonObject.get(SESSION_KEY_ACCESS_TOKEN);
        request.getSession().setAttribute(SESSION_KEY_ACCESS_TOKEN, accessToken);
        request.getSession().setAttribute(SESSION_KEY_CODE, code);

        // resp.getWriter().println(json);
        return true;
    }
    private boolean validateToken(String token) throws ClientProtocolException, IOException {
        // System.out.println("########################### Validating token
        // ###########################");
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_ACCESS_TOKEN, token);

        String body = post(tokenURL, params);
        // System.out.println(body);
        JSONObject jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (JSONObject) new JSONParser().parse(body);
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse json " + body);
        }

        if (!clientId.equals(jsonObject.get(audience))) {
            return redirectLogin();
        }
        // if (isTokenExpiredOrNearlySo(...) { ... }
        return true;
    }

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

    private void removePrincipal() throws LoginException {
        if (subject.isReadOnly()) {
            throw new LoginException("Read-only subject");
        }
        subject.getPrincipals().removeAll(principals);
    }

    private void clear() {
        subject = null;
        callbackHandler = null;
        principals = null;
    }

    private static String createRandomHexString(int length){
        Random random = new Random();
        StringBuilder sb = new StringBuilder();
        while (sb.length() < length) {
            sb.append(Integer.toHexString(random.nextInt()));
        }
        return sb.toString();
    }

    private boolean redirectLogin() throws IOException {
        String state=createRandomHexString(16); //should be stored in session
        StringBuilder oauthUrl = new StringBuilder().append(authoriseURL)
                .append("?response_type=").append("code")
                .append("&client_id=").append(clientId) // the client id from the api console registration
                .append("&redirect_uri=").append(callbackUri) // the servlet that github redirects to after
                // authorization
                .append("&scope=").append("user public_repo")
                .append("&state=").append(state)
                .append("&access_type=offline") // here we are asking to access to user's data while they are not
                // signed in
                .append("&approval_prompt=force"); // this requires them to verify which account to use, if they are
        // already signed in
        logger.debug(oauthUrl.toString());
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
