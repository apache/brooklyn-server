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
package org.apache.brooklyn.rest.filter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class GoogleOauthFilter implements Filter {

    private static final String SESSION_KEY_CODE = "code";

    private static final String SESSION_KEY_ACCESS_TOKEN = "access_token";

    public static final String PARAM_URI_TOKEN_INFO = "uriTokenInfo";
    private String uriTokenInfo = "";
    public static final String PARAM_URI_GETTOKEN = "uriGetToken";
    private String uriGetToken = "";
    public static final String PARAM_URI_LOGIN_REDIRECT = "uriLoginRedirect";
    private String uriTokenRedirect = "";
    public static final String PARAM_CLIENT_ID = "clientId";
    private String clientId = "";
    public static final String PARAM_CLIENT_SECRET = "clientSecret";
    private String clientSecret = "";
    public static final String PARAM_CALLBACK_URI = "callbackUri";
    private String callbackUri = "";
    public static final String PARAM_AUDIENCE = "audience";
    private String audience = "";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        initializateParams(filterConfig);
    }

    private void initializateParams(FilterConfig filterConfig) {
        Enumeration<String> enums = filterConfig.getInitParameterNames();

        while (enums.hasMoreElements()) {
            String paramKey = enums.nextElement();
            String paramValue = filterConfig.getInitParameter(paramKey);
            System.out.println(paramKey + ":" + paramValue);
            switch (paramKey) {
            case PARAM_URI_TOKEN_INFO:
                uriTokenInfo = paramValue;
                break;
            case PARAM_URI_GETTOKEN:
                uriGetToken = paramValue;
                break;
            case PARAM_URI_LOGIN_REDIRECT:
                uriTokenRedirect = paramValue;
                break;
            case PARAM_CLIENT_ID:
                clientId = paramValue;
                break;
            case PARAM_CLIENT_SECRET:
                clientSecret = paramValue;
                break;
            case PARAM_CALLBACK_URI:
                callbackUri = paramValue;
                break;
            case PARAM_AUDIENCE:
                audience = paramValue;
                break;
            default:
                System.out.println("Ignored param: " + paramKey + ":" + paramValue);
            }
        }
    }

    @Override
    public void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) req;
        // Redirection from the authenticator server
        String code = req.getParameter(SESSION_KEY_CODE);

        // Getting token, if exists, from the current session
        String token = (String) request.getSession().getAttribute(SESSION_KEY_ACCESS_TOKEN);

        boolean continueFilterProcessing;
        if (code != null && !"".equals(code)) { // in brooklyn, have
                                                // Strings.isNonBlank(code)
            continueFilterProcessing = getToken(req, resp, chain);
        } else if (token == null || "".equals(token)) { // isBlank
            continueFilterProcessing = redirectLogin(resp);
        } else {
            continueFilterProcessing = validateToken(token, resp);
        }
        if (continueFilterProcessing) {
            chain.doFilter(req, resp);
        }
    }

    private boolean validateToken(String token, ServletResponse resp) throws ClientProtocolException, IOException {
        // System.out.println("########################### Validating token
        // ###########################");
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_ACCESS_TOKEN, token);

        String body = post(uriTokenInfo, params);
        // System.out.println(body);
        JSONObject jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (JSONObject) new JSONParser().parse(body);
        } catch (ParseException e) {
            throw new RuntimeException("Unable to parse json " + body);
        }

        if (!clientId.equals(jsonObject.get(audience))) {
            return redirectLogin(resp);
        }
        // if (isTokenExpiredOrNearlySo(...) { ... }
        return true;
    }

    private boolean getToken(ServletRequest req, ServletResponse resp, FilterChain chain)
            throws ClientProtocolException, IOException, ServletException {
        String code = req.getParameter(SESSION_KEY_CODE);

        // get the access token by post to Google
        HashMap<String, String> params = new HashMap<String, String>();
        params.put(SESSION_KEY_CODE, code);
        params.put("client_id", clientId);
        params.put("client_secret", clientSecret);
        params.put("redirect_uri", callbackUri);
        params.put("grant_type", "authorization_code");

        String body = post(uriGetToken, params);

        JSONObject jsonObject = null;

        // get the access token from json and request info from Google
        try {
            jsonObject = (JSONObject) new JSONParser().parse(body);
        } catch (ParseException e) {
            // throw new RuntimeException("Unable to parse json " + body);
            return redirectLogin(resp);
        }

        // Left token and code in session
        String accessToken = (String) jsonObject.get(SESSION_KEY_ACCESS_TOKEN);
        HttpServletRequest request = (HttpServletRequest) req;
        request.getSession().setAttribute(SESSION_KEY_ACCESS_TOKEN, accessToken);
        request.getSession().setAttribute(SESSION_KEY_CODE, code);

        // resp.getWriter().println(json);
        return true;
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

    // makes a GET request to url and returns body as a string
    public String get(String url) throws ClientProtocolException, IOException {
        return execute(new HttpGet(url));
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

    private boolean redirectLogin(ServletResponse response) throws IOException {
        HttpServletResponse res = (HttpServletResponse) response;
        res.setContentType(ContentType.APPLICATION_XML.toString());
        res.sendRedirect(uriTokenRedirect);
        return false;
    }

    @Override
    public void destroy() {
        // TODO Auto-generated method stub
    }

}
