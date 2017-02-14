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
import java.util.Arrays;
import java.util.List;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.NewCookie;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.rest.api.ServerApi;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.collections.EnumerationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Causes the server to return a "Csrf-Token" cookie in certain circumstances,
 * and thereafter to require clients to send that token as an "X-Csrf-Token" header on certain requests.
 * <p>
 * To enable REST requests to work from the <code>br</code> CLI and regular <code>curl</code> without CSRF,
 * this CSRF protection only applies when a session is in effect.
 * Sessions are only established by some REST requests:
 * {@link ServerApi#getUpExtended()} is a good choice (/v1/server/up/extended).
 * It can also be forced by using the {@link #CSRF_TOKEN_REQUIRED_HEADER} header on any REST request.
 * Browser-based UI clients should use one of the above methods early in operation.
 * <p>
 * The standard model is that the token is required for all <i>write</i> (ie non-GET) requests being made
 * with a valid session cookie (ie the request is part of an ongoing session).
 * In such cases, the client must send the X-Csrf-Token as a header.
 * This prevents a third-party site from effecting a mutating cross-site request via the browser. 
 * <p>
 * For transitional reasons, the default behaviour in the current implementation is to warn (not fail)
 * if no token is supplied in the above case. This will likely be changed to enforce the standard model 
 * in a subsequent version, but it avoids breaking backwards compatibility for any existing session-based clients.
 * <p>
 * Clients can force different required behaviour (e.g. "fail") by including the
 * {@link #CSRF_TOKEN_REQUIRED_HEADER} with one of the values in {@link CsrfTokenRequiredForRequests},
 * viz. to require the header on ALL requests, or on NONE, instead of just on WRITE requests (the default).
 * If such a mode is explicitly specified it is enforced (instead of just displaying the transitional warning),
 * so while transitioning to enforce CSRF this header should be supplied.
 * The Brooklyn UI does this.
 * <p>
 * This uses *two* names, <code>Csrf-Token</code> and <code>Xsrf-Token</code>.
 * The former seems the usual convention, but Angular apps use the latter.
 * This strategy should mean that Angular apps should get CSRF protection with no special configuration.
 * <p>
 * The cookies are sent by the client on every request, by virtue of being cookies,
 * although this is not necessary (and rather wasteful). A client may optimise by deleting the cookies 
 * and caching the information in another form. 
 * The cookie names do not have any salt/uid, so in dev on localhost you may get conflicts, e.g. between
 * multiple Brooklyn instances or other apps that use the same token names.
 * (In contrast the session ID token, usually JSESSIONID, has a BROOKLYN<uid> field at runtime to prevent
 * such collisions.)
 * <p>
 * Additional CSRF strategies might be worth considering, in addition to the one described above:
 * <ul>
 * <li> Compare "Referer / Origin" against "Host" / "X-Forwarded-Host" (or app knowing its location)
 *      (disallowing if different is probably a good idea)
 * <li> Look for "X-Requested-With: XMLHttpRequest" on POST, esp if there is cookie and/or user agent is a browser
 *      (but angular drops this one)
 * </ul>
 * More info at:  https://www.owasp.org/index.php/Cross-Site_Request_Forgery_(CSRF)_Prevention_Cheat_Sheet .
 * (These have not been implemented because the cookie pattern is sufficient, and one of the strongest.)
 * 
 */
@Provider
@Priority(400)
public class CsrfTokenFilter implements ContainerRequestFilter, ContainerResponseFilter {
    
    private static final Logger log = LoggerFactory.getLogger(CsrfTokenFilter.class);

    private static final String CSRF_TOKEN_VALUE_BASE = "Csrf-Token";
    public static final String CSRF_TOKEN_VALUE_COOKIE = CSRF_TOKEN_VALUE_BASE.toUpperCase();
    public static final String CSRF_TOKEN_VALUE_HEADER = HEADER_OF_COOKIE(CSRF_TOKEN_VALUE_BASE);
    // also send this so angular works as expected
    public static final String CSRF_TOKEN_VALUE_BASE_ANGULAR_NAME = "Xsrf-Token";
    public static final String CSRF_TOKEN_VALUE_COOKIE_ANGULAR_NAME = CSRF_TOKEN_VALUE_BASE_ANGULAR_NAME.toUpperCase();
    public static final String CSRF_TOKEN_VALUE_HEADER_ANGULAR_NAME = HEADER_OF_COOKIE(CSRF_TOKEN_VALUE_BASE_ANGULAR_NAME);
    
    public static final String CSRF_TOKEN_VALUE_ATTR = CsrfTokenFilter.class.getName()+"."+CSRF_TOKEN_VALUE_COOKIE;
    
    public static String HEADER_OF_COOKIE(String cookieName) {
        return "X-"+cookieName;
    }
    
    public static final String CSRF_TOKEN_REQUIRED_HEADER = "X-Csrf-Token-Required-For-Requests";
    public static final String CSRF_TOKEN_REQUIRED_ATTR = CsrfTokenFilter.class.getName()+"."+CSRF_TOKEN_REQUIRED_HEADER;
    public static enum CsrfTokenRequiredForRequests { ALL, WRITE, NONE, }
    public static CsrfTokenRequiredForRequests DEFAULT_REQUIRED_FOR_REQUESTS = CsrfTokenRequiredForRequests.WRITE;

    @Context
    private HttpServletRequest request;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        // if header supplied, check it is valid
        String requiredWhenS = request.getHeader(CSRF_TOKEN_REQUIRED_HEADER);
        if (Strings.isNonBlank(requiredWhenS) && getRequiredForRequests(requiredWhenS, null)==null) {
            fail(requestContext, ApiError.builder().errorCode(Response.Status.BAD_REQUEST)
                .message(HEADER_OF_COOKIE(CSRF_TOKEN_REQUIRED_HEADER)+" header if supplied must be one of "
                    + Arrays.asList(CsrfTokenRequiredForRequests.values()))
                .build());
            return;
    }
        
        if (!request.isRequestedSessionIdValid()) {
            // no session; just return
            return;
        }
        
        @SuppressWarnings("unchecked")
        List<String> suppliedTokensDefault = EnumerationUtils.toList(request.getHeaders(CSRF_TOKEN_VALUE_HEADER));
        @SuppressWarnings("unchecked")
        List<String> suppliedTokensAngular = EnumerationUtils.toList(request.getHeaders(CSRF_TOKEN_VALUE_HEADER_ANGULAR_NAME));
        List<String> suppliedTokens = Lists.newArrayList(suppliedTokensDefault);
        suppliedTokens.addAll(suppliedTokensAngular);

        Object requiredToken = request.getSession().getAttribute(CSRF_TOKEN_VALUE_ATTR);
        CsrfTokenRequiredForRequests whenRequired = (CsrfTokenRequiredForRequests) request.getSession().getAttribute(CSRF_TOKEN_REQUIRED_ATTR);

        boolean isRequired;
        
        if (whenRequired==null) {
            if (suppliedTokens.isEmpty()) {
                log.warn("No CSRF token expected or supplied but a cookie-session is active: client should be updated"
                    + " (in future CSRF tokens or instructions may be required for session-based clients)"
                    + " - " + Entitlements.getEntitlementContext());
                whenRequired = CsrfTokenRequiredForRequests.NONE;
            } else {
                // default
                whenRequired = DEFAULT_REQUIRED_FOR_REQUESTS;
            }
            // remember it to suppress warnings subsequently
            request.getSession().setAttribute(CSRF_TOKEN_REQUIRED_ATTR, whenRequired);
        }
        
        switch (whenRequired) {
        case NONE:
            isRequired = false;
            break;
        case WRITE:
            isRequired = !org.eclipse.jetty.http.HttpMethod.GET.toString().equals(requestContext.getMethod());
            break;
        case ALL:
            isRequired = true;
            break;
        default:
            log.warn("Unexpected "+CSRF_TOKEN_REQUIRED_ATTR+" value "+whenRequired);
            isRequired = true;
        }
        
        if (Iterables.any(suppliedTokens, Predicates.equalTo(requiredToken))) {
            // matching token supplied, or not being used 
            return;
        }
        
        if (!isRequired) {
            // csrf not required, but it doesn't match
            if (requiredToken!=null) {
                log.trace("CSRF optional token mismatch: client did not send valid token, but it isn't required so proceeding");
            }
            return;
        }

        if (suppliedTokens.isEmpty()) {
            fail(requestContext, ApiError.builder().errorCode(Response.Status.UNAUTHORIZED)
                    .message(HEADER_OF_COOKIE(CSRF_TOKEN_VALUE_COOKIE)+" header is required, containing token previously returned from server in cookie")
                    .build());
        } else {
            fail(requestContext, ApiError.builder().errorCode(Response.Status.UNAUTHORIZED)
                .message(HEADER_OF_COOKIE(CSRF_TOKEN_VALUE_COOKIE)+" header did not match expected CSRF token")
                .build());
        }
    }

    private void fail(ContainerRequestContext requestContext, ApiError apiError) {
        requestContext.abortWith(apiError.asJsonResponse());
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        HttpSession session = request.getSession(false);
        String token = (String) (session==null ? null : session.getAttribute(CSRF_TOKEN_VALUE_ATTR));
        String requiredWhenS = request.getHeader(CSRF_TOKEN_REQUIRED_HEADER);
        
        if (session==null) {
            if (Strings.isBlank(requiredWhenS)) {
                // no session and no requirement specified, bail out 
                return;
            }
            // explicit requirement request forces a session  
            session = request.getSession();
        }
        
        CsrfTokenRequiredForRequests requiredWhen;
        if (Strings.isNonBlank(requiredWhenS)) {
            requiredWhen = getRequiredForRequests(requiredWhenS, DEFAULT_REQUIRED_FOR_REQUESTS);
            request.getSession().setAttribute(CSRF_TOKEN_REQUIRED_ATTR, requiredWhen);
            if (Strings.isNonBlank(token)) {
                // already set csrf token, and the client got it
                // (with the session token if they are in a session;
                // or if client didn't get it it isn't in a session)
                return;
            }
        } else {
            if (Strings.isNonBlank(token)) {
                // already set csrf token, and the client got it
                // (with the session token if they are in a session;
                // or if client didn't get it it isn't in a session)
                return;
            }
            requiredWhen = (CsrfTokenRequiredForRequests) request.getSession().getAttribute(CSRF_TOKEN_REQUIRED_ATTR);
            if (requiredWhen==null) {
                requiredWhen = DEFAULT_REQUIRED_FOR_REQUESTS;
                request.getSession().setAttribute(CSRF_TOKEN_REQUIRED_ATTR, requiredWhen);
            }
        }

        if (requiredWhen==CsrfTokenRequiredForRequests.NONE) {
            // not required; so don't set
            return;
        }

        // create the token
        token = Identifiers.makeRandomId(16);
        request.getSession().setAttribute(CSRF_TOKEN_VALUE_ATTR, token);
        
        addCookie(responseContext, CSRF_TOKEN_VALUE_COOKIE, token, "Clients should send this value in header "+CSRF_TOKEN_VALUE_HEADER+" for validation");
        addCookie(responseContext, CSRF_TOKEN_VALUE_COOKIE_ANGULAR_NAME, token, "Compatibility cookie for "+CSRF_TOKEN_VALUE_COOKIE+" following AngularJS conventions");
    }

    protected NewCookie addCookie(ContainerResponseContext responseContext, String cookieName, String token, String comment) {
        NewCookie cookie = new NewCookie(cookieName, token, "/", null, comment, -1, false);
        responseContext.getHeaders().add("Set-Cookie", cookie);
        return cookie;
    }

    protected CsrfTokenRequiredForRequests getRequiredForRequests(String requiredWhenS, CsrfTokenRequiredForRequests defaultMode) {
        CsrfTokenRequiredForRequests result = null;
        if (requiredWhenS!=null) {
            result = CsrfTokenRequiredForRequests.valueOf(requiredWhenS.toUpperCase());
        }
        if (result!=null) return result;
        return defaultMode;
    }

}
