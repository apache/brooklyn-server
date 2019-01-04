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
import java.security.Principal;
import java.util.function.Function;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.PostAuthenticator;
import org.apache.commons.codec.binary.Base64;
import org.apache.http.auth.BasicUserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a filter that performs authentication with the {@link SecurityProvider}
 * as configured according to {@link BrooklynWebConfig#SECURITY_PROVIDER_CLASSNAME}.
 * 
 * This replaces the JAAS {@link BrooklynLoginModule} because that login module requires
 * Basic auth, which is not flexible enough to support redirect-based solutions like Oauth. 
 */
@Provider
@Priority(100)
public class BrooklynSecurityProviderFilter implements ContainerRequestFilter {

    /**
     * The session attribute set for authenticated users; for reference
     * (but should not be relied up to confirm authentication, as
     * the providers may impose additional criteria such as timeouts,
     * or a null user (no login) may be permitted)
     */
    public static final String AUTHENTICATED_USER_SESSION_ATTRIBUTE = "brooklyn.user";

    private static final Logger log = LoggerFactory.getLogger(BrooklynSecurityProviderFilter.class);

    
    @Context
    HttpServletRequest webRequest;
    
    @Context
    private ContextResolver<ManagementContext> mgmtC;
    
    private ManagementContext mgmt() {
        return mgmtC.getContext(ManagementContext.class);
    }
    
    public static class SimpleSecurityContext implements SecurityContext {
        final Principal principal;
        final Function<String,Boolean> roleChecker;
        final boolean secure;
        final String authScheme;
        
        public SimpleSecurityContext(String username, Function<String, Boolean> roleChecker, boolean secure, String authScheme) {
            this.principal = new BasicUserPrincipal(username);
            this.roleChecker = roleChecker;
            this.secure = secure;
            this.authScheme = authScheme;
        }

        @Override
        public Principal getUserPrincipal() {
            return principal;
        }

        @Override
        public boolean isUserInRole(String role) {
            return roleChecker.apply(role);
        }

        @Override
        public boolean isSecure() {
            return secure;
        }

        @Override
        public String getAuthenticationScheme() {
            return authScheme;
        }
    }
    
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        SecurityProvider provider = getProvider();
        HttpSession session = webRequest.getSession(false);
        
        if (provider.isAuthenticated(session)) {
            return;
        }
        
        String user = null, pass = null;
        if (provider.requiresUserPass()) {
            String authorization = webRequest.getHeader("Authorization");
            if (authorization != null) {
                String userpass = new String(Base64.decodeBase64(authorization.substring(6)));
                int idxColon = userpass.indexOf(":");
                if (idxColon >= 0) {
                    user = userpass.substring(0, idxColon);
                    pass = userpass.substring(idxColon + 1);
                } else {
                    abort(requestContext, "Invalid authorization string");
                    return;
                }
            } else {
                abort(requestContext, "Authorization required");
                return;
            }
        }
        
        if (session==null) {
            // only create the session if an auth string is supplied
            session = webRequest.getSession(true);
        }
        session.setAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE, webRequest.getRemoteAddr());
        
        if (provider.authenticate(session, user, pass)) {
            if (provider instanceof PostAuthenticator) {
                ((PostAuthenticator)provider).postAuthenticate(requestContext);
            }
            if (user != null) {
                session.setAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
                if (requestContext.getSecurityContext().getUserPrincipal()==null) {
                    requestContext.setSecurityContext(new SimpleSecurityContext(user, (role) -> false, 
                        webRequest.isSecure(), SecurityContext.BASIC_AUTH));
                }
            }
            return;
        }
    
        abort(requestContext, "Authentication failed");
        return;
    }
    
    private void abort(ContainerRequestContext requestContext, String message) {
        requestContext.abortWith(Response.status(Response.Status.UNAUTHORIZED)
            .type(MediaType.TEXT_PLAIN)
            .entity(message)
            .header("WWW-Authenticate", "Basic realm=\"brooklyn\"")
            .build());
    }

    protected SecurityProvider getProvider() {
        // we don't cache here (could, it might be faster) but the delegate does use a cache
        return new DelegatingSecurityProvider(mgmt());
    }

//    private static ThreadLocal<String> originalRequest = new ThreadLocal<String>();
//
//    @Override
//    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
//        HttpServletRequest httpRequest = (HttpServletRequest) request;
//        HttpServletResponse httpResponse = (HttpServletResponse) response;
//        String uri = httpRequest.getRequestURI();
//
//        if (provider == null) {
//            log.warn("No security provider available: disallowing web access to brooklyn");
//            httpResponse.sendError(HttpServletResponse.SC_SERVICE_UNAVAILABLE);
//            return;
//        }
//
//        if (originalRequest.get() != null) {
//            // clear the entitlement context before setting to avoid warnings
//            Entitlements.clearEntitlementContext();
//        } else {
//            originalRequest.set(uri);
//        }
//
//        boolean authenticated = provider.isAuthenticated(httpRequest.getSession());
//        if ("/logout".equals(uri) || "/v1/logout".equals(uri)) {
//            httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"brooklyn\"");
//            if (authenticated && httpRequest.getSession().getAttributeNames().hasMoreElements()) {
//                logout(httpRequest);
//                httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
//            } else {
//                RequestDispatcher dispatcher = httpRequest.getRequestDispatcher("/");
//                log.debug("Not authenticated, forwarding request for {} to {}", uri, dispatcher);
//                dispatcher.forward(httpRequest, httpResponse);
//            }
//            return;
//        }
//
//        if (!(httpRequest.getSession().getAttributeNames().hasMoreElements() && provider.isAuthenticated(httpRequest.getSession())) ||
//                "/logout".equals(originalRequest.get())) {
//            authenticated = authenticate(httpRequest);
//        }
//
//        if (!authenticated) {
//            httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"brooklyn\"");
//            httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED);
//            return;
//        }
//
//        // Note that the attribute AUTHENTICATED_USER_SESSION_ATTRIBUTE is only set in the call to authenticate(httpRequest),
//        // so must not try to get the user until that is done.
//        String uid = RequestTaggingFilter.getTag();
//        String user = Strings.toString(httpRequest.getSession().getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE));
//        try {
//            WebEntitlementContext entitlementContext = new WebEntitlementContext(user, httpRequest.getRemoteAddr(), uri, uid);
//            Entitlements.setEntitlementContext(entitlementContext);
//
//            chain.doFilter(request, response);
//        } catch (Throwable e) {
//            if (!response.isCommitted()) {
//                httpResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//            }
//        } finally {
//            originalRequest.remove();
//            Entitlements.clearEntitlementContext();
//        }
//    }
//
//    protected boolean authenticate(HttpServletRequest request) {
//
//    protected void logout(HttpServletRequest request) {
//        log.info("REST logging {} out of session {}",
//                request.getSession().getAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE), request.getSession().getId());
//        provider.logout(request.getSession());
//        request.getSession().removeAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE);
//        request.getSession().removeAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE);
//        request.getSession().invalidate();
//    }

}
