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

import java.util.Set;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.rest.BrooklynWebConfig;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.SecurityProviderDeniedAuthentication;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.commons.codec.binary.Base64;
import org.eclipse.jetty.http.HttpHeader;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.session.SessionHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a filter that performs authentication with the {@link SecurityProvider}
 * as configured according to {@link BrooklynWebConfig#SECURITY_PROVIDER_CLASSNAME}.
 * 
 * This replaces the JAAS "BrooklynLoginModule" because that login module requires
 * Basic auth, which is not flexible enough to support redirect-based solutions like Oauth.
 * 
 * Unfortunately we seem to need two filters, the Jersey filter for the REST bundle,
 * and the Javax filter for the static content bundles (in brooklyn-ui/ui-modules).
 * (We could set up our own Jersey servlet or blueprint for the static content bundles
 * to re-use the Jersey filter, but that seems like overkill; and surely there's an easy
 * way to set the Javax filter to run for the REST bundle inside blueprint.xml, but a
 * few early attempts didn't succeed and the approach of having two filters seems easiest
 * (especially as they share code for the significant parts, in this class).
 * 
 * This does give us the opportunity to differentiate the redirect, so that
 * jersey (REST) requests don't redirect to the auth site, as the redirect requires human intervention.
 */
public class BrooklynSecurityProviderFilterHelper {

    public interface Responder {
        void error(String message, boolean requiresBasicAuth) throws SecurityProviderDeniedAuthentication;
    }
    
    /**
     * The session attribute set for authenticated users; for reference
     * (but should not be relied up to confirm authentication, as
     * the providers may impose additional criteria such as timeouts,
     * or a null user (no login) may be permitted)
     */
    public static final String AUTHENTICATED_USER_SESSION_ATTRIBUTE = "brooklyn.user";

    public static Set<SessionHandler> SESSION_MANAGER_CACHE = MutableSet.of();
    
    private static final Logger log = LoggerFactory.getLogger(BrooklynSecurityProviderFilterHelper.class);

    // TODO this should be parametrisable
    public static final String BASIC_REALM_NAME = "brooklyn";
    
    public static final String BASIC_REALM_HEADER_VALUE = "BASIC realm="+StringEscapes.JavaStringEscapes.wrapJavaString(BASIC_REALM_NAME);
    
    /* check all contexts for sessions; surprisingly hard to configure session management for karaf/pax web container.
     * they _really_ want each servlet to have their own sessions. how you're meant to do oauth for multiple servlets i don't know! */
    public HttpSession getSession(HttpServletRequest webRequest, ManagementContext mgmt, boolean create) {
        String requestedSessionId = webRequest.getRequestedSessionId();
        
        log.trace("SESSION for {}, wants session {}", webRequest.getRequestURI(), requestedSessionId);
        
        if (webRequest instanceof Request) {
            SessionHandler sm = ((Request)webRequest).getSessionHandler();
            boolean added = SESSION_MANAGER_CACHE.add( sm );
            log.trace("SESSION MANAGER found for {}: {} (added={})", webRequest.getRequestURI(), sm, added);
        } else {
            log.trace("SESSION MANAGER NOT found for {}: {}", webRequest.getRequestURI(), webRequest);
        }
        
        if (requestedSessionId!=null) {
            for (SessionHandler m: SESSION_MANAGER_CACHE) {
                HttpSession s = m.getHttpSession(requestedSessionId);
                if (s!=null) {
                    log.trace("SESSION found for {}: {} (valid={})", webRequest.getRequestURI(), s, m.isValid(s));
                    return s;
                }
            }
        }
        
        if (create) {
            HttpSession session = webRequest.getSession(true);
            log.trace("SESSION creating for {}: {}", webRequest.getRequestURI(), session);
            return session;
        }
        
        return null;  // not found
    }
    
    public void run(HttpServletRequest webRequest, ManagementContext mgmt) throws SecurityProviderDeniedAuthentication {
        SecurityProvider provider = getProvider(mgmt);
        HttpSession session = getSession(webRequest, mgmt, false);
        
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
                    throw abort("Invalid authorization string", provider.requiresUserPass());
                }
            } else {
                throw abort("Authorization required", provider.requiresUserPass());
            }
        }
        
        if (session==null) {
            // only create the session if an auth string is supplied
            session = getSession(webRequest, mgmt, true);
        }
        session.setAttribute(BrooklynWebConfig.REMOTE_ADDRESS_SESSION_ATTRIBUTE, webRequest.getRemoteAddr());
        
        if (provider.authenticate(session, user, pass)) {
            if (user != null) {
                session.setAttribute(AUTHENTICATED_USER_SESSION_ATTRIBUTE, user);
            }
            return;
        }
    
        throw abort("Authentication failed", provider.requiresUserPass());
    }
    
    private SecurityProviderDeniedAuthentication abort(String msg, boolean requiresUserPass) throws SecurityProviderDeniedAuthentication {
        ResponseBuilder response = Response.status(Status.UNAUTHORIZED);
        if (requiresUserPass) {
            response.header(HttpHeader.WWW_AUTHENTICATE.asString(), BASIC_REALM_HEADER_VALUE);
        }
        response.header(HttpHeader.CONTENT_TYPE.asString(), MediaType.TEXT_PLAIN);
        response.entity(msg);
        throw new SecurityProviderDeniedAuthentication(response.build());
    }

    protected SecurityProvider getProvider(ManagementContext mgmt) {
        // we don't cache here (could, it might be faster) but the delegate does use a cache
        return new DelegatingSecurityProvider(mgmt);
    }

}
