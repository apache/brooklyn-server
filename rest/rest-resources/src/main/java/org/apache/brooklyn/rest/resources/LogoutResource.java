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
package org.apache.brooklyn.rest.resources;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.rest.api.LogoutApi;
import org.apache.brooklyn.rest.filter.BrooklynSecurityProviderFilterHelper;
import org.apache.brooklyn.rest.security.provider.DelegatingSecurityProvider;
import org.apache.brooklyn.rest.util.MultiSessionAttributeAdapter;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.eclipse.jetty.server.session.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LogoutResource extends AbstractBrooklynRestResource implements LogoutApi {

    private static final Logger log = LoggerFactory.getLogger(LogoutResource.class);

    public static final String DID_LOGOUT = "org.apache.brooklyn.server.DidLogout";
    
    @Context HttpServletRequest req;
    @Context UriInfo uri;

    @Deprecated
    @Override
    public Response unAuthorize() {
        return Response.status(Status.UNAUTHORIZED)
               // NB: 2019-01 no longer returns a realm (there might not be a realm; in this code we don't know)
               // method is now deprecated anyway
               .build();
    }

    @Deprecated
    @Override
    public Response logoutUser(String user) {
        WebEntitlementContext ctx = (WebEntitlementContext) Entitlements.getEntitlementContext();
        if (user.equals(ctx.user())) {
            doLogout();

            return Response.status(Status.OK)
                   // 2019-01 no longer returns unauthorized, returns OK to indicate user is successfully logged out
                   // also the realm  is removed (there might not be a realm; in this code we don't know)
                   .build();
        } else {
            return Response.temporaryRedirect(uri.getBaseUriBuilder().path(LogoutApi.class).path(LogoutApi.class, "redirect").build()).
                entity("User requested to log out does not match actual user logged in").build();
        }
    }

    @Override
    public Response logout(String unauthorize, String requestedUser) {
        MultiSessionAttributeAdapter session = MultiSessionAttributeAdapter.of(req, false);
        WebEntitlementContext ctx = (WebEntitlementContext) Entitlements.getEntitlementContext();
        String currentUser = ctx==null ? null : ctx.user();
        log.debug("Logging out: {}, session id {} ({})"+", unauthorized={}", currentUser, (session!=null ? session.getId()+" " : ""), session, unauthorize);
        
        MutableMap<String,String> body = MutableMap.of();
        body.addIfNotNull("currentUser", currentUser);
        body.addIfNotNull("requestedUser", requestedUser);
        body.addIfNotNull("sessionId", session==null ? null : session.getId());
        body.addIfNotNull("requestedSessionId", req.getRequestedSessionId());
        
        if (requestedUser!=null && !requestedUser.equals(currentUser)) {
            return Response.status(Status.FORBIDDEN)
                .entity(body.add("message", "The user requested to be logged out is not the user currently logged in"))
                .build();
        }
        doLogout();

        if (unauthorize!=null) {
            // returning 401 UNAUTHORIZED has the nice property that it causes browser (mostly)
            // to re-prompt for cached credentials to set in the "Authorization: " header to re-login;
            // however it's not 100%; e.g. in Chrome if there is an active ajax/$http request that had prompted for
            // credentials it will keep the Authorization header and log you back in (eg /server/up/extended);
            // it is a known issue that we cannot absolutely clear creds in most browsers, but returning 401 helps
            return Response.status(Status.UNAUTHORIZED)
                .entity(body.add("message", unauthorize))
                .build();
        }
        
        return Response.status(Status.OK)
                .entity(body.add("message", "Logged out user "+currentUser))
                .build();
    }

    private void doLogout() {
        MultiSessionAttributeAdapter multi = MultiSessionAttributeAdapter.of(req);

        // if we need to intercept session creation then can use this
        // create TrackingSessionHandler which delegates (no-op if delegate==null esp in setSessionTrackingMode)
        // and log with stack trace in newHttpSession
        //   can remove this after we've been running happily with new session management model for a while
//        HttpServletRequest jreq = req;
//        if (jreq instanceof ThreadLocalHttpServletRequest) jreq = ((ThreadLocalHttpServletRequest)jreq).get();
//        if (jreq instanceof ServletRequestWrapper) jreq = (HttpServletRequest) ((ServletRequestWrapper)jreq).getRequest();
//        if (jreq instanceof Request) {
//            log.warn("SWAPPING "+MultiSessionAttributeAdapter.info(jreq));
//            ((Request)jreq).setSessionHandler(new TrackingSessionHandler(((Request)jreq).getSessionHandler()));
//        } else {
//            log.warn("UNABLE to swap request"+MultiSessionAttributeAdapter.info(jreq));
//        }
        
        multi.configureWhetherToSetInAll(true)
            .removeAttribute(BrooklynSecurityProviderFilterHelper.AUTHENTICATED_USER_SESSION_ATTRIBUTE);
        // security provider logout
        new DelegatingSecurityProvider(mgmt()).logout(multi.getPreferredSession());
        
        try {
            req.logout();
        } catch (ServletException e) {
            Exceptions.propagate(e);
        }
        req.setAttribute(DID_LOGOUT, true);
        
        multi.getPreferredSession().invalidate();
        if (multi.getOriginalSession() instanceof Session) {
            if (((Session)multi.getOriginalSession()).isValid()) {
                throw new IllegalStateException(MultiSessionAttributeAdapter.info(multi.getOriginalSession())+
                    " is valid after invaildating "+MultiSessionAttributeAdapter.info(multi.getPreferredSession()));
            }
        }
    }
}
