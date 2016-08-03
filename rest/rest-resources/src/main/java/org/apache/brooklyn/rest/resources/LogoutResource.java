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

import java.net.URI;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.rest.api.LogoutApi;
import org.apache.brooklyn.rest.security.jaas.BrooklynLoginModule;
import org.apache.brooklyn.util.exceptions.Exceptions;

import com.google.common.net.HttpHeaders;

public class LogoutResource extends AbstractBrooklynRestResource implements LogoutApi {
    
    private static final String BASIC_REALM_WEBCONSOLE = "Basic realm=\""+BrooklynLoginModule.DEFAULT_ROLE+"\"";
    
    @Context HttpServletRequest req;
    @Context UriInfo uri;

    @Override
    public Response logout() {
        WebEntitlementContext ctx = (WebEntitlementContext) Entitlements.getEntitlementContext();
        
        if (ctx==null) {
            return Response.status(Status.BAD_REQUEST)
                .entity("No user logged in")
                .header(HttpHeaders.WWW_AUTHENTICATE, BASIC_REALM_WEBCONSOLE)
                .build();            
        }
        
        URI dest = uri.getBaseUriBuilder().path(LogoutApi.class).path(LogoutApi.class, "logoutUser").build(ctx.user());

        // When execution gets here we don't know whether this is the first fetch of logout() or a subsequent one
        // with a re-authenticated user. The only way to tell is compare if user names changed. So redirect to an URL
        // which contains the user name.
        return Response.temporaryRedirect(dest).build();
    }

    @Override
    public Response unAuthorize() {
        return Response.status(Status.UNAUTHORIZED)
            .header(HttpHeaders.WWW_AUTHENTICATE, BASIC_REALM_WEBCONSOLE)
            .build();
    }

    @Override
    public Response logoutUser(String user) {
        // Will work when switching users, but will keep re-authenticating if user types in same user name.
        // Could improve by keeping state in cookies to decide whether to request auth or declare successfull re-auth.
        WebEntitlementContext ctx = (WebEntitlementContext) Entitlements.getEntitlementContext();
        if (user.equals(ctx.user())) {
            doLogout();

            return Response.status(Status.UNAUTHORIZED)
                    .header(HttpHeaders.WWW_AUTHENTICATE, BASIC_REALM_WEBCONSOLE)
                    .build();
        } else {
            return Response.temporaryRedirect(uri.getAbsolutePathBuilder().replacePath("/").build()).build();
        }
    }

    private void doLogout() {
        try {
            req.logout();
        } catch (ServletException e) {
            Exceptions.propagate(e);
        }

        req.getSession().invalidate();
    }

}
