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

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.util.text.Strings;

@Provider
@Priority(400)
public class EntitlementContextFilter implements ContainerRequestFilter, ContainerResponseFilter {
    @Context
    private HttpServletRequest request;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String userName = null;

        // first see if there is a principal
        SecurityContext securityContext = requestContext.getSecurityContext();
        Principal user = securityContext.getUserPrincipal();
        if (user!=null) {
            userName = user.getName();
        } else {

            // now look in session attribute - because principals hard to set from javax filter
            if (request!=null) {
                HttpSession s = request.getSession(false);
                if (s!=null) {
                    userName = Strings.toString(s.getAttribute(
                            BrooklynSecurityProviderFilterHelper.AUTHENTICATED_USER_SESSION_ATTRIBUTE));
                }
            }
        }

        if (userName != null) {
            EntitlementContext oldEntitlement = Entitlements.getEntitlementContext();
            if (oldEntitlement!=null && !userName.equals(oldEntitlement.user())) {
                throw new IllegalStateException("Illegal entitement context switch, from user "+oldEntitlement.user()+" to "+userName);
            }

            String uri = request.getRequestURI();
            String remoteAddr = request.getRemoteAddr();

            String uid = RequestTaggingRsFilter.getTag();
            WebEntitlementContext entitlementContext = new WebEntitlementContext(userName, remoteAddr, uri, uid);
            Entitlements.setEntitlementContext(entitlementContext);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        Entitlements.clearEntitlementContext();
    }

}
