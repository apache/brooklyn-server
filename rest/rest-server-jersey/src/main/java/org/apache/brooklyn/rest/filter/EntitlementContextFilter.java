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

import java.security.Principal;

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.SecurityContext;

import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;

import com.sun.jersey.core.util.Priority;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

@Priority(400)
public class EntitlementContextFilter implements ContainerRequestFilter, ContainerResponseFilter {
    @Context
    private HttpServletRequest servletRequest;

    @Override
    public ContainerRequest filter(ContainerRequest request) {
        SecurityContext securityContext = request.getSecurityContext();
        Principal user = securityContext.getUserPrincipal();

        if (user != null) {
            String uri = servletRequest.getRequestURI();
            String remoteAddr = servletRequest.getRemoteAddr();

            String uid = RequestTaggingFilter.getTag();
            WebEntitlementContext entitlementContext = new WebEntitlementContext(user.getName(), remoteAddr, uri, uid);
            Entitlements.setEntitlementContext(entitlementContext);
        }
        return request;
    }

    @Override
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        Entitlements.clearEntitlementContext();
        return response;
    }

}
