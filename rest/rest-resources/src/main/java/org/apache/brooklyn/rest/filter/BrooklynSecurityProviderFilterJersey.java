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
import java.util.Set;
import java.util.stream.Collectors;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.rest.security.provider.SecurityProvider;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.SecurityProviderDeniedAuthentication;
import org.apache.brooklyn.util.text.Strings;
import org.eclipse.jetty.http.HttpHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** See {@link BrooklynSecurityProviderFilterHelper} */
@Provider
@Priority(100)
public class BrooklynSecurityProviderFilterJersey implements ContainerRequestFilter {

    private static final Logger log = LoggerFactory.getLogger(BrooklynSecurityProviderFilterJersey.class);
    public static final String LOGIN_PAGE_HEADER = "X_BROOKLYN_LOGIN_PAGE";

    private final Set<String> headersToForward = ImmutableSet.of(
            HttpHeaders.WWW_AUTHENTICATE, // defines the HTTP authentication methods ("challenges") that might be used to gain access to a specific resource
            SecurityProvider.UNAUTHORIZED_MESSAGE_HEADER // helper message from the security provider
            );

    @Context
    HttpServletRequest webRequest;

    @Context
    private ContextResolver<ManagementContext> mgmtC;

    @SuppressWarnings("resource")
    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        log.trace("BrooklynSecurityProviderFilterJersey.filter {}", requestContext);
        ManagementContext mgmt = mgmtC.getContext(ManagementContext.class);
        try {
            new BrooklynSecurityProviderFilterHelper().run(webRequest, mgmt);
        } catch (SecurityProviderDeniedAuthentication e) {
            Response rin = e.getResponse();
            if (rin==null) rin = Response.status(Status.UNAUTHORIZED).build();
            
            if (rin.getStatus()==Status.FOUND.getStatusCode()) {
                String location = rin.getHeaderString(HttpHeader.LOCATION.asString());
                if (location!=null) {
                    log.trace("Redirect to {} for authentication",location);
                    final UriBuilder uriBuilder = UriBuilder.fromPath(location);
                    rin = Response.temporaryRedirect(uriBuilder.build()).entity("Authentication is required at "+location).build();
                } else {
                    log.trace("Unauthorized");
                    rin = Response.status(Status.UNAUTHORIZED).entity("Authentication is required").build();
                }
            }
            if (rin.getStatus()==Status.UNAUTHORIZED.getStatusCode() &&
                    Strings.isNonBlank(mgmt.getConfig().getConfig(BrooklynSecurityProviderFilterJavax.LOGIN_FORM))) {
                rin = Response.status(Status.UNAUTHORIZED).entity("Authentication is required").header(LOGIN_PAGE_HEADER, mgmt.getConfig().getConfig(BrooklynSecurityProviderFilterJavax.LOGIN_FORM)).build();
            }
            // adding headers in `headersToForward` if they are present in the original response
            MultivaluedMap<String, Object> responseHeaders = e.getResponse().getHeaders();
            if(responseHeaders != null && !responseHeaders.isEmpty()){
                for(String headerKey: headersToForward) {
                    if(responseHeaders.containsKey(headerKey)) {
                        rin = Response.fromResponse(rin).header(headerKey, responseHeaders.get(headerKey).stream().map(Object::toString).collect(Collectors.joining(", "))).build();
                    }
                };
            }
            requestContext.abortWith(rin);
        }
    }

}

