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

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.HttpMethod;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.util.text.Identifiers;

/**
 * If client specifies {@link #REQUEST_CSRF_TOKEN_HEADER}
 * (as any of the values {@link #REQUEST_CSRF_TOKEN_HEADER_LOGIN}, {@link #REQUEST_CSRF_TOKEN_HEADER_NEW}, {@link #REQUEST_CSRF_TOKEN_HEADER_TRUE})
 * then the server will return a {@link #REQUIRED_CSRF_TOKEN_HEADER} header containing a token.
 * <p>
 * The server will require that header with the given token on subsequent POST requests
 * (and other actions -- excluding GET which is always permitted;
 * future enhancement may allow client to control whether GET is allowed or not).
 */
@Provider
@Priority(400)
public class CsrfTokenFilter implements ContainerRequestFilter, ContainerResponseFilter {
    
    public static final String REQUIRED_CSRF_TOKEN_ATTR = CsrfTokenFilter.class.getName()+"."+"REQUIRED_CSRF_TOKEN";
    
    public static final String REQUIRED_CSRF_TOKEN_HEADER = CsrfTokenFilter.class.getName()+"."+"X-CsrfToken";
    public static final String REQUEST_CSRF_TOKEN_HEADER = CsrfTokenFilter.class.getName()+"."+"X-CsrfToken-Request";
    /** means to return the token */
    public static final String REQUEST_CSRF_TOKEN_HEADER_TRUE = "true";
    /** means to create a new token */
    public static final String REQUEST_CSRF_TOKEN_HEADER_NEW = "new";
    /** means to create and return a token on login only (ie if one doesn't exist for the session) */
    public static final String REQUEST_CSRF_TOKEN_HEADER_LOGIN = "login";
    
    @Context
    private HttpServletRequest request;

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        Object requiredToken = request.getSession().getAttribute(REQUIRED_CSRF_TOKEN_ATTR);
        if (requiredToken!=null) {
            String suppliedToken = request.getHeader(REQUIRED_CSRF_TOKEN_HEADER);
            if (suppliedToken==null) {
                if (HttpMethod.GET.equals(requestContext.getMethod())) {
                    // GETs are permitted with no token (but an invalid token will be caught)
                    return;
                }

                fail(requestContext, ApiError.builder().errorCode(Response.Status.UNAUTHORIZED)
                        .message(REQUIRED_CSRF_TOKEN_HEADER+" header required containing token previously supplied")
                        .build());
            } else if (suppliedToken.equals(requiredToken)) {
                // approved
            } else {
                fail(requestContext, ApiError.builder().errorCode(Response.Status.UNAUTHORIZED)
                    .message(REQUIRED_CSRF_TOKEN_HEADER+" header did not match current token")
                    .build());
            }
        }
    }

    private void fail(ContainerRequestContext requestContext, ApiError apiError) {
        requestContext.abortWith(apiError.asJsonResponse());
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        String requestHeader = request.getHeader(REQUEST_CSRF_TOKEN_HEADER);
        if (requestHeader==null) return;
        String oldToken = (String) request.getSession().getAttribute(REQUIRED_CSRF_TOKEN_ATTR);
        String tokenToReturn = null;
        boolean createToken = false;
        
        if (REQUEST_CSRF_TOKEN_HEADER_LOGIN.equals(requestHeader)) {
            if (oldToken==null) createToken = true;
            else return;
        } else if (REQUEST_CSRF_TOKEN_HEADER_TRUE.equals(requestHeader)) {
            tokenToReturn = oldToken;
            if (tokenToReturn==null) createToken = true;
        } else if (REQUEST_CSRF_TOKEN_HEADER_NEW.equals(requestHeader)) {
            createToken = true;
        }
    
        if (createToken) {
            tokenToReturn = Identifiers.makeRandomId(16);
            request.getSession().setAttribute(REQUIRED_CSRF_TOKEN_ATTR, tokenToReturn);
        }
        
        if (tokenToReturn==null) {
            fail(requestContext, ApiError.builder().errorCode(Response.Status.UNAUTHORIZED)
                .message(REQUEST_CSRF_TOKEN_HEADER+" contained invalid value; expected: "+
                    REQUEST_CSRF_TOKEN_HEADER_TRUE+" | "+
                    REQUEST_CSRF_TOKEN_HEADER_LOGIN+" | "+
                    REQUEST_CSRF_TOKEN_HEADER_NEW)
                .build());
        } else {
            responseContext.getHeaders().add(REQUIRED_CSRF_TOKEN_HEADER, tokenToReturn);
        }
    }

}
