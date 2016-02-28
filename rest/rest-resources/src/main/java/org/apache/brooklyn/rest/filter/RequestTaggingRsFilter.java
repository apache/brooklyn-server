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

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.util.text.Identifiers;

/**
 * Tags each request with a probabilistically unique id. Should be included before other
 * filters to make sense.
 */
@Provider
@Priority(100)
public class RequestTaggingRsFilter implements ContainerRequestFilter, ContainerResponseFilter {
    public static final String ATT_REQUEST_ID = RequestTaggingRsFilter.class.getName() + ".id";

    @Context
    private HttpServletRequest req;

    private static ThreadLocal<String> tag = new ThreadLocal<String>();

    protected static String getTag() {
        // Alternatively could use
        // PhaseInterceptorChain.getCurrentMessage().getId()

        return checkNotNull(tag.get());
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String requestId = getRequestId();
        tag.set(requestId);
    }

    private String getRequestId() {
        Object id = req.getAttribute(ATT_REQUEST_ID);
        if (id != null) {
            return id.toString();
        } else {
            return Identifiers.makeRandomId(6);
        }
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        tag.remove();
    }

}
