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

import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.core.Context;

import org.apache.brooklyn.util.text.Identifiers;

import com.sun.jersey.core.util.Priority;
import com.sun.jersey.spi.container.ContainerRequest;
import com.sun.jersey.spi.container.ContainerRequestFilter;
import com.sun.jersey.spi.container.ContainerResponse;
import com.sun.jersey.spi.container.ContainerResponseFilter;

/**
 * Tags each request with a probabilistically unique id. Should be included before other
 * filters to make sense.
 */
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
    public ContainerRequest filter(ContainerRequest request) {
        String requestId = getRequestId();
        tag.set(requestId);
        return request;
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
    public ContainerResponse filter(ContainerRequest request, ContainerResponse response) {
        tag.remove();
        return response;
    }

}
