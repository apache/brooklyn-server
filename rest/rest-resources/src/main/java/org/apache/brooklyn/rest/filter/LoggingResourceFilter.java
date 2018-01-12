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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import javax.annotation.Priority;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.core.SecurityContext;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableSet;

/**
 * Logs inbound REST api calls, and their responses.
 */
@Provider
@Priority(200)
public class LoggingResourceFilter implements ContainerRequestFilter, ContainerResponseFilter {

    /* 
     * TODO:
     *  - containerRequestContext.getLength() seems to only be non-negative if 'Content-Length' 
     *    header is supplied (which we do not do from `br` etc).
     *  - We don't log details of the request body (e.g. we just get told 'applications' when
     *    deploying an app, rather than anything about the yaml of the app being deployed.
     *  - Should we have separate loggers for request headers etc (so can be enabled/disabled
     *    more easily by configuring logging levels)?
     */

    private enum LogLevel {
        TRACE,
        DEBUG,
        INFO;
    }
    
    private static final Logger LOG = LoggerFactory.getLogger(BrooklynLogging.REST);

    /** Methods logged at trace. */
    private static final Set<String> UNINTERESTING_METHODS = ImmutableSet.of("GET", "HEAD");

    /** Headers whose values will not be logged. */
    private static final Set<String> CENSORED_HEADERS = ImmutableSet.of("Authorization");

    /** Log all requests that take this time or longer to complete. */
    private static final Duration REQUEST_DURATION_LOG_POINT = Duration.FIVE_SECONDS;

    /**
     * Used to correlate the requests and responses - records the timestamp (in nanos) when the  
     * request was received, so can calculate how long it took to process the request.
     */
    private final WeakHashMap<HttpServletRequest, Long> requestTimestamps = new WeakHashMap<>();
    
    @Context
    private HttpServletRequest servletRequest;

    public LoggingResourceFilter() {
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        requestTimestamps.put(servletRequest, System.nanoTime());
        
        String method = requestContext.getMethod();
        boolean isInteresting = !UNINTERESTING_METHODS.contains(method.toUpperCase());
        LogLevel logLevel = isInteresting ? LogLevel.DEBUG : LogLevel.TRACE;
        
        logRequest(requestContext, logLevel);
    }

    @Override
    public void filter(ContainerRequestContext requestContext, ContainerResponseContext responseContext) throws IOException {
        Long requestTime = requestTimestamps.remove(servletRequest);
        Duration requestDuration = null;
        if (requestTime != null) {
            long responseTime = System.nanoTime();
            requestDuration = Duration.nanos(responseTime - requestTime);
        }
        
        String method = requestContext.getMethod();
        boolean isInteresting = !UNINTERESTING_METHODS.contains(method.toUpperCase())
                || (requestDuration != null && requestDuration.isLongerThan(REQUEST_DURATION_LOG_POINT));
        LogLevel logLevel = isInteresting ? LogLevel.DEBUG : LogLevel.TRACE;

        logResponse(requestContext, responseContext, requestDuration, logLevel);
    }

    private void logRequest(ContainerRequestContext requestContext, LogLevel level) {
        if (!isLogEnabled(LOG, level)) return;
        
        String method = requestContext.getMethod();
        String path = requestContext.getUriInfo().getPath();
        requestContext.getSecurityContext();
        
        SecurityContext securityContext = requestContext.getSecurityContext();
        Principal userPrincipal = (securityContext != null) ? requestContext.getSecurityContext().getUserPrincipal() : null;
        String userName = (userPrincipal != null) ? userPrincipal.getName() : "<no-user>";
        String remoteAddr = servletRequest.getRemoteAddr();
        
        StringBuilder message = new StringBuilder("Request received: ")
                .append(method)
                .append(" ")
                .append(path)
                .append(" from ")
                .append(userName)
                .append(" @ ")
                .append(remoteAddr);

        log(LOG, level, message.toString());
    }

    private void logResponse(ContainerRequestContext requestContext, ContainerResponseContext responseContext, Duration requestDuration, LogLevel level) {
        if (!isLogEnabled(LOG, level)) return;
        
        int status = responseContext.getStatus();
        String method = requestContext.getMethod();
        String path = requestContext.getUriInfo().getPath();
        requestContext.getSecurityContext();
        MultivaluedMap<String, String> queryParams = requestContext.getUriInfo().getQueryParameters();
        
        SecurityContext securityContext = requestContext.getSecurityContext();
        Principal userPrincipal = (securityContext != null) ? requestContext.getSecurityContext().getUserPrincipal() : null;
        String userName = (userPrincipal != null) ? userPrincipal.getName() : "<no-user>";
        String remoteAddr = servletRequest.getRemoteAddr();
        
        boolean includeHeaders = (responseContext.getStatus() / 100 == 5) || LOG.isDebugEnabled();

        StringBuilder message = new StringBuilder("Request completed: ")
                .append("status ")
                .append(status)
                .append(" in ")
                .append(requestDuration)
                .append(", ")
                .append(method)
                .append(" ")
                .append(path)
                .append(" from ")
                .append(userName)
                .append(" @ ")
                .append(remoteAddr);

        if (!queryParams.isEmpty()) {
            message.append(", queryParams: {");
            message.append(Joiner.on(", ").withKeyValueSeparator("=").join(queryParams));
            message.append("}");
        }
        if (requestContext.getLength() > 0) {
            // TODO `getLength` is based on the presence of `Content-Length` header, rather than the measured length.
            int len = requestContext.getLength();
            message.append(", mediaType=").append(requestContext.getMediaType())
                    .append(" (length=").append(len).append(")");
        }
        if (includeHeaders) {
            MultivaluedMap<String, String> headers = requestContext.getHeaders();
            message.append(", headers={");
            if (!headers.isEmpty()) {
                boolean first = true;
                for (Map.Entry<String, List<String>> entry : headers.entrySet()) {
                    if (first) {
                        first = false;
                    } else {
                        message.append(", ");
                    }
                    String headerName = entry.getKey();
                    message.append(headerName).append(": ");
                    if (CENSORED_HEADERS.contains(headerName)) {
                        message.append("******");
                    } else {
                        message.append(entry.getValue());
                    }
                }
            }
            message.append("}");
        }

        log(LOG, level, message.toString());
    }
    
    private boolean isLogEnabled(Logger logger, LogLevel level) {
        switch (level) {
        case TRACE: return logger.isTraceEnabled();
        case DEBUG: return logger.isDebugEnabled();
        case INFO: return logger.isInfoEnabled();
        default: throw new IllegalStateException("Unexpected log level: "+level);
        }
    }

    private void log(Logger logger, LogLevel level, String msg) {
        switch (level) {
        case TRACE: 
            logger.trace(msg);
            break;
        case DEBUG: 
            logger.debug(msg);
            break;
        case INFO: 
            logger.info(msg);
            break;
        default: throw new IllegalStateException("Unexpected log level: "+level);
        }
    }
}
