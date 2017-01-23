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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.rest.util.OsgiCompat;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.collect.Sets;

/**
 * Checks that for requests that want HA master state, the server is up and in that state.
 * <p>
 * Post POSTs and PUTs are assumed to need master state, with the exception of shutdown.
 * Requests with {@link #SKIP_CHECK_HEADER} set as a header skip this check.
 * 
 * @deprecated since 0.9.0. Use JAX-RS {@link HaHotCheckResourceFilter} instead.
 */
@Deprecated
public class HaMasterCheckFilter implements Filter {

    private static final Set<String> SAFE_STANDBY_METHODS = Sets.newHashSet("GET", "HEAD");

    protected ServletContext servletContext;
    protected ManagementContext mgmt;

    private HaHotCheckHelperAbstract helper = new HaHotCheckHelperAbstract() {
        @Override
        public ManagementContext mgmt() {
            return mgmt;
        }
    };

    @Override
    public void init(FilterConfig config) throws ServletException {
        servletContext = config.getServletContext();
        mgmt = OsgiCompat.getManagementContext(servletContext);
    }

    private String lookForProblem(ServletRequest request) {
        if (helper.isSkipCheckHeaderSet(((HttpServletRequest)request).getHeader(HaHotCheckHelperAbstract.SKIP_CHECK_HEADER))) 
            return null;
        
        if (!isMasterRequiredForRequest(request))
            return null;
        
        Maybe<String> problem = helper.getProblemMessageIfServerNotRunning();
        if (problem.isPresent()) 
            return problem.get();
        
        if (!isMaster()) 
            return "server not in required HA master state";
        
        return null;
    }
    
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        String problem = lookForProblem(request);
        if (problem!=null) {
            WebResourceUtils.applyJsonResponse(mgmt, helper.disallowResponse(problem, request.getParameterMap()), (HttpServletResponse)response);
        } else {
            chain.doFilter(request, response);
        }
    }

    @Override
    public void destroy() {
    }

    private boolean isMaster() {
        return ManagementNodeState.MASTER.equals(mgmt.getHighAvailabilityManager().getNodeState());
    }

    private boolean isMasterRequiredForRequest(ServletRequest request) {
        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            
            String method = httpRequest.getMethod().toUpperCase();
            // gets usually okay
            if (SAFE_STANDBY_METHODS.contains(method)) return false;
            
            // explicitly allow calls to shutdown
            // (if stopAllApps is specified, the method itself will fail; but we do not want to consume parameters here, that breaks things!)
            // TODO combine with HaHotCheckResourceFilter and use an annotation HaAnyStateAllowed or similar
            if ("/v1/server/shutdown".equals(httpRequest.getRequestURI()) ||
                    "/server/shutdown".equals(httpRequest.getRequestURI())) return false;
            
            // master required for everything else
            return true;
        }
        // previously non-HttpServletRequests were allowed but I don't think they should be
        return true;
    }

}
