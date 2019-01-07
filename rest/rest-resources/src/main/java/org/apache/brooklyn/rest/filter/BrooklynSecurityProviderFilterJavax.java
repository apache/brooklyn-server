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

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.rest.security.provider.SecurityProvider.SecurityProviderDeniedAuthentication;
import org.apache.brooklyn.rest.util.ManagementContextProvider;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/** See {@link BrooklynSecurityProviderFilterHelper} */
public class BrooklynSecurityProviderFilterJavax implements Filter {
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(BrooklynSecurityProviderFilterJavax.class);
    
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // no init needed
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        try {
            ManagementContext mgmt = new ManagementContextProvider(request.getServletContext()).getManagementContext();
            Preconditions.checkNotNull(mgmt, "Brooklyn management context not available; cannot authenticate");
            new BrooklynSecurityProviderFilterHelper().run((HttpServletRequest)request, mgmt);

            chain.doFilter(request, response);

        } catch (SecurityProviderDeniedAuthentication e) {
            HttpServletResponse rout = ((HttpServletResponse)response);
            Response rin = e.getResponse();
            if (rin==null) rin = Response.status(Status.UNAUTHORIZED).build();
     
            rout.setStatus(rin.getStatus());
            
            // TODO does content type need to be set explicitly?
            rin.getHeaders().forEach((k,v) -> v.forEach(v2 -> rout.addHeader(k, Strings.toString(v2))));
            
            Object body = rin.getEntity();
            if (body!=null) {
                response.getWriter().write(Strings.toString(body));
                response.getWriter().flush();
            }
        }
    }

    @Override
    public void destroy() {
        // no clean-up needed
    }
    
}