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
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.Set;

import javax.annotation.Priority;
import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerRequestFilter;
import javax.ws.rs.container.ResourceInfo;
import javax.ws.rs.core.Context;
import javax.ws.rs.ext.ContextResolver;
import javax.ws.rs.ext.Provider;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

/** 
 * Checks that if the method or resource class corresponding to a request
 * has a {@link HaHotStateRequired} annotation,
 * that the server is in that state (and up). 
 * Requests with {@link #SKIP_CHECK_HEADER} set as a header skip this check.
 * <p>
 * This follows a different pattern to {@link HaMasterCheckFilter} 
 * as this needs to know the method being invoked. 
 */
@Provider
@Priority(300)
public class HaHotCheckResourceFilter implements ContainerRequestFilter {
    private static final Set<String> SAFE_STANDBY_METHODS = ImmutableSet.of("GET", "HEAD");
    public static final String SKIP_CHECK_HEADER = HaHotCheckHelperAbstract.SKIP_CHECK_HEADER;
    
    // Not quite standards compliant. Should instead be:
    // @Context Providers providers
    // ....
    // ContextResolver<ManagementContext> resolver = providers.getContextResolver(ManagementContext.class, MediaType.WILDCARD_TYPE)
    // ManagementContext engine = resolver.get(ManagementContext.class);
    @Context
    private ContextResolver<ManagementContext> mgmt;

    @Context
    private ResourceInfo resourceInfo;
    
    private HaHotCheckHelperAbstract helper = new HaHotCheckHelperAbstract() {
        @Override
        public ManagementContext mgmt() {
            return mgmt.getContext(ManagementContext.class);
        }
    };
    
    public HaHotCheckResourceFilter() {
    }

    @VisibleForTesting
    public HaHotCheckResourceFilter(ContextResolver<ManagementContext> mgmt) {
        this.mgmt = mgmt;
    }

    @Override
    public void filter(ContainerRequestContext requestContext) throws IOException {
        String problem = lookForProblem(requestContext);
        if (Strings.isNonBlank(problem)) {
            requestContext.abortWith(helper.disallowResponse(problem, requestContext.getUriInfo().getAbsolutePath()+"/"+resourceInfo.getResourceMethod()));
        }
    }

    private String lookForProblem(ContainerRequestContext requestContext) {
        if (helper.isSkipCheckHeaderSet(requestContext.getHeaderString(SKIP_CHECK_HEADER))) 
            return null;
        
        if (isMasterRequiredForRequest(requestContext) && !isMaster()) {
            return "server not in required HA master state";
        }
        
        if (!isHaHotStateRequired())
            return null;
        
        Maybe<String> problem = helper.getProblemMessageIfServerNotRunning();
        if (problem.isPresent()) 
            return problem.get();
        
        if (!helper.isHaHotStatus())
            return "server not in required HA hot state";
        if (helper.isStateNotYetValid())
            return "server not yet completed loading data for required HA hot state";
        
        return null;
    }

    /** @deprecated since 0.9.0 use {@link BrooklynRestResourceUtils#getProblemMessageIfServerNotRunning(ManagementContext)} */
    public static String lookForProblemIfServerNotRunning(ManagementContext mgmt) {
        return HaHotCheckHelperAbstract.getProblemMessageIfServerNotRunning(mgmt).orNull();
    }
    
    private boolean isMaster() {
        return ManagementNodeState.MASTER.equals(
                mgmt.getContext(ManagementContext.class)
                    .getHighAvailabilityManager()
                    .getNodeState());
    }

    private boolean isMasterRequiredForRequest(ContainerRequestContext requestContext) {
        // gets usually okay
        if (SAFE_STANDBY_METHODS.contains(requestContext.getMethod())) return false;
        
        String uri = requestContext.getUriInfo().getPath();
        // explicitly allow calls to shutdown
        // (if stopAllApps is specified, the method itself will fail; but we do not want to consume parameters here, that breaks things!)
        // TODO use an annotation HaAnyStateAllowed or HaHotCheckRequired(false) or similar
        if ("server/shutdown".equals(uri)) return false;
        
        return true;
    }

    protected boolean isHaHotStateRequired() {
        // TODO support super annotations
        Method m = resourceInfo.getResourceMethod();
        return getAnnotation(m, HaHotStateRequired.class) != null;
    }

    private <T extends Annotation> T getAnnotation(Method m, Class<T> annotation) {
        T am = m.getAnnotation(annotation);
        if (am != null) {
            return am;
        }
        Class<?> superClass = m.getDeclaringClass();
        T ac = superClass.getAnnotation(annotation);
        if (ac != null) {
            return ac;
        }
        // TODO could look in super classes but not needed now, we are in control of where to put the annotation
        return null;
    }

}
