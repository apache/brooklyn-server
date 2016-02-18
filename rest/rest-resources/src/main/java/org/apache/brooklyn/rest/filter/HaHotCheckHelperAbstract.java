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

import java.util.Set;

import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;

@Beta
public abstract class HaHotCheckHelperAbstract {

    private static final Logger log = LoggerFactory.getLogger(HaHotCheckHelperAbstract.class);
    
    public static final String SKIP_CHECK_HEADER = "Brooklyn-Allow-Non-Master-Access";

    private static final Set<ManagementNodeState> HOT_STATES = ImmutableSet.of(
        ManagementNodeState.MASTER, ManagementNodeState.HOT_STANDBY, ManagementNodeState.HOT_BACKUP);

    /** Returns a string describing the problem if mgmt is null or not running; returns absent if no problems */
    public static Maybe<String> getProblemMessageIfServerNotRunning(ManagementContext mgmt) {
        if (mgmt==null) return Maybe.of("no management context available");
        if (!mgmt.isRunning()) return Maybe.of("server no longer running");
        if (!mgmt.isStartupComplete()) return Maybe.of("server not in required startup-completed state");
        return Maybe.absent();
    }
    
    public Maybe<String> getProblemMessageIfServerNotRunning() {
        return getProblemMessageIfServerNotRunning(mgmt());
    }

    public Response disallowResponse(String problem, Object info) {
        log.warn("Disallowing web request as "+problem+": "+info+" (caller should set '"+HaHotCheckHelperAbstract.SKIP_CHECK_HEADER+"' to force)");
        return ApiError.builder()
            .message("This request is only permitted against an active master Brooklyn server")
            .errorCode(Response.Status.FORBIDDEN).build().asJsonResponse();
    }

    public boolean isSkipCheckHeaderSet(String headerValueString) {
        return "true".equalsIgnoreCase(headerValueString);
    }

    public boolean isHaHotStatus() {
        ManagementNodeState state = mgmt().getHighAvailabilityManager().getNodeState();
        return HOT_STATES.contains(state);
    }

    public abstract ManagementContext mgmt();

    // Maybe there should be a separate state to indicate that we have switched state
    // but still haven't finished rebinding. (Previously there was a time delay and an
    // isRebinding check, but introducing RebindManager#isAwaitingInitialRebind() seems cleaner.)
    public boolean isStateNotYetValid() {
        return mgmt().getRebindManager().isAwaitingInitialRebind();
    }
    
}
