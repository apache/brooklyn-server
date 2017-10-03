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
package org.apache.brooklyn.core.mgmt.usage;

import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.server.BrooklynServerConfig;

import com.google.common.annotations.Beta;

/**
 * Listener to be notified of this Brooklyn node's state.
 * 
 * See {@link BrooklynServerConfig#MANAGEMENT_NODE_STATE_LISTENERS}.
 */
@Beta
public interface ManagementNodeStateListener {

    /**
     * A no-op implementation of {@link ManagementNodeStateListener}, for users to extend.
     * 
     * Users are encouraged to extend this class, which will shield the user 
     * from the addition of other usage event methods being added. If additional
     * methods are added in a future release, a no-op implementation will be
     * added to this class.
     */
    @Beta
    public static class BasicListener implements ManagementNodeStateListener {
        @Override
        public void onStateChange(ManagementNodeState state) {
        }
    }
    
    ManagementNodeStateListener NOOP = new BasicListener();

    @Beta
    void onStateChange(ManagementNodeState state);
}
