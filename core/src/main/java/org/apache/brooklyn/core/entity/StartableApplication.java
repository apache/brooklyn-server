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
package org.apache.brooklyn.core.entity;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

public interface StartableApplication extends Application, Startable {
    
    ConfigKey<QuorumCheck> UP_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "quorum.up")
            .description("Logic for checking whether this service is up, based on children and members, defaulting to all must be up")
            .defaultValue(QuorumCheck.QuorumChecks.all())
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
            .build();
    
    ConfigKey<QuorumCheck> RUNNING_QUORUM_CHECK = ConfigKeys.builder(QuorumCheck.class, "quorum.running") 
            .description("Logic for checking whether this service is healthy, based on children and members running, defaulting to requiring none to be ON-FIRE")
            .defaultValue(QuorumCheck.QuorumChecks.all())
            .runtimeInheritance(BasicConfigInheritance.NOT_REINHERITED)
            .build();

    ConfigKey<Boolean> DESTROY_ON_STOP = ConfigKeys.newBooleanConfigKey("application.stop.shouldDestroy",
        "Whether the app should be removed from management after a successful stop (if it is a root); "
        + "true by default.", true);

    @SetFromFlag("startLatch")
    ConfigKey<Boolean> START_LATCH = BrooklynConfigKeys.START_LATCH;

}
