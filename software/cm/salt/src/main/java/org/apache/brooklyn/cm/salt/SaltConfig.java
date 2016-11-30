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
package org.apache.brooklyn.cm.salt;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.config.SetConfigKey;
import org.apache.brooklyn.util.core.flags.SetFromFlag;

/**
 * {@link ConfigKey}s used to configure Salt entities.
 *
 * @see SaltConfigs
 */
@Beta
public interface SaltConfig {

    enum SaltMode {
        /** Master entity */
        MASTER, 
        /** Minion entity */
        MINION,
        /** Masterless entity using salt-ssh */
        MASTERLESS
    }

    @SetFromFlag("salt.mode")
    ConfigKey<SaltMode> SALT_MODE = ConfigKeys.newConfigKey(SaltMode.class, "brooklyn.salt.mode",
            "SaltStack execution mode (master/minion/masterless)", SaltMode.MASTERLESS);

    @SetFromFlag("pillars")
    SetConfigKey<String> SALT_PILLARS = new SetConfigKey<>(String.class, "brooklyn.salt.pillars",
            "List of Salt Pillar identifiers for top.sls", ImmutableSet.<String>of());

    @SetFromFlag("pillarUrls")
    SetConfigKey<String> SALT_PILLAR_URLS = new SetConfigKey<>(String.class, "brooklyn.salt.pillarUrls",
            "List of Salt Pillar archive URLs", ImmutableSet.<String>of());

    @SetFromFlag("formulas")
    SetConfigKey<String> SALT_FORMULAS = new SetConfigKey<>(String.class, "brooklyn.salt.formulaUrls",
            "List of Salt formula URLs", ImmutableSet.<String>of());

    @SetFromFlag("start_states")
    SetConfigKey<String> START_STATES = new SetConfigKey<>(String.class, "brooklyn.salt.start.states",
            "Set of Salt states to apply to start entity", ImmutableSet.<String>of());

    @SetFromFlag("stop_states")
    SetConfigKey<String> STOP_STATES = new SetConfigKey<>(String.class, "brooklyn.salt.stop.states",
            "Set of Salt states to apply to stop entity", ImmutableSet.<String>of());

    @SetFromFlag("restart_states")
    SetConfigKey<String> RESTART_STATES = new SetConfigKey<>(String.class, "brooklyn.salt.restart.states",
            "Set of Salt states to apply to restart entity", ImmutableSet.<String>of());

    MapConfigKey<Object> SALT_SSH_LAUNCH_ATTRIBUTES = new MapConfigKey<>(Object.class, "brooklyn.salt.ssh.launch.attributes", "TODO");

}
