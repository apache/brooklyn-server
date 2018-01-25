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
package org.apache.brooklyn.policy.failover;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.annotations.Beta;

@Beta
public interface ElectPrimaryConfig {

    public enum TargetMode { CHILDREN, MEMBERS, AUTO }
    public static final ConfigKey<TargetMode> TARGET_MODE = ConfigKeys.builder(TargetMode.class, "primary.target.mode")
        .description("where should the policy look for primary candidates; one of 'children', 'members', or 'auto' (members if it's a group)")
        .defaultValue(TargetMode.AUTO)
        .build();
    
    public enum SelectionMode { FAILOVER, BEST, STRICT }
    public static final ConfigKey<SelectionMode> SELECTION_MODE = ConfigKeys.builder(SelectionMode.class, "primary.selection.mode")
        .description("under what circumstances should the primary change:  `failover` to change only if an existing primary is unhealthy, `best` to change so one with the highest weight is always selected, or `strict` to act as `best` but fail if several advertise the highest weight (for use when the weight sensor is updated by the nodes and should tell us unambiguously who was elected)")
        .defaultValue(SelectionMode.FAILOVER)
        .build();

    public static final ConfigKey<Duration> BEST_WAIT_TIMEOUT = ConfigKeys.newDurationConfigKey("primary.stopped.wait.timeout",
        "if the highest-ranking primary is not starting, the effector will wait this long for it to be starting before picking a less highly-weighted primary; "
        + "default 5s, typically long enough to avoid races where multiple children are started concurrently but they complete extremely quickly and one completes before a better one starts, "
        + "the short duration is sufficient for the theoretical best to become waiting where the `primary.starting.wait.timeout` applies",
        Duration.seconds(5));

    public static final ConfigKey<Duration> BEST_STARTING_WAIT_TIMEOUT = ConfigKeys.newDurationConfigKey("primary.starting.wait.timeout",
        "if the highest-ranking primary is starting, the effector will wait this long for it to be running before picking a less highly-weighted primary "
        + "(or in the case of `strict` before failing if there are ties); "
        + "default 5m, typically long enough to avoid races where multiple children are started and a sub-optimal one comes online before the best one",
        Duration.minutes(5));

    public static final ConfigKey<String> PRIMARY_SENSOR_NAME = ConfigKeys.newStringConfigKey("primary.sensor.name",
        "name of sensor to publish, defaulting to 'primary'",
        PrimaryDefaultSensorsAndEffectors.PRIMARY.getName());

    public static final ConfigKey<String> PRIMARY_WEIGHT_NAME = ConfigKeys.newStringConfigKey("primary.weight.name",
        "config key or sensor to scan from candidate nodes to determine who should be primary",
        PrimaryDefaultSensorsAndEffectors.PRIMARY_WEIGHT_SENSOR.getName());

    public static final ConfigKey<String> PROMOTE_EFFECTOR_NAME = ConfigKeys.newStringConfigKey("primary.promote.effector.name",
        "effector to invoke on promotion, trying on this entity or if not present then at new primary, default `promote` and with no error if not present at either entity (but if set explicitly it will cause an error if not present)",
        PrimaryDefaultSensorsAndEffectors.PROMOTE.getName());

    public static final ConfigKey<String> DEMOTE_EFFECTOR_NAME = ConfigKeys.newStringConfigKey("primary.demote.effector.name",
        "effector to invoke on demotion, trying on this entity or if not present then at old primary, default `demote` and with no error if not present at either entity (but if set explicitly it will cause an error if not present)",
        PrimaryDefaultSensorsAndEffectors.DEMOTE.getName());

    
    public interface PrimaryDefaultSensorsAndEffectors {
        public static final AttributeSensor<Entity> PRIMARY = Sensors.newSensor(Entity.class, "primary");
        public static final ConfigKey<Double> PRIMARY_WEIGHT_CONFIG = ConfigKeys.newDoubleConfigKey("ha.primary.weight");
        public static final AttributeSensor<Double> PRIMARY_WEIGHT_SENSOR = Sensors.newDoubleSensor("ha.primary.weight");
        
        public static final Effector<String> PROMOTE = Effectors.effector(String.class, "promote").buildAbstract();
        public static final Effector<String> DEMOTE = Effectors.effector(String.class, "demote").buildAbstract();
    }

}
