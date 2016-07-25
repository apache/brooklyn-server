/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.time.Duration;

/**
 * Entity that can target another entity for the purpose of testing
 */
@ImplementedBy(value = TargetableTestComponentImpl.class)
public interface TargetableTestComponent extends Entity, Startable {

    /**
     * The target entity to test. Optional, use either this or
     * {@link #TARGET_ID targetId}. If both are set then this
     * will take precedence.
     */
    AttributeSensorAndConfigKey<Entity, Entity> TARGET_ENTITY = ConfigKeys.newSensorAndConfigKey(Entity.class, "target", "Entity under test");

    /**
     * Id of the target entity to test.
     *
     * @see {@link #TARGET_ENTITY target}.
     */
    ConfigKey<String> TARGET_ID = ConfigKeys.newStringConfigKey("targetId", "Id of the entity under test");

    /**
     * The duration to wait for an entity with the given targetId to exist, before throwing an exception.
     */
    ConfigKey<Duration> TARGET_RESOLUTION_TIMEOUT = ConfigKeys.newConfigKey(
            Duration.class, 
            "targetResolutionTimeout", 
            "Time to wait for targetId to exist (defaults to zero, i.e. must exist immediately)",
            Duration.ZERO);

    AttributeSensor<String> TARGET_ENTITY_ID = Sensors.newStringSensor("test.target.entity.id", "Id of the target entity");
    AttributeSensor<String> TARGET_ENTITY_NAME = Sensors.newStringSensor("test.target.entity.name", "Display name of the target entity");
    AttributeSensor<String> TARGET_ENTITY_TYPE = Sensors.newStringSensor("test.target.entity.type", "Type of the target entity");

    /**
     * Get the target of the test.
     *
     * @return The target entity
     * @throws {@link IllegalArgumentException} if the target cannot be found
     */
    Entity resolveTarget();

}
