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
package org.apache.brooklyn.entity.group;

import java.util.Collection;
import java.util.Objects;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

public class SensorMatchingRemovalStrategy<T> extends RemovalStrategy {
    public static final ConfigKey<AttributeSensor> SENSOR = ConfigKeys.newConfigKey(AttributeSensor.class, "sensor.matching.sensor");
    // Would be nice to use ConfigKey<T>, but TypeToken<T> cannot be instantiated at runtime
    public static final ConfigKey<Object> DESIRED_VALUE = ConfigKeys.newConfigKey(Object.class, "sensor.matching.value");

    @Nullable
    @Override
    public Entity apply(@Nullable Collection<Entity> input) {
        AttributeSensor<T> sensor = config().get(SENSOR);
        Object desiredValue = config().get(DESIRED_VALUE);
        for (Entity entity : input) {
            if (Objects.equals(desiredValue, entity.sensors().get(sensor))) {
                return entity;
            }
        }
        return null;
    }
}
