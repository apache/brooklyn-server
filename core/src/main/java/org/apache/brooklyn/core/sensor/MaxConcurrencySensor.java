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
package org.apache.brooklyn.core.sensor;

import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Can be used as:
 * <pre>
 * {@code
 * brooklyn.initializers:
 * - type: org.apache.brooklyn.core.sensor.MaxConcurrencySensor
 *   brooklyn.config:
 *     name: start-latch-value
 *     latch.concurrency.max: 10
 * }
 *
 * and is the short hand for:
 *
 * <pre>
 * {@code
 * brooklyn.initializers:
 * - type: org.apache.brooklyn.core.sensor.StaticSensor
 *   brooklyn.config:
 *     name: start-latch-value
 *     static.value:
 *       $brooklyn.object:
 *         type: org.apache.brooklyn.core.sensor.ReleaseableLatch$Factory
 *         factoryMethod.name: newMaxConcurrencyLatch
 *         factoryMethod.args: [10]
 * }
 * </pre>
 */
public class MaxConcurrencySensor implements EntityInitializer {
    private static final Logger log = LoggerFactory.getLogger(MaxConcurrencySensor.class);

    public static final ConfigKey<String> SENSOR_NAME = ConfigKeys.newStringConfigKey("name", "The name of the sensor to create");
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newConfigKeyWithDefault(AddSensor.SENSOR_TYPE, ReleaseableLatch.class.getName());
    public static final ConfigKey<Integer> MAX_CONCURRENCY = ConfigKeys.newIntegerConfigKey(
            "latch.concurrency.max",
            "The maximum number of threads that can execute the step for the latch this sensors is used at, in parallel.",
            Integer.MAX_VALUE);

    private Object maxConcurrency;
    private String sensorName;

    public MaxConcurrencySensor(ConfigBag params) {
        this.sensorName = params.get(SENSOR_NAME);
        this.maxConcurrency = params.getStringKey(MAX_CONCURRENCY.getName());
    }

    @Override
    public void apply(@SuppressWarnings("deprecation") final org.apache.brooklyn.api.entity.EntityLocal entity) {
        final AttributeSensor<ReleaseableLatch> sensor = Sensors.newSensor(ReleaseableLatch.class, sensorName);
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);

        final Task<ReleaseableLatch> resolveValueTask = DependentConfiguration.maxConcurrency(maxConcurrency);

        class SetValue implements Runnable {
            @Override
            public void run() {
                ReleaseableLatch releaseableLatch = resolveValueTask.getUnchecked();
                log.debug(this+" setting sensor "+sensor+" to "+releaseableLatch+" on "+entity);
                entity.sensors().set(sensor, releaseableLatch);
            }
        }
        Task<ReleaseableLatch> setValueTask = Tasks.<ReleaseableLatch>builder().displayName("Setting " + sensor + " on " + entity).body(new SetValue()).build();

        Entities.submit(entity, Tasks.sequential("Resolving and setting " + sensor + " on " + entity, resolveValueTask, setValueTask));
    }

}
