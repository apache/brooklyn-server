/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.policy.action;

import java.util.Set;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.DurationSinceSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class StopAfterDurationPolicy extends AbstractPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(StopAfterDurationPolicy.class);

    public static final ConfigKey<Duration> LIFETIME = ConfigKeys.builder(Duration.class)
            .name("lifetime")
            .description("The duration the entity is allowed to remain running")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Lifecycle> STATE = ConfigKeys.builder(Lifecycle.class)
            .name("state")
            .description("The state the entity must enter before the stop-timer is started")
            .defaultValue(Lifecycle.RUNNING)
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Duration> POLL_PERIOD = ConfigKeys.builder(Duration.class)
            .name("pollPeriod")
            .description("Period in which duration-since sensor should be updated")
            .defaultValue(Duration.THIRTY_SECONDS)
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<Boolean> HAS_STARTED_TIMER = ConfigKeys.builder(Boolean.class)
            .name("timer-started")
            .description("For internal use only")
            .defaultValue(false)
            .build();

    public static final ConfigKey<Boolean> FIRED_STOP = ConfigKeys.builder(Boolean.class)
            .name("fired-stop")
            .description("For internal use only")
            .defaultValue(false)
            .build();

    private final Object eventLock = new Object[0];

    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);
        entity.subscriptions().subscribe(entity, Attributes.SERVICE_STATE_ACTUAL, new LifecycleListener());
        entity.subscriptions().subscribe(entity, Sensors.newSensor(Duration.class, getSensorName()), new TimeIsUpListener());
    }

    @Override
    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        Set<ConfigKey<?>> accepted = ImmutableSet.<ConfigKey<?>>of(
                HAS_STARTED_TIMER,
                FIRED_STOP,
                LIFETIME);
        if (!accepted.contains(key)) {
            super.doReconfigureConfig(key, val);
        }
    }

    private String getSensorName() {
        return "duration.since.first-" + config().get(STATE).name().toLowerCase();
    }

    private class LifecycleListener implements SensorEventListener<Lifecycle> {
        @Override
        public void onEvent(SensorEvent<Lifecycle> event) {
            synchronized (eventLock) {
                if (!config().get(HAS_STARTED_TIMER) && config().get(STATE).equals(event.getValue())) {
                    DurationSinceSensor sensor = new DurationSinceSensor(ConfigBag.newInstance(ImmutableMap.of(
                            DurationSinceSensor.SENSOR_NAME, getSensorName(),
                            DurationSinceSensor.SENSOR_PERIOD, config().get(POLL_PERIOD),
                            DurationSinceSensor.SENSOR_TYPE, Duration.class.getName())));
                    sensor.apply(entity);
                    config().set(HAS_STARTED_TIMER, true);
                }
            }
        }
    }

    private class TimeIsUpListener implements SensorEventListener<Duration> {
        @Override
        public void onEvent(SensorEvent<Duration> event) {
            synchronized (eventLock) {
                if (!config().get(FIRED_STOP)) {
                    if (config().get(LIFETIME).subtract(event.getValue()).isNegative()) {
                        LOG.debug("Stopping {}: lifetime ({}) has expired", entity, config().get(LIFETIME));
                        entity.invoke(Startable.STOP, ImmutableMap.<String, Object>of());
                        config().set(FIRED_STOP, true);
                    }
                }
            }
        }
    }

}
