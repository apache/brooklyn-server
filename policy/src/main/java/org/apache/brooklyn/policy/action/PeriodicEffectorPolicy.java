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

import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.DurationPredicates;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

/**
 * <pre>{@code
 * brooklyn.policies:
 *   - type: org.apache.brooklyn.policy.action.ScheduledEffectorPolicy
 *     brooklyn.config:
 *       effector: repaveCluster
 *       args:
 *         k: $brooklyn:config("repave.size")
 *       period: 1 day
 *       time: 17:00:00
 * }</pre>
 */
@Beta
public class PeriodicEffectorPolicy extends AbstractScheduledEffectorPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicEffectorPolicy.class);

    public static final ConfigKey<Duration> PERIOD = ConfigKeys.builder(Duration.class)
            .name("period")
            .description("The duration between executions of this policy")
            .constraint(DurationPredicates.positive())
            .defaultValue(Duration.hours(1))
            .build();

    public static final AttributeSensor<Boolean> START_SCHEDULER = Sensors.newBooleanSensor("scheduler.start", "Start the periodic effector execution after this becomes true");

    protected AtomicBoolean running = new AtomicBoolean(false);

    public PeriodicEffectorPolicy() {
        this(MutableMap.<String,Object>of());
    }

    public PeriodicEffectorPolicy(Map<String,?> props) {
        super(props);
    }

    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);

        subscriptions().subscribe(entity, START_SCHEDULER, handler);
    }

    private final SensorEventListener<Object> handler = new SensorEventListener<Object>() {
        @Override
        public void onEvent(SensorEvent<Object> event) {
            synchronized (mutex) {
                LOG.debug("{}: Got event {}", PeriodicEffectorPolicy.this, event);
                if (event.getSensor().getName().equals(START_SCHEDULER.getName())) {
                    Boolean start = (Boolean) event.getValue();
                    if (start && running.compareAndSet(false, true)) {
                        Duration period = Preconditions.checkNotNull(config().get(PERIOD), "The period must be configured for this policy");
                        Date time = config().get(TIME);
                        Duration wait = config().get(WAIT);
                        if (time != null) {
                            wait = getWaitUntil(time);
                        } else if (wait == null) {
                            wait = period;
                        }

                        LOG.debug("{}: Scheduling {} every {} in {}", new Object[] { PeriodicEffectorPolicy.this, effector.getName(),
                                Time.fromDurationToTimeStringRounded().apply(period), Time.fromDurationToTimeStringRounded().apply(wait) });
                        executor.scheduleAtFixedRate(PeriodicEffectorPolicy.this, wait.toMilliseconds(), period.toMilliseconds(), TimeUnit.MILLISECONDS);
                        LOG.debug("{}: Scheduled", PeriodicEffectorPolicy.this);
                    }
                }
            }
        }
    };

}
