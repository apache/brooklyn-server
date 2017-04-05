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

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

/**
 * <pre>{@code
 * brooklyn.policies:
 *   - type: org.apache.brooklyn.policy.action.ScheduledEffectorPolicy
 *     brooklyn.config:
 *       effector: repaveCluster
 *       args:
 *         k: $brooklyn:config("repave.size")
 *       time: 12:00:00
 * }</pre>
 */
@Beta
public class ScheduledEffectorPolicy extends AbstractScheduledEffectorPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledEffectorPolicy.class);

    public static final AttributeSensor<Boolean> INVOKE_IMMEDIATELY = Sensors.newBooleanSensor("scheduler.invoke.now", "Invoke the configured effector immediately when this becomes true");
    public static final AttributeSensor<Date> INVOKE_AT = Sensors.newSensor(Date.class, "scheduler.invoke.at", "Invoke the configured effector at this time");

    public ScheduledEffectorPolicy() {
        this(MutableMap.<String,Object>of());
    }

    public ScheduledEffectorPolicy(Map<String,?> props) {
        super(props);
    }

    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);

        subscriptions().subscribe(entity, INVOKE_IMMEDIATELY, handler);
        subscriptions().subscribe(entity, INVOKE_AT, handler);

        Date time = config().get(TIME);
        Duration wait = config().get(WAIT);
        if (time != null) {
            scheduleAt(time);
        } else if (wait != null) {
            executor.schedule(this, wait.toMilliseconds(), TimeUnit.MILLISECONDS);
        }
    }

    protected void scheduleAt(Date time) {
        Duration wait = getWaitUntil(time);
        LOG.debug("{}: Scheduling {} at {} (in {})", new Object[] { this, effector.getName(), time, Time.fromDurationToTimeStringRounded().apply(wait) });
        executor.schedule(this, wait.toMilliseconds(), TimeUnit.MILLISECONDS);
    }

    private final SensorEventListener<Object> handler = new SensorEventListener<Object>() {
        @Override
        public void onEvent(SensorEvent<Object> event) {
            synchronized (mutex) {
                LOG.debug("{}: Got event {}", ScheduledEffectorPolicy.this, event);
                if (event.getSensor().getName().equals(INVOKE_AT.getName())) {
                    Date time = (Date) event.getValue();
                    if (time != null) {
                        scheduleAt(time);
                    }
                }
                if (event.getSensor().getName().equals(INVOKE_IMMEDIATELY.getName())) {
                    Boolean invoke = (Boolean) event.getValue();
                    if (invoke) {
                        executor.submit(ScheduledEffectorPolicy.this);
                    }
                }
            }
        }
    };

}
