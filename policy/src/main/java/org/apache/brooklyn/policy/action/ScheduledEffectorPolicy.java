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

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

/**
 * A {@link Policy} the executes an {@link Effector} at a specific time in the future.
 * <p>
 * <pre>{@code
 * brooklyn.policies:
 *   - type: org.apache.brooklyn.policy.action.ScheduledEffectorPolicy
 *     brooklyn.config:
 *       effector: update
 *       time: 12:00:00
 * }</pre>
 */
@Beta
public class ScheduledEffectorPolicy extends AbstractScheduledEffectorPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledEffectorPolicy.class);

    public static final AttributeSensor<Boolean> INVOKE_IMMEDIATELY = Sensors.newBooleanSensor("scheduler.invoke.now", "Invoke the configured effector immediately when this becomes true");
    public static final AttributeSensor<Date> INVOKE_AT = Sensors.newSensor(Date.class, "scheduler.invoke.at", "Invoke the configured effector at this time");

    public ScheduledEffectorPolicy() {
        super();
    }

    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);

        subscriptions().subscribe(entity, INVOKE_IMMEDIATELY, this);
        subscriptions().subscribe(entity, INVOKE_AT, this);
    }

    @Override
    public void start() {
        String time = config().get(TIME);
        Duration wait = config().get(WAIT);

        if (time != null) {
            wait = getWaitUntil(time);
        }

        if (wait != null) {
            schedule(wait);
        }
    }

    @Override
    public void onEvent(SensorEvent<Object> event) {
        super.onEvent(event);

        if (running.get()) {
            if (event.getSensor().getName().equals(INVOKE_AT.getName())) {
                String time = (String) event.getValue();
                if (time != null) {
                    schedule(getWaitUntil(time));
                }
            }
            if (event.getSensor().getName().equals(INVOKE_IMMEDIATELY.getName())) {
                Boolean invoke = (Boolean) event.getValue();
                if (invoke) {
                    schedule(Duration.ZERO);
                }
            }
        }
    }
}
