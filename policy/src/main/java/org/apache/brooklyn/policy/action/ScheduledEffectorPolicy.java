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

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
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
 *       time: 2017-12-11 12:00:00
 * }</pre>
 */
@Beta
public class ScheduledEffectorPolicy extends AbstractScheduledEffectorPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(ScheduledEffectorPolicy.class);

    public static final AttributeSensor<Boolean> INVOKE_IMMEDIATELY = Sensors.newBooleanSensor("scheduler.invoke.now", "Invoke the configured effector immediately when this becomes true");
    public static final AttributeSensor<String> INVOKE_AT = Sensors.newStringSensor("scheduler.invoke.at", "Invoke the configured effector at this time");

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

        String time = config().get(TIME);
        if (Strings.isNonBlank(time)) {
            scheduleAt(time);
        }
    }

    protected void scheduleAt(String time) {
        try {
            Date when = FORMATTER.parse(time);
            Date now = new Date();
            if (when.before(now)) {
                throw new IllegalStateException("The time provided must be in the future: " + FORMATTER.format(time));
            }
            long difference = Math.max(0, when.getTime() - now.getTime());
            LOG.debug("{} scheduling {} at {} (in {}ms)", new Object[] { this, effector.getName(), time, difference });
            executor.schedule(this, difference, TimeUnit.MILLISECONDS);
        } catch (ParseException e) {
            LOG.warn("The time must be formatted as " + TIME_FORMAT + " for this policy", e);
            Exceptions.propagate(e);
        }
    }

    private final SensorEventListener<Object> handler = new SensorEventListener<Object>() {
        @Override
        public void onEvent(SensorEvent<Object> event) {
            synchronized (mutex) {
                LOG.debug("{} got event {}", ScheduledEffectorPolicy.this, event);
                if (event.getSensor().getName().equals(INVOKE_AT.getName())) {
                    String time = (String) event.getValue();
                    if (Strings.isNonBlank(time)) {
                        scheduleAt(time);
                    }
                }
                if (event.getSensor().getName().equals(INVOKE_IMMEDIATELY.getName())) {
                    Boolean invoke = Boolean.parseBoolean(Strings.toString(event.getValue()));
                    if (invoke) {
                        executor.submit(ScheduledEffectorPolicy.this);
                    }
                }
            }
        }
    };

}
