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
package org.apache.brooklyn.policy;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;

/**
 * Invokes the given effector when the policy changes.
 * 
 * TODO
 * * support parameters
 * * support conditions
 * * allow to be triggered by sensors on members
 */
public class InvokeEffectorOnSensorChange extends AbstractInvokeEffectorPolicy implements SensorEventListener<Object> {
    
    private static final Logger LOG = LoggerFactory.getLogger(InvokeEffectorOnSensorChange.class);

    public static final ConfigKey<Object> SENSOR = ConfigKeys.builder(Object.class)
            .name("sensor")
            .description("Sensor to be monitored, as string or sensor type")
            .constraint(Predicates.notNull())
            .build();

    public static final ConfigKey<String> EFFECTOR = ConfigKeys.builder(String.class)
            .name("effector")
            .description("Name of effector to invoke")
            .constraint(StringPredicates.isNonBlank())
            .build();

    private AttributeSensor<Object> sensor;

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        Preconditions.checkNotNull(getConfig(EFFECTOR), EFFECTOR);
        sensor = getSensor();
        subscriptions().subscribe(entity, sensor, this);
        LOG.debug("{} subscribed to {} events on {}", new Object[]{this, sensor, entity});
    }

    @Override
    public void onEvent(SensorEvent<Object> event) {
        final Effector<?> eff = getEffectorNamed(getConfig(EFFECTOR)).get();
        if (isBusySensorEnabled()) {
            final Object currentSensorValue = entity.sensors().get(sensor);
            setMoreUpdatesComing(event.getTimestamp(), event.getValue(), currentSensorValue);
        }
        invoke(eff, MutableMap.<String, Object>of());
    }

    private AttributeSensor<Object> getSensor() {
        final Object configVal = Preconditions.checkNotNull(getConfig(SENSOR), SENSOR);
        final AttributeSensor<Object> sensor;
        if (configVal == null) {
            throw new NullPointerException("Value for " + SENSOR.getName() + " is null");
        } else if (configVal instanceof String) {
            sensor = Sensors.newSensor(Object.class, (String) configVal);
        } else if (configVal instanceof AttributeSensor) {
            sensor = (AttributeSensor<Object>) configVal;
        } else {
            sensor = TypeCoercions.tryCoerce(configVal, new TypeToken<AttributeSensor<Object>>() {}).get();
        }
        return sensor;
    }
    
}
