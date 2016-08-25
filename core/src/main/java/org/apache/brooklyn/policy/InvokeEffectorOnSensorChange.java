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

import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Invokes the given effector when the policy changes.
 * 
 * TODO
 * * support parameters
 * * support conditions
 * * allow to be triggered by sensors on members
 */
public class InvokeEffectorOnSensorChange extends AbstractPolicy implements SensorEventListener<Object> {
    
    private static final Logger LOG = LoggerFactory.getLogger(InvokeEffectorOnSensorChange.class);

    public static final ConfigKey<Object> SENSOR = ConfigKeys.newConfigKey(Object.class, 
            "sensor", "Sensor to be monitored, as string or sensor type");

    public static final ConfigKey<String> EFFECTOR = ConfigKeys.newStringConfigKey(
            "effector", "Name of effector to invoke");

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        Preconditions.checkNotNull(getConfig(EFFECTOR), EFFECTOR);
        Object sensor = Preconditions.checkNotNull(getConfig(SENSOR), SENSOR);
        if (sensor instanceof String) sensor = Sensors.newSensor(Object.class, (String)sensor);
        subscriptions().subscribe(entity, (Sensor<?>)sensor, this);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void onEvent(SensorEvent<Object> event) {
        entity.invoke((Effector)entity.getEntityType().getEffectorByName( getConfig(EFFECTOR) ).get(), (Map)MutableMap.of());
    }
    
}
