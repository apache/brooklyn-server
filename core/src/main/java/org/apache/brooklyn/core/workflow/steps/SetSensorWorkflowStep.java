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
package org.apache.brooklyn.core.workflow.steps;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetSensorWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SetSensorWorkflowStep.class);

    public static final String SHORTHAND = "[ ${sensor.type} ] ${sensor.name} \"=\" ${value}";

    public static final ConfigKey<EntityValueToSet> SENSOR = ConfigKeys.newConfigKey(EntityValueToSet.class, "sensor");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        EntityValueToSet sensor = context.getInput(SENSOR);
        if (sensor==null) throw new IllegalArgumentException("Sensor name is required");
        String sensorName = context.resolve(sensor.name, String.class);
        if (Strings.isBlank(sensorName)) throw new IllegalArgumentException("Sensor name is required");
        TypeToken<?> type = context.lookupType(sensor.type, () -> TypeToken.of(Object.class));
        Object resolvedValue = context.getInput(VALUE.getName(), type);
        Entity entity = sensor.entity;
        if (entity==null) entity = context.getEntity();
        entity.sensors().set( (AttributeSensor<Object>) Sensors.newSensor(type, sensorName), resolvedValue);

//        // might need to be careful if defined type is different or more generic than type specified here;
//        // but that should be fine as XML persistence preserves types
//        Sensor<?> sd = entity.getEntityType().getSensor(sensorName);
//        if (!type.isSupertypeOf(sd.getTypeToken())) {
//            ...
//        }

        return context.getPreviousStepOutput();
    }

}
