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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

public class SetSensorWorkflowStep extends WorkflowStepDefinition {

    EntityValueToSet sensor;
    Object value;

    public EntityValueToSet getSensor() {
        return sensor;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void setShorthand(String expression) {
        this.sensor = EntityValueToSet.parseFromShorthand(expression, this::setValue);
    }

    @Override
    protected Task<?> newTask(String name, WorkflowExecutionContext workflowExecutionContext) {
        return Tasks.create(getDefaultTaskName(workflowExecutionContext), () -> {
            if (sensor==null) throw new IllegalArgumentException("Sensor name is required");
            String sensorName = workflowExecutionContext.resolve(sensor.name);
            if (Strings.isBlank(sensorName)) throw new IllegalArgumentException("Sensor name is required");
            TypeToken<?> type = workflowExecutionContext.lookupType(sensor.type, () -> TypeToken.of(Object.class));
            Object resolvedValue = workflowExecutionContext.resolve(value, type);
            workflowExecutionContext.getEntity().sensors().set(Sensors.newSensor(Object.class, sensorName), resolvedValue);
        });
    }

}
