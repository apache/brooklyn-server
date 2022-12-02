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
package org.apache.brooklyn.core.workflow.steps.appmodel;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

public class SetSensorWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SetSensorWorkflowStep.class);

    public static final String SHORTHAND = "[ ${sensor.type} ] ${sensor.name} [ \"=\" ${value...} ]";

    public static final ConfigKey<EntityValueToSet> SENSOR = ConfigKeys.newConfigKey(EntityValueToSet.class, "sensor");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<DslPredicates.DslPredicate> REQUIRE = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "require");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    public static class SensorRequirementFailed extends RuntimeException {
        private final Object sensorValue;

        public SensorRequirementFailed(String message, Object value) {
            super(message);
            this.sensorValue = value;
        }

        public Object getSensorValue() {
            return sensorValue;
        }
    }
    public static class SensorRequirementFailedAbsent extends SensorRequirementFailed {
        public SensorRequirementFailedAbsent(String message) {
            super(message, null);
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        EntityValueToSet sensor = context.getInput(SENSOR);
        if (sensor==null) throw new IllegalArgumentException("Sensor name is required");
        String sensorName = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, sensor.name, String.class);
        if (Strings.isBlank(sensorName)) throw new IllegalArgumentException("Sensor name is required");
        TypeToken<?> type = context.lookupType(sensor.type, () -> TypeToken.of(Object.class));
        final Entity entity = sensor.entity!=null ? sensor.entity : context.getEntity();
        AttributeSensor<Object> s = (AttributeSensor<Object>) Sensors.newSensor(type, sensorName);
        AtomicReference<Object> resolvedValue = new AtomicReference<>();
        Object oldValue;

        Runnable resolve = () -> {
//        // might need to be careful if defined type is different or more generic than type specified here;
//        // but that should be fine as XML persistence preserves types
//        Sensor<?> sd = entity.getEntityType().getSensor(sensorName);
//        if (!type.isSupertypeOf(sd.getTypeToken())) {
//            ...
//        }

            resolvedValue.set(context.getInput(VALUE.getName(), type));
        };

        DslPredicates.DslPredicate require = context.getInput(REQUIRE);
        if (require==null) {
            resolve.run();
            oldValue = entity.sensors().set(s, resolvedValue.get());
        } else {
            oldValue = entity.sensors().modify(s, old -> {
                if (old==null && !((AbstractEntity.BasicSensorSupport)entity.sensors()).contains(s.getName())) {
                    DslPredicates.DslEntityPredicateDefault requireTweaked = new DslPredicates.DslEntityPredicateDefault();
                    requireTweaked.sensor = s.getName();
                    requireTweaked.check = require;
                    if (!requireTweaked.apply(entity)) {
                        throw new SensorRequirementFailedAbsent("Sensor "+s.getName()+" unset or unavailable when there is a non-absent requirement");
                    }
                } else {
                    if (!require.apply(old)) {
                        throw new SensorRequirementFailed("Sensor "+s.getName()+" value does not match requirement", old);
                    }
                }
                resolve.run();
                return Maybe.of(resolvedValue.get());
            });
        }

        context.noteOtherMetadata("Value set", resolvedValue.get());
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
