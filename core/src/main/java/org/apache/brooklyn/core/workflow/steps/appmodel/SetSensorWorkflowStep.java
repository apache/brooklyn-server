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

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import com.google.common.base.Suppliers;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity.BasicSensorSupport;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.core.workflow.utils.WorkflowSettingItemsUtils;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.predicates.DslPredicates.DslEntityPredicateDefault;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetSensorWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(SetSensorWorkflowStep.class);

    static final boolean ALWAYS_USE_SENSOR_MODIFY = true;

    public static final String SHORTHAND = "[ ${sensor.type} ] ${sensor.name} [ \" on \" ${sensor.entity} ] [ \"=\" ${value...} ]";

    public static final ConfigKey<EntityValueToSet> SENSOR = ConfigKeys.newConfigKey(EntityValueToSet.class, "sensor");
    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<DslPredicates.DslPredicate> REQUIRE = ConfigKeys.newConfigKey(DslPredicates.DslPredicate.class, "require",
            "Require a condition in order to set the value; if the condition is not satisfied, the sensor is not set");

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

        Pair<String, List<Object>> sensorNameAndIndices = WorkflowSettingItemsUtils.resolveNameAndBracketedIndices(context, sensor.name, false);
        if (sensorNameAndIndices==null) throw new IllegalArgumentException("Sensor name is required");

        final TypeToken<?> type = context.lookupType(sensor.type, () -> TypeToken.of(Object.class));
        Object entityO1 = context.getInput(ENTITY);
        if (entityO1!=null && sensor.entity!=null && !Objects.equals(entityO1, sensor.entity))
            throw new IllegalArgumentException("Cannot specify different entities in 'entity' and 'sensor.entity' when setting sensor");
        Object entityO2 = ObjectUtils.firstNonNull(sensor.entity, entityO1, context.getEntity());
        final Entity entity = WorkflowStepResolution.findEntity(context, entityO2).get();

        Supplier<Object> resolveOnceValueSupplier = Suppliers.memoize(() -> {
//        // might need to be careful if defined type is different or more generic than type specified here;
//        // but that should be fine as XML persistence preserves types
//        Sensor<?> sd = entity.getEntityType().getSensor(sensorName);
//        if (!type.isSupertypeOf(sd.getTypeToken())) {
//            ...
//        }
            return context.getInput(VALUE.getName(), type);
        });

        AttributeSensor<Object> sensorBase = (AttributeSensor<Object>) Sensors.newSensor(type, sensorNameAndIndices.getLeft());
        DslPredicates.DslPredicate require = context.getInput(REQUIRE);
        AtomicReference<Pair<Object, Object>> oldValues = new AtomicReference<>();
        if (!ALWAYS_USE_SENSOR_MODIFY && require == null && sensorNameAndIndices.getRight().isEmpty()) {
            // can do simple if not using 'require' and no indices - though there is no reason not to
            // and if there is some special value which does an operation on the sensor, it's nice if we do
            Pair<Object, Object> oldValuesP = WorkflowSettingItemsUtils.setAtIndex(sensorNameAndIndices, true,
                    _oldInner -> resolveOnceValueSupplier.get(),
                    // outer getter
                    _name -> entity.sensors().get(sensorBase),
                    // outer setter
                    (_name, nv) -> entity.sensors().set(sensorBase, nv)
            );
            oldValues.set(oldValuesP);

        } else {
            // with 'require' we contractually need to do it all in the modify loop,
            // performing the check (on the inner value if there are indices);
            // even without, there might be concurrent blocks updating the same map/list if indexes are supplied

            // see testBeefySensorRequireForAtomicityAndConfigCountsMap for a thorough test
            // (look for references to SetSensorWorkflowStep.REQUIRE)

            entity.sensors().modify(sensorBase,
                    (oldOuterX) -> {
                        AtomicReference<Object> newValue = new AtomicReference<>();
                        Pair<Object, Object> oldValuesP = WorkflowSettingItemsUtils.setAtIndex(sensorNameAndIndices, true,
                                oldInner -> {
                                    if (require!=null) {
                                        if (oldInner == null && !((BasicSensorSupport) entity.sensors()).contains(sensorBase.getName())) {
                                            DslEntityPredicateDefault requireTweaked = new DslEntityPredicateDefault();
                                            requireTweaked.sensor = sensorNameAndIndices.getLeft();
                                            requireTweaked.check = WrappedValue.of(require);
                                            if (!requireTweaked.apply(entity)) {
                                                throw new SensorRequirementFailedAbsent("Sensor " + sensorNameAndIndices.getLeft() + " unset or unavailable when there is a non-absent requirement");
                                            }
                                        } else {
                                            if (!require.apply(oldInner)) {
                                                throw new SensorRequirementFailed("Sensor " + sensorNameAndIndices.getLeft() + " value does not match requirement", oldInner);
                                            }
                                        }
                                    }

                                    return resolveOnceValueSupplier.get();
                                },
                                // outer getter
                                _name -> oldOuterX,
                                // outer setter
                                (_name, nv) -> {
                                    newValue.set(nv);
                                    return oldOuterX;
                                }
                        );
                        oldValues.set(oldValuesP);
                        return Maybe.of(newValue.get());
                    });
        }

        WorkflowSettingItemsUtils.noteValueSetNestedMetadata(context, sensorNameAndIndices, resolveOnceValueSupplier.get(), oldValues.get());
        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
