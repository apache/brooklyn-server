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

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
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

        String sensorNameFull = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, sensor.name, String.class);
        if (Strings.isBlank(sensorNameFull)) throw new IllegalArgumentException("Sensor name is required");

        List<Object> sensorNameIndexes = MutableList.of();
        String sensorNameBase = extractSensorNameBaseAndPopulateIndices(sensorNameFull, sensorNameIndexes);

        TypeToken<?> type = context.lookupType(sensor.type, () -> TypeToken.of(Object.class));
        final Entity entity = sensor.entity!=null ? sensor.entity : context.getEntity();
        AttributeSensor<Object> sensorBase = (AttributeSensor<Object>) Sensors.newSensor(type, sensorNameBase);
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
        if (require==null && sensorNameIndexes.isEmpty()) {
            resolve.run();
            oldValue = entity.sensors().set(sensorBase, resolvedValue.get());
        } else {
            oldValue = entity.sensors().modify(sensorBase, oldBase -> {
                if (require!=null) {
                    Object old = oldBase;
                    MutableList<Object> indexes = MutableList.copyOf(sensorNameIndexes);
                    while (!indexes.isEmpty()) {
                        Object i = indexes.remove(0);
                        if (old == null) break;
                        if (old instanceof Map) old = ((Map) old).get(i);
                        else if (old instanceof Iterable && i instanceof Integer) {
                            int ii = (Integer)i;
                            int size = Iterables.size((Iterable) old);
                            if (ii==-1) ii = size-1;
                            old = (ii<0 || ii>=size) ? null : Iterables.get((Iterable) old, ii);
                        } else {
                            throw new IllegalArgumentException("Cannot find argument '" + i + "' in " + old);
                        }
                    }

                    if (old == null && !((AbstractEntity.BasicSensorSupport) entity.sensors()).contains(sensorBase.getName())) {
                        DslPredicates.DslEntityPredicateDefault requireTweaked = new DslPredicates.DslEntityPredicateDefault();
                        requireTweaked.sensor = sensorNameFull;
                        requireTweaked.check = WrappedValue.of(require);
                        if (!requireTweaked.apply(entity)) {
                            throw new SensorRequirementFailedAbsent("Sensor " + sensorNameFull + " unset or unavailable when there is a non-absent requirement");
                        }
                    } else {
                        if (!require.apply(old)) {
                            throw new SensorRequirementFailed("Sensor " + sensorNameFull + " value does not match requirement", old);
                        }
                    }
                }

                resolve.run();

                // now set
                Object result;

                if (!sensorNameIndexes.isEmpty()) {
                    result = oldBase;

                    // ensure mutable
                    result = makeMutable(result, sensorNameIndexes);

                    Object target = result;
                    MutableList<Object> indexes = MutableList.copyOf(sensorNameIndexes);
                    while (!indexes.isEmpty()) {
                        Object i = indexes.remove(0);
                        boolean isLast = indexes.isEmpty();
                        Object nextTarget;

                        if (target instanceof Map) {
                            nextTarget = ((Map) target).get(i);
                            if (nextTarget==null || isLast || !(nextTarget instanceof MutableMap)) {
                                // ensure mutable
                                nextTarget = isLast ? resolvedValue.get() : makeMutable(nextTarget, indexes);
                                ((Map) target).put(i, nextTarget);
                            }

                        } else if (target instanceof Iterable && i instanceof Integer) {
                            int ii = (Integer)i;
                            int size = Iterables.size((Iterable) target);
                            if (ii==-1) ii = size-1;
                            boolean outOfBounds = ii < 0 || ii >= size;
                            nextTarget = outOfBounds ? null : Iterables.get((Iterable) target, ii);

                            if (nextTarget==null || isLast || (!(nextTarget instanceof MutableMap) && !(nextTarget instanceof MutableSet) && !(nextTarget instanceof MutableList))) {
                                nextTarget = isLast ? resolvedValue.get() : makeMutable(nextTarget, indexes);
                                if (outOfBounds) {
                                    ((Collection) target).add(nextTarget);
                                } else {
                                    if (!(target instanceof List)) throw new IllegalStateException("Cannot set numerical position index in a non-list collection (and was not otherwise known as mutable; e.g. use MutableSet): "+target);
                                    ((List) target).set(ii, nextTarget);
                                }
                            }

                        } else {
                            throw new IllegalArgumentException("Cannot find argument '" + i + "' in " + target);
                        }
                        target = nextTarget;
                    }
                } else {
                    result = resolvedValue.get();
                }

                return Maybe.of(result);
            });
        }

        context.noteOtherMetadata("Value set", resolvedValue.get());
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);

        return context.getPreviousStepOutput();
    }

    static String extractSensorNameBaseAndPopulateIndices(String sensorNameFull, List<Object> sensorNameIndexes) {
        int bracket = sensorNameFull.indexOf('[');
        String sensorNameBase;
        if (bracket > 0) {
            sensorNameBase = sensorNameFull.substring(0, bracket);
            String brackets = sensorNameFull.substring(bracket);
            while (!brackets.isEmpty()) {
                if (!brackets.startsWith("[")) throw new IllegalArgumentException("Expected '[' for sensor index");
                brackets = brackets.substring(1).trim();
                int bi = brackets.indexOf(']');
                if (bi<0) throw new IllegalArgumentException("Mismatched ']' in sensor name");
                String bs = brackets.substring(0, bi).trim();
                if (bs.startsWith("\"") || bs.startsWith("\'")) bs = StringEscapes.BashStringEscapes.unwrapBashQuotesAndEscapes(bs);
                else if (bs.matches("-? *[0-9]+")) {
                    sensorNameIndexes.add(Integer.parseInt(bs));
                    bs = null;
                }
                if (bs!=null) {
                    sensorNameIndexes.add(bs);
                }
                brackets = brackets.substring(bi+1).trim();
            }
        } else if (bracket == 0) {
            throw new IllegalArgumentException("Sensor name cannot start with '['");
        } else {
            sensorNameBase = sensorNameFull;
        }
        return sensorNameBase;
    }

    static Object makeMutable(Object x, List<Object> indexesRemaining) {
        if (x==null) {
            // look ahead to see if it should be a list
            if (indexesRemaining!=null && !indexesRemaining.isEmpty() && indexesRemaining.get(0) instanceof Integer) return MutableList.of();
            return MutableMap.of();
        }

        if (x instanceof Set) {
            if (!(x instanceof MutableSet)) return MutableSet.copyOf((Set) x);
            return x;
        }
        if (x instanceof Map && !(x instanceof MutableMap)) return MutableMap.copyOf((Map) x);
        else if (x instanceof Iterable && !(x instanceof MutableList)) return MutableList.copyOf((Iterable) x);
        return x;
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
