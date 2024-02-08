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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.utils.WorkflowSettingItemsUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.commons.lang3.tuple.Pair;

public class ClearSensorWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ ${sensor.type} ] ${sensor.name}";

    public static final ConfigKey<EntityValueToSet> SENSOR = ConfigKeys.newConfigKey(EntityValueToSet.class, "sensor");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        EntityValueToSet sensor = context.getInput(SENSOR);
        if (sensor==null) throw new IllegalArgumentException("Sensor name is required");

        Pair<String, List<Object>> sensorNameAndIndices = WorkflowSettingItemsUtils.resolveNameAndBracketedIndices(context, sensor.name, false);
        if (sensorNameAndIndices==null) throw new IllegalArgumentException("Sensor name is required");

        TypeToken<?> type = context.lookupType(sensor.type, () -> TypeToken.of(Object.class));
        Entity entity = sensor.entity;
        if (entity==null) entity = context.getEntity();

        // TODO use WorkflowSettingItemsUtils
        if (sensorNameAndIndices.getRight().isEmpty()) {
            ((EntityInternal) entity).sensors().remove(Sensors.newSensor(Object.class, sensorNameAndIndices.getLeft()));
        } else {
            ((EntityInternal) entity).sensors().modify(Sensors.newSensor(Object.class, sensorNameAndIndices.getLeft()), old -> {

                boolean setLast = false;

                Object newTarget = WorkflowSettingItemsUtils.makeMutableOrUnchangedDefaultingToMap(old);
                Object target = newTarget;

                MutableList<Object> indexes = MutableList.copyOf(sensorNameAndIndices.getRight());
                while (!indexes.isEmpty()) {
                    Object i = indexes.remove(0);
                    boolean isLast = indexes.isEmpty();
                    Object nextTarget;

                    if (target==null) {
                        // not found, exit
                        break;
                    }

                    if (target instanceof Map) {
                        if (isLast) {
                            setLast = true;
                            ((Map) target).remove(i);
                            nextTarget = null;
                        } else {
                            nextTarget = ((Map) target).get(i);
                            if (nextTarget==null) break;
                            ((Map) target).put(i, WorkflowSettingItemsUtils.makeMutableOrUnchangedDefaultingToMap(nextTarget));
                        }

                    } else if (target instanceof Iterable && i instanceof Integer) {
                        int ii = (Integer)i;
                        int size = Iterables.size((Iterable) target);
                        if (ii==-1) ii = size-1;
                        boolean outOfBounds = ii < 0 || ii >= size;

                        if (outOfBounds) {
                            nextTarget = null;
                            break;
                        } else if (isLast) {
                            setLast = true;
                            if (target instanceof List) {
                                ((List) target).remove(ii);
                            } else {
                                Iterator ti = ((Iterable) target).iterator();
                                for (int j=0; j<ii; j++) {
                                    ti.next();
                                }
                                ti.remove();
                            }

                            nextTarget = null;
                            break;
                        } else {
                            Object t0 = Iterables.get((Iterable) target, ii);
                            nextTarget = WorkflowSettingItemsUtils.makeMutableOrUnchangedDefaultingToMap(t0);
                            if (t0!=nextTarget) {
                                if (!(target instanceof List)) throw new IllegalStateException("Cannot set numerical position index in a non-list collection (and was not otherwise known as mutable; e.g. use MutableSet): "+target);
                                ((List) target).set(ii, nextTarget);
                            }
                        }

                    } else {
                        throw new IllegalArgumentException("Cannot find argument '" + i + "' in " + target);
                    }

                    target = nextTarget;
                }

                if (setLast) return Maybe.of(newTarget);
                else return Maybe.ofDisallowingNull(old);
            });
        }

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
