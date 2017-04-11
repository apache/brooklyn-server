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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

/**
 * Subscribes to events on a collection {@link AttributeSensor} and invokes the named
 * effectors for each element that was added and removed.
 * <p>
 * The policy only detects <em>replacements</em> of the collection; it does not act on
 * modifications. If the sensor has value <code>A</code> and an element is added &ndash;
 * value <code>A'</code> &ndash; the on-added effector is not invoked. If the sensor is
 * later set to <code>B</code> the delta is made between <code>A</code> and <code>B</code>,
 * not <code>A'</code> and <code>B</code>.
 * <p>
 * To simplify the detection of additions and removals the collection is converted to a
 * {@link Set}. This means that only a single event will fire for duplicate elements in
 * the collection. Null values for the sensor are considered an empty collection.
 * <p>
 * The effectors are provided the elements that changed in their parameter map. If the
 * sensor is a collection of maps the elements are provided with their keys coerced to
 * strings and their values unchanged. Otherwise the elements are provided in a
 * single-entry map keyed by the value for {@link #PARAMETER_NAME}.
 * <p>
 * Effectors are asynchronous. If elements are added and removed in quick succession
 * there are no guarantees that the `onAdded' task will have finished before the
 * corresponding `onRemoved' task is invoked.
 */
public class InvokeEffectorOnCollectionSensorChange extends AbstractInvokeEffectorPolicy implements SensorEventListener<Collection<?>> {

    private static final Logger LOG = LoggerFactory.getLogger(InvokeEffectorOnCollectionSensorChange.class);

    public static final ConfigKey<AttributeSensor<? extends Collection<?>>> TRIGGER_SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<? extends Collection<?>>>() {},
            "sensor",
            "Sensor to be monitored.");

    public static final ConfigKey<String> ON_ADDED_EFFECTOR_NAME = ConfigKeys.newStringConfigKey(
            "onAdded",
            "Name of the effector to invoke when entries are added to the collection.");

    public static final ConfigKey<String> ON_REMOVED_EFFECTOR_NAME = ConfigKeys.newStringConfigKey(
            "onRemoved",
            "Name of the effector to invoke when entries are removed from the collection.");

    public static final ConfigKey<String> PARAMETER_NAME = ConfigKeys.newStringConfigKey(
            "parameterName",
            "The name of the parameter to supply to the effectors",
            "value");

    /** The previous version of the set against which events will be compared. */
    private Set<Object> previous = Collections.emptySet();

    /** Guards accesses of previous. */
    private final Object[] updateLock = new Object[0];

    @Override
    public void setEntity(EntityLocal entity) {
        /*
         * A warning to future modifiers of this method: it is called on rebind
         * so any changes must be compatible with existing persisted state.
         */
        super.setEntity(entity);
        Sensor<? extends Collection<?>> sensor =
                checkNotNull(getConfig(TRIGGER_SENSOR), "Value required for " + TRIGGER_SENSOR.getName());

        checkArgument(Strings.isNonBlank(getConfig(PARAMETER_NAME)), "Value required for " + PARAMETER_NAME.getName());

        // Fail straight away if neither effector is found.
        if (getEffector(getOnAddedEffector()).isAbsentOrNull() &&
                getEffector(getOnRemovedEffector()).isAbsentOrNull()) {
            throw new IllegalArgumentException("Value required for one or both of " + ON_ADDED_EFFECTOR_NAME.getName() +
                    " and " + ON_REMOVED_EFFECTOR_NAME.getName());
        }

        // Initialise `present` before subscribing.
        Collection<?> current = entity.sensors().get(getTriggerSensor());
        synchronized (updateLock) {
            previous = (current != null) ? new HashSet<>(current) : Collections.emptySet();
        }
        subscriptions().subscribe(entity, sensor, this);
    }

    @Override
    public void onEvent(SensorEvent<Collection<?>> event) {
        final Set<Object> newValue = event.getValue() != null
                ? new LinkedHashSet<>(event.getValue())
                : ImmutableSet.of();
        if (isBusySensorEnabled()) {
            // There are more events coming that this policy hasn't been notified of if the
            // value received in the event does not match the current value of the sensor.
            final Collection<?> sensorVal = entity.sensors().get(getTriggerSensor());
            final Set<Object> sensorValSet = sensorVal != null
                ? new LinkedHashSet<>(sensorVal)
                : ImmutableSet.of();
            setMoreUpdatesComing(event.getTimestamp(), newValue, sensorValSet);
        }
        final Set<Object> added = new LinkedHashSet<>(), removed = new LinkedHashSet<>();
        // It's only necessary to hold updateLock just to calculate the difference but
        // it is useful to guarantee that all the effectors are queued before the next
        // event is handled.
        synchronized (updateLock) {
            // Not using .immutableCopy() in case either set contains `null`.
            Sets.difference(newValue, previous).copyInto(added);
            Sets.difference(previous, newValue).copyInto(removed);
            for (Object o : added) {
                onAdded(o);
            }
            for (Object o : removed) {
                onRemoved(o);
            }
            this.previous = Collections.unmodifiableSet(newValue);
        }
    }

    private void onAdded(Object newElement) {
        invokeEffector(getOnAddedEffector(), newElement);
    }

    private void onRemoved(Object newElement) {
        invokeEffector(getOnRemovedEffector(), newElement);
    }

    private void invokeEffector(String effectorName, Object parameter) {
        Maybe<Effector<?>> effector = getEffector(effectorName);
        if (effector.isPresentAndNonNull()) {
            final Map<String, Object> parameters;
            if (parameter instanceof Map) {
                Map<?, ?> param = (Map) parameter;
                parameters = MutableMap.of();
                for (Map.Entry<?, ?> entry : param.entrySet()) {
                    String key = TypeCoercions.coerce(entry.getKey(), String.class);
                    parameters.put(key, entry.getValue());
                }
            } else {
                parameters = MutableMap.of(getConfig(PARAMETER_NAME), parameter);
            }

            LOG.debug("{} invoking {} on {} with parameters {}", new Object[]{this, effector, entity, parameters});
            invoke(effector.get(), parameters);
        }
    }

    private Maybe<Effector<?>> getEffector(String name) {
        return entity.getEntityType().getEffectorByName(name);
    }

    private String getOnAddedEffector() {
        return getConfig(ON_ADDED_EFFECTOR_NAME);
    }

    private String getOnRemovedEffector() {
        return getConfig(ON_REMOVED_EFFECTOR_NAME);
    }

    private AttributeSensor<? extends Collection<?>> getTriggerSensor() {
        return getConfig(TRIGGER_SENSOR);
    }

}
