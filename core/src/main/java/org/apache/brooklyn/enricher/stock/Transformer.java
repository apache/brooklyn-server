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
package org.apache.brooklyn.enricher.stock;

import static com.google.common.base.Preconditions.checkArgument;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.catalog.CatalogConfig;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.Tasks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.reflect.TypeToken;

@Catalog(name="Transformer", description="Transforms sensors of an entity")
@SuppressWarnings("serial")
public class Transformer<T,U> extends AbstractTransformer<T,U> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(Transformer.class);

    // exactly one of these should be supplied to set a value
    @CatalogConfig(label = "Target value")
    public static final ConfigKey<Object> TARGET_VALUE = ConfigKeys.newConfigKey(
            Object.class,
            "enricher.targetValue",
            "The value for the target sensor. This can use the Brooklyn DSL, which will be "
                    + "re-evaluated each time the trigger sensor(s) change");

    public static final ConfigKey<Function<?, ?>> TRANSFORMATION_FROM_VALUE = ConfigKeys.newConfigKey(
            new TypeToken<Function<?, ?>>() {},
            "enricher.transformation",
            "A function for computing the target sensor's new value (will be passed the trigger sensor's value each time)");

    public static final ConfigKey<Function<?, ?>> TRANSFORMATION_FROM_EVENT = ConfigKeys.newConfigKey(
            new TypeToken<Function<?, ?>>() {},
            "enricher.transformation.fromevent",
            "A function for computing the target sensor's new value (will be passed the trigger sensor's change-event each time)");

    public Transformer() { }
    
    /** returns a function for transformation, for immediate use only (not for caching, as it may change) */
    @Override
    @SuppressWarnings("unchecked")
    protected Function<SensorEvent<T>, U> getTransformation() {
        MutableSet<Object> suppliers = MutableSet.of();
        suppliers.addIfNotNull(config().getRaw(TARGET_VALUE).orNull());
        suppliers.addIfNotNull(config().getRaw(TRANSFORMATION_FROM_EVENT).orNull());
        suppliers.addIfNotNull(config().getRaw(TRANSFORMATION_FROM_VALUE).orNull());
        checkArgument(suppliers.size()==1,  
            "Must set exactly one of: %s, %s, %s", TARGET_VALUE.getName(), TRANSFORMATION_FROM_VALUE.getName(), TRANSFORMATION_FROM_EVENT.getName());
        
        final Function<SensorEvent<? super T>, ?> fromEvent = (Function<SensorEvent<? super T>, ?>) config().get(TRANSFORMATION_FROM_EVENT);
        if (fromEvent != null) {
            // wraps function so can handle DSL response.
            // named class not necessary as result should not be serialized
            return new Function<SensorEvent<T>, U>() {
                @Override public U apply(SensorEvent<T> input) {
                    Object targetValueRaw = fromEvent.apply(input);
                    return resolveImmediately(targetValueRaw, targetSensor);
                }
                @Override
                public String toString() {
                    return ""+fromEvent;
                }
            };
        }
        
        final Function<T, ?> fromValueFn = (Function<T, ?>) config().get(TRANSFORMATION_FROM_VALUE);
        if (fromValueFn != null) {
            // named class not necessary as result should not be serialized
            return new Function<SensorEvent<T>, U>() {
                @Override public U apply(SensorEvent<T> input) {
                    // input can be null if using `triggerSensors`, rather than `sourceSensor`
                    Object targetValueRaw = fromValueFn.apply(input == null ? null : input.getValue());
                    return resolveImmediately(targetValueRaw, targetSensor);
                }
                @Override
                public String toString() {
                    return ""+fromValueFn;
                }
            };
        }

        // from target value
        // named class not necessary as result should not be serialized
        final Object targetValueRaw = config().getRaw(TARGET_VALUE).orNull();
        return new Function<SensorEvent<T>, U>() {
            @Override public U apply(SensorEvent<T> input) {
                return resolveImmediately(targetValueRaw, targetSensor);
            }
            @Override
            public String toString() {
                return ""+targetValueRaw;
            }
        };
    }
    
    @SuppressWarnings("unchecked")
    private U resolveImmediately(Object rawVal, Sensor<U> targetSensor) {
        if (rawVal == Entities.UNCHANGED || rawVal == Entities.REMOVE) {
            // If it's a special marker-object, then don't try to transform it
            return (U) rawVal;
        }

        // evaluate immediately, or return null.
        // For vals that implement ImmediateSupplier, we'll use that to get the value
        // (or Maybe.absent) without blocking.
        // Otherwise, the Tasks.resolving will give it its best shot at resolving without
        // blocking on external events (such as waiting for another entity's sensor).
        return (U) Tasks.resolving(rawVal).as(targetSensor.getType())
                .context(entity)
                .description("Computing sensor "+targetSensor+" from "+rawVal)
                .deep(true)
                .immediately(true)
                .getMaybe().orNull();
    }
}
