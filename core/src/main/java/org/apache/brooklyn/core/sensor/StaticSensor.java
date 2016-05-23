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
package org.apache.brooklyn.core.sensor;

import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.enricher.stock.Propagator;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Supplier;

/** 
 * Provides an initializer/feed which simply sets a given value.
 * <p>
 * {@link Task}/{@link Supplier} values are resolved when written,
 * unlike config values which are resolved on each read.
 * <p>
 * This supports a {@link StaticSensor#SENSOR_PERIOD} 
 * which can be useful if the supplied value is such a function.
 * However when the source is another sensor,
 * consider using {@link Propagator} which listens for changes instead.
 */
public class StaticSensor<T> extends AddSensor<T> {

    private static final Logger log = LoggerFactory.getLogger(StaticSensor.class);
    
    public static final ConfigKey<Object> STATIC_VALUE = ConfigKeys.newConfigKey(Object.class, "static.value");
    public static final ConfigKey<Duration> TIMEOUT = ConfigKeys.newConfigKey(
            Duration.class, "static.timeout", "Duration to wait for the value to resolve", Duration.PRACTICALLY_FOREVER);

    private final Object value;
    private final Duration timeout;

    public StaticSensor(ConfigBag params) {
        super(params);
        value = params.get(STATIC_VALUE);
        timeout = params.get(TIMEOUT);
    }

    @Override
    public void apply(final EntityLocal entity) {
        super.apply(entity);

        class ResolveValue implements Callable<Maybe<T>> {
            @SuppressWarnings("unchecked")
            @Override
            public Maybe<T> call() throws Exception {
                return Tasks.resolving(value)
                        .as((Class<T>)sensor.getType())
                        .timeout(timeout)
                        .getMaybe();
            }
        }
        final Task<Maybe<T>> resolveValue = Tasks.<Maybe<T>>builder()
                .displayName("resolving " + value)
                .body(new ResolveValue())
                .build();

        class SetValue implements Callable<T> {
            @Override
            public T call() throws Exception {
                Maybe<T> v = resolveValue.get();
                if (!v.isPresent()) {
                    log.debug(this+" not setting sensor "+sensor+"; cannot resolve "+value+" after timeout " + timeout);
                    return null;
                }
                log.debug(this+" setting sensor "+sensor+" to "+v.get());
                return entity.sensors().set(sensor, v.get());
            }
        }
        Task<T> setValue = Tasks.<T>builder().displayName("setting " + sensor).body(new SetValue()).build();

        Entities.submit(entity, Tasks.sequential("resolving and setting " + sensor, resolveValue, setValue));
    }
}
