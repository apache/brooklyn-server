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
package org.apache.brooklyn.core.effector;

import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import com.google.common.reflect.TypeToken;

/**
 * <pre>{@code
 * brooklyn.initializers:
 *   - type: org.apache.brooklyn.core.effector.SetSensorEffector
 *     brooklyn.config:
 *       name: setStatus
 *       sensor: $brooklyn:sensor("myentity.status")
 * }</pre>
 *
 * @since 0.11.0
 */
@Beta
public class SetSensorEffector extends AddEffector {

    private static final Logger LOG = LoggerFactory.getLogger(SetSensorEffector.class);

    public static final ConfigKey<AttributeSensor<Object>> SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<Object>>() { },
            "sensor",
            "The sensor whose value is to be set");

    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class,
            "value",
            "The value to be set on the sensor");

    private final Object mutex = new Object[0];

    public SetSensorEffector(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public SetSensorEffector(ConfigBag params) {
        this(newEffectorBuilder(params).build());
    }

    public SetSensorEffector(Effector<?> effector) {
        super(effector);
    }

    public static EffectorBuilder<Object> newEffectorBuilder(ConfigBag params) {
        EffectorBuilder<Object> eb = AddEffector.newEffectorBuilder(Object.class, params);
        EffectorBody<Object> body = new Body(eb.buildAbstract(), params);
        eb.impl(body);
        return eb;
    }

    protected static class Body extends EffectorBody<Object> {
        protected final Effector<?> effector;
        protected final ConfigBag config;
        protected final Object mutex = new Object[0];

        public Body(Effector<?> eff, ConfigBag config) {
            this.effector = eff;
            this.config = config;

            Preconditions.checkNotNull(config.getAllConfigRaw().get(SENSOR.getName()), "The sensor must be supplied when defining this effector");
        }

        @Override
        public Object call(final ConfigBag params) {
            synchronized (mutex) {
                LOG.debug("{}: Effector called with config {}, params {}", new Object[] { this, config, params });
                AttributeSensor<Object> sensor = EntityInitializers.resolve(config, SENSOR);
                Object value = EntityInitializers.resolve(params, VALUE);
                Object old = entity().sensors().set(sensor, value);
                LOG.debug("{}: Effector set {} to {} (was {})", new Object[] { this, sensor.getName(), value, old });
                return old;
            }
        }
    }
}
