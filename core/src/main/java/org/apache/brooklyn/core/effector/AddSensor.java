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

import com.google.common.reflect.TypeToken;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;

/**
 * Creates a new {@link AttributeSensor} on an entity.
 * <p>
 * The configuration can include the sensor {@code name}, {@code period} and {@code targetType}.
 * For the targetType, currently this only supports classes on the initial classpath, not those in
 * OSGi bundles added at runtime.
 *
 * @since 0.7.0
 * @deprecated since 1.1 use {@link AddSensorInitializer}
 */
@Beta @Deprecated
public class AddSensor<T> extends EntityInitializers.InitializerPatternWithFieldsFromConfigKeys implements AddSensorInitializerAbstractProto<T> {

    protected String name;
    protected Duration period;
    protected String type;

    {
        addInitConfigMapping(SENSOR_NAME, v -> name = v);
        addInitConfigMapping(SENSOR_PERIOD, v -> period = v);
        addInitConfigMapping(SENSOR_TYPE, v -> type = v);
    }

    protected AttributeSensor<T> sensor;

    public AddSensor() {}
    public AddSensor(Map<String, String> params) { this(ConfigBag.newInstance(params)); }
    public AddSensor(final ConfigBag params) { super(params); }

    @Override
    public void apply(EntityLocal entity) {
        sensor = newSensor(entity);
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);
    }

    private AttributeSensor<T> newSensor(Entity entity) {
        TypeToken<T> clazz = AddSensorInitializerAbstractProto.getType(entity, type, name);
        return Sensors.newSensor(clazz, Preconditions.checkNotNull(name));
    }

}
