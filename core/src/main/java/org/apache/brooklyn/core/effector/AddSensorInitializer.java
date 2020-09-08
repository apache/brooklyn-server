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

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectStreamException;
import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.effector.ParameterType;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.Effectors.EffectorBuilder;
import org.apache.brooklyn.core.entity.EntityInitializers;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

/**
 * Entity initializer which adds a sensor to an entity.
 *
 * @since 0.7.0 */
@Beta
public class AddSensorInitializer<T> extends EntityInitializers.InitializerPatternWithConfigKeys implements Serializable {

    public static final ConfigKey<String> SENSOR_NAME = ConfigKeys.newStringConfigKey("name", "The name of the sensor to create");
    public static final ConfigKey<Duration> SENSOR_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms; default 5 minutes", Duration.FIVE_MINUTES);
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newStringConfigKey("targetType", "Target type for the value; default String", "java.lang.String");

    // constructor for use in code to conveniently supply params
    protected AddSensorInitializer(ConfigBag params) {
        super(params);
    }
    // JSON deserialization constructor
    protected AddSensorInitializer() {}

    @Override
    public void apply(EntityLocal entity) {
        addSensor(entity);
    }

    protected AttributeSensor<T> addSensor(EntityLocal entity) {
        AttributeSensor<T> sensor = sensor(entity);
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);
        return sensor;
    }

    private AttributeSensor<T> sensor(Entity entity) {
        String className = getFullClassName(initParam(SENSOR_TYPE));
        Class<T> clazz = getType(entity, className);
        return Sensors.newSensor(clazz, Preconditions.checkNotNull(initParam(SENSOR_NAME)));
    }

    @SuppressWarnings("unchecked")
    protected Class<T> getType(Entity entity, String className) {
        return AddSensorInitializerAbstractProto.getType(entity, className, initParam(SENSOR_NAME), this);
    }

    protected String getFullClassName(String className) {
        return AddSensorInitializerAbstractProto.getFullClassName(className);
    }

    // kept for backwards deserialization compatibility
    private String name;
    private Duration period;
    private String type;
    private AttributeSensor<T> sensor;
    private ConfigBag params;
    // introduced in 1.1 for legacy compatibility
    protected Object readResolve() {
        super.readResolve();
        initFromConfigBag(ConfigBag.newInstance()
                .putIfAbsentAndNotNull(SENSOR_NAME, name)
                .putIfAbsentAndNotNull(SENSOR_PERIOD, period)
                .putIfAbsentAndNotNull(SENSOR_TYPE, type)
        );
        name = null;
        period = null;
        type = null;
        sensor = null;
        params = null;

        return this;
    }

}
