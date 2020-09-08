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

/**
 * Entity initializer which adds a sensor to an entity.
 *
 * @since 0.7.0 */
@Beta
interface AddSensorInitializerAbstractProto<T> extends EntityInitializer {

    public static final ConfigKey<String> SENSOR_NAME = ConfigKeys.newStringConfigKey("name", "The name of the sensor to create");
    public static final ConfigKey<Duration> SENSOR_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms; default 5 minutes", Duration.FIVE_MINUTES);
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newStringConfigKey("targetType", "Target type for the value; default String", "java.lang.String");

    @SuppressWarnings("unchecked")
    static <T> Class<T> getType(Entity entity, String className, String name, Object contextInstanceForClassloading) {
        try {
            // TODO use OSGi loader (low priority however); also ensure that allows primitives
            Maybe<Class<?>> primitive = Boxing.getPrimitiveType(className);
            if (primitive.isPresent()) return (Class<T>) primitive.get();

            return (Class<T>) new ClassLoaderUtils(contextInstanceForClassloading, entity).loadClass(className);
        } catch (ClassNotFoundException e) {
            if (!className.contains(".")) {
                // could be assuming "java.lang" package; try again with that
                try {
                    return (Class<T>) Class.forName("java.lang."+className);
                } catch (ClassNotFoundException e2) {
                    throw new IllegalArgumentException("Invalid target type for sensor "+name+": " + className+" (also tried java.lang."+className+")");
                }
            } else {
                throw new IllegalArgumentException("Invalid target type for sensor "+name+": " + className);
            }
        }
    }

    static String getFullClassName(String className) {
        if (className.equalsIgnoreCase("string")) {
            return "java.lang.String";
        } else if (className.equalsIgnoreCase("int") || className.equalsIgnoreCase("integer")) {
            return "java.lang.Integer";
        } else if (className.equalsIgnoreCase("long")) {
            return "java.lang.Long";
        } else if (className.equalsIgnoreCase("float")) {
            return "java.lang.Float";
        } else if (className.equalsIgnoreCase("double")) {
            return "java.lang.Double";
        } else if (className.equalsIgnoreCase("bool") || className.equalsIgnoreCase("boolean")) {
            return "java.lang.Boolean";
        } else if (className.equalsIgnoreCase("byte")) {
            return "java.lang.Byte";
        } else if (className.equalsIgnoreCase("char") || className.equalsIgnoreCase("character")) {
            return "java.lang.Character";
        } else if (className.equalsIgnoreCase("object")) {
            return "java.lang.Object";
        } else {
            return className;
        }
    }
}
