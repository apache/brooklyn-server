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

import java.lang.reflect.Field;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.yoml.YomlConfigBagConstructor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
 */
@Beta
@YomlConfigBagConstructor("")
@YomlAllFieldsTopLevel
@YomlRenameDefaultKey("name")
public class AddSensor<T> implements EntityInitializer {

    private static final Logger log = LoggerFactory.getLogger(AddSensor.class);
    
    public static final ConfigKey<String> SENSOR_NAME = ConfigKeys.newStringConfigKey("name", "The name of the sensor to create");
    public static final ConfigKey<Duration> SENSOR_PERIOD = ConfigKeys.newConfigKey(Duration.class, "period", "Period, including units e.g. 1m or 5s or 200ms; default 5 minutes", Duration.FIVE_MINUTES);
    @Alias({"sensor-type","value-type"})
    public static final ConfigKey<String> SENSOR_TYPE = ConfigKeys.newStringConfigKey("targetType", "Target type for the value; default String", "java.lang.String");

    protected final String name;
    protected final Duration period;
    protected final String targetType;
    protected AttributeSensor<T> sensor;
    
    private ConfigBag extraParams;
    
    public AddSensor(Map<String, String> params) {
        this(ConfigBag.newInstance(params));
    }

    public AddSensor(final ConfigBag params) {
        this.name = Preconditions.checkNotNull(params.get(SENSOR_NAME), "Name must be supplied when defining a sensor");
        this.period = params.get(SENSOR_PERIOD);
        
        this.targetType = params.get(SENSOR_TYPE);
        this.type = null;
    }
    
    protected void rememberUnusedParams(ConfigBag bag) {
        saveExtraParams(bag, false);
    }
    protected void rememberAllParams(ConfigBag bag) {
        saveExtraParams(bag, false);
    }
    private void saveExtraParams(ConfigBag bag, boolean justUnused) {
        if (extraParams==null) {
            extraParams = ConfigBag.newInstance();
        }
        if (justUnused) {
            extraParams.putAll(params.getUnusedConfig());
        } else {
            this.extraParams.copy(bag);
        }
    }
    protected ConfigBag getRememberedParams() {
        if (params!=null) {
            synchronized (this) {
                readResolve();
            }
        }
        if (extraParams==null) return ConfigBag.newInstance();
        return extraParams;
    }
    
    @Override
    public void apply(EntityLocal entity) {
        sensor = newSensor(entity);
        ((EntityInternal) entity).getMutableEntityType().addSensor(sensor);
    }

    // old names, for XML deserializaton compatiblity
    private final String type;
    private ConfigBag params;
    private Object readResolve() {
        try {
            if (type!=null) {
                if (targetType==null) {
                    Field f = Reflections.findField(getClass(), "targetType");
                    f.setAccessible(true);
                    f.set(this, type);
                } else if (!targetType.equals(type)) {
                    throw new IllegalStateException("Incompatible target types found for "+this+": "+type+" vs "+targetType);
                }
                        
                Field f = Reflections.findField(getClass(), "type");
                f.setAccessible(true);
                f.set(this, null);
            }
            
            if (params!=null) {
                if (extraParams==null) {
                    extraParams = params;
                } else if (!extraParams.getAllConfigAsConfigKeyMap().equals(params.getAllConfigAsConfigKeyMap())) {
                    throw new IllegalStateException("Incompatible extra params found for "+this+": "+params+" vs "+extraParams);
                }
                params = null;
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
        return this;
    }
    
    private Object writeReplace() {
        try {
            // make this null if there's nothing
            if (extraParams!=null && extraParams.isEmpty()) {
                Field f = Reflections.findField(getClass(), "extraParams");
                f.setAccessible(true);
                f.set(this, null);
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
        return this;
    }
    
    private AttributeSensor<T> newSensor(Entity entity) {
        String className = getFullClassName(targetType);
        Class<T> clazz = getType(entity, className);
        return Sensors.newSensor(clazz, name);
    }

    @SuppressWarnings("unchecked")
    protected Class<T> getType(Entity entity, String className) {
        try {
            // TODO use OSGi loader (low priority however); also ensure that allows primitives
            Maybe<Class<?>> primitive = Boxing.getPrimitiveType(className);
            if (primitive.isPresent()) return (Class<T>) primitive.get();
            
            return (Class<T>) new ClassLoaderUtils(this, entity).loadClass(className);
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

    protected String getFullClassName(String className) {
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
