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
package org.apache.brooklyn.core.objs;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.enricher.EnricherDynamicType;
import org.apache.brooklyn.core.entity.EntityDynamicType;
import org.apache.brooklyn.core.policy.PolicyDynamicType;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

public class BrooklynTypes {

    private static class ImmutableEntityType extends EntityDynamicType {
        public ImmutableEntityType(Class<? extends Entity> clazz) {
            super(clazz);
        }
        @Override
        public void setName(String name) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addSensor(Sensor<?> newSensor) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addSensorIfAbsent(Sensor<?> newSensor) {
            throw new UnsupportedOperationException();
        }
        @Override
        public Sensor<?> addSensorIfAbsentWithoutPublishing(Sensor<?> newSensor) {
            throw new UnsupportedOperationException();
        }
        @Override
        public void addSensors(Iterable<? extends Sensor<?>> newSensors) {
            throw new UnsupportedOperationException();
        }
        @Override
        public boolean removeSensor(Sensor<?> sensor) {
            throw new UnsupportedOperationException();
        }
        @Override
        public Sensor<?> removeSensor(String sensorName) {
            throw new UnsupportedOperationException();
        }
    }
    
    private static class DefinedBrooklynTypeLoader implements Callable<BrooklynDynamicType<?, ?>> {
        private Class<?> brooklynClass;

        public DefinedBrooklynTypeLoader(Class<?> type) {
            this.brooklynClass = type;
        }

        @SuppressWarnings("unchecked")
        @Override
        public BrooklynDynamicType<?, ?> call() throws Exception {
            if (Entity.class.isAssignableFrom(brooklynClass)) {
                return new ImmutableEntityType((Class<? extends Entity>)brooklynClass);
            } else if (Location.class.isAssignableFrom(brooklynClass)) {
                return new ImmutableEntityType((Class<? extends Entity>)brooklynClass);
            } else if (Policy.class.isAssignableFrom(brooklynClass)) {
                return new PolicyDynamicType((Class<? extends Policy>)brooklynClass); // TODO immutable?
            } else if (Enricher.class.isAssignableFrom(brooklynClass)) {
                return new EnricherDynamicType((Class<? extends Enricher>)brooklynClass); // TODO immutable?
            } else {
                throw new IllegalStateException("Invalid brooklyn type "+brooklynClass);
            }
        }
        
    }
    
    // weakKeys (no softKeys exists) because we need the entry only if there's a loaded class that matches a key;
    // softKeys for the values because we want them around even if no hard references, until memory needs to be reclaimed;
    @SuppressWarnings("rawtypes")
    private static final Cache<Class,BrooklynDynamicType<?,?>> cache = CacheBuilder.newBuilder().weakKeys().softValues().build();
    
    public static EntityDynamicType getDefinedEntityType(Class<? extends Entity> entityClass) {
        return (EntityDynamicType) BrooklynTypes.getDefinedBrooklynType(entityClass);
    }

    public static BrooklynDynamicType<?,?> getDefinedBrooklynType(Class<? extends BrooklynObject> brooklynClass) {
        try {
            return cache.get(brooklynClass, new DefinedBrooklynTypeLoader(brooklynClass));
        } catch (ExecutionException e) {
            throw Exceptions.propagate(e);
        }
    }

    public static Map<String, ConfigKey<?>> getDefinedConfigKeys(Class<? extends BrooklynObject> brooklynClass) {
        return getDefinedBrooklynType(brooklynClass).getConfigKeys();
    }
    
    @SuppressWarnings("unchecked")
    public static Map<String, ConfigKey<?>> getDefinedConfigKeys(String brooklynTypeName) {
        try {
            return getDefinedConfigKeys((Class<? extends BrooklynObject>) new ClassLoaderUtils(BrooklynTypes.class).loadClass(brooklynTypeName));
        } catch (ClassNotFoundException e) {
            throw Exceptions.propagate(e);
        }
    }
    
    public static Map<String, Sensor<?>> getDefinedSensors(Class<? extends Entity> entityClass) {
        return getDefinedEntityType(entityClass).getSensors();
    }
    
    @SuppressWarnings("unchecked")
    public static Map<String, Sensor<?>> getDefinedSensors(String entityTypeName) {
        try {
            return getDefinedSensors((Class<? extends Entity>) new ClassLoaderUtils(BrooklynTypes.class).loadClass(entityTypeName));
        } catch (ClassNotFoundException e) {
            throw Exceptions.propagate(e);
        }
    }

}
