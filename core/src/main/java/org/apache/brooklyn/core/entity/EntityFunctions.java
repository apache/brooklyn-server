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
package org.apache.brooklyn.core.entity;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.Identifiable;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Functionals;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.Iterables;

public class EntityFunctions {

    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static <T> Function<Entity, T> attributeOld(final AttributeSensor<T> attribute) {
        // TODO PERSISTENCE WORKAROUND
        class GetEntityAttributeFunction implements Function<Entity, T> {
            @Override public T apply(Entity input) {
                return (input == null) ? null : input.getAttribute(attribute);
            }
        }
        return new GetEntityAttributeFunction();
    }
    
    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static <T> Function<Entity, T> configOld(final ConfigKey<T> key) {
        // TODO PERSISTENCE WORKAROUND
        class GetEntityConfigFunction implements Function<Entity, T> {
            @Override public T apply(Entity input) {
                return (input == null) ? null : input.getConfig(key);
            }
        }
        return new GetEntityConfigFunction();
    }
    
    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static Function<Entity, String> displayNameOld() {
        // TODO PERSISTENCE WORKAROUND
        class GetEntityDisplayName implements Function<Entity, String> {
            @Override public String apply(Entity input) {
                return (input == null) ? null : input.getDisplayName();
            }
        }
        return new GetEntityDisplayName();
    }
    
    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static Function<Identifiable, String> idOld() {
        // TODO PERSISTENCE WORKAROUND
        class GetIdFunction implements Function<Identifiable, String> {
            @Override public String apply(Identifiable input) {
                return (input == null) ? null : input.getId();
            }
        }
        return new GetIdFunction();
    }

    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static Function<Entity,Void> settingSensorsConstantOld(final Map<AttributeSensor<?>,Object> values) {
        // TODO PERSISTENCE WORKAROUND
        class SettingSensorsConstantFunction implements Function<Entity, Void> {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            @Override public Void apply(Entity input) {
                for (Map.Entry<AttributeSensor<?>,Object> entry : values.entrySet()) {
                    AttributeSensor sensor = (AttributeSensor)entry.getKey();
                    Object value = entry.getValue();
                    if (value==Entities.UNCHANGED) {
                        // nothing
                    } else if (value==Entities.REMOVE) {
                        ((EntityInternal)input).removeAttribute(sensor);
                    } else {
                        value = TypeCoercions.coerce(value, sensor.getTypeToken());
                        ((EntityInternal)input).sensors().set(sensor, value);
                    }
                }
                return null;
            }
        }
        return new SettingSensorsConstantFunction();
    }

    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static <K,V> Function<Entity, Void> updatingSensorMapEntryOld(final AttributeSensor<Map<K,V>> mapSensor, final K key, final Supplier<? extends V> valueSupplier) {
        // TODO PERSISTENCE WORKAROUND
        class UpdatingSensorMapEntryFunction implements Function<Entity, Void> {
            @Override public Void apply(Entity input) {
                ServiceStateLogic.updateMapSensorEntry((EntityLocal)input, mapSensor, key, valueSupplier.get());
                return null;
            }
        }
        return new UpdatingSensorMapEntryFunction();
    }

    /** @deprecated since 0.9.0 kept only to allow conversion of non-static inner classes */
    @SuppressWarnings("unused") @Deprecated 
    private static Supplier<Collection<Application>> applicationsOld(final ManagementContext mgmt) {
        // TODO PERSISTENCE WORKAROUND
        class AppsSupplier implements Supplier<Collection<Application>> {
            @Override
            public Collection<Application> get() {
                return mgmt.getApplications();
            }
        }
        return new AppsSupplier();
    }

    public static Function<Entity, Object> attribute(String attributeName) {
        return attribute(Sensors.newSensor(Object.class, attributeName));
    }

    public static <T> Function<Entity, T> attribute(AttributeSensor<T> attribute) {
        return new GetEntityAttributeFunction<T>(checkNotNull(attribute, "attribute"));
    }

    protected static class GetEntityAttributeFunction<T> implements Function<Entity, T> {
        private final AttributeSensor<T> attribute;
        protected GetEntityAttributeFunction(AttributeSensor<T> attribute) {
            this.attribute = attribute;
        }
        @Override public T apply(Entity input) {
            return (input == null) ? null : input.getAttribute(attribute);
        }
    }

    public static Function<Entity, String> attribute(String attributeName, String format) {
        return attribute(Sensors.newSensor(Object.class, attributeName), format);
    }

    public static Function<Entity, String> attribute(AttributeSensor<?> attribute, String format) {
        return new FormatEntityAttributeFunction(checkNotNull(attribute, "attribute"), checkNotNull(format, "format"));
    }

    protected static class FormatEntityAttributeFunction implements Function<Entity, String> {
        private final AttributeSensor<?> attribute;
        private final String format;
        protected FormatEntityAttributeFunction(AttributeSensor<?> attribute, String format) {
            this.attribute = attribute;
            this.format = format;
        }
        @Override public String apply(Entity input) {
            return (input == null) ? null : String.format(format, input.getAttribute(attribute));
        }
    }

    public static <T> Function<Object, T> attribute(Entity entity, AttributeSensor<T> attribute) {
        return new GetFixedEntityAttributeFunction<>(entity, attribute);
    }

    protected static class GetFixedEntityAttributeFunction<T> implements Function<Object, T> {
        private final Entity entity;
        private final AttributeSensor<T> attribute;
        protected GetFixedEntityAttributeFunction(Entity entity, AttributeSensor<T> attribute) {
            this.entity = entity;
            this.attribute = attribute;
        }
        @Override public T apply(Object input) {
            return entity.getAttribute(attribute);
        }
    }

    public static Function<Entity, Object> config(String keyName) {
        return config(ConfigKeys.newConfigKey(Object.class, keyName));
    }

    public static <T> Function<Entity, T> config(ConfigKey<T> key) {
        return new GetEntityConfigFunction<T>(checkNotNull(key, "key"));
    }

    protected static class GetEntityConfigFunction<T> implements Function<Entity, T> {
        private final ConfigKey<T> key;

        protected GetEntityConfigFunction(ConfigKey<T> key) {
            this.key = key;
        }

        @Override public T apply(Entity input) {
            return (input == null) ? null : input.getConfig(key);
        }
    }

    public static Function<Entity, String> config(String keyName, String format) {
        return config(ConfigKeys.newConfigKey(Object.class, keyName), format);
    }

    public static Function<Entity, String> config(ConfigKey<?> key, String format) {
        return new FormatEntityConfigFunction(checkNotNull(key, "key"), checkNotNull(format, "format"));
    }

    protected static class FormatEntityConfigFunction implements Function<Entity, String> {
        private final ConfigKey<?> key;
        private final String format;

        protected FormatEntityConfigFunction(ConfigKey<?> key, String format) {
            this.key = key;
            this.format = format;
        }

        @Override public String apply(Entity input) {
            return (input == null) ? null : String.format(format, input.getConfig(key));
        }
    }

    public static Function<Entity, String> displayName() {
        return GetEntityDisplayName.INSTANCE;
    }

    protected static class GetEntityDisplayName implements Function<Entity, String> {
        public static final GetEntityDisplayName INSTANCE = new GetEntityDisplayName();
        @Override public String apply(Entity input) {
            return (input == null) ? null : input.getDisplayName();
        }
    }

    public static Function<Identifiable, String> id() {
        return GetIdFunction.INSTANCE;
    }
    
    protected static class GetIdFunction implements Function<Identifiable, String> {
        public static final GetIdFunction INSTANCE = new GetIdFunction();
        @Override public String apply(Identifiable input) {
            return (input == null) ? null : input.getId();
        }
    }


    /** returns a function which sets the given sensors on the entity passed in,
     * with {@link Entities#UNCHANGED} and {@link Entities#REMOVE} doing those actions. */
    public static Function<Entity,Void> settingSensorsConstant(final Map<AttributeSensor<?>,Object> values) {
        return new SettingSensorsConstantFunction(checkNotNull(values, "values"));
    }

    protected static class SettingSensorsConstantFunction implements Function<Entity, Void> {
        private final Map<AttributeSensor<?>, Object> values;

        protected SettingSensorsConstantFunction(Map<AttributeSensor<?>, Object> values) {
            this.values = values;
        }
        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override public Void apply(Entity input) {
            for (Map.Entry<AttributeSensor<?>,Object> entry : values.entrySet()) {
                AttributeSensor sensor = (AttributeSensor)entry.getKey();
                Object value = entry.getValue();
                if (value==Entities.UNCHANGED) {
                    // nothing
                } else if (value==Entities.REMOVE) {
                    ((EntityInternal)input).sensors().remove(sensor);
                } else {
                    value = TypeCoercions.coerce(value, sensor.getTypeToken());
                    ((EntityInternal)input).sensors().set(sensor, value);
                }
            }
            return null;
        }
    }

    /** as {@link #settingSensorsConstant(Map)} but as a {@link Runnable} */
    public static Runnable settingSensorsConstant(final Entity entity, final Map<AttributeSensor<?>,Object> values) {
        checkNotNull(entity, "entity");
        checkNotNull(values, "values");
        return Functionals.runnable(Suppliers.compose(settingSensorsConstant(values), Suppliers.ofInstance(entity)));
    }

    public static <K,V> Function<Entity, Void> updatingSensorMapEntry(final AttributeSensor<Map<K,V>> mapSensor, final K key, final Supplier<? extends V> valueSupplier) {
        return new UpdatingSensorMapEntryFunction<K,V>(mapSensor, key, valueSupplier);
    }
    
    protected static class UpdatingSensorMapEntryFunction<K, V> implements Function<Entity, Void> {
        private final AttributeSensor<Map<K, V>> mapSensor;
        private final K key;
        private final Supplier<? extends V> valueSupplier;

        public UpdatingSensorMapEntryFunction(AttributeSensor<Map<K, V>> mapSensor, K key, Supplier<? extends V> valueSupplier) {
            this.mapSensor = mapSensor;
            this.key = key;
            this.valueSupplier = valueSupplier;
        }
        @Override public Void apply(Entity input) {
            ServiceStateLogic.updateMapSensorEntry((EntityLocal)input, mapSensor, key, valueSupplier.get());
            return null;
        }
    }

    public static <K,V> Runnable updatingSensorMapEntry(final Entity entity, final AttributeSensor<Map<K,V>> mapSensor, final K key, final Supplier<? extends V> valueSupplier) {
        return Functionals.runnable(Suppliers.compose(updatingSensorMapEntry(mapSensor, key, valueSupplier), Suppliers.ofInstance(entity)));
    }

    public static Supplier<Collection<Application>> applications(ManagementContext mgmt) {
        return new AppsSupplier(checkNotNull(mgmt, "mgmt"));
    }
    
    protected static class AppsSupplier implements Supplier<Collection<Application>> {
        private final ManagementContext mgmt;

        public AppsSupplier(ManagementContext mgmt) {
            this.mgmt = mgmt;
        }
        @Override
        public Collection<Application> get() {
            return mgmt.getApplications();
        }
    }

    public static Function<Entity, Location> locationMatching(Predicate<? super Location> filter) {
        return new LocationMatching(filter);
    }
    
    private static class LocationMatching implements Function<Entity, Location> {
        private Predicate<? super Location> filter;
        
        @SuppressWarnings("unused")
        private LocationMatching() { /* for xstream */ 
        }
        public LocationMatching(Predicate<? super Location> filter) {
            this.filter = filter;
        }
        @Override public Location apply(Entity input) {
            return Iterables.find(input.getLocations(), filter);
        }
    }
    
    public static Function<Entity, Entity> parent() {
        return new EntityParent();
    }
    
    private static class EntityParent implements Function<Entity, Entity> {
        @Override public Entity apply(Entity input) {
            return input==null ? null : input.getParent();
        }
    }

    /** Returns a function that finds the best match for the given config key on an entity */
    public static <T> Function<Entity, ConfigKey<T>> configKeyFinder(ConfigKey<T> queryKey, @Nullable ConfigKey<T> defaultValue) {
        return new EntityKeyFinder<T>(queryKey, defaultValue);
    }

    /** As {@link #configKeyFinder(ConfigKey,ConfigKey)} using the query key as the default value */
    public static <T> Function<Entity, ConfigKey<T>> configKeyFinder(ConfigKey<T> queryKey) {
        return new EntityKeyFinder<T>(queryKey, queryKey);
    }

    private static class EntityKeyFinder<T> implements Function<Entity, ConfigKey<T>> {
        private ConfigKey<T> queryKey;
        private ConfigKey<T> defaultValue;

        @SuppressWarnings("unused")
        private EntityKeyFinder() { /* for xstream */ }
        public EntityKeyFinder(ConfigKey<T> queryKey, ConfigKey<T> defaultValue) {
            this.queryKey = queryKey;
            this.defaultValue = defaultValue;
        }

        @SuppressWarnings("unchecked")
        @Override public ConfigKey<T> apply(Entity entity) {
            return entity!=null ? (ConfigKey<T>)elvis(entity.getEntityType().getConfigKey(queryKey.getName()), defaultValue) : defaultValue;
        }
    }

}
