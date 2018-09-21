/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.core.config;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectPredicate;
import org.apache.brooklyn.core.objs.ConstraintSerialization;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

/**
 * Checks configuration constraints on entities and their adjuncts.
 *
 * @since 0.9.0
 */
public abstract class ConfigConstraints<T extends BrooklynObject> {

    public static final Logger LOG = LoggerFactory.getLogger(ConfigConstraints.class);

    private final T brooklynObject;

    /**
     * Checks all constraints of all config keys available to an entity.
     * <p>
     * If a constraint is a {@link BrooklynObjectPredicate} then
     * {@link BrooklynObjectPredicate#apply(Object, BrooklynObject)} will be used.
     */
    public static void assertValid(Entity entity) {
        Iterable<ConfigKey<?>> violations = new EntityConfigConstraints(entity).getViolations();
        if (!Iterables.isEmpty(violations)) {
            throw new ConstraintViolationException(errorMessage(entity, violations));
        }
    }

    /**
     * Checks all constraints of all config keys available to an entity adjunct.
     * <p>
     * If a constraint is a {@link BrooklynObjectPredicate} then
     * {@link BrooklynObjectPredicate#apply(Object, BrooklynObject)} will be used.
     */
    public static void assertValid(EntityAdjunct adjunct) {
        Iterable<ConfigKey<?>> violations = new EntityAdjunctConstraints(adjunct).getViolations();
        if (!Iterables.isEmpty(violations)) {
            throw new ConstraintViolationException(errorMessage(adjunct, violations));
        }
    }

    public static <T> void assertValid(Entity entity, ConfigKey<T> key, T value) {
        assertValid(new EntityConfigConstraints(entity), entity, key, value);
    }

    public static <T> void assertValid(Location location, ConfigKey<T> key, T value) {
        assertValid(new LocationConfigConstraints(location), location, key, value);
    }
    
    private static <T> void assertValid(ConfigConstraints<?> constrants, Object context, ConfigKey<T> key, T value) {
        ReferenceWithError<Predicate<?>> validity = constrants.validateValue(key, value);
        if (validity.hasError()) {
            throw new ConstraintViolationException("Invalid value for " + key + " on " + context + " (" + value + "); it should satisfy "+validity.getWithoutError());
        }
    }

    private static String errorMessage(BrooklynObject object, Iterable<ConfigKey<?>> violations) {
        StringBuilder message = new StringBuilder("Error configuring ")
                .append(object.getDisplayName())
                .append(": [");
        Iterator<ConfigKey<?>> it = violations.iterator();
        while (it.hasNext()) {
            ConfigKey<?> config = it.next();
            message.append(config.getName())
                    .append(":")
                    .append(config.getConstraint());
            if (it.hasNext()) {
                message.append(", ");
            }
        }
        return message.append("]").toString();
    }

    public ConfigConstraints(T brooklynObject) {
        this.brooklynObject = brooklynObject;
    }

    abstract Iterable<ConfigKey<?>> getBrooklynObjectTypeConfigKeys();

    public Iterable<ConfigKey<?>> getViolations() {
        ExecutionContext exec = 
            getBrooklynObject() instanceof EntityInternal ? ((EntityInternal)getBrooklynObject()).getExecutionContext() :
            getBrooklynObject() instanceof AbstractEntityAdjunct ? ((AbstractEntityAdjunct)getBrooklynObject()).getExecutionContext() :
            null;
        if (exec!=null) {
            return exec.get(
                Tasks.<Iterable<ConfigKey<?>>>builder().dynamic(false).displayName("Validating config").body(
                    () -> validateAll() ).build() );
        } else {
            return validateAll();
        }
    }

    @SuppressWarnings("unchecked")
    protected Iterable<ConfigKey<?>> validateAll() {
        List<ConfigKey<?>> violating = Lists.newLinkedList();
        Iterable<ConfigKey<?>> configKeys = getBrooklynObjectTypeConfigKeys();
        LOG.trace("Checking config keys on {}: {}", getBrooklynObject(), configKeys);
        for (ConfigKey<?> configKey : configKeys) {
            BrooklynObjectInternal.ConfigurationSupportInternal configInternal = getConfigurationSupportInternal();
            // getNonBlocking method coerces the value to the config key's type.
            Maybe<?> maybeValue = configInternal.getNonBlocking(configKey);
            if (maybeValue.isPresent()) {
                // Cast is safe because the author of the constraint on the config key had to
                // keep its type to Predicte<? super T>, where T is ConfigKey<T>.
                ConfigKey<Object> ck = (ConfigKey<Object>) configKey;
                if (!isValueValid(ck, maybeValue.get())) {
                    violating.add(configKey);
                }
            }
        }
        return violating;
    }

    <V> boolean isValueValid(ConfigKey<V> configKey, V value) {
        return !validateValue(configKey, value).hasError();
    }
    
    /** returns reference to null without error if valid; otherwise returns reference to predicate and a good error message */
    @SuppressWarnings("unchecked")
    <V> ReferenceWithError<Predicate<?>> validateValue(ConfigKey<V> configKey, V value) {
        try {
            Predicate<? super V> po = configKey.getConstraint();
            boolean valid;
            if (po instanceof BrooklynObjectPredicate) {
                valid = BrooklynObjectPredicate.class.cast(po).apply(value, brooklynObject);
            } else {
                valid = po.apply(value);
            }
            if (!valid) {
                return ReferenceWithError.newInstanceThrowingError(po, new IllegalStateException("Invalid value for " + configKey.getName() + ": " + value));
            }
        } catch (Exception e) {
            LOG.debug("Error checking constraint on " + configKey.getName(), e);
        }
        return ReferenceWithError.newInstanceWithoutError(null);
    }

    private BrooklynObjectInternal.ConfigurationSupportInternal getConfigurationSupportInternal() {
        return ((BrooklynObjectInternal) brooklynObject).config();
    }

    protected T getBrooklynObject() {
        return brooklynObject;
    }

    /**
     * Convenience method to get the serialization routines.
     */
    public static ConstraintSerialization serialization() {
        return ConstraintSerialization.INSTANCE;
    }
    
    private static class EntityConfigConstraints extends ConfigConstraints<Entity> {
        public EntityConfigConstraints(Entity brooklynObject) {
            super(brooklynObject);
        }

        @Override
        Iterable<ConfigKey<?>> getBrooklynObjectTypeConfigKeys() {
            return getBrooklynObject().getEntityType().getConfigKeys();
        }
    }

    private static class EntityAdjunctConstraints extends ConfigConstraints<EntityAdjunct> {
        public EntityAdjunctConstraints(EntityAdjunct brooklynObject) {
            super(brooklynObject);
        }

        @Override
        Iterable<ConfigKey<?>> getBrooklynObjectTypeConfigKeys() {
            return ((AbstractEntityAdjunct) getBrooklynObject()).getAdjunctType().getConfigKeys();
        }
    }

    private static class LocationConfigConstraints extends ConfigConstraints<Location> {
        public LocationConfigConstraints(Location brooklynObject) {
            super(brooklynObject);
        }

        @Override
        Iterable<ConfigKey<?>> getBrooklynObjectTypeConfigKeys() {
            return Collections.emptyList();
        }
    }

    public static <T> Predicate<T> required() {
        return new RequiredPredicate<T>();
    }
    
    /** Predicate indicating a field is required:  it must not be null and if a string it must not be empty */
    public static class RequiredPredicate<T> implements Predicate<T> {
        @Override
        public boolean apply(T input) {
            if (input==null) return false;
            if (input instanceof CharSequence && ((CharSequence)input).length()==0) return false;
            return true;
        }
        @Override
        public String toString() {
            return "required()";
        }
    }
    
    private static abstract class OtherKeyPredicate implements BrooklynObjectPredicate<Object> {
        private String otherKeyName;

        public OtherKeyPredicate(String otherKeyName) {
            this.otherKeyName = otherKeyName;
        }

        public abstract String predicateName();
        
        @Override
        public String toString() {
            return predicateName()+"("+JavaStringEscapes.wrapJavaString(otherKeyName)+")";
        }
        
        @Override
        public boolean apply(Object input) {
            return apply(input, BrooklynTaskTags.getContextEntity(Tasks.current()));
        }

        @Override
        public boolean apply(Object input, BrooklynObject context) {
            if (context==null) return true;
            // would be nice to offer an explanation, but that will need a richer API or a thread local
            return test(input, context.config().get(ConfigKeys.newConfigKey(Object.class, otherKeyName)));
        }
        
        public abstract boolean test(Object thisValue, Object otherValue);
        
    }
    
    public static Predicate<Object> forbiddenIf(String otherKeyName) { return new ForbiddenIfPredicate(otherKeyName); }
    public static class ForbiddenIfPredicate extends OtherKeyPredicate {
        public ForbiddenIfPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "forbiddenIf"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue==null) || (otherValue==null);
        } 
    }
    
    public static Predicate<Object> forbiddenUnless(String otherKeyName) { return new ForbiddenUnlessPredicate(otherKeyName); }
    public static class ForbiddenUnlessPredicate extends OtherKeyPredicate {
        public ForbiddenUnlessPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "forbiddenUnless"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue==null) || (otherValue!=null);
        } 
    }
    
    public static Predicate<Object> requiredIf(String otherKeyName) { return new RequiredIfPredicate(otherKeyName); }
    public static class RequiredIfPredicate extends OtherKeyPredicate {
        public RequiredIfPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "requiredIf"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue!=null) || (otherValue==null);
        } 
    }
    
    public static Predicate<Object> requiredUnless(String otherKeyName) { return new RequiredUnlessPredicate(otherKeyName); }
    public static class RequiredUnlessPredicate extends OtherKeyPredicate {
        public RequiredUnlessPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "requiredUnless"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue!=null) || (otherValue!=null);
        } 
    }
    
}
