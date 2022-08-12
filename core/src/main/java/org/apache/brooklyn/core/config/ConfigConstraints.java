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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
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
import org.apache.brooklyn.core.validation.BrooklynValidation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Checks configuration constraints on entities and their adjuncts.
 *
 * @since 0.9.0
 */
public abstract class ConfigConstraints<T> {

    public static final Logger LOG = LoggerFactory.getLogger(ConfigConstraints.class);

    private final T source;

    /**
     * Checks all constraints of all config keys available to an entity.
     * <p>
     * If a constraint is a {@link BrooklynObjectPredicate} then
     * {@link BrooklynObjectPredicate#apply(Object, BrooklynObject)} will be used.
     */
    public static void assertValid(Entity entity) {
        new EntityConfigConstraints(entity).assertValid();
    }

    /**
     * Checks all constraints of all config keys available to an entity adjunct.
     * <p>
     * If a constraint is a {@link BrooklynObjectPredicate} then
     * {@link BrooklynObjectPredicate#apply(Object, BrooklynObject)} will be used.
     */
    public static void assertValid(EntityAdjunct adjunct) {
        new EntityAdjunctConstraints(adjunct).assertValid();
    }

    public static <T> void assertValid(Entity entity, ConfigKey<T> key, T value) {
        assertValid(new EntityConfigConstraints(entity), entity, key, value);
    }

    public static <T> void assertValid(EntityAdjunct adj, ConfigKey<T> key, T value) {
        assertValid(new EntityAdjunctConstraints(adj), adj, key, value);
    }

    public static <T> void assertValid(Location location, ConfigKey<T> key, T value) {
        assertValid(new LocationConfigConstraints(location), location, key, value);
    }
    
    private static <T> void assertValid(ConfigConstraints<?> constrants, Object context, ConfigKey<T> key, T value) {
        try {
            ReferenceWithError<?> validity = constrants.validateValue(key, value);
            if (validity.hasError()) {
                // messages now handled by the CVE class
//                String msg = "Invalid value for " + key + " on " + context + " (" + value + ")";
//                if (validity.getWithoutError() != null) {
//                    throw new ConstraintViolationException(Exceptions.collapseText(validity.getError()) + ": " + msg + "; it should satisfy " + validity.getWithoutError());
//                } else {
//                    throw new ConstraintViolationException(msg, validity.getError());
//                }
                throw validity.getError();
            }
        } catch (Throwable e) {
            Exceptions.propagateIfFatal(e);
            throw ConstraintViolationException.of(e, context, key, value);
        }
    }

    public ConfigConstraints(T source) {
        this.source = source;
    }

    public void assertValid() {
        Map<ConfigKey<?>, Throwable> violations = getViolationsDetails();
        if (!violations.isEmpty()) {
            throw ConstraintViolationException.CompoundConstraintViolationException.of(getDisplayName(), violations);
        }
    }

    public abstract String getDisplayName();

    public abstract Iterable<ConfigKey<?>> getConfigKeys();

    public abstract Maybe<?> getValue(ConfigKey<?> configKey);

    public abstract @Nullable ExecutionContext getExecutionContext();

    /** @deprecated since 1.1.0 use {@link #getViolationsDetails()} */
    @Deprecated  // not used
    public Iterable<ConfigKey<?>> getViolations() {
        return getViolationsDetails().keySet();
    }

    /** runs blocking in task on context entity if possible, else runs in caller's thread */
    public Map<ConfigKey<?>,Throwable> getViolationsDetails() {
        ExecutionContext exec = getExecutionContext();
        if (exec!=null) {
            return exec.get(
                BrooklynTaskTags.setTransient(   // set transient so gets GC and doesn't pollute the "top-level" view
                    Tasks.<Map<ConfigKey<?>,Throwable>>builder().dynamic(false).displayName("Validating config").body( () -> validateAll() ).build() ));
        } else {
            return validateAll();
        }
    }

    @SuppressWarnings("unchecked")
    protected Map<ConfigKey<?>, Throwable> validateAll() {
        Map<ConfigKey<?>,Throwable> violating = MutableMap.of();
        Iterable<ConfigKey<?>> configKeys = getConfigKeys();
        LOG.trace("Checking config keys on {}: {}", getSource(), configKeys);
        for (ConfigKey<?> configKey : configKeys) {
            try {
                Maybe<?> maybeValue = getValue(configKey);
                if (maybeValue.isPresent()) {
                    // Cast is safe because the author of the constraint on the config key had to
                    // keep its type to Predicate<? super T>, where T is ConfigKey<T>.
                    ReferenceWithError<?> validation = validateValue((ConfigKey<Object>) configKey, maybeValue.get());
                    if (validation.hasError()) {
                        violating.put(configKey, validation.getError());
                    }
                } else {
                    // absent means did not resolve in time or not coercible;
                    // code will return `Maybe.of(null)` if it is unset,
                    // and coercion errors are handled when the value is _set_ or _needed_
                    // (this allows us to deal with edge cases where we can't *immediately* coerce)
                }
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                violating.put(configKey, e);
            }
        }
        return violating;
    }

    <V> boolean isValueValid(ConfigKey<V> configKey, V value) {
        return !validateValue(configKey, value).hasError();
    }
    
    /** returns reference to null without error if valid; otherwise should
     * contain an error whose message can be displayed at least one, and ideally a value whose toString is suitable for describing the requirement (eg the predicate).
     * <p>
     * this does not return the value, i.e. this is not suitable for use in a fluent api.
     * </p>
     * <p>
     * this is for running constraints only.
     * if value supplied to this method is not of the type declared by the config key, this returns no error.
     * thus predicates can assume they will only see instances of the type declared by the config key.
     * <p>
     * in particular if it is a {@link DeferredSupplier} or {@link Future} no validation is done.
     * the caller of this method is responsible for ensuring it is coerced and reporting failures if it is not of the right type.
     * (those are not considered "constraint" violations so not part of this method -- they are a different class of violation.)
     * <p>
     * used to validate single constraints and multiple constraints.
     * </p>
     */
    @SuppressWarnings("unchecked")
    <V> ReferenceWithError<?> validateValue(ConfigKey<V> configKey, V value) {

        // previously we ignored problems if validation threw an error; now we skip the check altogether if the type is wrong (future, or coercible),
        // but report an error if the predicate throws one

        Predicate<? super V> po = null;
        if (value!=null) {
            if (configKey.getType()==Object.class) {
                if ((value instanceof Future || value instanceof DeferredSupplier)) {
                    return ReferenceWithError.newInstanceWithoutError(null);
                }
            } else {
                if (!configKey.getType().isInstance(value)) {
                    return ReferenceWithError.newInstanceWithoutError(null);
                }
            }
        }
        try {
            po = configKey.getConstraint();
            boolean valid;
            if (getSource() instanceof BrooklynObject && po instanceof BrooklynObjectPredicate) {
                valid = BrooklynObjectPredicate.class.cast(po).apply(value, (BrooklynObject) getSource());
            } else {
                if (Objects.isNull(value)
                        && !po.toString().contains("required")
                        && !po.toString().contains("Predicates.notNull()")) {
                    // Skip validation if config key is optional and not supplied.
                    return ReferenceWithError.newInstanceWithoutError(null);
                }
                valid = po.apply(value);
            }
            if (!valid) {
                throw new IllegalArgumentException("Constraint '"+po+"' not satisfied");
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return ReferenceWithError.newInstanceThrowingError(po, ConstraintViolationException.of(e, getSource(), configKey, value));
        }

        try {
            BrooklynValidation.getInstance().validateIfPresent(value);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            return ReferenceWithError.newInstanceThrowingError(null, new IllegalArgumentException("Invalid value for " + configKey.getName() + ": " + value + "; " + Exceptions.collapseText(e), e));
        }
        return ReferenceWithError.newInstanceWithoutError(null);
    }

    protected T getSource() {
        return source;
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
        public String getDisplayName() {
            return getSource().getDisplayName();
        }

        @Override
        public Iterable<ConfigKey<?>> getConfigKeys() {
            return getSource().getEntityType().getConfigKeys();
        }

        @Override
        public Maybe<?> getValue(ConfigKey<?> configKey) {
            // getNonBlocking method coerces the value to the config key's type.
            return ((BrooklynObjectInternal)getSource()).config().getNonBlocking(configKey, false);
        }

        @Nullable
        @Override
        public ExecutionContext getExecutionContext() {
            return ((EntityInternal)getSource()).getExecutionContext();

        }
    }

    private static class EntityAdjunctConstraints extends ConfigConstraints<EntityAdjunct> {
        public EntityAdjunctConstraints(EntityAdjunct brooklynObject) {
            super(brooklynObject);
        }

        @Override
        public String getDisplayName() {
            return getSource().getDisplayName();
        }

        @Override
        public Iterable<ConfigKey<?>> getConfigKeys() {
            return ((AbstractEntityAdjunct) getSource()).getAdjunctType().getConfigKeys();
        }

        @Override
        public Maybe<?> getValue(ConfigKey<?> configKey) {
            // getNonBlocking method coerces the value to the config key's type.
            return ((BrooklynObjectInternal)getSource()).config().getNonBlocking(configKey, false);
        }

        @Nullable
        @Override
        public ExecutionContext getExecutionContext() {
            return getSource() instanceof AbstractEntityAdjunct ? ((AbstractEntityAdjunct)getSource()).getExecutionContext() : null;
        }
    }

    private static class LocationConfigConstraints extends ConfigConstraints<Location> {
        public LocationConfigConstraints(Location brooklynObject) {
            super(brooklynObject);
        }

        @Override
        public String getDisplayName() {
            return getSource().getDisplayName();
        }

        @Override
        public Iterable<ConfigKey<?>> getConfigKeys() {
            return Collections.emptyList();
        }

        @Override
        public Maybe<?> getValue(ConfigKey<?> configKey) {
            // getNonBlocking method coerces the value to the config key's type.
            return ((BrooklynObjectInternal)getSource()).config().getNonBlocking(configKey, false);
        }

        @Nullable
        @Override
        public ExecutionContext getExecutionContext() {
            return null;
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
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof RequiredPredicate) && obj.getClass().equals(getClass());
        }
        @Override
        public int hashCode() {
            return Objects.hash(toString());
        }
    }
    
    private static abstract class OtherKeyPredicate extends OtherKeysPredicate {
        public OtherKeyPredicate(String otherKeyName) {
            super(ImmutableList.of(otherKeyName));
        }

        @Override
        public boolean test(Object thisValue, List<Object> otherValues) {
            return test(thisValue, Iterables.getOnlyElement(otherValues));
        }

        public abstract boolean test(Object thisValue, Object otherValue);
    }
    
    private static abstract class OtherKeysPredicate implements BrooklynObjectPredicate<Object> {
        private final List<String> otherKeyNames;

        public OtherKeysPredicate(List<String> otherKeyNames) {
            this.otherKeyNames = otherKeyNames;
        }

        public abstract String predicateName();
        
        @Override
        public String toString() {
            String params = otherKeyNames.stream()
                    .map(k -> JavaStringEscapes.wrapJavaString(k))
                    .collect(Collectors.joining(", "));
            return predicateName()+"("+params+")";
        }
        
        @Override
        public boolean apply(Object input) {
            return apply(input, BrooklynTaskTags.getContextEntity(Tasks.current()));
        }

        @Override
        public boolean apply(Object input, BrooklynObject context) {
            if (context==null) return true;
            // would be nice to offer an explanation, but that will need a richer API or a thread local
            List<Object> vals = new ArrayList<>();
            for (String otherKeyName : otherKeyNames) {
                // Use getNonBlocking, in case the value is an `attributeWhenReady` and the 
                // attribute is not yet available - don't want to hang.
                ConfigKey<Object> otherKey = ConfigKeys.newConfigKey(Object.class, otherKeyName);
                BrooklynObjectInternal.ConfigurationSupportInternal configInternal = ((BrooklynObjectInternal) context).config();
                
                Maybe<?> maybeValue = configInternal.getNonBlocking(otherKey, false);
                if (maybeValue.isPresent()) {
                    vals.add(maybeValue.get());
                } else {
                    return true; // abort; assume valid
                }
            }
            return test(input, vals);
        }
        
        public abstract boolean test(Object thisValue, List<Object> otherValues);
    }
    
    public static Predicate<Object> forbiddenIf(String otherKeyName) { return new ForbiddenIfPredicate(otherKeyName); }
    protected static class ForbiddenIfPredicate extends OtherKeyPredicate {
        public ForbiddenIfPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "forbiddenIf"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue==null) || (otherValue==null);
        } 
    }
    
    public static Predicate<Object> forbiddenUnless(String otherKeyName) { return new ForbiddenUnlessPredicate(otherKeyName); }
    protected static class ForbiddenUnlessPredicate extends OtherKeyPredicate {
        public ForbiddenUnlessPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "forbiddenUnless"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue==null) || (otherValue!=null);
        } 
    }
    
    public static Predicate<Object> requiredIf(String otherKeyName) { return new RequiredIfPredicate(otherKeyName); }
    protected static class RequiredIfPredicate extends OtherKeyPredicate {
        public RequiredIfPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "requiredIf"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue!=null) || (otherValue==null);
        } 
    }
    
    public static Predicate<Object> requiredUnless(String otherKeyName) { return new RequiredUnlessPredicate(otherKeyName); }
    protected static class RequiredUnlessPredicate extends OtherKeyPredicate {
        public RequiredUnlessPredicate(String otherKeyName) { super(otherKeyName); }
        @Override public String predicateName() { return "requiredUnless"; }
        @Override public boolean test(Object thisValue, Object otherValue) { 
            return (thisValue!=null) || (otherValue!=null);
        } 
    }
    
    public static Predicate<Object> forbiddenUnlessAnyOf(List<String> otherKeyNames) { return new ForbiddenUnlessAnyOfPredicate(otherKeyNames); }
    protected static class ForbiddenUnlessAnyOfPredicate extends OtherKeysPredicate {
        public ForbiddenUnlessAnyOfPredicate(List<String> otherKeyNames) { super(otherKeyNames); }
        @Override public String predicateName() { return "forbiddenUnlessAnyOf"; }
        @Override public boolean test(Object thisValue, List<Object> otherValue) {
            return (thisValue==null) || (otherValue!=null && Iterables.tryFind(otherValue, Predicates.notNull()).isPresent());
        } 
    }
    
    public static Predicate<Object> requiredUnlessAnyOf(List<String> otherKeyNames) { return new RequiredUnlessAnyOfPredicate(otherKeyNames); }
    protected static class RequiredUnlessAnyOfPredicate extends OtherKeysPredicate {
        public RequiredUnlessAnyOfPredicate(List<String> otherKeyNames) { super(otherKeyNames); }
        @Override public String predicateName() { return "requiredUnlessAnyOf"; }
        @Override public boolean test(Object thisValue, List<Object> otherValue) { 
            return (thisValue!=null) || (otherValue!=null && Iterables.tryFind(otherValue, Predicates.notNull()).isPresent());
        } 
    }
}
