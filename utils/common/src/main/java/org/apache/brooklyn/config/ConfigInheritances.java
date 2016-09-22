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
package org.apache.brooklyn.config;

import java.util.Iterator;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.config.ConfigInheritance.ConfigInheritanceContext;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;

public class ConfigInheritances {
    
    /** 
     * given a key and local value, together with an optional record of ancestor containers (eg an entity) and associated data,
     * this finds the value for a config key <b>applying the appropriate inheritance strategies</b>.
     * for instance this may merge a map throughout a container hierarchy, 
     * or this may traverse up until a non-reinheritable key definition is found and in the absence of values lower
     * in the hierarchy this will return the default value of the key 
     * <p>
     * this uses an interface on the input so that:
     * - the caller can supply the hierarchy
     * - hierarchy is only traversed as far as needed
     * - caller can do full resolution as required for local values
     * <p>
     * this returns in interface so that caller can get the value, 
     * and if needed also find the container where the key is defined.
     * that can be useful for when the value needs to be further resolved,
     * e.g. a DSL function or a URL. the returned value may be evaluated lazily,
     * i.e. the actual traversal and evaluation may be deferred until a method on
     * the returned object is invoked.
     * <p>
     * this object is taken as the default inheritance and used if no inheritance is
     * defined on the key.
     * <p>
     * so that the caller can determine if a key/value is to be exported to children from a container,
     * this method should accept an iterable whose first entry has a null container
     * and whose second entry gives the container, key, and potential value to be exported.
     * if null is returned the caller knows nothing is to be exported to children.
     */
    public static <TContainer,TValue> ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resolveInheriting(
            @Nullable TContainer container,
            ConfigKey<TValue> key,
            @Nullable Maybe<TValue> localValue,
            @Nullable Maybe<TValue> defaultValue,
            
            Iterator<? extends ConfigValueAtContainer<TContainer,TValue>> ancestorContainerKeyValues,
            ConfigInheritanceContext context,
            ConfigInheritance defaultInheritance) {
        return resolveInheriting(new BasicConfigValueAtContainer<TContainer,TValue>(container, key, localValue, localValue.isPresent(), defaultValue), 
            ancestorContainerKeyValues, key, context, defaultInheritance);
    }
    
    /** as {@link #resolveInheriting(Object, ConfigKey, Maybe, Iterator, ConfigInheritanceContext, ConfigInheritance)}
     * but convenient when the local info is already in a {@link ConfigValueAtContainer} */
    public static <TContainer,TValue> ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resolveInheriting(
        ConfigValueAtContainer<TContainer,TValue> local,
        Iterator<? extends ConfigValueAtContainer<TContainer,TValue>> ancestorContainerKeyValues,
        ConfigKey<TValue> queryKey,
        ConfigInheritanceContext context,
        ConfigInheritance defaultInheritance) {
                    
        if (ancestorContainerKeyValues.hasNext()) {
            ConfigValueAtContainer<TContainer, TValue> parent = ancestorContainerKeyValues.next();
            ConfigInheritance parentInheritance = findInheritance(parent, context, null);
            if (parentInheritance==null || parentInheritance.isReinheritable(parent, context)) {
                ConfigInheritance currentInheritance = findInheritance(local, context, findInheritance(queryKey, context, defaultInheritance));
                if (currentInheritance.considerParent(local, parent, context)) {
                    ReferenceWithError<ConfigValueAtContainer<TContainer, TValue>> parentResult = resolveInheriting(parent, ancestorContainerKeyValues, queryKey, context, currentInheritance);
                    ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resultWithParent = currentInheritance.resolveWithParent(local, parentResult.getWithoutError(), context);
                    if (resultWithParent!=null && resultWithParent.getWithoutError()!=null && resultWithParent.getWithoutError().isValueExplicitlySet()) {
                        if (!resultWithParent.hasError() && parentResult!=null && parentResult.hasError()) {
                            return ReferenceWithError.newInstanceThrowingError(resultWithParent.getWithoutError(), parentResult.getError());
                        }
                        return resultWithParent;
                    }
                }
            }
        }
        BasicConfigValueAtContainer<TContainer, TValue> result = new BasicConfigValueAtContainer<TContainer, TValue>(local);
        if (!local.isValueExplicitlySet() && local.getDefaultValue().isPresent()) {
            result.value = local.getDefaultValue();
        }
        return ReferenceWithError.newInstanceWithoutError(result);
    }
    
    /** finds the {@link ConfigInheritance} to use based on the given container, or the default if none is present there */
    public static ConfigInheritance findInheritance(ConfigValueAtContainer<?,?> local, ConfigInheritanceContext context, ConfigInheritance defaultInheritance) {
        if (local==null) return defaultInheritance;
        return findInheritance(local.getKey(), context, defaultInheritance);
    }
    public static ConfigInheritance findInheritance(ConfigKey<?> localKey, ConfigInheritanceContext context, ConfigInheritance defaultInheritance) {
        if (localKey==null) return defaultInheritance;
        ConfigInheritance keyInheritance = localKey.getInheritanceByContext(context);
        if (keyInheritance==null) return defaultInheritance;
        return keyInheritance;
    }

    
    public static class BasicConfigValueAtContainer<TContainer,TValue> implements ConfigValueAtContainer<TContainer,TValue> {
        
        @Nullable TContainer container = null;
        @Nonnull Maybe<? extends TValue> value = Maybe.absent();
        boolean valueWasExplicitlySet = false;
        @Nullable ConfigKey<? extends TValue> key = null;
        @Nullable Maybe<TValue> defaultValue = null;
        
        public BasicConfigValueAtContainer() {}
        public BasicConfigValueAtContainer(ConfigValueAtContainer<TContainer,TValue> toCopy) {
            this(toCopy.getContainer(), toCopy.getKey(), toCopy.asMaybe(), toCopy.isValueExplicitlySet(), toCopy.getDefaultValue());
        }
        public BasicConfigValueAtContainer(@Nullable TContainer container, @Nullable ConfigKey<? extends TValue> key, 
            @Nullable Maybe<? extends TValue> value) {
            this(container, key, value, value.isPresent());
        }
        public BasicConfigValueAtContainer(@Nullable TContainer container, @Nullable ConfigKey<? extends TValue> key, @Nullable Maybe<? extends TValue> value, boolean isValueSet) {
            this(container, key, value, isValueSet, null);
        }
        /** Creates an instance, configuring all parameters.
         * 
         * @param container May be null as per contract.
         * @param key May be null as per contract.
         * @param value Null means always to take {@link #getDefaultValue()}; if absent and isValueSet is false, it will also take {@link #getDefaultValue()}.
         * @param isValueSet
         * @param defaultValue Null means to take a default value from the key ({@link #getKey()}), otherwise this {@link Maybe} will be preferred to that value
         * (even if absent).
         */
        public BasicConfigValueAtContainer(@Nullable TContainer container, @Nullable ConfigKey<? extends TValue> key, @Nullable Maybe<? extends TValue> value, boolean isValueSet, @Nullable Maybe<TValue> defaultValue) {
            this.container = container;
            this.key = key;
            this.valueWasExplicitlySet = isValueSet;
            this.defaultValue = defaultValue;
            this.value = value!=null && (value.isPresent() || isValueSet || getDefaultValue().isPresent()) ? value 
                : getDefaultValue();
        }

        @Override public TContainer getContainer() { return container; }
        @Override public TValue get() { return value.orNull(); }
        @Override public Maybe<? extends TValue> asMaybe() { return value; }
        @Override public boolean isValueExplicitlySet() { return valueWasExplicitlySet; }
        @Override public ConfigKey<? extends TValue> getKey() { return key; }
        
        public void setContainer(TContainer container) {
            this.container = container;
        }
        public void setValue(Maybe<? extends TValue> value) {
            this.value = value;
        }
        public void setValueWasExplicitlySet(boolean valueWasExplicitlySet) {
            this.valueWasExplicitlySet = valueWasExplicitlySet;
        }
        public void setKey(ConfigKey<? extends TValue> key) {
            this.key = key;
        }
        
        public Maybe<TValue> getDefaultValue() { 
            if (defaultValue!=null) return defaultValue;
            // explicit absent default value means don't look at key
            return key!=null && key.hasDefaultValue() ? Maybe.ofAllowingNull((TValue) key.getDefaultValue()) : Maybe.<TValue>absent(); 
        }
        
        @Override
        public String toString() {
            return super.toString()+"[key="+key+"; value="+value+"; container="+container+"]";
        }
    }
    
    /** determine whether a key is reinheritable from the point in the given inheritance hierarchy where it is introduced;
     * default is true, but some keys may define not being reinherited or may have that effective result
     * <p>
     * note that this does not mean a value should never be *inherited*; 
     * callers should query with the key defined at a given point in a hierarchy,
     * so if a key is not defined at some point in the hierarchy
     * (eg not on a type in the type hierarchy, or not an an entity in the runtime management hierarchy)
     * then null should be passed and values will be reinheritable */
    public static <T> boolean isKeyReinheritable(final ConfigKey<T> key, final ConfigInheritanceContext context) {
        ConfigInheritance inh = ConfigInheritances.findInheritance(key, context, null);
        if (inh==null) return true;
        return inh.isReinheritable(null, null);
    }

}
