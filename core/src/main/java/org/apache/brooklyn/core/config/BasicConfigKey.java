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
package org.apache.brooklyn.core.config;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritance.ConfigInheritanceContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

public class BasicConfigKey<T> implements ConfigKeySelfExtracting<T>, Serializable {
    
    private static final Logger log = LoggerFactory.getLogger(BasicConfigKey.class);
    private static final long serialVersionUID = -1762014059150215376L;
    
    private static final Splitter dots = Splitter.on('.');

    public static <T> Builder<T,?> builder(TypeToken<T> type) {
        return new ConcreteBuilder<T>().type(type);
    }

    public static <T> Builder<T,?> builder(Class<T> type) {
        return new ConcreteBuilder<T>().type(type);
    }

    public static <T> Builder<T,?> builder(TypeToken<T> type, String name) {
        return new ConcreteBuilder<T>(type, name);
    }

    public static <T> Builder<T,?> builder(Class<T> type, String name) {
        return new ConcreteBuilder<T>().type(type).name(name);
    }

    public static <T> Builder<T,?> builder(String newName, ConfigKey<T> key) {
        return new ConcreteBuilder<T>(newName, key);
    }

    public static <T> Builder<T,?> builder(ConfigKey<T> key) {
        return new ConcreteBuilder<T>(key);
    }

    private static class ConcreteBuilder<T> extends Builder<T, ConcreteBuilder<T>> {
        public ConcreteBuilder() {
        }
        public ConcreteBuilder(TypeToken<T> type, String name) {
            super(type, name);
        }
        public ConcreteBuilder(String newName, ConfigKey<T> key) {
            super(newName, key);
        }
        public ConcreteBuilder(ConfigKey<T> key) {
            super(key);
        }
        @Override protected ConcreteBuilder<T> self() {
            return this;
        }
    }

    public abstract static class Builder<T, B extends Builder<T,B>> {
        protected String name;
        protected Collection<String> deprecatedNames = ImmutableList.of();
        protected TypeToken<T> type;
        protected String description;
        protected T defaultValue;
        protected boolean reconfigurable;
        protected Predicate<? super T> constraint = Predicates.alwaysTrue();
        protected ConfigInheritance runtimeInheritance;
        protected ConfigInheritance typeInheritance;
        
        protected abstract B self();

        public Builder() {
        }
        public Builder(TypeToken<T> type, String name) {
            this.type = type;
            this.name = name;
        }
        public Builder(Class<T> type, String name) {
            this(TypeToken.of(type), name);
        }
        public Builder(ConfigKey<T> key) {
            this(key.getName(), key);
        }
        public Builder(String newName, ConfigKey<T> key) {
            this.type = checkNotNull(key.getTypeToken(), "type");
            this.name = checkNotNull(newName, "name");
            this.deprecatedNames = checkNotNull(key.getDeprecatedNames(), "deprecatedNames");
            description(key.getDescription());
            defaultValue(key.getDefaultValue());
            reconfigurable(key.isReconfigurable());
            runtimeInheritance(key.getInheritanceByContext(InheritanceContext.RUNTIME_MANAGEMENT));
            typeInheritance(key.getInheritanceByContext(InheritanceContext.TYPE_DEFINITION));
            constraint(key.getConstraint());
        }
        public B name(String val) {
            this.name = val; return self();
        }
        public B deprecatedNames(Collection<String> val) {
            this.deprecatedNames = val; return self();
        }
        public B deprecatedNames(String... val) {
            return deprecatedNames(val == null ? ImmutableList.of() : ImmutableList.copyOf(val));
        }
        public B type(Class<T> val) {
            this.type = TypeToken.of(val); return self();
        }
        public B type(TypeToken<T> val) {
            this.type = val; return self();
        }
        public B description(String val) {
            this.description = val; return self();
        }
        public B defaultValue(T val) {
            this.defaultValue = val; return self();
        }
        public B reconfigurable(boolean val) {
            this.reconfigurable = val; return self();
        }
        /**
         * @deprecated since 0.10.0; use {@link #runtimeInheritance(ConfigInheritance)}
         */ 
        @Deprecated
        public B parentInheritance(ConfigInheritance val) {
            this.runtimeInheritance = val; return self();
        }
        public B runtimeInheritance(ConfigInheritance val) {
            this.runtimeInheritance = val; return self();
        }
        public B typeInheritance(ConfigInheritance val) {
            this.typeInheritance = val; return self();
        }
        /**
         * @deprecated since 0.10.0; use {@link #parentInheritance(ConfigInheritance)}
         */
        @Deprecated
        public B inheritance(ConfigInheritance val) {
            return runtimeInheritance(val);
        }
        @Beta
        public B constraint(Predicate<? super T> constraint) {
            this.constraint = checkNotNull(constraint, "constraint"); return self();
        }
        public BasicConfigKey<T> build() {
            return new BasicConfigKey<T>(this);
        }
        
        public String getName() {
            return name;
        }
        public String getDescription() {
            return description;
        }
    }
    
    protected String name;
    protected Collection<String> deprecatedNames;
    protected TypeToken<T> typeToken;
    protected Class<? super T> type;
    protected String description;
    protected T defaultValue;
    protected boolean reconfigurable;
    protected ConfigInheritance typeInheritance;
    protected ConfigInheritance runtimeInheritance;
    protected Predicate<? super T> constraint;

    /**
     * Kept only for backwards compatibility with serialised state; when read, it's value is used 
     * for {@link #inheritanceByContext} and then set to null.
     * @deprecated since 0.10.0
     */
    @Deprecated
    private ConfigInheritance inheritance;
    /**
     * Kept only for backwards compatibility with serialised state; when read, it's value is used 
     * for {@link #inheritanceByContext} and then set to null.
     * @deprecated since 0.10.0
     */
    @Deprecated
    protected ConfigInheritance parentInheritance;
    
    // FIXME In groovy, fields were `public final` with a default constructor; do we need the gson?
    public BasicConfigKey() { /* for gson */ }

    public BasicConfigKey(Class<T> type, String name) {
        this(TypeToken.of(type), name);
    }

    public BasicConfigKey(Class<T> type, String name, String description) {
        this(TypeToken.of(type), name, description);
    }

    public BasicConfigKey(Class<T> type, String name, String description, T defaultValue) {
        this(TypeToken.of(type), name, description, defaultValue);
    }

    public BasicConfigKey(TypeToken<T> type, String name) {
        this(type, name, name, null);
    }
    
    public BasicConfigKey(TypeToken<T> type, String name, String description) {
        this(type, name, description, null);
    }
    
    public BasicConfigKey(TypeToken<T> type, String name, String description, T defaultValue) {
        this.description = description;
        this.name = checkNotNull(name, "name");
        this.deprecatedNames = ImmutableList.of();
        
        this.type = TypeTokens.getRawTypeIfRaw(checkNotNull(type, "type"));
        this.typeToken = TypeTokens.getTypeTokenIfNotRaw(type);
        
        this.defaultValue = defaultValue;
        this.reconfigurable = false;
        this.constraint = Predicates.alwaysTrue();
    }

    public BasicConfigKey(Builder<T,?> builder) {
        this.name = checkNotNull(builder.name, "name");
        this.deprecatedNames = checkNotNull(builder.deprecatedNames, "deprecatedNames");
        this.type = TypeTokens.getRawTypeIfRaw(checkNotNull(builder.type, "type"));
        this.typeToken = TypeTokens.getTypeTokenIfNotRaw(builder.type);
        this.description = builder.description;
        this.defaultValue = builder.defaultValue;
        this.reconfigurable = builder.reconfigurable;
        this.runtimeInheritance = builder.runtimeInheritance;
        this.typeInheritance = builder.typeInheritance;
        // Note: it's intentionally possible to have default values that are not valid
        // per the configured constraint. If validity were checked here any class that
        // contained a weirdly-defined config key would fail to initialise.
        this.constraint = checkNotNull(builder.constraint, "constraint");
    }

    /** @see ConfigKey#getName() */
    @Override public String getName() { return name; }

    /** @see ConfigKey#getDeprecatedNames() */
    @Override public Collection<String> getDeprecatedNames() {
        // check for null, for backwards compatibility of serialized state
        if (deprecatedNames == null) deprecatedNames = ImmutableList.of();
        return deprecatedNames;
    }

    /** @see ConfigKey#getTypeName() */
    @Override public String getTypeName() { return getType().getName(); }

    /** @see ConfigKey#getType() */
    @Override public Class<? super T> getType() { return TypeTokens.getRawType(typeToken, type); }

    /** @see ConfigKey#getTypeToken() */
    @Override public TypeToken<T> getTypeToken() { return TypeTokens.getTypeToken(typeToken, type); }
    
    /** @see ConfigKey#getDescription() */
    @Override public String getDescription() { return description; }

    /** @see ConfigKey#getDefaultValue() */
    @Override public T getDefaultValue() { return defaultValue; }

    /** @see ConfigKey#hasDefaultValue() */
    @Override public boolean hasDefaultValue() {
        return defaultValue != null;
    }

    /** @see ConfigKey#isReconfigurable() */
    @Override
    public boolean isReconfigurable() {
        return reconfigurable;
    }
    
    @Override @Nullable 
    public ConfigInheritance getInheritanceByContext(ConfigInheritanceContext context) {
        if (context==InheritanceContext.RUNTIME_MANAGEMENT) {
            // for backwards compatbility
            if (parentInheritance!=null) {
                runtimeInheritance = parentInheritance;
                parentInheritance = null;
            }
            if (inheritance!=null) {
                runtimeInheritance = inheritance;
                inheritance = null;
            }
            
            return runtimeInheritance;
        }
        
        if (context==InheritanceContext.TYPE_DEFINITION) {
            return typeInheritance;
        }
        
        return null;
    }
    
    @Override
    public Map<ConfigInheritanceContext,ConfigInheritance> getInheritanceByContext() {
        MutableMap<ConfigInheritanceContext, ConfigInheritance> result = MutableMap.of();
        for (InheritanceContext context: InheritanceContext.values()) {
            result.addIfNotNull(context, getInheritanceByContext(context));
        }
        return result;
    }

    @Deprecated @Override @Nullable
    public ConfigInheritance getInheritance() {
        return getParentInheritance();
    }

    @Deprecated @Override @Nullable
    public ConfigInheritance getTypeInheritance() {
        return getInheritanceByContext(InheritanceContext.TYPE_DEFINITION);
    }

    @Deprecated @Override @Nullable
    public ConfigInheritance getParentInheritance() {
        return getInheritanceByContext(InheritanceContext.RUNTIME_MANAGEMENT);
    }

    /** @see ConfigKey#getConstraint() */
    @Override @Nonnull
    public Predicate<? super T> getConstraint() {
        // Could be null after rebinding
        if (constraint != null) {
            return constraint;
        } else {
            return Predicates.alwaysTrue();
        }
    }

    /** @see ConfigKey#isValueValid(T) */
    @Override
    public boolean isValueValid(T value) {
        // The likeliest source of an exception is a constraint from Guava that expects a non-null input.
        try {
            return getConstraint().apply(value);
        } catch (Exception e) {
            log.debug("Suppressing exception when testing validity of " + this, e);
            return false;
        }
    }

    /** @see ConfigKey#getNameParts() */
    @Deprecated
    @Override public Collection<String> getNameParts() {
        return Lists.newArrayList(dots.split(name));
    }
 
    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (!(obj instanceof BasicConfigKey)) return false;
        BasicConfigKey<?> o = (BasicConfigKey<?>) obj;
        
        return Objects.equal(name,  o.name);
    }
    
    @Override
    public int hashCode() {
        return Objects.hashCode(name);
    }
    
    @Override
    public String toString() {
        return String.format("%s[ConfigKey:%s]", name, getTypeName());
    }

    /**
     * Retrieves the value corresponding to this config key from the given map.
     * Could be overridden by more sophisticated config keys, such as MapConfigKey etc.
     */
    @SuppressWarnings("unchecked")
    @Override
    public T extractValue(Map<?,?> vals, ExecutionContext exec) {
        Object v = vals.get(this);
        try {
            return (T) resolveValue(v, exec);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @Override
    public boolean isSet(Map<?,?> vals) {
        return vals.containsKey(this);
    }
    
    protected Object resolveValue(Object v, ExecutionContext exec) throws ExecutionException, InterruptedException {
        if (v instanceof Collection || v instanceof Map) {
            return Tasks.resolveDeepValue(v, Object.class, exec, "config "+name);
        } else {
            return Tasks.resolveValue(v, getType(), exec, "config "+name);
        }
    }

    /** used to record a key which overwrites another; only needed at disambiguation time 
     * if a class declares a key and an equivalent one (often inherited) which overwrites it.
     * See org.apache.brooklyn.core.entity.ConfigEntityInheritanceTest, and uses of this class, for more explanation.
     */
    public static class BasicConfigKeyOverwriting<T> extends BasicConfigKey<T> {
        private static final long serialVersionUID = -3458116971918128018L;

        private final ConfigKey<T> parentKey;

        /** builder here should be based on the same key passed in as parent */
        @Beta
        public BasicConfigKeyOverwriting(Builder<T,?> builder, ConfigKey<T> parent) {
            super(builder);
            parentKey = parent;
            Preconditions.checkArgument(Objects.equal(builder.name, parent.getName()), "Builder must use key of the same name.");
        }
        
        public BasicConfigKeyOverwriting(ConfigKey<T> key, T defaultValue) {
            this(builder(key).defaultValue(defaultValue), key);
        }
        
        public BasicConfigKeyOverwriting(ConfigKey<T> key, String newDescription, T defaultValue) {
            this(builder(key).description(newDescription).defaultValue(defaultValue), key);
        }
        
        public ConfigKey<T> getParentKey() {
            return parentKey;
        }
    }
}
