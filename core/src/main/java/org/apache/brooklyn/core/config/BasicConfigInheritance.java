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

import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;

import javax.annotation.Nullable;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;

public class BasicConfigInheritance implements ConfigInheritance {

    private static final long serialVersionUID = -5916548049057961051L;

    // TODO javadoc
    public static BasicConfigInheritance NOT_REINHERITED = new BasicConfigInheritance(false,"overwrite",false);
    public static BasicConfigInheritance NOT_REINHERITED_ELSE_DEEP_MERGE = new BasicConfigInheritance(false,"deep_merge",false);
    public static BasicConfigInheritance NEVER_INHERITED = new BasicConfigInheritance(false,"overwrite",true);
    public static BasicConfigInheritance OVERWRITE = new BasicConfigInheritance(true,"overwrite",false);
    public static BasicConfigInheritance DEEP_MERGE = new BasicConfigInheritance(true,"deep_merge",false);
    
    // reinheritable? true/false; if false, children/descendants/inheritors will never see it; default true
    protected final boolean isReinherited;
    // conflict-resolution-strategy? overwrite or deep_merge or null; default null meaning caller supplies
    // (overriding resolveConflict)
    protected final String conflictResolutionStrategy;
    // use-local-default-value? true/false; if true, overwrite above means "always ignore (even if null)"; default false
    // whereas merge means "use local default (if non-null)"
    protected final boolean useLocalDefaultValue;

    protected BasicConfigInheritance(boolean isReinherited, String conflictResolutionStrategy, boolean useLocalDefaultValue) {
        super();
        this.isReinherited = isReinherited;
        this.conflictResolutionStrategy = conflictResolutionStrategy;
        this.useLocalDefaultValue = useLocalDefaultValue;
    }

    @SuppressWarnings("deprecation")
    @Override
    public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
        return null;
    }
    
    @Override
    public <T> ContainerAndValue<T> resolveInheriting(
            @Nullable ConfigKey<T> key, Maybe<T> localValue, Object container,
            Iterator<? extends ContainerAndKeyValue<T>> ancestorContainerKeyValues, ConfigInheritanceContext context) {
        ConfigInheritance inh = key==null ? null : key.getInheritanceByContext(context);
        if (inh==null) inh = this;
        if (inh!=this) return inh.resolveInheriting(key, localValue, container, ancestorContainerKeyValues, context);
        
        ContainerAndValue<T> v2 = null;
        if (OVERWRITE.conflictResolutionStrategy.equals(conflictResolutionStrategy) && localValue.isPresent()) {
            // don't inherit
        } else if (ancestorContainerKeyValues==null || !ancestorContainerKeyValues.hasNext()) {
            // nothing to inherit
        } else {
            // check whether parent allows us to get inherited value
            ContainerAndKeyValue<T> c = ancestorContainerKeyValues.next();
            ConfigInheritance inh2 = c.getKey()==null ? null : c.getKey().getInheritanceByContext(context);
            if (inh2!=null && !ConfigKeys.isReinherited(c.getKey(), context)) {
                // can't inherit
            } else {
                // get inherited value
                if (inh2==null) inh2=this;
                v2 = inh2.resolveInheriting(c.getKey()!=null ? c.getKey() : null, 
                    c.isValueSet() ? Maybe.of(c.getValue()) : Maybe.<T>absent(), c.getContainer(), 
                        ancestorContainerKeyValues, context);
            }
        }

        Maybe<T> localValueOrConflictableDefault = localValue.isPresent() ? localValue : 
            useLocalDefaultValue ? Maybe.ofAllowingNull(key==null || !key.hasDefaultValue() ? null : key.getDefaultValue()) :
            Maybe.<T>absent();
        if (v2!=null && v2.isValueSet() && !localValueOrConflictableDefault.isPresent()) return v2;
        Result<T> v = new Result<T>();
        v.container = container;
        if (v2==null || !v2.isValueSet()) {
            v.isValueSet = localValue.isPresent();
            v.value = v.isValueSet() ? localValue.get() : key!=null ? key.getDefaultValue() : null; 
        } else {
            v.value = resolveConflict(key, localValue, Maybe.ofAllowingNull(v2.getValue()));
            v.isValueSet = true;
        }
        return v;
    }
    /** only invoked if there is an ancestor value; custom strategies can overwrite */
    protected <T> T resolveConflict(ConfigKey<T> key, Maybe<T> localValue, Maybe<T> ancestorValue) {
        if (OVERWRITE.conflictResolutionStrategy.equals(conflictResolutionStrategy)) {
            if (localValue.isPresent()) return localValue.get();
            if (useLocalDefaultValue) return (key==null || !key.hasDefaultValue()) ? null : key.getDefaultValue();
            return ancestorValue.orNull();
        }
        if (DEEP_MERGE.conflictResolutionStrategy.equals(conflictResolutionStrategy)) {
            localValue = localValue.isPresent() ? localValue : 
                useLocalDefaultValue && key!=null && key.hasDefaultValue() ? Maybe.ofAllowingNull(key.getDefaultValue()) :
                Maybe.<T>absent();
            return deepMerge(localValue, ancestorValue).orNull();
        }
        throw new IllegalStateException("Unknown config conflict resolution strategy '"+conflictResolutionStrategy+"' evaluating "+key);
    }
    private static class Result<T> implements ContainerAndValue<T> {
        Object container = null;
        T value = null;
        boolean isValueSet = false;
        @Override public Object getContainer() { return container; }
        @Override public T getValue() { return value; }
        @Override public boolean isValueSet() { return isValueSet; }
    }
    private static <T> Maybe<? extends T> deepMerge(Maybe<? extends T> val1, Maybe<? extends T> val2) {
        if (val2.isAbsent() || val2.isNull()) {
            return val1;
        } else if (val1.isAbsent()) {
            return val2;
        } else if (val1.isNull()) {
            return val1; // an explicit null means an override; don't merge
        } else if (val1.get() instanceof Map && val2.get() instanceof Map) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Maybe<T> result = (Maybe)Maybe.of(CollectionMerger.builder().build().merge((Map<?,?>)val1.get(), (Map<?,?>)val2.get()));
            return result;
        } else {
            // cannot merge; just return val1
            return val1;
        }
    }

    public static class BasicContainerAndKeyValue<TContainer,TValue> implements ContainerAndKeyValue<TValue> {
        private final TContainer container;
        private final ConfigKey<TValue> key;
        private final Function<TContainer,Maybe<TValue>> evaluationFunction;
        private Maybe<TValue> resolved;
        
        public BasicContainerAndKeyValue(ConfigKey<TValue> key, TContainer container, Function<TContainer, Maybe<TValue>> evaluationFunction) {
            this.key = key;
            this.container = container;
            this.evaluationFunction = evaluationFunction;
        }

        protected synchronized Maybe<TValue> resolve() {
            if (resolved==null) { 
                resolved = evaluationFunction.apply(getContainer());
            }
            return resolved;
        }

        @Override
        public TContainer getContainer() {
            return container;
        }

        @Override
        public TValue getValue() {
            if (resolve().isPresent()) return resolve().get();
            return getDefaultValue();
        }

        @Override
        public boolean isValueSet() {
            return resolve().isPresent();
        }

        @Override
        public ConfigKey<TValue> getKey() {
            return key;
        }

        @Override
        public TValue getDefaultValue() {
            return key.getDefaultValue();
        }
    }
    
    public static class AncestorContainerAndKeyValueIterator<TContainer,TValue> implements Iterator<ContainerAndKeyValue<TValue>> {
        private TContainer lastContainer;
        private final Function<TContainer, ConfigKey<TValue>> keyFindingFunction; 
        private final Function<TContainer, Maybe<TValue>> localEvaluationFunction; 
        private final Function<TContainer, TContainer> parentFunction;
        
        public AncestorContainerAndKeyValueIterator(TContainer childContainer, 
                Function<TContainer, ConfigKey<TValue>> keyFindingFunction, 
                Function<TContainer, Maybe<TValue>> localEvaluationFunction, 
                Function<TContainer, TContainer> parentFunction) {
            this.lastContainer = childContainer;
            this.keyFindingFunction = keyFindingFunction;
            this.localEvaluationFunction = localEvaluationFunction;
            this.parentFunction = parentFunction;
        }

        @Override
        public boolean hasNext() {
            return parentFunction.apply(lastContainer)!=null;
        }
        
        @Override
        public ContainerAndKeyValue<TValue> next() {
            TContainer nextContainer = parentFunction.apply(lastContainer);
            if (nextContainer==null) throw new NoSuchElementException("Cannot search ancestors further than "+lastContainer);
            lastContainer = nextContainer;
            return new BasicContainerAndKeyValue<TContainer,TValue>(keyFindingFunction.apply(lastContainer), lastContainer, localEvaluationFunction);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support removal");
        }
    }
    
}
