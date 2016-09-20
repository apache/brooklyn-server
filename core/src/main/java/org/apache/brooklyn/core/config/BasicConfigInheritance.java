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
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.core.config.internal.BasicConfigValueAtContainer;
import org.apache.brooklyn.core.config.internal.LazyContainerAndKeyValue;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.base.Function;

public class BasicConfigInheritance implements ConfigInheritance {

    private static final long serialVersionUID = -5916548049057961051L;

    /** Indicates that a config key value should not be passed down from a container where it is defined.
     * Unlike {@link #NEVER_INHERITED} these values can be passed down if set as anonymous keys at a container
     * (ie the container does not expect it) to a container which does expect it, but it will not be passed down further. 
     * If the inheritor also defines a value the parent's value is ignored irrespective 
     * (as in {@link #OVERWRITE}; see {@link #NOT_REINHERITED_ELSE_DEEP_MERGE} if merging is desired). */
    public static BasicConfigInheritance NOT_REINHERITED = new BasicConfigInheritance(false,"overwrite",false);
    /** As {@link #NOT_REINHERITED} but in cases where a value is inherited because a parent did not recognize it,
     * if the inheritor also defines a value the two values should be merged. */
    public static BasicConfigInheritance NOT_REINHERITED_ELSE_DEEP_MERGE = new BasicConfigInheritance(false,"deep_merge",false);
    /** Indicates that a key's value should never be inherited, even if defined on a container that does not know the key.
     * Most usages will prefer {@link #NOT_REINHERITED}. */
    public static BasicConfigInheritance NEVER_INHERITED = new BasicConfigInheritance(false,"overwrite",true);
    /** Indicates that if a key has a value at both an ancestor and a descendant, the descendant and his descendants
     * will prefer the value at the descendant. */
    public static BasicConfigInheritance OVERWRITE = new BasicConfigInheritance(true,"overwrite",false);
    /** Indicates that if a key has a value at both an ancestor and a descendant, the descendant and his descendants
     * should attempt to merge the values. If the values are not mergable behaviour is undefined
     * (and often the descendant's value will simply overwrite). */
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
    public <TContainer,TValue> ConfigValueAtContainer<TContainer,TValue> resolveInheriting(
            @Nullable ConfigKey<TValue> key, Maybe<TValue> localValue, TContainer container,
            Iterator<? extends ConfigValueAtContainer<TContainer,TValue>> ancestorContainerKeyValues, ConfigInheritanceContext context) {
        ConfigInheritance inh = key==null ? null : key.getInheritanceByContext(context);
        if (inh==null) inh = this;
        if (inh!=this) return inh.resolveInheriting(key, localValue, container, ancestorContainerKeyValues, context);
        
        ConfigValueAtContainer<TContainer,TValue> v2 = null;
        if (OVERWRITE.conflictResolutionStrategy.equals(conflictResolutionStrategy) && 
                (localValue.isPresent() || useLocalDefaultValue)) {
            // don't inherit
        } else if (ancestorContainerKeyValues==null || !ancestorContainerKeyValues.hasNext()) {
            // nothing to inherit
        } else {
            // check whether parent allows us to get inherited value
            ConfigValueAtContainer<TContainer,TValue> c = ancestorContainerKeyValues.next();
            ConfigInheritance inh2 = c.getKey()==null ? null : c.getKey().getInheritanceByContext(context);
            if (inh2!=null && !ConfigKeys.isKeyReinheritable(c.getKey(), context)) {
                // can't inherit
            } else {
                // get inherited value
                if (inh2==null) inh2=this;
                v2 = inh2.resolveInheriting(c.getKey()!=null ? c.getKey() : null, 
                    c.isValueExplicitlySet() ? c.asMaybe() : Maybe.<TValue>absent(), c.getContainer(), 
                        ancestorContainerKeyValues, context);
            }
        }

        BasicConfigValueAtContainer<TContainer,TValue> v = new BasicConfigValueAtContainer<TContainer,TValue>();
        v.setContainer(container);
        v.setKey(key);
        
        Maybe<TValue> localValueOrConflictableDefault = localValue.isPresent() ? localValue : 
            useLocalDefaultValue ? v.getDefaultValueMaybe() : Maybe.<TValue>absent();
        if (v2!=null && v2.isValueExplicitlySet() && !localValueOrConflictableDefault.isPresent()) return v2;
        if (v2==null || !v2.isValueExplicitlySet()) {
            v.setValueWasExplicitlySet(localValue.isPresent());
            v.setValue(v.isValueExplicitlySet() ? localValue : v.getDefaultValueMaybe()); 
        } else {
            v.setValue(resolveConflict(key, localValue, v2.asMaybe()));
            v.setValueWasExplicitlySet(true);
        }
        return v;
    }
    /** only invoked if there is an ancestor value; custom strategies can overwrite */
    protected <T> Maybe<T> resolveConflict(ConfigKey<T> key, Maybe<T> localValue, Maybe<T> ancestorValue) {
        if (OVERWRITE.conflictResolutionStrategy.equals(conflictResolutionStrategy)) {
            if (localValue.isPresent()) return localValue;
            if (useLocalDefaultValue) return (key==null || !key.hasDefaultValue()) ? Maybe.<T>absent() : Maybe.ofAllowingNull(key.getDefaultValue());
            return ancestorValue;
        }
        if (DEEP_MERGE.conflictResolutionStrategy.equals(conflictResolutionStrategy)) {
            localValue = localValue.isPresent() ? localValue : 
                useLocalDefaultValue && key!=null && key.hasDefaultValue() ? Maybe.ofAllowingNull(key.getDefaultValue()) :
                Maybe.<T>absent();
            @SuppressWarnings("unchecked")
            Maybe<T> result = (Maybe<T>) deepMerge(localValue, ancestorValue);
            return result;
        }
        throw new IllegalStateException("Unknown config conflict resolution strategy '"+conflictResolutionStrategy+"' evaluating "+key);
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

    public static class AncestorContainerAndKeyValueIterator<TContainer,TValue> implements Iterator<ConfigValueAtContainer<TContainer,TValue>> {
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
        public ConfigValueAtContainer<TContainer,TValue> next() {
            TContainer nextContainer = parentFunction.apply(lastContainer);
            if (nextContainer==null) throw new NoSuchElementException("Cannot search ancestors further than "+lastContainer);
            lastContainer = nextContainer;
            return new LazyContainerAndKeyValue<TContainer,TValue>(keyFindingFunction.apply(lastContainer), lastContainer, localEvaluationFunction);
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("This iterator does not support removal");
        }
    }
    
}
