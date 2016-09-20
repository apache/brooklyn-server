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

import java.util.Map;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritances;
import org.apache.brooklyn.config.ConfigInheritances.BasicConfigValueAtContainer;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigValueAtContainer;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;

public class BasicConfigInheritance implements ConfigInheritance {

    private static final long serialVersionUID = -5916548049057961051L;

    public static final String CONFLICT_RESOLUTION_STRATEGY_DEEP_MERGE = "deep_merge";
    public static final String CONFLICT_RESOLUTION_STRATEGY_OVERWRITE = "overwrite";
    
    /** Indicates that a config key value should not be passed down from a container where it is defined.
     * Unlike {@link #NEVER_INHERITED} these values can be passed down if set as anonymous keys at a container
     * (ie the container does not expect it) to a container which does expect it, but it will not be passed down further. 
     * If the inheritor also defines a value the parent's value is ignored irrespective 
     * (as in {@link #OVERWRITE}; see {@link #NOT_REINHERITED_ELSE_DEEP_MERGE} if merging is desired). */
    public static BasicConfigInheritance NOT_REINHERITED = new BasicConfigInheritance(false,CONFLICT_RESOLUTION_STRATEGY_OVERWRITE,false);
    /** As {@link #NOT_REINHERITED} but in cases where a value is inherited because a parent did not recognize it,
     * if the inheritor also defines a value the two values should be merged. */
    public static BasicConfigInheritance NOT_REINHERITED_ELSE_DEEP_MERGE = new BasicConfigInheritance(false,CONFLICT_RESOLUTION_STRATEGY_DEEP_MERGE,false);
    /** Indicates that a key's value should never be inherited, even if defined on a container that does not know the key.
     * Most usages will prefer {@link #NOT_REINHERITED}. */
    public static BasicConfigInheritance NEVER_INHERITED = new BasicConfigInheritance(false,CONFLICT_RESOLUTION_STRATEGY_OVERWRITE,true);
    /** Indicates that if a key has a value at both an ancestor and a descendant, the descendant and his descendants
     * will prefer the value at the descendant. */
    public static BasicConfigInheritance OVERWRITE = new BasicConfigInheritance(true,CONFLICT_RESOLUTION_STRATEGY_OVERWRITE,false);
    /** Indicates that if a key has a value at both an ancestor and a descendant, the descendant and his descendants
     * should attempt to merge the values. If the values are not mergable behaviour is undefined
     * (and often the descendant's value will simply overwrite). */
    public static BasicConfigInheritance DEEP_MERGE = new BasicConfigInheritance(true,CONFLICT_RESOLUTION_STRATEGY_DEEP_MERGE,false);
    
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
    public <TContainer, TValue> boolean isReinheritable(ConfigValueAtContainer<TContainer, TValue> parent, ConfigInheritanceContext context) {
        if (!equals(ConfigInheritances.findInheritance(parent, context, this))) 
            throw new IllegalStateException("Method can only be invoked on inheritance at "+parent);
        return isReinherited();
    }
    
    @Override
    public <TContainer,TValue> boolean considerParent(
            ConfigValueAtContainer<TContainer,TValue> local,
            ConfigValueAtContainer<TContainer,TValue> parent,
            ConfigInheritanceContext context) {
        if (!equals(ConfigInheritances.findInheritance(local, context, this))) 
            throw new IllegalStateException("Method can only be invoked on inheritance at "+local);
        if (parent==null) return false;
        if (CONFLICT_RESOLUTION_STRATEGY_OVERWRITE.equals(conflictResolutionStrategy)) {
            // overwrite means ignore if there's an explicit value, or we're using the local default
            return !local.isValueExplicitlySet() && !getUseLocalDefaultValue();
        }
        return true;
    }

    @Override
    public <TContainer,TValue> ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resolveWithParent(
            ConfigValueAtContainer<TContainer,TValue> local,
            ConfigValueAtContainer<TContainer,TValue> parent,
            ConfigInheritanceContext context) {
        
        if (!parent.isValueExplicitlySet() && !getUseLocalDefaultValue()) 
            return ReferenceWithError.newInstanceWithoutError(new BasicConfigValueAtContainer<TContainer,TValue>(local));
        
        // parent explicitly set (or we might have to merge defaults), 
        // and by the contract of this method we can assume reinheritable
        if (!local.isValueExplicitlySet() && !getUseLocalDefaultValue())
            return ReferenceWithError.newInstanceWithoutError(new BasicConfigValueAtContainer<TContainer,TValue>(parent));

        // both explicitly set or defaults applicable, and not overwriting; it should be merge
        if (CONFLICT_RESOLUTION_STRATEGY_DEEP_MERGE.equals(conflictResolutionStrategy)) {
            BasicConfigValueAtContainer<TContainer, TValue> result = new BasicConfigValueAtContainer<TContainer,TValue>(local);
            ReferenceWithError<Maybe<? extends TValue>> resolvedValue = deepMerge(
                local.isValueExplicitlySet() ? local.asMaybe() : local.getDefaultValue(), 
                parent.isValueExplicitlySet() ? parent.asMaybe() : parent.getDefaultValue());
            result.setValue(resolvedValue.getWithoutError());
            return ReferenceWithError.newInstanceThrowingError(result, resolvedValue.getError());
        }
        
        return resolveWithParentCustomStrategy(local, parent, context);
    }
    
    protected <TContainer, TValue> ReferenceWithError<ConfigValueAtContainer<TContainer, TValue>> resolveWithParentCustomStrategy(
            ConfigValueAtContainer<TContainer, TValue> local, ConfigValueAtContainer<TContainer, TValue> parent,
            ConfigInheritanceContext context) {
        throw new IllegalStateException("Unknown config conflict resolution strategy '"+conflictResolutionStrategy+"' evaluating "+local+"/"+parent);
    }
    
    private static <T> ReferenceWithError<Maybe<? extends T>> deepMerge(Maybe<? extends T> val1, Maybe<? extends T> val2) {
        if (val2.isAbsent() || val2.isNull()) {
            return ReferenceWithError.newInstanceWithoutError(val1);
        } else if (val1.isAbsent()) {
            return ReferenceWithError.newInstanceWithoutError(val2);
        } else if (val1.isNull()) {
            return ReferenceWithError.newInstanceWithoutError(val1); // an explicit null means an override; don't merge
        } else if (val1.get() instanceof Map && val2.get() instanceof Map) {
            @SuppressWarnings({ "unchecked", "rawtypes" })
            Maybe<T> result = (Maybe)Maybe.of(CollectionMerger.builder().build().merge((Map<?,?>)val1.get(), (Map<?,?>)val2.get()));
            return ReferenceWithError.newInstanceWithoutError(result);
        } else {
            // cannot merge; just return val1
            return ReferenceWithError.newInstanceThrowingError(val1, new IllegalArgumentException("Cannot merge '"+val1.get()+"' and '"+val2.get()+"'"));
        }
    }
    
    public boolean isReinherited() {
        return isReinherited;
    }
    
    public String getConflictResolutionStrategy() {
        return conflictResolutionStrategy;
    }
    
    public boolean getUseLocalDefaultValue() {
        return useLocalDefaultValue;
    }
    
}
