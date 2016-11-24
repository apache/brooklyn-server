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

import java.io.Serializable;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.config.ConfigInheritances.BasicConfigValueAtContainer;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.Beta;

@SuppressWarnings("serial")
public interface ConfigInheritance extends Serializable {
    
    /** marker interface for inheritance contexts, for keys which can define one or more inheritance patterns;
     * implementers can define their own, e.g. in an enum */
    public interface ConfigInheritanceContext {}

    /** @deprecated since 0.10.0 see implementations of this interface */ @Deprecated
    @Beta
    public enum InheritanceMode {
        NONE,
        IF_NO_EXPLICIT_VALUE,
        DEEP_MERGE
    }
    
    /** @deprecated since 0.10.0 see implementations of this interface (look for NOT_REINHERITED, or possibly NEVER_REINHERITED) */ @Deprecated
    public static final ConfigInheritance NONE = new Legacy.None();
    /** @deprecated since 0.10.0 see implementations of this interface (look for OVERWRITE) */ @Deprecated
    public static final ConfigInheritance ALWAYS = new Legacy.Always();
    /** @deprecated since 0.10.0 see implementations of this interface (look for the same name, DEEP_MERGE) */ @Deprecated
    public static final ConfigInheritance DEEP_MERGE = new Legacy.Merged();
    
    /** @deprecated since 0.10.0 more complex inheritance conditions now require other methods */
    @Deprecated
    InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to);

    /** Returns whether any value from the given node or ancestors can be considered 
     * for inheritance by descendants, according to the {@link ConfigInheritance} defined there.
     * Implementations should not normally consider the value here
     * as there may be other ancestors whose values have not yet been considered and are not supplied.
     * <p> 
     * If there is a {@link ConfigInheritance} defined at this node,
     * this method must be called on that instance and that instance only.
     * In that case it is an error to invoke this method on any other {@link ConfigInheritance} instance. 
     * If there is not one, the config generally should be considered reinheritable;
     * callers will typically not invoke this method from a descendant inheritance context.
     * <p>
     * Consumers will typically find the methods in {@link ConfigInheritances} more convenient. */
    public <TContainer,TValue> boolean isReinheritable(
        @Nullable ConfigValueAtContainer<TContainer,TValue> parent,
        ConfigInheritanceContext context);
    
    /** Returns whether any value from the parent or its ancestors should be considered 
     * by the given local container, according to the {@link ConfigInheritance} defined there.
     * This defines the {@link ConfigInheritance} of the local container typically considering
     * the value of the key there. 
     * Implementations should not normally consider the value of the parent
     * as there may be other ancestors whose values have not yet been considered and are not supplied,
     * but it may determine that a local value is sufficient to render it unnecessary to consider the parent.
     * <p>
     * If there is a {@link ConfigInheritance} defined at the local container,
     * this method must be called on that instance and that instance only.
     * In that case it is an error to invoke this method on any other {@link ConfigInheritance} instance. 
     * <p>
     * Consumers should consider this in conjuction with the 
     * {@link #isReinheritable(ConfigValueAtContainer, ConfigInheritanceContext)}
     * status of the parent (if present).
     * Implementers need not duplicate a call to that method. 
     * Consumers will typically find the methods in {@link ConfigInheritances} more convenient. */
    public <TContainer,TValue> boolean considerParent(
        @Nonnull ConfigValueAtContainer<TContainer,TValue> local,
        @Nullable ConfigValueAtContainer<TContainer,TValue> parent,
        ConfigInheritanceContext context);

    /** Returns the result after inheritance between the local container and a "resolveParent" 
     * representation of the parent's evaluation of the key considering its ancestors.
     * The parent here can be assumed to be the result of resolution with its ancestors,
     * and reinheritance can be assumed to be permitted. 
     * Consumers should invoke this only after checking 
     * {@link #considerParent(ConfigValueAtContainer, ConfigValueAtContainer, ConfigInheritanceContext)}
     * on the local node and {@link #isReinheritable(ConfigValueAtContainer, ConfigInheritanceContext)}
     * on the original parent node, 
     * and then {@link #resolveWithParent(ConfigValueAtContainer, ConfigValueAtContainer, ConfigInheritanceContext)}
     * on the original parent node with its respective resolvedParent.
     * <p>
     * If there is a {@link ConfigInheritance} defined at the local container,
     * this method must be called on that instance and that instance only.
     * In that case it is an error to invoke this method on any other {@link ConfigInheritance} instance. 
     * <p>
     * Consumers will typically find the methods in {@link ConfigInheritances} more convenient. */
    public <TContainer,TValue> ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resolveWithParent(
        @Nonnull ConfigValueAtContainer<TContainer,TValue> local,
        @Nonnull ConfigValueAtContainer<TContainer,TValue> resolvedParent,
        ConfigInheritanceContext context);

    /** @deprecated since 0.10.0 see implementations of this interface */ @Deprecated
    public static class Legacy {
        public static ConfigInheritance fromString(String val) {
            if (Strings.isBlank(val)) return null;
            switch (val.toLowerCase().trim()) {
            case "none":
                return NONE;
            case "always": 
                return ALWAYS;
            case "deepmerge" :
            case "deep_merge" :
                return DEEP_MERGE;
            default:
                throw new IllegalArgumentException("Invalid config-inheritance '"+val+"' (legal values are none, always or merge)");
            }
        }
        private static Map<ConfigInheritance,ConfigInheritance> REPLACEMENTS = MutableMap.of();
        /** used to assist in migration to new classes */
        public static void registerReplacement(ConfigInheritance old, ConfigInheritance replacement) {
            REPLACEMENTS.put(old, replacement);
        }
        private static ConfigInheritance orReplacement(ConfigInheritance orig) {
            ConfigInheritance repl = REPLACEMENTS.get(orig);
            if (repl!=null) return repl;
            return orig;
        }
        private abstract static class LegacyAbstractConversion implements ConfigInheritance {

            @Override
            public <TContainer, TValue> boolean isReinheritable(ConfigValueAtContainer<TContainer, TValue> parent, ConfigInheritanceContext context) {
                return getMode()!=InheritanceMode.NONE;
            }
            
            @Override
            public <TContainer,TValue> boolean considerParent(
                    ConfigValueAtContainer<TContainer,TValue> local,
                    @Nullable ConfigValueAtContainer<TContainer,TValue> parent,
                    ConfigInheritanceContext context) {
                if (parent==null) return false;
                if (getMode()==InheritanceMode.NONE) return false;
                if (getMode()==InheritanceMode.IF_NO_EXPLICIT_VALUE) return !local.isValueExplicitlySet();
                return true;
            }

            @Override
            public <TContainer,TValue> ReferenceWithError<ConfigValueAtContainer<TContainer,TValue>> resolveWithParent(
                    ConfigValueAtContainer<TContainer,TValue> local,
                    ConfigValueAtContainer<TContainer,TValue> parent,
                    ConfigInheritanceContext context) {
                // parent can be assumed to be set, but might not have a value
                if (!parent.isValueExplicitlySet())
                    return ReferenceWithError.newInstanceWithoutError(new BasicConfigValueAtContainer<TContainer,TValue>(local));
                    
                if (!local.isValueExplicitlySet()) 
                    return ReferenceWithError.newInstanceWithoutError(new BasicConfigValueAtContainer<TContainer,TValue>(parent));

                // both explicitly set, and not overwrite or none
                if (getMode()==InheritanceMode.DEEP_MERGE) {
                    BasicConfigValueAtContainer<TContainer, TValue> result = new BasicConfigValueAtContainer<TContainer,TValue>(local);
                    result.setValue( deepMerge(local.asMaybe(), parent.asMaybe()) );
                    return ReferenceWithError.newInstanceWithoutError(result);
                }
                
                throw new IllegalStateException("Unknown config conflict resolution strategy '"+getMode()+"' evaluating "+local+"/"+parent);
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
            
            @Override
            public InheritanceMode isInherited(ConfigKey<?> key, Object from, Object to) {
                return getMode();
            }
            protected abstract InheritanceMode getMode();
        }
        private static class Always extends LegacyAbstractConversion {
            @Override
            public InheritanceMode getMode() {
                return InheritanceMode.IF_NO_EXPLICIT_VALUE;
            }
            @SuppressWarnings("unused") // standard deserialization method
            private ConfigInheritance readResolve() {
                return orReplacement(ConfigInheritance.ALWAYS);
            }
        }
        private static class None extends LegacyAbstractConversion {
            @Override
            public InheritanceMode getMode() {
                return InheritanceMode.NONE;
            }
            @SuppressWarnings("unused") // standard deserialization method
            private ConfigInheritance readResolve() {
                return orReplacement(ConfigInheritance.NONE);
            }
        }
        private static class Merged extends LegacyAbstractConversion {
            @Override
            public InheritanceMode getMode() {
                return InheritanceMode.DEEP_MERGE;
            }
            @SuppressWarnings("unused") // standard deserialization method
            private ConfigInheritance readResolve() {
                return orReplacement(ConfigInheritance.DEEP_MERGE);
            }
        }
    }
    
}
