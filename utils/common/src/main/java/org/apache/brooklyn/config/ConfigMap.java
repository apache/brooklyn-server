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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;

public interface ConfigMap {
    
    /** @see #getConfig(ConfigKey, Object), with default value as per the key, or null */
    public <T> T getConfig(ConfigKey<T> key);
    
    /** @see #getConfig(ConfigKey, Object), with default value as per the key, or null */
    public <T> T getConfig(HasConfigKey<T> key);

    /** returns the value stored against the given key, 
     * <b>not</b> any default,
     * <b>not</b> resolved (and guaranteed non-blocking),
     * and <b>not</b> type-coerced.
     * @param key  key to look up
     * @param includeInherited  for {@link ConfigMap} instances which have an inheritance hierarchy, 
     *        whether to traverse it or not; has no effects where there is no inheritance 
     * @return raw, unresolved, uncoerced value of key in map,  
     *         but <b>not</b> any default on the key
     *         
     * @deprecated since 0.10.0 in favour of {@link ConfigMapWithInheritance} methods
     */
    @Deprecated  // and confirmed no uses
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited);

    /** returns the value stored against the given key, 
     * <b>not</b> any default,
     * <b>not</b> inherited (if there is an inheritance hierarchy),
     * <b>not</b> type-coerced or further resolved (eg a task or supplier, if such rules are applicable)
     * @param key  key to look up
     * @return raw, unresolved, uncoerced value of key explicitly in map
     */
    public Maybe<Object> getConfigLocalRaw(ConfigKey<?> key);

    /** returns a read-only map of all local config keys with their raw (unresolved+uncoerced) contents */
    public Map<ConfigKey<?>,Object> getAllConfigLocalRaw();
    
    /** returns a map of all config keys to their raw (unresolved+uncoerced) contents 
     *         
     * @deprecated since 0.10.0 in favour of {@link #getAllConfigLocalRaw()} for local
     * and {@link ConfigMapWithInheritance} methods for inherited;
     * kept on some sub-interfaces (eg Brooklyn properties) */
    @Deprecated  // and confirmed no uses (besides sub-interface)
    public Map<ConfigKey<?>,Object> getAllConfig();

    /** returns submap matching the given filter predicate; see ConfigPredicates for common predicates
     * @deprecated since 0.10.0 use {@link #findKeys(Predicate)} then do whatever is desired for the values;
     * kept on {@link StringConfigMap} */
    @Deprecated  // and confirmed no uses (besides sub-interface)
    // deprecated because this becomes irritating to implement in a hierarchical world, it requires caching the predicate;
    // also it encourages subsequent calls to deprecated methods such as #getAllConfig
    public ConfigMap submap(Predicate<ConfigKey<?>> filter);
    
    /** returns all keys matching the given filter predicate; see ConfigPredicates for common predicates */
    public Set<ConfigKey<?>> findKeys(Predicate<? super ConfigKey<?>> filter);

    /** returns a read-only map view which has string keys (corresponding to the config key names);
     * callers encouraged to use the typed keys (and so not use this method),
     * but in some compatibility areas having a Properties-like view is useful
     * 
     * @deprecated since 0.10.0 use the corresponding methods to return {@link ConfigKey}-based maps,
     * then pass to a ConfigBag to get a string-map view; kept for {@link StringConfigMap}
     */
    @Deprecated  // and confirmed no uses (besides sub-interface)
    public Map<String,Object> asMapWithStringKeys();
    
    public int size();
    
    public boolean isEmpty();
    
    public interface ConfigMapWithInheritance<TContainer> extends ConfigMap {
        
        /** Returns the container where a given key is defined, in addition to the value and data on the key it is defined against,
         * wrapped in something which will report any error during evaluation (in some cases these errors may otherwise
         * be silently ignored) */
        public <T> ReferenceWithError<ConfigValueAtContainer<TContainer,T>> getConfigAndContainer(ConfigKey<T> key);
        
        /** As {@link #getConfigLocalRaw(ConfigKey)} but respecting the inheritance rules.
         * Because inheritance may not be applicable when working with raw values the result
         * is wrapped in a {@link ReferenceWithError}, and if any aspect of inheritance 
         * could not be applied, calls to {@link ReferenceWithError#get()} will throw
         * (but {@link ReferenceWithError#getWithoutError()} will not).
         * Default values will be available from {@link ConfigValueAtContainer#getDefaultValue()}
         * but will not be considered for {@link ConfigValueAtContainer#get()} or {@link ConfigValueAtContainer#asMaybe()}.
         * <p>
         * Note that most modes (eg overwrite, not-inherit) will not cause issues during inheritance
         * so this method is ordinarily fine to use.  Problems arise when a merge inheritance mode
         * is specified and it is attempting to merge a raw value which is not mergeable.
         * <p>
         * See also {@link #getConfigAllInheritedRaw(ConfigKey)} which returns the complete set
         * of inherited values.
         * 
         * @param key  key to look up
         */    
        public ReferenceWithError<ConfigValueAtContainer<TContainer,?>> getConfigInheritedRaw(ConfigKey<?> key);
    
        /** As {@link #getConfigLocalRaw(ConfigKey)} but returning all containers through the inheritance
         * hierarchy where a value is set (but stopping if it encounters a key is not inheritable from a container).
         * <p>
         * The list is in order with the highest ancestor first. Config inheritance strategies are not
         * applied, apart from aborting ancestor traversal if inheritance of a key is blocked there. */
        public List<ConfigValueAtContainer<TContainer,?>> getConfigAllInheritedRaw(ConfigKey<?> key);
        
        /** returns a read-only map of all config keys local and inherited together with best-effort raw values
         * as per {@link #getConfigAllInheritedRaw(ConfigKey)} */
        @Beta
        public Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> getAllConfigInheritedRawWithErrors();
        /** as {@link #getAllConfigInheritedRawWithErrors()} but simplified API, just giving values, ignoring any errors */
        @Beta
        public Map<ConfigKey<?>,Object> getAllConfigInheritedRawValuesIgnoringErrors();

        /** as {@link #getAllConfigInheritedRaw()} but removes any entries which should not be re-inheritable by descendants */
        public Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> getAllReinheritableConfigRaw();
    }
    
}
