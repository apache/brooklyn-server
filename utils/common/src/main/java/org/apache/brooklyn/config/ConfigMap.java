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
    
    /** returns the config, resolved and inheritance rules applied, with default value as per the key, or null */
    public <T> T getConfig(ConfigKey<T> key);
    public <T> Maybe<T> getConfigMaybe(ConfigKey<T> key);
    
    /** @see #getConfig(ConfigKey), with default value as per the key, or null */
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
    @Deprecated  // and confirmed no uses of this method on the interface (implementations still use this method)
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

    /** returns all keys present in the map matching the given filter predicate; see ConfigPredicates for common predicates.
     * if the map is associated with a container or type context where reference keys are defined,
     * those keys are included in the result whether or not present in the map (unlike {@link #findKeysPresent(Predicate)}) */
    // TODO should be findKeysDeclaredOrPresent - if you want just the declared ones, look up the type
    // TODO should ignore sub element config keys, but for now the caller can do that
    public Set<ConfigKey<?>> findKeysDeclared(Predicate<? super ConfigKey<?>> filter);

    /** as {@link #findKeysDeclared(Predicate)} but restricted to keys actually present in the map
     * <p>
     * if there is a container or type context defining reference keys, those key definitions will be
     * preferred over any config keys used as keys in this map. */
    // TODO should include structured config keys if they have a sub element config present
    public Set<ConfigKey<?>> findKeysPresent(Predicate<? super ConfigKey<?>> filter);

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

        /** as {@link #getAllConfigInheritedRawWithErrors()} but removes any entries which should not be re-inheritable by descendants */
        public Map<ConfigKey<?>,ReferenceWithError<ConfigValueAtContainer<TContainer,?>>> getAllReinheritableConfigRaw();
    }
    
}
