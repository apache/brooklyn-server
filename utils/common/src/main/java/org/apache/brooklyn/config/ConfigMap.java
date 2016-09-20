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

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.util.guava.Maybe;

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
     */
    // TODO behaviour of this is undefined if the key specifies a merge
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited);

    // TODO sketch of new/better implementations
//    /** As {@link #getConfigLocalRaw(ConfigKey)} but respecting the inheritance rules.
//     * Because inheritance may not be applicable when working with raw values the result
//     * is wrapped in a {@link ReferenceWithError}, and if any aspect of inheritance 
//     * could not be applied, calls to {@link ReferenceWithError#get()} will throw
//     * (but {@link ReferenceWithError#getWithoutError()} will not).
//     * <p>
//     * Note that most modes (eg overwrite, not-inherit) will not cause issues during inheritance
//     * so this method is ordinarily fine to use.  Problems arise when a merge inheritance mode
//     * is specified and it is attempting to merge a raw value which is not mergeable.
//     * <p>
//     * See also {@link #getConfigAllInheritedRaw(ConfigKey)} which returns the complete set
//     * of inherited values.
//     * 
//     * @param key  key to look up
//     */    
//    public ReferenceWithError<ContainerAndValue<Object>> getConfigInheritedRaw(ConfigKey<?> key);
//
//    /** As {@link #getConfigLocalRaw(ConfigKey)} but returning all containers through the inheritance
//     * hierarchy where a value is set (but stopping if it encounters a key is not inheritable from a container). */
//    public List<ContainerAndValue<Object>> getConfigAllInheritedRaw(ConfigKey<?> key);
    
    /** returns the value stored against the given key, 
     * <b>not</b> any default,
     * <b>not</b> inherited (if there is an inheritance hierarchy),
     * <b>not</b> type-coerced or further resolved (eg a task or supplier, if such rules are applicable)
     * @param key  key to look up
     * @return raw, unresolved, uncoerced value of key explicitly in map
     */
    public Maybe<Object> getConfigLocalRaw(ConfigKey<?> key);

    /** returns a map of all config keys to their raw (unresolved+uncoerced) contents */
    // TODO deprecate
    public Map<ConfigKey<?>,Object> getAllConfig();

    /** returns submap matching the given filter predicate; see ConfigPredicates for common predicates */
    public ConfigMap submap(Predicate<ConfigKey<?>> filter);

    /** returns a read-only map view which has string keys (corresponding to the config key names);
     * callers encouraged to use the typed keys (and so not use this method),
     * but in some compatibility areas having a Properties-like view is useful */
    public Map<String,Object> asMapWithStringKeys();
    
    public int size();
    
    public boolean isEmpty();
    
}
