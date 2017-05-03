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
package org.apache.brooklyn.core.objs;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.mgmt.rebind.Rebindable;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.config.ConfigMap.ConfigMapWithInheritance;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateUnsupportedException;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;

public interface BrooklynObjectInternal extends BrooklynObject, Rebindable {
    
    void setCatalogItemId(String id);
    void setCatalogItemIdAndSearchPath(String catalogItemId, List<String> searchPath);
    void addSearchPath(List<String> searchPath);

    /**
     * Moves the current catalog item id onto the start of the search path,
     * then sets the catalog item id to the supplied value.
     */
    void stackCatalogItemId(String id);
    
    // subclasses typically apply stronger typing
    @Override
    RebindSupport<?> getRebindSupport();
    
    ManagementContext getManagementContext();
    
    @Override
    ConfigurationSupportInternal config();

    @Override
    SubscriptionSupportInternal subscriptions();

    @Beta
    public interface ConfigurationSupportInternal extends Configurable.ConfigurationSupport {

        /**
         * Returns a read-only view of all the config key/value pairs on this entity, backed by a string-based map,
         * including config names that did not match anything on this entity.
         *
         * This method gives no information about which config is inherited versus local;
         * this means {@link ConfigKey#getInheritanceByContext()} cannot be respected
         * if an anonymous key (not matching a declared config key) is set but the
         * strongly typed key is accessed.
         * <p> 
         * It does not identify the container where it is defined, meaning URLs and deferred config values 
         * cannot be resolved in the context of the appropriate ancestor.
         * <p>
         * For these reasons it is recommended to use a different accessor,
         * and callers should be advised this beta method may be removed. 
         */
        @Beta
        // TODO deprecate. used fairly extensively, mostly in tests. a bit more care will be needed to refactor.
        ConfigBag getBag();

        /**
         * Returns a read-only view of the local (i.e. not inherited) config key/value pairs on this entity,
         * backed by a string-based map, including config names that did not match anything on this entity.
         */
        @Beta
        // TODO deprecate. used extensively in tests but should be easy (if tedious) to refactor.
        ConfigBag getLocalBag();
        
        /** Returns all config defined here, in {@link #getLocalRaw(ConfigKey)} format */
        Map<ConfigKey<?>,Object> getAllLocalRaw();

        /**
         * Returns the uncoerced value for this config key, if available, not taking any default.
         * If there is no local value and there is an explicit inherited value, will return the inherited.
         * Returns {@link Maybe#absent()} if the key is not explicitly set on this object or an ancestor.
         * <p>
         * See also {@link #getLocalRaw(ConfigKey).
         */
        @Beta
        Maybe<Object> getRaw(ConfigKey<?> key);

        /**
         * @see {@link #getRaw(ConfigKey)}
         */
        @Beta
        Maybe<Object> getRaw(HasConfigKey<?> key);

        /**
         * Returns the uncoerced value for this config key, if available,
         * not following any inheritance chains and not taking any default.
         * Returns {@link Maybe#absent()} if the key is not explicitly set on this object.
         * <p>
         * See also {@link #getRaw(ConfigKey).
         */
        @Beta
        Maybe<Object> getLocalRaw(ConfigKey<?> key);

        /**
         * @see {@link #getLocalRaw(ConfigKey)}
         */
        @Beta
        Maybe<Object> getLocalRaw(HasConfigKey<?> key);

        /**
         * Attempts to coerce the value for this config key, if available,
         * including returning a default if the config key is unset,
         * returning a {@link Maybe#absent absent} if the uncoerced
         * does not support immediate resolution.
         * <p>
         * Note: if no value for the key is available, not even as a default,
         * this returns a {@link Maybe#isPresent()} containing <code>null</code>
         * (following the semantics of {@link #get(ConfigKey)} 
         * rather than {@link #getRaw(ConfigKey)}).
         * Thus a {@link Maybe#absent()} definitively indicates that
         * the absence is due to the request to evaluate immediately.
         * <p>
         * This will include catching {@link ImmediateUnsupportedException} 
         * and returning it as an absence, thus making the semantics here slightly
         * "safer" than that of {@link ImmediateSupplier#getImmediately()}.
         */
        @Beta
        <T> Maybe<T> getNonBlocking(ConfigKey<T> key);

        /**
         * @see {@link #getNonBlocking(ConfigKey)}
         */
        @Beta
        <T> Maybe<T> getNonBlocking(HasConfigKey<T> key);

        /** Adds keys or strings, making anonymous keys from strings; throws on other keys */
        @Beta
        void putAll(Map<?, ?> vals);
        
        /** @deprecated since 0.10.0 use {@link #putAll(Map)} instead */
        @Deprecated  // and confirmed no uses
        void set(Map<?, ?> vals);

        @Beta
        void removeKey(String key);

        @Beta
        void removeKey(ConfigKey<?> key);
        
        @Beta
        void refreshInheritedConfig();

        @Beta
        void refreshInheritedConfigOfChildren();
        
        /** This is currently the only way to get some rolled up collections and raw,
         * and also to test for the presence of a value (without any default).
         * As more accessors are added callers may be asked to migrate. 
         * Callers may also consider using {@link #findKeysDeclared(com.google.common.base.Predicate)}
         * although efficiency should be considered (this gives direct access whereas that does lookups and copies). */
        @Beta  // TODO provide more accessors and deprecate this
        ConfigMapWithInheritance<? extends BrooklynObject> getInternalConfigMap();

        /** Clears all local config, e.g. on tear-down */
        void removeAllLocalConfig();
    }
    
    @Beta
    public interface SubscriptionSupportInternal extends BrooklynObject.SubscriptionSupport {
        public void unsubscribeAll();
    }
    
    @Override
    RelationSupportInternal<?> relations();
    
    public interface RelationSupportInternal<T extends BrooklynObject> extends BrooklynObject.RelationSupport<T> {
        @Beta
        RelationSupport<T> getLocalBackingStore();
    }
}
