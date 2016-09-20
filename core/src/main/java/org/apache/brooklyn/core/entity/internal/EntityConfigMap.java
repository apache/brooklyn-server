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
package org.apache.brooklyn.core.entity.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritance.ContainerAndValue;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.BasicConfigInheritance.AncestorContainerAndKeyValueIterator;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.EntityFunctions;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal.ConfigurationSupportInternal;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class EntityConfigMap extends AbstractConfigMapImpl {

    private static final Logger LOG = LoggerFactory.getLogger(EntityConfigMap.class);

    public EntityConfigMap(EntityInternal entity) {
        super(checkNotNull(entity, "entity must be specified"));
    }
    
    public EntityConfigMap(EntityInternal entity, Map<ConfigKey<?>, Object> storage) {
        super(checkNotNull(entity, "entity must be specified"), checkNotNull(storage, "storage map must be specified"));
    }

    /** entity against which config resolution / task execution will occur
     * @deprecated since 0.10.0 kept for serialization */ @Deprecated
    private EntityInternal entity;
    @Override
    protected BrooklynObjectInternal getBrooklynObject() {
        BrooklynObjectInternal result = super.getBrooklynObject();
        if (result!=null) return result;

        synchronized (this) {
            result = super.getBrooklynObject();
            if (result!=null) return result;
            bo = entity;
            entity = null;
        }
        return super.getBrooklynObject();
    }

    protected EntityInternal getEntity() {
        return (EntityInternal) getBrooklynObject();
    }
    
    @Override
    protected ExecutionContext getExecutionContext(BrooklynObject bo) {
        return ((EntityInternal)bo).getExecutionContext();
    }
    
    @Override
    protected void postSetConfig() {
        getEntity().config().refreshInheritedConfigOfChildren();
    }

    @Override
    protected void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue) {
        // TEMPORARY CODE
        // We're notifying of config-changed because currently persistence needs to know when the
        // attributeWhenReady is complete (so it can persist the result).
        // Long term, we'll just persist tasks properly so the call to onConfigChanged will go!
        if (rawValue.isPresent() && (rawValue.get() instanceof Task)) {
            ((EntityInternal)bo).getManagementSupport().getEntityChangeListener().onConfigChanged(key);
        }
    }
    
    @Override
    protected <T> T getConfigImpl(ConfigKey<T> key) {
        Function<Entity, ConfigKey<T>> keyFn = EntityFunctions.configKeyFinder(key, null);
        
        // In case this entity class has overridden the given key (e.g. to set default), then retrieve this entity's key
        ConfigKey<T> ownKey = keyFn.apply(getEntity());
        if (ownKey==null) ownKey = key;
        
        LocalEvaluateKeyValue<Entity,T> evalFn = new LocalEvaluateKeyValue<Entity,T>(ownKey);

        if (ownKey instanceof ConfigKeySelfExtracting) {
            Maybe<T> ownExplicitValue = evalFn.apply(getEntity());
            
            AncestorContainerAndKeyValueIterator<Entity, T> ckvi = new AncestorContainerAndKeyValueIterator<Entity,T>(
                getEntity(), keyFn, evalFn, EntityFunctions.parent());
            
            ContainerAndValue<T> result = getDefaultRuntimeInheritance().resolveInheriting(ownKey,
                ownExplicitValue, getEntity(),
                ckvi, InheritanceContext.RUNTIME_MANAGEMENT);
        
            if (result.getValue()!=null) return result.getValue();
        } else {
            LOG.warn("Config key {} of {} is not a ConfigKeySelfExtracting; cannot retrieve value; returning default", ownKey, this);
        }
        return null;
    }

    private ConfigInheritance getDefaultRuntimeInheritance() {
        return BasicConfigInheritance.OVERWRITE; 
    }

    @Override
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited) {
        if (ownConfig.containsKey(key)) return Maybe.of(ownConfig.get(key));
        if (!includeInherited || getEntity().getParent()==null) return Maybe.absent(); 
        return ((EntityInternal)getEntity().getParent()).config().getRaw(key);
    }
    
    /** an immutable copy of the config visible at this entity, local and inherited (preferring local) */
    // TODO deprecate because key inheritance not respected
    public Map<ConfigKey<?>,Object> getAllConfig() {
        Map<ConfigKey<?>,Object> result = new LinkedHashMap<ConfigKey<?>,Object>();
        if (getEntity().getParent()!=null)
            result.putAll( ((BrooklynObjectInternal)getEntity().getParent()).config().getInternalConfigMap().getAllConfig() );
        result.putAll(ownConfig);
        return Collections.unmodifiableMap(result);
    }
    
    /** Creates an immutable copy of the config visible at this entity, local and inherited (preferring local), including those that did not match config keys */
    // TODO deprecate because key inheritance not respected
    public ConfigBag getAllConfigBag() {
        ConfigBag result = ConfigBag.newInstance().putAll(ownConfig);
        if (getEntity().getParent()!=null) {
            result.putIfAbsent(
                ((EntityConfigMap) ((BrooklynObjectInternal)getEntity().getParent()).config().getInternalConfigMap()).getAllConfigBag() );
        }
        return result.seal();
    }
    
    @Override
    // TODO deprecate or clarify syntax 
    public EntityConfigMap submap(Predicate<ConfigKey<?>> filter) {
        EntityConfigMap m = new EntityConfigMap(getEntity(), Maps.<ConfigKey<?>, Object>newLinkedHashMap());
        synchronized (ownConfig) {
            for (Map.Entry<ConfigKey<?>,Object> entry: ownConfig.entrySet()) {
                if (filter.apply(entry.getKey())) {
                    m.ownConfig.put(entry.getKey(), entry.getValue());
                }
            }
        }
        if (getEntity().getParent()!=null) {
            merge(m, ((EntityConfigMap) ((ConfigurationSupportInternal)getEntity().getParent().config()).getInternalConfigMap()).submap(filter));
        }
        return m;
    }

    private void merge(EntityConfigMap local, EntityConfigMap parent) {
        for (ConfigKey<?> k: parent.ownConfig.keySet()) {
            // TODO apply inheritance
            if (!local.ownConfig.containsKey(k)) {
                local.ownConfig.put(k, parent.ownConfig.get(k));
            }
        }
    }

}
