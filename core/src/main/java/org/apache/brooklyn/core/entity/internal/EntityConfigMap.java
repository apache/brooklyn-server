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

import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigInheritance.ConfigInheritanceContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class EntityConfigMap extends AbstractConfigMapImpl<Entity> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(EntityConfigMap.class);

    public EntityConfigMap(EntityInternal entity) {
        super(checkNotNull(entity, "entity must be specified"));
    }
    
    public EntityConfigMap(EntityInternal entity, Map<ConfigKey<?>, Object> storage) {
        super(checkNotNull(entity, "entity must be specified"), checkNotNull(storage, "storage map must be specified"));
    }

    @Override
    public EntityInternal getContainer() {
        Entity result = super.getContainer();
        if (result!=null) return (EntityInternal) result;

        synchronized (this) {
            result = super.getContainer();
            if (result!=null) return (EntityInternal) result;
        }
        return (EntityInternal) super.getBrooklynObject();
    }

    @Override
    public <T> void assertValid(ConfigKey<T> key, T val) {
        ConfigConstraints.assertValid(getContainer(), key, val);
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
    protected Entity getParentOfContainer(Entity container) {
        if (container==null) return null;
        return container.getParent();
    }

    @Override
    protected <T> ConfigKey<?> getKeyAtContainerImpl(Entity container, ConfigKey<T> queryKey) {
        if (queryKey==null) return null;
        ConfigKey<?> kOnType = container.getEntityType().getConfigKey(queryKey.getName());
        if (kOnType!=null) return kOnType;
        ConfigKey<?> kOnTypeUndeclared;
        Map<ConfigKey<?>, Object> ownConfig = ((EntityConfigMap) ((EntityInternal) container).config().getInternalConfigMap()).ownConfig;
        synchronized (ownConfig) {
            kOnTypeUndeclared = ownConfig.keySet().stream().filter(ck -> Objects.equals(queryKey.getName(), ck.getName())).findAny().orElse(null);
        }
        if (kOnTypeUndeclared!=null) {
            // if a never inherited key is set, but not declared, it should be returned
            if (BasicConfigInheritance.NEVER_INHERITED.equals(kOnTypeUndeclared.getInheritanceByContext(InheritanceContext.RUNTIME_MANAGEMENT))) {
                return kOnTypeUndeclared;
            }
        }
        return null;
    }

    @Override
    protected Set<ConfigKey<?>> getKeysAtContainer(Entity container) {
        return container.getEntityType().getConfigKeys();
    }

    @Deprecated
    private void merge(EntityConfigMap local, EntityConfigMap parent) {
        for (ConfigKey<?> k: parent.ownConfig.keySet()) {
            // should apply inheritance; but only used in submap which is deprecated
            if (!local.ownConfig.containsKey(k)) {
                local.ownConfig.put(k, parent.ownConfig.get(k));
            }
        }
    }

}
