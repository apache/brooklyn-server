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

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;

public class AdjunctConfigMap extends AbstractConfigMapImpl<EntityAdjunct> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AdjunctConfigMap.class);

    public AdjunctConfigMap(AbstractEntityAdjunct adjunct) {
        super( Preconditions.checkNotNull(adjunct, "AbstractEntityAdjunct must be specified") );
    }

    /** policy against which config resolution / task execution will occur 
     * @deprecated since 0.10.0 kept for serialization */ @Deprecated
    private AbstractEntityAdjunct adjunct;
    @Override
    public EntityAdjunct getContainer() {
        EntityAdjunct result = super.getContainer();
        if (result!=null) return result;

        synchronized (this) {
            result = super.getContainer();
            if (result!=null) return result;
            bo = adjunct;
            adjunct = null;
        }
        return super.getContainer();
    }

    @Override
    protected void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue) { /* noop */ }

    @Override
    protected void postSetConfig() { /* noop */ }

    @Override
    protected ExecutionContext getExecutionContext(BrooklynObject bo) {
        return ((AbstractEntityAdjunct)bo).getExecutionContext();
    }
    
    @Override
    public AdjunctConfigMap submap(Predicate<ConfigKey<?>> filter) {
        AdjunctConfigMap m = new AdjunctConfigMap((AbstractEntityAdjunct)getContainer());
        for (Map.Entry<ConfigKey<?>,Object> entry: ownConfig.entrySet())
            if (filter.apply(entry.getKey()))
                m.ownConfig.put(entry.getKey(), entry.getValue());
        return m;
    }

    @Override
    protected EntityAdjunct getParentOfContainer(EntityAdjunct container) {
        return null;
    }

    @Override
    protected <T> ConfigKey<?> getKeyAtContainerImpl(EntityAdjunct container, ConfigKey<T> queryKey) {
        return ((AbstractEntityAdjunct)container).getAdjunctType().getConfigKey(queryKey.getName());
    }
    
    @Override
    protected Set<ConfigKey<?>> getKeysAtContainer(EntityAdjunct container) {
        return ((AbstractEntityAdjunct)container).getAdjunctType().getConfigKeys();
    }

}
