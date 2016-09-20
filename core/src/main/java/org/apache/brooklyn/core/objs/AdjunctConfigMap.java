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

import static org.apache.brooklyn.util.groovy.GroovyJavaMethods.elvis;

import java.util.Collections;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.Maps;

public class AdjunctConfigMap extends AbstractConfigMapImpl {

    private static final Logger LOG = LoggerFactory.getLogger(AdjunctConfigMap.class);

    public AdjunctConfigMap(AbstractEntityAdjunct adjunct) {
        super( Preconditions.checkNotNull(adjunct, "AbstractEntityAdjunct must be specified") );
    }

    /** policy against which config resolution / task execution will occur 
     * @deprecated since 0.10.0 kept for serialization */ @Deprecated
    private AbstractEntityAdjunct adjunct;
    @Override
    protected BrooklynObjectInternal getBrooklynObject() {
        BrooklynObjectInternal result = super.getBrooklynObject();
        if (result!=null) return result;

        synchronized (this) {
            result = super.getBrooklynObject();
            if (result!=null) return result;
            bo = adjunct;
            adjunct = null;
        }
        return super.getBrooklynObject();
    }

    protected AbstractEntityAdjunct getAdjunct() {
        return (AbstractEntityAdjunct) getBrooklynObject();
    }

    @Override
    protected void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue) { /* noop */ }

    @Override
    protected void postSetConfig() { /* noop */ }

    @Override
    protected ExecutionContext getExecutionContext(BrooklynObject bo) {
        // TODO expose ((AbstractEntityAdjunct)bo).execution ?
        Entity entity = ((AbstractEntityAdjunct)bo).entity;
        return (entity != null) ? ((EntityInternal)entity).getExecutionContext() : null;
    }
    
    protected <T> T getConfigImpl(ConfigKey<T> key) {
        // tasks won't resolve if we aren't yet connected to an entity
        
        // no need for inheritance, so much simpler than other impls
        
        @SuppressWarnings("unchecked")
        ConfigKey<T> ownKey = getAdjunct()!=null ? (ConfigKey<T>)elvis(getAdjunct().getAdjunctType().getConfigKey(key.getName()), key) : key;
        
        if (ownKey instanceof ConfigKeySelfExtracting) {
            if (((ConfigKeySelfExtracting<T>)ownKey).isSet(ownConfig)) {
                return ((ConfigKeySelfExtracting<T>)ownKey).extractValue(ownConfig, getExecutionContext(getAdjunct()));
            }
        } else {
            LOG.warn("Config key {} of {} is not a ConfigKeySelfExtracting; cannot retrieve value; returning default", ownKey, this);
        }
        return TypeCoercions.coerce(ownKey.getDefaultValue(), key.getTypeToken());
    }
    
    @Override
    public Maybe<Object> getConfigRaw(ConfigKey<?> key, boolean includeInherited) {
        if (ownConfig.containsKey(key)) return Maybe.of(ownConfig.get(key));
        return Maybe.absent();
    }
    
    /** returns the config of this policy */
    @Override
    public Map<ConfigKey<?>,Object> getAllConfig() {
        // Don't use ImmutableMap because valide for values to be null
        return Collections.unmodifiableMap(Maps.newLinkedHashMap(ownConfig));
    }

    @Override
    public AdjunctConfigMap submap(Predicate<ConfigKey<?>> filter) {
        AdjunctConfigMap m = new AdjunctConfigMap(getAdjunct());
        for (Map.Entry<ConfigKey<?>,Object> entry: ownConfig.entrySet())
            if (filter.apply(entry.getKey()))
                m.ownConfig.put(entry.getKey(), entry.getValue());
        return m;
    }

}
