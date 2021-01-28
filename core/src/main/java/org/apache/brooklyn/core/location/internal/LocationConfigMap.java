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
package org.apache.brooklyn.core.location.internal;

import java.util.Collection;
import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

public class LocationConfigMap extends AbstractConfigMapImpl<Location> {

    private static final Logger log = LoggerFactory.getLogger(LocationConfigMap.class);
    
    public LocationConfigMap(AbstractLocation loc) {
        super(loc);
    }

    public LocationConfigMap(AbstractLocation loc, Map<ConfigKey<?>, Object> storage) {
        super(loc, storage);
    }

    @Override
    protected ExecutionContext getExecutionContext(BrooklynObject bo) {
        // Support DSL in location config. DSL expects to be resolved in the execution context of an entity.
        // Since there's no location-entity relation try to infer it from the context of the caller.
        Entity contextEntity = BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
        if (contextEntity != null) {
            return ((EntityInternal)contextEntity).getExecutionContext();
        } else {
            // Known places we get called without entity context:
            // * unmanaging entity and subsequently its location
            // * EntityResource.listTasks(String, String) returning a Task<SshMachineLocation> and calling toString() on the return value
            log.trace("No resolving context found, will use global execution context. Could lead to NPE on DSL resolving.");
            if (bo==null) return null;
            ManagementContext mgmt = ((AbstractLocation)bo).getManagementContext();
            if (mgmt==null) return null;
            return mgmt.getServerExecutionContext();
        }
    }

    @Override
    public <T> void assertValid(ConfigKey<T> key, T val) {
        ConfigConstraints.assertValid((Location) getContainer(), key, val);
    }

    @Override
    protected void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue) {
        /* nothing needed */
    }

    @Override
    protected void postSetConfig() {
        /* nothing needed */
    }

    @Override
    public LocationConfigMap submap(Predicate<ConfigKey<?>> filter) {
        throw new UnsupportedOperationException("Location does not support submap");
    }

    @Override
    protected <T> Object coerceConfigValAndValidate(ConfigKey<T> key, Object v, boolean validate) {
        if ((Class.class.isAssignableFrom(key.getType()) || Function.class.isAssignableFrom(key.getType())) && v instanceof String) {
            // for locations only strings can be written where classes/functions are permitted;
            // this because an occasional pattern only for locations because validation wasn't enforced there
            // (and locations do a lot more config in brooklyn.properties) - eg ImageChooser in jclouds
            // TODO phase it out -- just image chooser? since 1.0
            log.warn("Setting string where class/function is wanted for location, on "+getContainer()+" key "+key+" value "+v+"; key should be changed to take appropriate type");
            return v;
        }
        
        return super.coerceConfigValAndValidate(key, v, validate);
    }

    @Override
    protected Location getParentOfContainer(Location container) {
        if (container==null) return null;
        return container.getParent();
    }

    @Override @Nullable protected <T> ConfigKey<?> getKeyAtContainerImpl(@Nonnull Location container, ConfigKey<T> queryKey) {
        return ((AbstractLocation)container).getLocationTypeInternal().getConfigKey(queryKey.getName());
    }
    
    @Override
    protected Collection<ConfigKey<?>> getKeysAtContainer(Location container) {
        return ((AbstractLocation)container).getLocationTypeInternal().getConfigKeys().values();
    }
    
}


