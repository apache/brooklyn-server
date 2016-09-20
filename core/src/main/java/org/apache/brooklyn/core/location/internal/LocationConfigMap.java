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

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

public class LocationConfigMap extends AbstractConfigMapImpl<Location> {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(LocationConfigMap.class);
    
    public LocationConfigMap(AbstractLocation loc) {
        super(loc);
    }

    public LocationConfigMap(AbstractLocation loc, Map<ConfigKey<?>, Object> storage) {
        super(loc, storage);
    }

    @Override
    protected ExecutionContext getExecutionContext(BrooklynObject bo) {
        if (bo==null) return null;
        ManagementContext mgmt = ((AbstractLocation)bo).getManagementContext();
        if (mgmt==null) return null;
        return mgmt.getServerExecutionContext();
    }

    @Override
    protected void postLocalEvaluate(ConfigKey<?> key, BrooklynObject bo, Maybe<?> rawValue, Maybe<?> resolvedValue) {
    }

    @Override
    protected void postSetConfig() {
    }

    @Override
    public LocationConfigMap submap(Predicate<ConfigKey<?>> filter) {
        throw new UnsupportedOperationException("Location does not support submap");
    }

    @Override
    protected Object coerceConfigVal(ConfigKey<?> key, Object v) {
        if ((Class.class.isAssignableFrom(key.getType()) || Function.class.isAssignableFrom(key.getType())) && v instanceof String) {
            // strings can be written where classes/functions are permitted; 
            // this because an occasional pattern only for locations because validation wasn't enforced there
            // (and locations do a lot more config in brooklyn.properties) - eg ImageChooser in jclouds
            // TODO slowly warn on this then phase it out
            return v;
        }
        
        return super.coerceConfigVal(key, v);
    }

    @Override
    protected Location getParentOfContainer(Location container) {
        if (container==null) return null;
        return container.getParent();
    }

    @Override @Nullable protected <T> ConfigKey<?> getKeyAtContainerImpl(@Nonnull Location container, ConfigKey<T> queryKey) {
        return ((AbstractLocation)container).getLocationTypeInternal().getConfigKey(queryKey.getName());
    }
    
}


