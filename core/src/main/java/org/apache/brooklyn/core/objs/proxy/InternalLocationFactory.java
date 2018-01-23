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
package org.apache.brooklyn.core.objs.proxy;

import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates locations of required types.
 * 
 * This is an internal class for use by core-brooklyn. End-users are strongly discouraged from
 * using this class directly.
 * 
 * @author aled
 */
public class InternalLocationFactory extends InternalFactory {

    private static final Logger LOG = LoggerFactory.getLogger(InternalLocationFactory.class);

    public InternalLocationFactory(ManagementContextInternal managementContext) {
        super(managementContext);
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T extends Location> T createLocation(LocationSpec<T> spec) {
        if (spec.getFlags().containsKey("parent")) {
            throw new IllegalArgumentException("Spec's flags must not contain parent; use spec.parent() instead for "+spec);
        }
        if (spec.getFlags().containsKey("id")) {
            throw new IllegalArgumentException("Spec's flags must not contain id; use spec.id() instead for "+spec);
        }

        try {
            Class<? extends T> clazz = spec.getType();
            
            T loc = construct(clazz, spec, null);

            if (Locations.isManaged(loc)) {
                // Construct can return an existing instance, if using SpecialBrooklynObjectConstructor.Config.SPECIAL_CONSTRUCTOR.
                // In which case, don't reconfigure it (don't change its parent, etc).
                LOG.trace("Location-factory returning pre-existing location; skipping initialization of {}", loc);
                return loc;
            }

            managementContext.prePreManage(loc);

            final AbstractLocation location = (AbstractLocation) loc;
            if (spec.getDisplayName()!=null)
                location.setDisplayName(spec.getDisplayName());
            
            if (spec.getCatalogItemId()!=null) {
                location.setCatalogItemIdAndSearchPath(spec.getCatalogItemId(), spec.getCatalogItemIdSearchPath());
            }
            
            loc.tags().addTags(spec.getTags());
            
            if (isNewStyle(clazz)) {
                location.setManagementContext(managementContext);
                location.configure(ConfigBag.newInstance().putAll(spec.getFlags()).putAll(spec.getConfig()).getAllConfig());
            }
            
            for (Map.Entry<ConfigKey<?>, Object> entry : spec.getConfig().entrySet()) {
                location.config().set((ConfigKey)entry.getKey(), entry.getValue());
            }
            for (Entry<Class<?>, Object> entry : spec.getExtensions().entrySet()) {
                ((LocationInternal)loc).addExtension((Class)entry.getKey(), entry.getValue());
            }
            location.init();
            
            Location parent = spec.getParent();
            if (parent != null) {
                loc.setParent(parent);
            }
            return loc;
            
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    /**
     * Constructs a new-style entity (fails if no no-arg constructor).
     */
    public <T extends Location> T constructLocation(Class<T> clazz) {
        return super.constructNewStyle(clazz);
    }
    
    @Override
    protected <T> T constructOldStyle(Class<T> clazz, Map<String,?> flags) throws InstantiationException, IllegalAccessException, InvocationTargetException {
        if (flags.containsKey("parent") || flags.containsKey("owner")) {
            throw new IllegalArgumentException("Spec's flags must not contain parent or owner; use spec.parent() instead for "+clazz);
        }
        return super.constructOldStyle(clazz, flags);
    }
}
