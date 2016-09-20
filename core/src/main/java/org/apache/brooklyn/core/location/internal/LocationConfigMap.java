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

import static org.apache.brooklyn.util.groovy.GroovyJavaMethods.elvis;

import java.util.Map;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritance.ContainerAndValue;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigMap;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.BasicConfigInheritance.AncestorContainerAndKeyValueIterator;
import org.apache.brooklyn.core.config.ConfigKeys.InheritanceContext;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.core.internal.ConfigKeySelfExtracting;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;

public class LocationConfigMap extends AbstractConfigMapImpl {

    private static final Logger log = LoggerFactory.getLogger(LocationConfigMap.class);
    
    public LocationConfigMap(AbstractLocation loc) {
        super(loc);
    }

    public LocationConfigMap(AbstractLocation loc, Map<ConfigKey<?>, Object> storage) {
        super(loc, storage);
    }

    protected AbstractLocation getLocation() {
        return (AbstractLocation) getBrooklynObject();
    }
    
    @Override
    protected BrooklynObjectInternal getParent() {
        return (BrooklynObjectInternal) getLocation().getParent();
    }
    
    @Override
    protected <T> T getConfigImpl(final ConfigKey<T> key) {
        Function<Location, ConfigKey<T>> keyFn = new Function<Location, ConfigKey<T>>() {
            @SuppressWarnings("unchecked")
            @Override
            public ConfigKey<T> apply(Location input) {
                if (input instanceof AbstractLocation) {
                    return (ConfigKey<T>) elvis( ((AbstractLocation)input).getLocationTypeInternal().getConfigKey(key.getName()), key );
                }
                return key;
            }
        };
        
        // In case this entity class has overridden the given key (e.g. to set default), then retrieve this entity's key
        ConfigKey<T> ownKey = keyFn.apply(getLocation());
        if (ownKey==null) ownKey = key;
        
        LocalEvaluateKeyValue<Location,T> evalFn = new LocalEvaluateKeyValue<Location,T>(ownKey);

        if (ownKey instanceof ConfigKeySelfExtracting) {
            Maybe<T> ownExplicitValue = evalFn.apply(getLocation());
            
            AncestorContainerAndKeyValueIterator<Location, T> ckvi = new AncestorContainerAndKeyValueIterator<Location,T>(
                getLocation(), keyFn, evalFn, new Function<Location,Location>() {
                    @Override
                    public Location apply(Location input) {
                        if (input==null) return null;
                        return input.getParent();
                    }
                });
            
            ContainerAndValue<T> result = getDefaultRuntimeInheritance().resolveInheriting(ownKey,
                ownExplicitValue, getLocation(),
                ckvi, InheritanceContext.RUNTIME_MANAGEMENT);
        
            if (result.getValue()!=null) return result.getValue();
        } else {
            log.warn("Config key {} of {} is not a ConfigKeySelfExtracting; cannot retrieve value; returning default", ownKey, getBrooklynObject());
        }
        return null;
    }
    
    private ConfigInheritance getDefaultRuntimeInheritance() {
        return BasicConfigInheritance.OVERWRITE; 
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
    public ConfigMap submap(Predicate<ConfigKey<?>> filter) {
        throw new UnsupportedOperationException("Location does not support submap");
    }


}
