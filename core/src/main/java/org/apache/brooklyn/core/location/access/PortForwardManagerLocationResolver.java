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
package org.apache.brooklyn.core.location.access;

import java.util.Map;

import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationRegistry;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.AbstractLocationResolver;
import org.apache.brooklyn.core.location.LocationConfigUtils;
import org.apache.brooklyn.core.location.LocationPredicates;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.objs.proxy.SpecialBrooklynObjectConstructor;
import org.apache.brooklyn.core.objs.proxy.SpecialBrooklynObjectConstructor.Config;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class PortForwardManagerLocationResolver extends AbstractLocationResolver {

    private static final Logger LOG = LoggerFactory.getLogger(PortForwardManagerLocationResolver.class);

    public static final String PREFIX = "portForwardManager";

    @Override
    public String getPrefix() {
        return PREFIX;
    }

    @Override
    public LocationSpec<?> newLocationSpecFromString(String spec, Map<?, ?> locationFlags, LocationRegistry registry) {
        ConfigBag config = extractConfig(locationFlags, spec, registry);
        Map<?,?> globalProperties = registry.getProperties();
        String namedLocation = (String) locationFlags.get(LocationInternal.NAMED_SPEC_NAME.getName());
        String scope = config.get(PortForwardManager.SCOPE);

        return LocationSpec.create(PortForwardManagerImpl.class)
            .configure(config.getAllConfig())
            .configure(LocationConfigUtils.finalAndOriginalSpecs(spec, locationFlags, globalProperties, namedLocation))
            .configure(PortForwardManagerConstructor.SCOPE, scope)
            .configure(Config.SPECIAL_CONSTRUCTOR, PortForwardManagerConstructor.class);
    }

    @Override
    protected Class<? extends Location> getLocationType() {
        return PortForwardManager.class;
    }

    @Override
    protected SpecParser getSpecParser() {
        return new AbstractLocationResolver.SpecParser(getPrefix()).setExampleUsage("\"portForwardManager\" or \"portForwardManager(scope=global)\"");
    }
    
    @Override
    protected ConfigBag extractConfig(Map<?,?> locationFlags, String spec, LocationRegistry registry) {
        ConfigBag config = super.extractConfig(locationFlags, spec, registry);
        config.putAsStringKeyIfAbsent("name", "localhost");
        return config;
    }
    
    public static class PortForwardManagerConstructor implements SpecialBrooklynObjectConstructor {

        public static ConfigKey<String> SCOPE = ConfigKeys.newConfigKeyWithPrefix("brooklyn.object.constructor.portforward.",
            PortForwardManager.SCOPE);
        
        @SuppressWarnings("unchecked")
        @Override
        public <T> T create(ManagementContext mgmt, Class<T> type, AbstractBrooklynObjectSpec<?, ?> spec) {
            ConfigBag config = ConfigBag.newInstance(spec.getConfig());
            String scope = config.get(SCOPE);
            
            Optional<Location> result = findPortForwardManager(mgmt, scope);
            if (result.isPresent()) return (T) result.get();
            
            // have to create it, but first check again with synch lock
            synchronized (PortForwardManager.class) {
                result = findPortForwardManager(mgmt, scope);
                if (result.isPresent()) return (T) result.get();

                Object callerContext = config.get(CloudLocationConfig.CALLER_CONTEXT);
                LOG.info("Creating PortForwardManager(scope="+scope+")"+(callerContext!=null ? " for "+callerContext : ""));
                
                spec = LocationSpec.create((LocationSpec<?>)spec);
                // clear the special instruction to actually create it
                spec.configure(Config.SPECIAL_CONSTRUCTOR, (Class<SpecialBrooklynObjectConstructor>) null);
                spec.configure(PortForwardManager.SCOPE, scope);
                
                return (T) mgmt.getLocationManager().createLocation((LocationSpec<?>) spec);
            }
        }

        private Optional<Location> findPortForwardManager(ManagementContext mgmt, String scope) {
            return Iterables.tryFind(mgmt.getLocationManager().getLocations(), 
                Predicates.and(
                        Predicates.instanceOf(PortForwardManager.class), 
                        LocationPredicates.configEqualTo(PortForwardManager.SCOPE, scope)));
        }
        
    }
}
