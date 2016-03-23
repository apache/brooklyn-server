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
package org.apache.brooklyn.core.location.dynamic.clocker;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.BasicLocationDefinition;
import org.apache.brooklyn.core.location.dynamic.DynamicLocation;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimaps;
import com.google.common.collect.SetMultimap;

public class StubInfrastructureLocation extends AbstractLocation implements MachineProvisioningLocation<MachineLocation>, DynamicLocation<StubInfrastructure, StubInfrastructureLocation> {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(StubInfrastructureLocation.class);

    public static final ConfigKey<String> LOCATION_NAME = ConfigKeys.newStringConfigKey("locationName");

    @SetFromFlag("locationRegistrationId")
    private String locationRegistrationId;

    @SetFromFlag("machines")
    private final SetMultimap<StubHostLocation, String> containers = Multimaps.synchronizedSetMultimap(HashMultimap.<StubHostLocation, String>create());

    private transient StubInfrastructure infrastructure;

    @Override
    public void init() {
        super.init();
        infrastructure = (StubInfrastructure) checkNotNull(getConfig(OWNER), "owner");
    }
    
    @Override
    public void rebind() {
        super.rebind();

        infrastructure = (StubInfrastructure) getConfig(OWNER);
        
        if (getConfig(LOCATION_NAME) != null) {
            register();
        }
    }

    @Override
    public LocationDefinition register() {
        String locationName = checkNotNull(getConfig(LOCATION_NAME), "config %s", LOCATION_NAME.getName());

        LocationDefinition check = getManagementContext().getLocationRegistry().getDefinedLocationByName(locationName);
        if (check != null) {
            throw new IllegalStateException("Location " + locationName + " is already defined: " + check);
        }

        String locationSpec = String.format(StubResolver.DOCKER_INFRASTRUCTURE_SPEC, getId()) + String.format(":(name=\"%s\")", locationName);

        LocationDefinition definition = new BasicLocationDefinition(locationName, locationSpec, ImmutableMap.<String, Object>of());
        getManagementContext().getLocationRegistry().updateDefinedLocation(definition);
        
        locationRegistrationId = definition.getId();
        requestPersist();
        
        return definition;
    }
    
    @Override
    public void deregister() {
        if (locationRegistrationId != null) {
            getManagementContext().getLocationRegistry().removeDefinedLocation(locationRegistrationId);
            locationRegistrationId = null;
            requestPersist();
        }
    }
    
    @Override
    public StubInfrastructure getOwner() {
        return (StubInfrastructure) getConfig(OWNER);
    }

    @Override
    public MachineLocation obtain(Map<?, ?> flags) throws NoMachinesAvailableException {
        StubHost host = (StubHost) Iterables.get(infrastructure.getStubHostList(), 0);
        StubHostLocation hostLocation = host.getDynamicLocation();
        StubContainerLocation containerLocation = hostLocation.obtain(flags);
        containers.put(hostLocation, containerLocation.getId());
        return containerLocation;
    }

    @Override
    public void release(MachineLocation machine) {
        Set<StubHostLocation> set = Multimaps.filterValues(containers, Predicates.equalTo(machine.getId())).keySet();
        StubHostLocation hostLocation = Iterables.getOnlyElement(set);
        hostLocation.release((StubContainerLocation)machine);
        containers.remove(hostLocation, machine);
    }

    @Override
    public MachineProvisioningLocation<MachineLocation> newSubLocation(Map<?, ?> newFlags) {
        return this;
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return ImmutableMap.of();
    }
}