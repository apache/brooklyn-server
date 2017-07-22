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
package org.apache.brooklyn.entity.machine.pool;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Map;

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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class ServerPoolLocation extends AbstractLocation implements MachineProvisioningLocation<MachineLocation>,
        DynamicLocation<ServerPool, ServerPoolLocation> {

    private static final Logger LOG = LoggerFactory.getLogger(ServerPoolLocation.class);

    public static final ConfigKey<ServerPool> OWNER = ConfigKeys.builder(ServerPool.class, "owner")
            .deprecatedNames("pool.location.owner")
            .build();

    @SetFromFlag("locationName")
    public static final ConfigKey<String> LOCATION_NAME = ConfigKeys.newStringConfigKey("pool.location.name");

    @SetFromFlag("locationRegistrationId")
    private String locationRegistrationId;
    
    @Override
    public void init() {
        super.init();
        LOG.debug("Initialising. Owner is: {}", checkNotNull(getConfig(OWNER), OWNER.getName()));
    }

    @Override
    public void rebind() {
        super.rebind();

        if (config().get(LOCATION_NAME) != null) {
            register();
        }
    }

    @Override
    public LocationDefinition register() {
        String locationName = checkNotNull(config().get(LOCATION_NAME), "config %s", LOCATION_NAME.getName());
        LocationDefinition check = getManagementContext().getLocationRegistry().getDefinedLocationByName(locationName);
        if (check != null) {
            throw new IllegalStateException("Location " + locationName + " is already defined: " + check);
        }

        String locationSpec = String.format(ServerPoolLocationResolver.POOL_SPEC, getId()) + String.format(":(name=\"%s\")", locationName);

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
        }
    }
    
    @Override
    public ServerPool getOwner() {
        return getConfig(OWNER);
    }

    @Override
    public MachineLocation obtain(Map<?, ?> flags) throws NoMachinesAvailableException {
        // Call server pool and try to obtain one of its machines
        return getOwner().claimMachine(flags);
    }

    @Override
    public MachineProvisioningLocation<MachineLocation> newSubLocation(Map<?, ?> newFlags) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void release(MachineLocation machine) {
        getOwner().releaseMachine(machine);
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return Maps.newLinkedHashMap();
    }
}
