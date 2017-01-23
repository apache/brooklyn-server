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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.ManagementContext.PropertiesReloadListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.BasicLocationDefinition;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.dynamic.DynamicLocation;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class StubHostLocation extends AbstractLocation implements MachineProvisioningLocation<StubContainerLocation>, DynamicLocation<StubHost, StubHostLocation> {

    private static final Logger LOG = LoggerFactory.getLogger(StubHostLocation.class);

    public static final ConfigKey<String> LOCATION_NAME = ConfigKeys.newStringConfigKey("locationName");

    public static final ConfigKey<SshMachineLocation> MACHINE = ConfigKeys.newConfigKey(
            SshMachineLocation.class, 
            "machine");

    @SetFromFlag("locationRegistrationId")
    private String locationRegistrationId;

    private transient StubHost dockerHost;
    private transient SshMachineLocation machine;
    
    @Override
    public void init() {
        super.init();
        
        // TODO BasicLocationRebindsupport.addCustoms currently calls init() unfortunately!
        // Don't checkNotNull in that situation - it could be this location is orphaned!
        if (isRebinding()) {
            dockerHost = (StubHost) config().get(OWNER);
            machine = config().get(MACHINE);
        } else {
            dockerHost = (StubHost) checkNotNull(getConfig(OWNER), "owner");
            machine = checkNotNull(getConfig(MACHINE), "machine");
            addReloadListener();
        }
    }
    
    @Override
    public void rebind() {
        super.rebind();

        dockerHost = (StubHost) getConfig(OWNER);
        machine = getConfig(MACHINE);
        
        if (getConfig(LOCATION_NAME) != null) {
            register();
        }
        addReloadListener();
    }

    @SuppressWarnings("serial")
    private void addReloadListener() {
        getManagementContext().addPropertiesReloadListener(new PropertiesReloadListener() {
            @Override public void reloaded() {
                register();
            }});
    }

    @Override
    public LocationDefinition register() {
        String locationName = checkNotNull(getConfig(LOCATION_NAME), "config %s", LOCATION_NAME.getName());

        LocationDefinition check = getManagementContext().getLocationRegistry().getDefinedLocationByName(locationName);
        if (check != null) {
            throw new IllegalStateException("Location " + locationName + " is already defined: " + check);
        }

        String hostLocId = getId();
        String infraLocId = (getParent() != null) ? getParent().getId() : "";
        String locationSpec = String.format(StubResolver.DOCKER_HOST_MACHINE_SPEC, infraLocId, hostLocId) + String.format(":(name=\"%s\")", locationName);

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
    public StubHost getOwner() {
        return (StubHost) getConfig(OWNER);
    }

    @Override
    public StubContainerLocation obtain(Map<?, ?> flags) throws NoMachinesAvailableException {
        Entity callerContext = (Entity) flags.get(LocationConfigKeys.CALLER_CONTEXT.getName());
        if (callerContext == null) callerContext = getOwner();
        
        DynamicCluster cluster = dockerHost.getDockerContainerCluster();
        Entity added = cluster.addNode(machine, flags);
        if (added == null) {
            throw new NoMachinesAvailableException(String.format("Failed to create container at %s", dockerHost));
        } else {
            Entities.invokeEffector(callerContext, added, Startable.START,  MutableMap.of("locations", ImmutableList.of(machine))).getUnchecked();
        }
        StubContainer container = (StubContainer) added;

        return container.getDynamicLocation();
    }

    @Override
    public void release(StubContainerLocation machine) {
        DynamicCluster cluster = dockerHost.getDockerContainerCluster();
        StubContainer container = machine.getOwner();
        if (cluster.removeMember(container)) {
            LOG.info("Docker Host {}: member {} released", dockerHost, machine);
        } else {
            LOG.warn("Docker Host {}: member {} not found for release", dockerHost, machine);
        }

        // Now close and unmange the container
        try {
            container.stop();
            machine.close();
        } catch (Exception e) {
            LOG.warn("Error stopping container: " + container, e);
            Exceptions.propagateIfFatal(e);
        } finally {
            Entities.unmanage(container);
            Locations.unmanage(machine);
        }
    }

    @Override
    public MachineProvisioningLocation<StubContainerLocation> newSubLocation(Map<?, ?> newFlags) {
        return this;
    }

    @Override
    public Map<String, Object> getProvisioningFlags(Collection<String> tags) {
        return ImmutableMap.<String, Object>of();
    }
}