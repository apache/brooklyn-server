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

import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.location.BasicLocationDefinition;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.entity.group.Cluster;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.machine.MachineEntityImpl;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StubHostImpl extends MachineEntityImpl implements StubHost {
    
    private static final Logger LOG = LoggerFactory.getLogger(StubHostImpl.class);

    @Override
    public void init() {
        super.init();

        ConfigToAttributes.apply(this);

        EntitySpec<?> dockerContainerSpec = EntitySpec.create(StubContainer.class)
                .configure(StubContainer.DOCKER_HOST, this)
                .configure(StubContainer.DOCKER_INFRASTRUCTURE, getInfrastructure());

        DynamicCluster containers = addChild(EntitySpec.create(DynamicCluster.class)
                .configure(Cluster.INITIAL_SIZE, 0)
                .configure(DynamicCluster.QUARANTINE_FAILED_ENTITIES, false)
                .configure(DynamicCluster.MEMBER_SPEC, dockerContainerSpec)
                .configure(DynamicCluster.RUNNING_QUORUM_CHECK, QuorumChecks.atLeastOneUnlessEmpty())
                .configure(DynamicCluster.UP_QUORUM_CHECK, QuorumChecks.atLeastOneUnlessEmpty())
                .displayName("Docker Containers"));
        sensors().set(DOCKER_CONTAINER_CLUSTER, containers);
    }
    
    @Override
    public StubInfrastructure getInfrastructure() {
        return config().get(DOCKER_INFRASTRUCTURE);
    }

    @Override
    public DynamicCluster getDockerContainerCluster() {
        return sensors().get(DOCKER_CONTAINER_CLUSTER);
    }

    @Override
    public StubHostLocation getDynamicLocation() {
        return (StubHostLocation) sensors().get(DYNAMIC_LOCATION);
    }

    @Override
    public void preStart() {
        super.preStart();
        ConfigToAttributes.apply(this);
        
        Maybe<SshMachineLocation> found = Machines.findUniqueMachineLocation(getLocations(), SshMachineLocation.class);

        Map<String, ?> flags = MutableMap.<String, Object>builder()
                .putAll(config().get(LOCATION_FLAGS))
                .put("machine", found.get())
                .build();

        createLocation(flags);
        sensors().get(DOCKER_CONTAINER_CLUSTER).sensors().set(SERVICE_UP, Boolean.TRUE);
    }

    @Override
    public StubHostLocation createLocation(Map<String, ?> flags) {
        StubInfrastructure infrastructure = getInfrastructure();
        StubInfrastructureLocation docker = infrastructure.getDynamicLocation();
        String locationName = docker.getId() + "-" + getId();

        String locationSpec = String.format(StubResolver.DOCKER_HOST_MACHINE_SPEC, infrastructure.getId(), getId()) + String.format(":(name=\"%s\")", locationName);
        sensors().set(LOCATION_SPEC, locationSpec);

        LocationDefinition definition = new BasicLocationDefinition(locationName, locationSpec, flags);
        Location location = getManagementContext().getLocationRegistry().resolve(definition);
        sensors().set(DYNAMIC_LOCATION, location);
        sensors().set(LOCATION_NAME, location.getId());

        LOG.info("New Docker host location {} created", location);
        return (StubHostLocation) location;
    }

    @Override
    public boolean isLocationAvailable() {
        return sensors().get(DYNAMIC_LOCATION) != null;
    }

    @Override
    public void deleteLocation() {
        StubHostLocation loc = (StubHostLocation) sensors().get(DYNAMIC_LOCATION);
        if (loc != null) {
            Locations.unmanage(loc);
        }
        sensors().set(DYNAMIC_LOCATION, null);
        sensors().set(LOCATION_NAME, null);
    }
}
