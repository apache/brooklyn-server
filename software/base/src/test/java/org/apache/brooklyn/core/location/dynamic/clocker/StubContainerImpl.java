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

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.LocationManager;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.feed.ConfigToAttributes;
import org.apache.brooklyn.core.location.dynamic.DynamicLocation;
import org.apache.brooklyn.entity.stock.BasicStartableImpl;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StubContainerImpl extends BasicStartableImpl implements StubContainer {

    private static final Logger LOG = LoggerFactory.getLogger(StubContainerImpl.class);

    @Override
    public void init() {
        super.init();

        ConfigToAttributes.apply(this);
    }

    @Override
    public StubInfrastructure getInfrastructure() {
        return config().get(DOCKER_INFRASTRUCTURE);
    }

    @Override
    public StubHost getDockerHost() {
        return (StubHost) config().get(DOCKER_HOST);
    }

    @Override
    public StubContainerLocation getDynamicLocation() {
        return (StubContainerLocation) sensors().get(DYNAMIC_LOCATION);
    }

    @Override
    public void start(Collection<? extends Location> locations) {
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);

        Map<String, ?> flags = MutableMap.copyOf(config().get(LOCATION_FLAGS));
        createLocation(flags);

        super.start(locations);

        ServiceStateLogic.setExpectedState(this, Lifecycle.RUNNING);
    }

    @Override
    public StubContainerLocation createLocation(Map<String, ?> flags) {
        StubHost dockerHost = getDockerHost();
        StubHostLocation host = dockerHost.getDynamicLocation();

        SshMachineLocation containerMachine = getManagementContext().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4"));

        // Create our wrapper location around the container
        LocationSpec<StubContainerLocation> spec = LocationSpec.create(StubContainerLocation.class)
                .parent(host)
                .configure(flags)
                .configure(DynamicLocation.OWNER, this)
                .configure("machine", containerMachine)
                .configure(containerMachine.config().getBag().getAllConfig());
        StubContainerLocation location = getManagementContext().getLocationManager().createLocation(spec);

        sensors().set(DYNAMIC_LOCATION, location);
        sensors().set(LOCATION_NAME, location.getId());
        
        return location;
    }

    @Override
    public boolean isLocationAvailable() {
        return getDynamicLocation() != null;
    }

    @Override
    public void deleteLocation() {
        StubContainerLocation location = getDynamicLocation();

        if (location != null) {
            try {
                location.close();
            } catch (IOException ioe) {
                LOG.debug("Error closing container location", ioe);
            }
            LocationManager mgr = getManagementContext().getLocationManager();
            if (mgr.isManaged(location)) {
                mgr.unmanage(location);
            }
        }

        sensors().set(DYNAMIC_LOCATION, null);
        sensors().set(LOCATION_NAME, null);
    }
}