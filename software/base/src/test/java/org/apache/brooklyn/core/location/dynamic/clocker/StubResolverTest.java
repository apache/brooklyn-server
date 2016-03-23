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

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.dynamic.LocationOwner;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class StubResolverTest extends BrooklynAppUnitTestSupport {

    private StubInfrastructure infrastructure;
    private Location localhostLoc;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        StubResolver stubResolver = new StubResolver();
        ((BasicLocationRegistry)mgmt.getLocationRegistry()).registerResolver(stubResolver);

        infrastructure = app.createAndManageChild(EntitySpec.create(StubInfrastructure.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution())
                .configure(StubInfrastructure.DOCKER_HOST_CLUSTER_MIN_SIZE, 1));
        localhostLoc = mgmt.getLocationManager()
                .createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class));
        app.start(ImmutableList.of(localhostLoc));
    }

    @Test
    public void testResolveInfrastructure() {
        String spec = "stub:" + infrastructure.getDynamicLocation().getId();
        StubInfrastructureLocation loc = (StubInfrastructureLocation) mgmt.getLocationRegistry().resolve(spec);
        assertEquals(loc, infrastructure.getDynamicLocation());
        
        String spec2 = infrastructure.sensors().get(LocationOwner.LOCATION_SPEC);
        StubInfrastructureLocation loc2 = (StubInfrastructureLocation) mgmt.getLocationRegistry().resolve(spec2);
        assertEquals(loc2, infrastructure.getDynamicLocation());
    }

    @Test
    public void testResolveHost() {
        StubHost host = (StubHost) Iterables.getOnlyElement(infrastructure.getStubHostCluster().getMembers());
        
        String spec = "stub:" + infrastructure.getDynamicLocation().getId() + ":" + host.getDynamicLocation().getId();
        StubHostLocation loc = (StubHostLocation) mgmt.getLocationRegistry().resolve(spec);
        assertEquals(loc, host.getDynamicLocation());
        
        
        String spec2 = host.sensors().get(LocationOwner.LOCATION_SPEC);
        StubHostLocation loc2 = (StubHostLocation) mgmt.getLocationRegistry().resolve(spec2);
        assertEquals(loc2, host.getDynamicLocation());
    }
}
