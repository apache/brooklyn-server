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
import static org.testng.Assert.assertFalse;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ClockerDynamicLocationPatternTest extends BrooklynAppUnitTestSupport {

    private LocalhostMachineProvisioningLocation loc;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        
        StubResolver stubResolver = new StubResolver();
        ((BasicLocationRegistry)mgmt.getLocationRegistry()).registerResolver(stubResolver);
        
        loc = app.newLocalhostProvisioningLocation();
    }
    
    @Test
    public void testCreateAndReleaseDirectly() throws Exception {
        StubInfrastructure infra = mgmt.getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class));
        infra.start(ImmutableList.of(loc));
        
        StubInfrastructureLocation loc = infra.getDynamicLocation();
        MachineLocation machine = loc.obtain(ImmutableMap.of());
        
        StubHost host = (StubHost) Iterables.getOnlyElement(infra.getStubHostCluster().getMembers());
        StubHostLocation hostLoc = host.getDynamicLocation();
        
        StubContainer container = (StubContainer) Iterables.getOnlyElement(host.getDockerContainerCluster().getMembers());
        StubContainerLocation containerLoc = container.getDynamicLocation();
        assertEquals(containerLoc, machine);
        assertEquals(Iterables.getOnlyElement(hostLoc.getChildren()), machine);
        
        loc.release(machine);
        assertFalse(Entities.isManaged(container));
        assertFalse(Locations.isManaged(containerLoc));
    }
    
    @Test
    public void testThroughLocationRegistry() throws Exception {
        StubInfrastructure infra = mgmt.getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class));
        infra.start(ImmutableList.of(loc));
        
        String infraLocSpec = infra.sensors().get(StubInfrastructure.LOCATION_SPEC);
        StubInfrastructureLocation infraLoc = (StubInfrastructureLocation) mgmt.getLocationRegistry().resolve(infraLocSpec);

        MachineLocation machine = infraLoc.obtain(ImmutableMap.of());
        
        StubHost host = (StubHost) Iterables.getOnlyElement(infra.getStubHostCluster().getMembers());
        String hostLocSpec = host.sensors().get(StubInfrastructure.LOCATION_SPEC);
        StubHostLocation hostLoc = (StubHostLocation) mgmt.getLocationRegistry().resolve(hostLocSpec);

        StubContainer container = (StubContainer) Iterables.getOnlyElement(host.getDockerContainerCluster().getMembers());
        StubContainerLocation containerLoc = container.getDynamicLocation();
        assertEquals(containerLoc, machine);
        assertEquals(Iterables.getOnlyElement(hostLoc.getChildren()), machine);
        
        infraLoc.release(machine);
        assertFalse(Entities.isManaged(container));
        assertFalse(Locations.isManaged(containerLoc));
    }
}
