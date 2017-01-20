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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.fail;

import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * See explanation of what we're testing in {@link StubInfrastructure}.
 */
public class ClockerDynamicLocationPatternTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ClockerDynamicLocationPatternTest.class);

    private LocalhostMachineProvisioningLocation loc;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        
        StubResolver stubResolver = new StubResolver();
        ((BasicLocationRegistry)mgmt.getLocationRegistry()).registerResolver(stubResolver);
        
        loc = app.newLocalhostProvisioningLocation();
    }
    
    @Test
    public void testCreateAndReleaseDirectly() throws Exception {
        StubInfrastructure infra = mgmt.getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution()));
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
        StubInfrastructure infra = mgmt.getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution()));
        infra.start(ImmutableList.of(loc));
        
        String infraLocSpec = infra.sensors().get(StubInfrastructure.LOCATION_SPEC);
        String infraLocName = infra.sensors().get(StubInfrastructure.LOCATION_NAME);
        StubInfrastructureLocation infraLoc = (StubInfrastructureLocation) mgmt.getLocationRegistry().getLocationManaged(infraLocSpec);
        StubInfrastructureLocation infraLoc2 = (StubInfrastructureLocation) mgmt.getLocationRegistry().getLocationManaged(infraLocName);
        assertSame(infraLoc, infraLoc2);
        assertHasLocation(mgmt.getLocationRegistry().getDefinedLocations().values(), infraLocName);


        MachineLocation machine = infraLoc.obtain(ImmutableMap.of());
        
        StubHost host = (StubHost) Iterables.getOnlyElement(infra.getStubHostCluster().getMembers());
        String hostLocSpec = host.sensors().get(StubHost.LOCATION_SPEC);
        String hostLocName = host.sensors().get(StubHost.LOCATION_NAME);
        StubHostLocation hostLoc = (StubHostLocation) mgmt.getLocationRegistry().getLocationManaged(hostLocSpec);
        StubHostLocation hostLoc2 = (StubHostLocation) mgmt.getLocationRegistry().getLocationManaged(hostLocName);
        assertSame(hostLoc, hostLoc2);
        assertHasLocation(mgmt.getLocationRegistry().getDefinedLocations().values(), hostLocName);

        StubContainer container = (StubContainer) Iterables.getOnlyElement(host.getDockerContainerCluster().getMembers());
        StubContainerLocation containerLoc = container.getDynamicLocation();
        assertEquals(containerLoc, machine);
        assertEquals(Iterables.getOnlyElement(hostLoc.getChildren()), machine);
        
        infraLoc.release(machine);
        assertFalse(Entities.isManaged(container));
        assertFalse(Locations.isManaged(containerLoc));
    }
    
    @Test
    public void testThroughLocationRegistryAfterReloadProperties() throws Exception {
        StubInfrastructure infra = mgmt.getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution()));
        infra.start(ImmutableList.of(loc));
        
        String infraLocSpec = infra.sensors().get(StubInfrastructure.LOCATION_SPEC);
        String infraLocName = infra.sensors().get(StubInfrastructure.LOCATION_NAME);
        
        StubHost host = (StubHost) Iterables.getOnlyElement(infra.getStubHostCluster().getMembers());
        String hostLocSpec = host.sensors().get(StubHost.LOCATION_SPEC);
        String hostLocName = host.sensors().get(StubHost.LOCATION_NAME);

        // Force a reload of brooklyn.properties (causes location-registry to be repopulated from scratch) 
        mgmt.reloadBrooklynProperties();

        // Should still be listed
        assertHasLocation(mgmt.getLocationRegistry().getDefinedLocations().values(), infraLocName);
        assertHasLocation(mgmt.getLocationRegistry().getDefinedLocations().values(), hostLocName);

        // We are willing to re-register our resolver, because in real-life we'd register it through 
        // services in META-INF.
        ((BasicLocationRegistry)mgmt.getLocationRegistry()).registerResolver(new StubResolver());
        LOG.info("Locations to look up: infraLocSpec="+infraLocSpec+"; infraLocName="+infraLocName+"; hostLocSpec="+hostLocSpec+"; hostLocName="+hostLocName);

        // Confirm can still load infra-location
        StubInfrastructureLocation infraLoc = (StubInfrastructureLocation) mgmt.getLocationRegistry().getLocationManaged(infraLocSpec);
        StubInfrastructureLocation infraLoc2 = (StubInfrastructureLocation) mgmt.getLocationRegistry().getLocationManaged(infraLocName);
        assertNotNull(infraLoc);
        assertSame(infraLoc, infraLoc2);
        
        // Confirm can still load host-location
        StubHostLocation hostLoc = (StubHostLocation) mgmt.getLocationRegistry().getLocationManaged(hostLocSpec);
        StubHostLocation hostLoc2 = (StubHostLocation) mgmt.getLocationRegistry().getLocationManaged(hostLocName);
        assertNotNull(hostLoc);
        assertSame(hostLoc, hostLoc2);

        // Confirm can still use infra-loc (to obtain a machine)
        MachineLocation machine = infraLoc.obtain(ImmutableMap.of());
        
        StubContainer container = (StubContainer) Iterables.getOnlyElement(host.getDockerContainerCluster().getMembers());
        StubContainerLocation containerLoc = container.getDynamicLocation();
        assertEquals(containerLoc, machine);
        assertEquals(Iterables.getOnlyElement(hostLoc.getChildren()), machine);
        
        infraLoc.release(machine);
        assertFalse(Entities.isManaged(container));
        assertFalse(Locations.isManaged(containerLoc));
    }

    private void assertHasLocation(Iterable<? extends LocationDefinition> locs, String expectedName) {
        for (LocationDefinition loc : locs) {
            if (expectedName.equals(loc.getName())) {
                return;
            }
        }
        fail("No location named '"+expectedName+"' in "+locs);
    }
}
