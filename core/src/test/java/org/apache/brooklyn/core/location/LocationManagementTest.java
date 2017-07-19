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
package org.apache.brooklyn.core.location;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.LocationManager;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class LocationManagementTest extends BrooklynAppUnitTestSupport {

    private LocationManager locationManager;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        locationManager = mgmt.getLocationManager();
    }
    
    @Test
    public void testCreateLocationUsingSpec() {
        SshMachineLocation loc = locationManager.createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4"));
        
        assertEquals(loc.getAddress().getHostAddress(), "1.2.3.4");
        assertSame(locationManager.getLocation(loc.getId()), loc);
    }
    
    @Test
    public void testCreateLocationUsingResolver() {
        String spec = "byon:(hosts=\"1.1.1.1\")";
        @SuppressWarnings("unchecked")
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) mgmt.getLocationRegistry().getLocationManaged(spec);
        SshMachineLocation machine = Iterables.getOnlyElement(loc.getAllMachines());
        
        assertSame(locationManager.getLocation(loc.getId()), loc);
        assertSame(locationManager.getLocation(machine.getId()), machine);
    }
    
    @Test
    public void testChildrenOfManagedLocationAutoManaged() {
        String spec = "byon:(hosts=\"1.1.1.1\")";
        @SuppressWarnings("unchecked")
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) mgmt.getLocationRegistry().getLocationManaged(spec);
        SshMachineLocation machine = new SshMachineLocation(ImmutableMap.of("address", "1.2.3.4"));

        loc.addChild(machine);
        assertSame(locationManager.getLocation(machine.getId()), machine);
        assertTrue(machine.isManaged());
        
        loc.removeChild(machine);
        assertNull(locationManager.getLocation(machine.getId()));
        assertFalse(machine.isManaged());
    }
    
    @Test
    public void testManagedLocationsSimpleCreateAndCleanup() {
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(0));
        Location loc = mgmt.getLocationRegistry().getLocationManaged("localhost");
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(1));
        mgmt.getLocationManager().unmanage(loc);
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(0));
    }

    @Test
    public void testManagedLocationsNamedCreateAndCleanup() {
        Asserts.assertThat(mgmt.getLocationRegistry().getDefinedLocations(true).keySet(), CollectionFunctionals.sizeEquals(0));
        Asserts.assertThat(mgmt.getCatalog().getCatalogItems(), CollectionFunctionals.sizeEquals(0));
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(0));
        
        mgmt.getLocationRegistry().updateDefinedLocationNonPersisted( new BasicLocationDefinition("lh1", "localhost", null) );
        
        Asserts.assertThat(mgmt.getLocationRegistry().getDefinedLocations(true).keySet(), CollectionFunctionals.sizeEquals(1));
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(0));
        // currently such defined locations do NOT appear in catalog -- see CatalogYamlLocationTest
        Asserts.assertThat(mgmt.getCatalog().getCatalogItems(), CollectionFunctionals.sizeEquals(0));
        
        Location loc = mgmt.getLocationRegistry().getLocationManaged("lh1");
        Asserts.assertThat(mgmt.getLocationRegistry().getDefinedLocations(true).keySet(), CollectionFunctionals.sizeEquals(1));
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(1));
        
        mgmt.getLocationManager().unmanage(loc);
        Asserts.assertThat(mgmt.getLocationRegistry().getDefinedLocations(true).keySet(), CollectionFunctionals.sizeEquals(1));
        Asserts.assertThat(locationManager.getLocations(), CollectionFunctionals.sizeEquals(0));
    }

    @Test
    public void testLocalhostLocationUnmanagedOnStop() {
        LocationSpec<? extends Location> locationSpec = LocationSpec.create(LocalhostMachineProvisioningLocation.class);
        testLocationUnmanagedOnStop(locationSpec);
    }

    private void testLocationUnmanagedOnStop(LocationSpec<? extends Location> locationSpec) {
        EntitySpec<BasicApplication> appSpec = EntitySpec.create(BasicApplication.class)
            .location(locationSpec);

        BasicApplication app = mgmt.getEntityManager().createEntity(appSpec);
        app.start(ImmutableList.<Location>of());
        Location appLocation = Iterables.getOnlyElement(app.getLocations());

        NamedStringTag ownerEntityTag = BrooklynTags.findFirst(BrooklynTags.OWNER_ENTITY_ID, appLocation.tags().getTags());
        Assert.assertNotNull(ownerEntityTag);
        Assert.assertEquals(ownerEntityTag.getContents(), app.getId());

        app.stop();
        Assert.assertFalse(mgmt.getEntityManager().isManaged(app));
        Set<Location> locs = ImmutableSet.copyOf(mgmt.getLocationManager().getLocations());
        Assert.assertFalse(locs.contains(appLocation), locs + " should not contain " + appLocation);
    }

}
