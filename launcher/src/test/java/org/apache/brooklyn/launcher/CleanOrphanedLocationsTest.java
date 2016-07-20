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
package org.apache.brooklyn.launcher;

import static org.testng.Assert.assertFalse;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CleanOrphanedLocationsTest extends AbstractCleanOrphanedStateTest {

    @Test
    public void testDeletesOrphanedLocations() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation loc2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        assertTransformDeletes(new Deletions().locations(loc.getId(), loc2.getId()));
    }
    
    @Test
    public void testDeletesOrphanedLocationThatLinksToReachable() throws Exception {
        Location referantLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        Location refereeLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), referantLoc));
        origApp.addLocations(ImmutableList.of(referantLoc));
        assertTransformDeletes(new Deletions().locations(refereeLoc.getId()));
    }
    
    @Test
    public void testDeletesOrphanedSubGraph() throws Exception {
        Location loc1 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        Location loc2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc1));
        assertTransformDeletes(new Deletions().locations(loc1.getId(), loc2.getId()));
    }

    // Tests that we don't accidentally abort or throw exceptions. 
    @Test
    public void testHandlesDanglingReference() throws Exception {
        SshMachineLocation todelete = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.addLocations(ImmutableList.of(todelete));
        origApp.sensors().set(Sensors.newSensor(Object.class, "mysensor"), loc);
        origApp.config().set(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc);
        origApp.tags().addTag(loc);
        loc.config().set(ConfigKeys.newConfigKey(Object.class, "myconfig"), todelete);
        
        Locations.unmanage(todelete);
        assertFalse(getRawData().getLocations().containsKey(todelete.getId()));

        assertTransformIsNoop();
    }

    // Previously when iterating over the list, we'd abort on the first null rather than looking
    // at all the nodes in the list. This test doesn't quite manage to reproduce that. The non-null
    // dangling references are persisted.
    @Test
    public void testHandlesDanglingReferencesInLocationListSurroundingValidReference() throws Exception {
        SshMachineLocation todelete = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation todelete2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.addLocations(ImmutableList.of(todelete, loc, todelete2));
        Locations.unmanage(todelete);

        assertTransformIsNoop();
    }

    @Test
    public void testKeepsLocationsReferencedInEntityLocs() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation loc2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.addLocations(ImmutableList.of(loc, loc2));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedInSensor() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.sensors().set(Sensors.newSensor(Object.class, "mysensor"), loc);
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedInConfig() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.config().set(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc);
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedInComplexTypeSensor() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation loc2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.sensors().set(Sensors.newSensor(Object.class, "mysensor"), ImmutableMap.of("mykey", loc, loc2, "myval"));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedInTag() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.tags().addTag(loc);
        assertTransformIsNoop();
    }
    
    /**
     * TODO Fails because it is persisted as {@code <defaultValue class="locationProxy">biqwd7ukcd</defaultValue>},
     * rather than having locationProxy as the tag.
     * 
     * I (Aled) think we can live without this - hopefully no-one is setting location instances as 
     * config default values!
     */
    @Test(groups="WIP", enabled=false)
    public void testKeepsLocationsReferencedInConfigKeyDefault() throws Exception {
        SshMachineLocation loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.config().set(ConfigKeys.newConfigKey(MachineLocation.class, "myconfig", "my description", loc), (MachineLocation)null);
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsAncestorsOfReachableLocation() throws Exception {
        SshMachineLocation grandparentLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation parentLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .parent((grandparentLoc)));
        SshMachineLocation childLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .parent((parentLoc)));
        origApp.addLocations(ImmutableList.of(childLoc));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsDescendantsOfReachableLocation() throws Exception {
        SshMachineLocation grandparentLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        SshMachineLocation parentLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .parent((grandparentLoc)));
        mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .parent((parentLoc)));
        origApp.addLocations(ImmutableList.of(grandparentLoc));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsReferencesOfReachableLocation() throws Exception {
        Location referantLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        Location refereeLoc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), referantLoc));
        origApp.addLocations(ImmutableList.of(refereeLoc));
        assertTransformIsNoop();
    }

    @Test
    public void testKeepsTransitiveReferencesOfReachableLocation() throws Exception {
        Location loc3 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        Location loc2 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc3));
        Location loc1 = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc2));
        origApp.addLocations(ImmutableList.of(loc1));
        assertTransformIsNoop();
    }

    @Test
    public void testKeepsLocationsReferencedByEnricher() throws Exception {
        Location loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.enrichers().add(EnricherSpec.create(MyEnricher.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedByEnricherInFlag() throws Exception {
        Location loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.enrichers().add(EnricherSpec.create(MyEnricher.class)
                .configure("myobj", loc));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedByPolicy() throws Exception {
        Location loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        origApp.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc));
        assertTransformIsNoop();
    }
    
    @Test
    public void testKeepsLocationsReferencedByFeed() throws Exception {
        Location loc = mgmt().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class));
        MyFeed feed = new MyFeed();
        feed.config().set(ConfigKeys.newConfigKey(Object.class, "myconfig"), loc);
        origApp.feeds().add(feed);
        assertTransformIsNoop();
    }
}
