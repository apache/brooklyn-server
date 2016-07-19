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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Arrays;
import java.util.Set;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoRawData;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.mgmt.rebind.transformer.impl.DeleteOrphanedLocationsTransformer;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.commons.lang.builder.EqualsBuilder;
import org.testng.annotations.Test;

import com.beust.jcommander.internal.Sets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class CleanOrphanedLocationsTest extends RebindTestFixtureWithApp {

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
        origApp.config().set(ConfigKeys.newConfigKey(Object.class, "myconfig", "my description", loc), null);
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
    
    private void assertTransformIsNoop() throws Exception {
        BrooklynMementoRawData origData = getRawData();
        BrooklynMementoRawData transformedData = transformRawData(origData);
        assertRawData(transformedData, origData);
    }

    private void assertTransformDeletes(Deletions deletions) throws Exception {
        BrooklynMementoRawData origData = getRawData();
        BrooklynMementoRawData transformedData = transformRawData(origData);
        assertRawData(transformedData, origData, deletions);
    }

    protected BrooklynMementoRawData getRawData() throws Exception {
        RebindTestUtils.waitForPersisted(origApp);
        return mgmt().getRebindManager().retrieveMementoRawData();
    }

    protected BrooklynMementoRawData transformRawData(BrooklynMementoRawData rawData) throws Exception {
        DeleteOrphanedLocationsTransformer transformer = DeleteOrphanedLocationsTransformer.builder().build();
        return transformer.transform(rawData);
    }

    protected void assertRawData(BrooklynMementoRawData actual, BrooklynMementoRawData expected) {
        // asserting lots of times, to ensure we get a more specific error message!
        assertEquals(actual.getCatalogItems().keySet(), expected.getCatalogItems().keySet());
        assertEquals(actual.getCatalogItems(), expected.getCatalogItems());
        assertEquals(actual.getEntities().keySet(), expected.getEntities().keySet());
        assertEquals(actual.getEntities(), expected.getEntities());
        assertEquals(actual.getLocations().keySet(), expected.getLocations().keySet());
        assertEquals(actual.getLocations(), expected.getLocations());
        assertEquals(actual.getEnrichers().keySet(), expected.getEnrichers().keySet());
        assertEquals(actual.getEnrichers(), expected.getEnrichers());
        assertEquals(actual.getPolicies().keySet(), expected.getPolicies().keySet());
        assertEquals(actual.getPolicies(), expected.getPolicies());
        assertEquals(actual.getFeeds().keySet(), expected.getFeeds().keySet());
        assertEquals(actual.getFeeds(), expected.getFeeds());
        assertTrue(EqualsBuilder.reflectionEquals(actual, expected));
    }
    
    protected void assertRawData(BrooklynMementoRawData actual, BrooklynMementoRawData orig, Deletions deletions) {
        BrooklynMementoRawData expected = BrooklynMementoRawData.builder()
                .catalogItems(orig.getCatalogItems())
                .entities(orig.getEntities())
                .locations(MutableMap.<String, String>builder().putAll(orig.getLocations()).removeAll(deletions.locations).build())
                .feeds(MutableMap.<String, String>builder().putAll(orig.getFeeds()).removeAll(deletions.feeds).build())
                .enrichers(MutableMap.<String, String>builder().putAll(orig.getEnrichers()).removeAll(deletions.enrichers).build())
                .policies(MutableMap.<String, String>builder().putAll(orig.getPolicies()).removeAll(deletions.policies).build())
                .build();
        assertRawData(actual, expected);
    }
    
    @SuppressWarnings("unused")
    private static class Deletions {
        final Set<String> locations = Sets.newLinkedHashSet();
        final Set<String> feeds = Sets.newLinkedHashSet();
        final Set<String> enrichers = Sets.newLinkedHashSet();
        final Set<String> policies = Sets.newLinkedHashSet();
        
        Deletions locations(String... vals) {
            if (vals != null) locations.addAll(Arrays.asList(vals));
            return this;
        }
        Deletions feeds(String... vals) {
            if (vals != null) feeds.addAll(Arrays.asList(vals));
            return this;
        }
        Deletions enrichers(String... vals) {
            if (vals != null) enrichers.addAll(Arrays.asList(vals));
            return this;
        }
        Deletions policies(String... vals) {
            if (vals != null) policies.addAll(Arrays.asList(vals));
            return this;
        }
    }
    
    public static class MyEnricher extends AbstractEnricher {
        @SetFromFlag Object obj;
    }
    
    public static class MyPolicy extends AbstractPolicy {
    }
    
    public static class MyFeed extends AbstractFeed {
    }
}
