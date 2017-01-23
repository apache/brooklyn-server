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
package org.apache.brooklyn.rest.resources;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.LocationSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.test.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;


@Test(singleThreaded = true, 
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "LocationResourceTest")
public class LocationResourceTest extends BrooklynRestResourceTest {

    private static final Logger log = LoggerFactory.getLogger(LocationResourceTest.class);
    private String legacyLocationName = "my-jungle-legacy";
    private String legacyLocationVersion = "0.0.0.SNAPSHOT";
    
    private String locationName = "my-jungle";
    private String locationVersion = "0.1.2";

    private String configDisplayName = "config_displayName";
    private String testsDisplayName = "tests_displayName";
    private String byonHostname = "10.10.10.102";

    @Test
    @Deprecated
    public void testAddLegacyLocationDefinition() {
        Map<String, String> expectedConfig = ImmutableMap.of(
                "identity", "bob",
                "credential", "CR3dential");
        Response response = client().path("/locations")
                .type(MediaType.APPLICATION_JSON_TYPE)
                .post(new org.apache.brooklyn.rest.domain.LocationSpec(legacyLocationName, "aws-ec2:us-east-1", expectedConfig));

        URI addedLegacyLocationUri = response.getLocation();
        log.info("added legacy, at: " + addedLegacyLocationUri);
        LocationSummary location = client().path(response.getLocation()).get(LocationSummary.class);
        log.info(" contents: " + location);
        assertEquals(location.getSpec(), "brooklyn.catalog:"+legacyLocationName+":"+legacyLocationVersion);
        assertTrue(addedLegacyLocationUri.getPath().startsWith("/locations/"));

        JcloudsLocation l = (JcloudsLocation) getManagementContext().getLocationRegistry().getLocationManaged(legacyLocationName);
        Assert.assertEquals(l.getProvider(), "aws-ec2");
        Assert.assertEquals(l.getRegion(), "us-east-1");
        Assert.assertEquals(l.getIdentity(), "bob");
        Assert.assertEquals(l.getCredential(), "CR3dential");
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testAddNewLocationDefinition() {
        String yaml = Joiner.on("\n").join(ImmutableList.of(
                "brooklyn.catalog:",
                "  symbolicName: "+locationName,
                "  version: " + locationVersion,
                "  itemType: location",
                "  item:",
                "    type: "+"aws-ec2:us-east-1",
                "    brooklyn.config:",
                "      identity: bob",
                "      credential: CR3dential"));

        
        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());
        

        URI addedCatalogItemUri = response.getLocation();
        log.info("added, at: " + addedCatalogItemUri);
        
        // Ensure location definition exists
        CatalogLocationSummary locationItem = client().path("/catalog/locations/"+locationName + "/" + locationVersion)
                .get(CatalogLocationSummary.class);
        log.info(" item: " + locationItem);
        LocationSummary locationSummary = client().path(URI.create("/locations/"+locationName+"/")).get(LocationSummary.class);
        log.info(" summary: " + locationSummary);
        Assert.assertEquals(locationSummary.getSpec(), "brooklyn.catalog:"+locationName+":"+locationVersion);

        // Ensure location is usable - can instantiate, and has right config
        JcloudsLocation l = (JcloudsLocation) getManagementContext().getLocationRegistry().getLocationManaged(locationName);
        Assert.assertEquals(l.getProvider(), "aws-ec2");
        Assert.assertEquals(l.getRegion(), "us-east-1");
        Assert.assertEquals(l.getIdentity(), "bob");
        Assert.assertEquals(l.getCredential(), "CR3dential");
    }

    @SuppressWarnings("deprecation")
    @Test(dependsOnMethods = { "testAddNewLocationDefinition" })
    public void testListAllLocationDefinitions() {
        Set<LocationSummary> locations = client().path("/locations")
                .get(new GenericType<Set<LocationSummary>>() {});
        Iterable<LocationSummary> matching = Iterables.filter(locations, new Predicate<LocationSummary>() {
            @Override
            public boolean apply(@Nullable LocationSummary l) {
                return locationName.equals(l.getName());
            }
        });
        LocationSummary location = Iterables.getOnlyElement(matching);
        
        URI expectedLocationUri = URI.create(getEndpointAddress() + "/locations/"+locationName).normalize();
        Assert.assertEquals(location.getSpec(), "brooklyn.catalog:"+locationName+":"+locationVersion);
        Assert.assertEquals(location.getLinks().get("self").toString(), expectedLocationUri.getPath());
    }

    @SuppressWarnings("deprecation")
    @Test(dependsOnMethods = { "testListAllLocationDefinitions" })
    public void testGetSpecificLocation() {
        URI expectedLocationUri = URI.create("/locations/"+locationName);
        LocationSummary location = client().path(expectedLocationUri).get(LocationSummary.class);
        assertEquals(location.getSpec(), "brooklyn.catalog:"+locationName+":"+locationVersion);
    }

    @SuppressWarnings("deprecation")
    @Test
    public void testGetLocationConfig() {
        SimulatedLocation parentLoc = getManagementContext().getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class)
                .configure("myParentKey", "myParentVal"));
        SimulatedLocation loc = getManagementContext().getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class)
                .parent(parentLoc)
                .configure("mykey", "myval")
                .configure("password", "mypassword"));
    
        // "full" means including-inherited, filtered to exclude secrets
        LocationSummary summaryFull = client().path("/locations/"+loc.getId()).query("full","true").get(LocationSummary.class);
        assertEquals(summaryFull.getConfig(), ImmutableMap.of("mykey", "myval", "myParentKey", "myParentVal"), "conf="+summaryFull.getConfig());
        
        // Default is local-only, filtered to exclude secrets
        URI uriDefault = URI.create("/locations/"+loc.getId());
        LocationSummary summaryDefault = client().path(uriDefault).get(LocationSummary.class);
        assertEquals(summaryDefault.getConfig(), ImmutableMap.of("mykey", "myval"), "conf="+summaryDefault.getConfig());
    }

    @Test(dependsOnMethods = { "testAddLegacyLocationDefinition" })
    @Deprecated
    public void testDeleteLocation() {
        final int size = getLocationRegistry().getDefinedLocations().size();
        URI expectedLocationUri = URI.create("/locations/"+legacyLocationName);

        Response response = client().path(expectedLocationUri).delete();
        assertEquals(response.getStatus(), Response.Status.NO_CONTENT.getStatusCode());
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(getLocationRegistry().getDefinedLocations().size(), size - 1);
            }
        });
    }


    @SuppressWarnings("deprecation")
    @Test
    public void testDisplayNameInConfig() {
        String symbolicName = "test_config_displayName_id";
        String yaml = Joiner.on("\n").join(ImmutableList.of(
                "brooklyn.catalog:",
                "  version: " + locationVersion,
                "  items:",
                "  - id: " + symbolicName,
                "    itemType: location",
                "    item:",
                "      type: byon:(hosts=\"" + byonHostname + "\")",
                "      brooklyn.config:",
                "        displayName: " + configDisplayName));

        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        URI addedCatalogItemUri = response.getLocation();
        log.info("added, at: " + addedCatalogItemUri);

        // Ensure location definition exists
        CatalogLocationSummary locationItem = client().path("/catalog/locations/"+symbolicName + "/" + locationVersion)
                .get(CatalogLocationSummary.class);
        log.info(" item: " + locationItem);
        LocationSummary locationSummary = client().path(URI.create("/locations/"+symbolicName+"/")).get(LocationSummary.class);
        log.info(" summary: " + locationSummary);
        Assert.assertEquals(locationSummary.getConfig().get(LocationConfigKeys.DISPLAY_NAME.getName()), configDisplayName);

        FixedListMachineProvisioningLocation<?> l = (FixedListMachineProvisioningLocation<?>) getManagementContext().getLocationRegistry().getLocationManaged(symbolicName);
        Assert.assertEquals(l.getDisplayName(), configDisplayName);
    }


    @SuppressWarnings("deprecation")
    @Test
    public void testDisplayNameInItems() {
        String symbolicName = "test_items_displayName_id";
        String yaml = Joiner.on("\n").join(ImmutableList.of(
                "brooklyn.catalog:",
                "  version: " + locationVersion,
                "  items:",
                "  - id: " + symbolicName,
                "    itemType: location",
                "    displayName: " + testsDisplayName,
                "    item:",
                "      type: byon:(hosts=\"" + byonHostname + "\")"));

        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        URI addedCatalogItemUri = response.getLocation();
        log.info("added, at: " + addedCatalogItemUri);

        // Ensure location definition exists
        CatalogLocationSummary locationItem = client().path("/catalog/locations/"+symbolicName + "/" + locationVersion)
                .get(CatalogLocationSummary.class);
        log.info(" item: " + locationItem);
        LocationSummary locationSummary = client().path(URI.create("/locations/"+symbolicName+"/")).get(LocationSummary.class);
        log.info(" summary: " + locationSummary);
        Assert.assertEquals(locationSummary.getConfig().get(LocationConfigKeys.DISPLAY_NAME.getName()), testsDisplayName);

        FixedListMachineProvisioningLocation<?> l = (FixedListMachineProvisioningLocation<?>) getManagementContext().getLocationRegistry().getLocationManaged(symbolicName);
        Assert.assertEquals(l.getDisplayName(), testsDisplayName);
    }


    @SuppressWarnings("deprecation")
    @Test
    public void testDisplayNameInConfigOverridesItems() {
        String symbolicName = "test_config_overrides_items_displayName_id";
        String yaml = Joiner.on("\n").join(ImmutableList.of(
                "brooklyn.catalog:",
                "  version: " + locationVersion,
                "  items:",
                "  - id: " + symbolicName,
                "    itemType: location",
                "    displayName: " + testsDisplayName,
                "    item:",
                "      type: byon:(hosts=\"" + byonHostname + "\")",
                "      brooklyn.config:",
                "        displayName: " + configDisplayName));

        Response response = client().path("/catalog")
                .post(yaml);

        assertEquals(response.getStatus(), Response.Status.CREATED.getStatusCode());

        URI addedCatalogItemUri = response.getLocation();
        log.info("added, at: " + addedCatalogItemUri);

        // Ensure location definition exists
        CatalogLocationSummary locationItem = client().path("/catalog/locations/"+symbolicName + "/" + locationVersion)
                .get(CatalogLocationSummary.class);
        log.info(" item: " + locationItem);
        LocationSummary locationSummary = client().path(URI.create("/locations/"+symbolicName+"/")).get(LocationSummary.class);
        log.info(" summary: " + locationSummary);
        Assert.assertEquals(locationSummary.getConfig().get(LocationConfigKeys.DISPLAY_NAME.getName()), configDisplayName);

        FixedListMachineProvisioningLocation<?> l = (FixedListMachineProvisioningLocation<?>) getManagementContext().getLocationRegistry().getLocationManaged(symbolicName);
        Assert.assertEquals(l.getDisplayName(), configDisplayName);
    }
}
