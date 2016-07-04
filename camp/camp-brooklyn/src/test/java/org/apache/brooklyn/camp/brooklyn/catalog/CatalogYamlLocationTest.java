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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.text.StringFunctions;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class CatalogYamlLocationTest extends AbstractYamlTest {
    private static final String LOCALHOST_LOCATION_SPEC = "localhost";
    private static final String LOCALHOST_LOCATION_TYPE = LocalhostMachineProvisioningLocation.class.getName();
    private static final String SIMPLE_LOCATION_TYPE = "org.apache.brooklyn.test.osgi.entities.SimpleLocation";

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @AfterMethod
    public void tearDown() {
        for (RegisteredType ci : mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.IS_LOCATION)) {
            mgmt().getCatalog().deleteCatalogItem(ci.getSymbolicName(), ci.getVersion());
        }
    }
    
    @Test
    public void testAddCatalogItem() throws Exception {
        assertEquals(countCatalogLocations(), 0);

        String symbolicName = "my.catalog.location.id.load";
        addCatalogLocation(symbolicName, LOCALHOST_LOCATION_TYPE, null);
        assertAdded(symbolicName, LOCALHOST_LOCATION_TYPE);
        removeAndAssert(symbolicName);
    }

    @Test
    public void testAddCatalogItemOsgi() throws Exception {
        assertEquals(countCatalogLocations(), 0);

        String symbolicName = "my.catalog.location.id.load";
        addCatalogLocation(symbolicName, SIMPLE_LOCATION_TYPE, getOsgiLibraries());
        assertAdded(symbolicName, SIMPLE_LOCATION_TYPE);
        assertOsgi(symbolicName);
        removeAndAssert(symbolicName);
    }

    @Test
    public void testAddCatalogItemLegacySyntax() throws Exception {
        assertEquals(countCatalogLocations(), 0);

        String symbolicName = "my.catalog.location.id.load";
        addCatalogLocationLegacySyntax(symbolicName, LOCALHOST_LOCATION_TYPE, null);
        assertAdded(symbolicName, LOCALHOST_LOCATION_TYPE);
        removeAndAssert(symbolicName);
    }

    @Test
    public void testAddCatalogItemOsgiLegacySyntax() throws Exception {
        assertEquals(countCatalogLocations(), 0);

        String symbolicName = "my.catalog.location.id.load";
        addCatalogLocationLegacySyntax(symbolicName, SIMPLE_LOCATION_TYPE, getOsgiLibraries());
        assertAdded(symbolicName, SIMPLE_LOCATION_TYPE);
        assertOsgi(symbolicName);
        removeAndAssert(symbolicName);
    }

    private void assertOsgi(String symbolicName) {
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        Collection<OsgiBundleWithUrl> libs = item.getLibraries();
        assertEquals(libs.size(), 1);
        assertEquals(Iterables.getOnlyElement(libs).getUrl(), Iterables.getOnlyElement(getOsgiLibraries()));
    }

    @SuppressWarnings({ "rawtypes" })
    private void assertAdded(String symbolicName, String expectedJavaType) {
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);
        Assert.assertTrue(RegisteredTypes.isSubtypeOf(item, Location.class), "Expected Location, not "+item.getSuperTypes());
        assertEquals(countCatalogLocations(), 1);

        // Item added to catalog should automatically be available in location registry
        LocationDefinition def = mgmt().getLocationRegistry().getDefinedLocationByName(symbolicName);
        assertEquals(def.getId(), symbolicName);
        assertEquals(def.getName(), symbolicName);
        
        LocationSpec spec = (LocationSpec) mgmt().getTypeRegistry().createSpec(item, null, LocationSpec.class);
        assertEquals(spec.getType().getName(), expectedJavaType);
    }
    
    private void removeAndAssert(String symbolicName) {
        // Deleting item: should be gone from catalog, and from location registry
        deleteCatalogEntity(symbolicName);

        assertEquals(countCatalogLocations(), 0);
        assertNull(mgmt().getLocationRegistry().getDefinedLocationByName(symbolicName));
    }

    @Test
    public void testLaunchApplicationReferencingLocationClass() throws Exception {
        String symbolicName = "my.catalog.location.id.launch";
        addCatalogLocation(symbolicName, LOCALHOST_LOCATION_TYPE, null);
        runLaunchApplicationReferencingLocation(symbolicName, LOCALHOST_LOCATION_TYPE);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingLocationSpec() throws Exception {
        String symbolicName = "my.catalog.location.id.launch";
        addCatalogLocation(symbolicName, LOCALHOST_LOCATION_SPEC, null);
        runLaunchApplicationReferencingLocation(symbolicName, LOCALHOST_LOCATION_TYPE);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingLocationClassLegacySyntax() throws Exception {
        String symbolicName = "my.catalog.location.id.launch";
        addCatalogLocationLegacySyntax(symbolicName, LOCALHOST_LOCATION_TYPE, null);
        runLaunchApplicationReferencingLocation(symbolicName, LOCALHOST_LOCATION_TYPE);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingLocationSpecLegacySyntax() throws Exception {
        String symbolicName = "my.catalog.location.id.launch";
        addCatalogLocationLegacySyntax(symbolicName, LOCALHOST_LOCATION_SPEC, null);
        runLaunchApplicationReferencingLocation(symbolicName, LOCALHOST_LOCATION_TYPE);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingOsgiLocation() throws Exception {
        String symbolicName = "my.catalog.location.id.launch";
        addCatalogLocation(symbolicName, SIMPLE_LOCATION_TYPE, getOsgiLibraries());
        runLaunchApplicationReferencingLocation(symbolicName, SIMPLE_LOCATION_TYPE);
        
        deleteCatalogEntity(symbolicName);
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-248
    @Test
    public void testTypeInheritance() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  items:",
                "  - id: loc1",
                "    name: My Loc 1",
                "    item:",
                "      type: localhost",
                "      brooklyn.config:",
                "        mykey1: myval1",
                "        mykey1b: myval1b",
                "  - id: loc2",
                "    name: My Loc 2",
                "    item:",
                "      type: loc1",
                "      brooklyn.config:",
                "        mykey1: myvalOverridden",
                "        mykey2: myval2");
        
        addCatalogItems(yaml);

        Map<String, LocationDefinition> defs = mgmt().getLocationRegistry().getDefinedLocations();
        LocationDefinition def1 = checkNotNull(defs.get("loc1"), "loc1 missing; has %s", defs.keySet());
        LocationDefinition def2 = checkNotNull(defs.get("loc2"), "loc2 missing; has %s", defs.keySet());
        LocationSpec<? extends Location> spec1 = mgmt().getLocationRegistry().getLocationSpec(def1).get();
        LocationSpec<? extends Location> spec2 = mgmt().getLocationRegistry().getLocationSpec(def2).get();
        
        assertEquals(spec1.getCatalogItemId(), "loc1:0.1.2");
        assertEquals(spec1.getDisplayName(), "My Loc 1");
        assertContainsAll(spec1.getFlags(), ImmutableMap.of("mykey1", "myval1", "mykey1b", "myval1b"));
        assertEquals(spec2.getCatalogItemId(), "loc2:0.1.2");
        assertEquals(spec2.getDisplayName(), "My Loc 2");
        assertContainsAll(spec2.getFlags(), ImmutableMap.of("mykey1", "myvalOverridden", "mykey1b", "myval1b", "mykey2", "myval2"));
    }
    
    // TODO Debatable whether loc1/loc2 should use "My name within item" instead (which was Aled's 
    // initial expectation). That would be more consistent with the way entities behave - see
    // {@link ApplicationYamlTest#testNamePrecedence()}.
    // See discussion in https://issues.apache.org/jira/browse/BROOKLYN-248.
    @Test
    public void testNamePrecedence() throws Exception {
        String yaml1 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  name: My name in top-level metadata",
                "  items:",
                "  - id: loc1",
                "    name: My name in item metadata",
                "    item:",
                "      type: localhost",
                "      name: My name within item");

        String yaml2 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  name: My name in top-level metadata",
                "  items:",
                "  - id: loc2",
                "    item:",
                "      type: localhost",
                "      name: My name within item");

        String yaml3 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  items:",
                "  - id: loc3a",
                "    name: My name in item 3a metadata",
                "    item:",
                "      type: localhost",
                "      name: My name within item 3a",
                "  items:",
                "  - id: loc3b",
                "    item:",
                "      type: loc3a",
                "      name: My name within item 3b");
        
        addCatalogItems(yaml1);
        addCatalogItems(yaml2);
        addCatalogItems(yaml3);

        LocationDefinition def1 = mgmt().getLocationRegistry().getDefinedLocations().get("loc1");
        LocationSpec<? extends Location> spec1 = mgmt().getLocationRegistry().getLocationSpec(def1).get();
        assertEquals(spec1.getDisplayName(), "My name in item metadata");
        
        LocationDefinition def2 = mgmt().getLocationRegistry().getDefinedLocations().get("loc2");
        LocationSpec<? extends Location> spec2 = mgmt().getLocationRegistry().getLocationSpec(def2).get();
        assertEquals(spec2.getDisplayName(), "My name in top-level metadata");
        
        LocationDefinition def3b = mgmt().getLocationRegistry().getDefinedLocations().get("loc3b");
        LocationSpec<? extends Location> spec3b = mgmt().getLocationRegistry().getLocationSpec(def3b).get();
        assertEquals(spec3b.getDisplayName(), "My name within item 3b");
    }
    
    @Test
    public void testNameInCatalogMetadata() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  name: My name in top-level",
                "  items:",
                "  - id: loc1",
                "    item:",
                "      type: localhost");
        
        addCatalogItems(yaml);

        LocationDefinition def = mgmt().getLocationRegistry().getDefinedLocations().get("loc1");
        LocationSpec<? extends Location> spec = mgmt().getLocationRegistry().getLocationSpec(def).get();
        assertEquals(spec.getDisplayName(), "My name in top-level");
    }
    
    @Test
    public void testNameInItemMetadata() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  items:",
                "  - id: loc1",
                "    name: My name in item metadata",
                "    item:",
                "      type: localhost");
        
        addCatalogItems(yaml);

        LocationDefinition def = mgmt().getLocationRegistry().getDefinedLocations().get("loc1");
        LocationSpec<? extends Location> spec = mgmt().getLocationRegistry().getLocationSpec(def).get();
        assertEquals(spec.getDisplayName(), "My name in item metadata");
    }
    
    @Test
    public void testNameWithinItem() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: location",
                "  items:",
                "  - id: loc1",
                "    item:",
                "      type: localhost",
                "      name: My name within item");
        
        addCatalogItems(yaml);

        LocationDefinition def = mgmt().getLocationRegistry().getDefinedLocations().get("loc1");
        LocationSpec<? extends Location> spec = mgmt().getLocationRegistry().getLocationSpec(def).get();
        assertEquals(spec.getDisplayName(), "My name within item");
    }
    
    // TODO Can move to common test utility, if proves more generally useful
    public static void assertContainsAll(Map<?,?> actual, Map<?,?> expectedSubset) {
        String errMsg = "actual="+actual+"; expetedSubset="+expectedSubset;
        for (Map.Entry<?, ?> entry : expectedSubset.entrySet()) {
            assertTrue(actual.containsKey(entry.getKey()), errMsg);
            assertEquals(actual.get(entry.getKey()), entry.getValue(), errMsg);
        }
    }
    
    protected void runLaunchApplicationReferencingLocation(String locTypeInYaml, String locType) throws Exception {
        Entity app = createAndStartApplication(
            "name: simple-app-yaml",
            "location: ",
            "  "+locTypeInYaml+":",
            "    config2: config2 override",
            "    config3: config3",
            "services: ",
            "  - type: org.apache.brooklyn.entity.stock.BasicStartable");

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        Location location = Iterables.getOnlyElement(Entities.getAllInheritedLocations(simpleEntity));
        assertEquals(location.getClass().getName(), locType);
        assertEquals(location.getConfig(new BasicConfigKey<String>(String.class, "config1")), "config1");
        assertEquals(location.getConfig(new BasicConfigKey<String>(String.class, "config2")), "config2 override");
        assertEquals(location.getConfig(new BasicConfigKey<String>(String.class, "config3")), "config3");
    }

    private List<String> getOsgiLibraries() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        return ImmutableList.of(OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL);
    }
    
    private void addCatalogLocation(String symbolicName, String locationType, List<String> libraries) {
        ImmutableList.Builder<String> yaml = ImmutableList.<String>builder().add(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: location",
                "  name: My Catalog Location",
                "  description: My description");
        if (libraries!=null && libraries.size() > 0) {
            yaml.add("  libraries:")
                .addAll(Lists.transform(libraries, StringFunctions.prepend("  - url: ")));
        }
        yaml.add(
                "  item:",
                "    type: " + locationType,
                "    brooklyn.config:",
                "      config1: config1",
                "      config2: config2");
        
        
        addCatalogItems(yaml.build());
    }

    private void addCatalogLocationLegacySyntax(String symbolicName, String locationType, List<String> libraries) {
        ImmutableList.Builder<String> yaml = ImmutableList.<String>builder().add(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  name: My Catalog Location",
                "  description: My description",
                "  version: " + TEST_VERSION);
        if (libraries!=null && libraries.size() > 0) {
            yaml.add("  libraries:")
                .addAll(Lists.transform(libraries, StringFunctions.prepend("  - url: ")));
        }
        yaml.add(
                "",
                "brooklyn.locations:",
                "- type: " + locationType,
                "  brooklyn.config:",
                "    config1: config1",
                "    config2: config2");
        
        
        addCatalogItems(yaml.build());
    }

    private int countCatalogLocations() {
        return Iterables.size(mgmt().getTypeRegistry().getMatching(RegisteredTypePredicates.IS_LOCATION));
    }

    @Test
    public void testManagedLocationsCreateAndCleanup() {
        assertLocationRegistryCount(0);
        assertLocationManagerInstancesCount(0);
        assertCatalogCount(0);
        
        String symbolicName = "lh1";
        addCatalogLocation(symbolicName, LOCALHOST_LOCATION_TYPE, null);

        assertLocationRegistryCount(1);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(0);

        Location loc = mgmt().getLocationRegistry().getLocationManaged("lh1");

        assertLocationRegistryCount(1);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(1);

        mgmt().getLocationManager().unmanage(loc);
        

        assertLocationRegistryCount(1);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(0);

        deleteCatalogEntity("lh1");
        
        assertLocationRegistryCount(0);
        assertCatalogCount(0);
        assertLocationManagerInstancesCount(0);
    }
    
    private void assertLocationRegistryCount(int size) {
        Asserts.assertThat(mgmt().getLocationRegistry().getDefinedLocations().keySet(), CollectionFunctionals.sizeEquals(size));
    }
    private void assertLocationManagerInstancesCount(int size) {
        Asserts.assertThat(mgmt().getLocationManager().getLocations(), CollectionFunctionals.sizeEquals(size));
    }
    private void assertCatalogCount(int size) {
        Asserts.assertThat(mgmt().getCatalog().getCatalogItems(), CollectionFunctionals.sizeEquals(size));
    }
    
    @Test
    public void testLocationPartOfBlueprintDoesntLeak() {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  item:",
            "    type: "+ BasicEntity.class.getName(),
            "    location:",
            "      jclouds:aws-ec2: { identity: ignore, credential: ignore }"
            );
        
        assertLocationRegistryCount(0);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(0);
    }

    @Test
    public void testByonLocationHostsInConfig() {
        String symbolicName = "my.catalog.app.id.byon.config";
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + symbolicName,
                "    itemType: location",
                "    item:",
                "      type: byon",
                "      brooklyn.config:",
                "        displayName: testingdisplayName",
                "        user: testinguser",
                "        password: testingpassword",
                "        hosts:",
                "        - 10.10.10.102"
        );

        assertLocationRegistryCount(1);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(0);
    }

    @Test
    public void testByonLocationHostsInType() {
        String symbolicName = "my.catalog.app.id.byon.config.inline";
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + symbolicName,
                "    itemType: location",
                "    item:",
                "      type: byon:(hosts=\"10.10.10.102\")",
                "      brooklyn.config:",
                "        displayName: testingdisplayName",
                "        user: testinguser",
                "        password: testingpassword"
        );

        assertLocationRegistryCount(1);
        assertCatalogCount(1);
        assertLocationManagerInstancesCount(0);
    }

}
