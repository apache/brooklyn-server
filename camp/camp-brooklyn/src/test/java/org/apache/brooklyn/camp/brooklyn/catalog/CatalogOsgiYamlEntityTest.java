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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.InputStream;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;

public class CatalogOsgiYamlEntityTest extends AbstractYamlTest {
    
    // Some of these testes duplicate several of the non-osgi test. However, that is important 
    // because there are subtleties of which OSGi bundles a catalog item will use for loading,
    // particularly when nesting and/or sub-typing entities.
    //
    // The non-osgi tests are much faster to run!

    private static final String SIMPLE_ENTITY_TYPE = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;
   private static final String MORE_ENTITIES_POM_PROPERTIES_PATH =
      "META-INF/maven/org.apache.brooklyn.test.resources.osgi/brooklyn-test-osgi-more-entities/pom.properties";

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testAddOsgiCatalogItem() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.app.id.load";
        addCatalogOSGiEntity(symbolicName);
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingCatalog() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.app.id.launch";
        registerAndLaunchAndAssertSimpleEntity(symbolicName, SIMPLE_ENTITY_TYPE);
    }

    @Test
    public void testLaunchApplicationWithCatalogReferencingOtherCatalog() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String referencedSymbolicName = "my.catalog.app.id.referenced";
        String referrerSymbolicName = "my.catalog.app.id.referring";
        addCatalogOSGiEntity(referencedSymbolicName, SIMPLE_ENTITY_TYPE);
        addCatalogEntity(referrerSymbolicName, ver(referencedSymbolicName));

        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver(referrerSymbolicName));

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(simpleEntity.getEntityType().getName(), SIMPLE_ENTITY_TYPE);

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }

    @Test
    public void testLaunchApplicationChildWithCatalogReferencingOtherCatalog() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String referencedSymbolicName = "my.catalog.app.id.child.referenced";
        String referrerSymbolicName = "my.catalog.app.id.child.referring";
        addCatalogOSGiEntity(referencedSymbolicName, SIMPLE_ENTITY_TYPE);
        addCatalogChildEntity(referrerSymbolicName, ver(referencedSymbolicName));

        Entity app = createAndStartApplication(
            "name: simple-app-yaml",
            "location: localhost",
            "services:",
            "- type: "+BasicEntity.class.getName(),
            "  brooklyn.children:",
            "  - type: " + ver(referrerSymbolicName));

        Entity child = Iterables.getOnlyElement(app.getChildren());
        assertEquals(child.getEntityType().getName(), BasicEntity.class.getName());
        Entity grandChild = Iterables.getOnlyElement(child.getChildren());
        assertEquals(grandChild.getEntityType().getName(), BasicEntity.class.getName());
        Entity grandGrandChild = Iterables.getOnlyElement(grandChild.getChildren());
        assertEquals(grandGrandChild.getEntityType().getName(), SIMPLE_ENTITY_TYPE);

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }

    @Test
    public void testLaunchApplicationChildWithCatalogReferencingOtherCatalogServicesBlock() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String referencedSymbolicName = "my.catalog.app.id.child.referenced";
        String referrerSymbolicName = "my.catalog.app.id.child.referring";
        addCatalogOSGiEntity(referencedSymbolicName, SIMPLE_ENTITY_TYPE);
        addCatalogChildOSGiEntityWithServicesBlock(referrerSymbolicName, ver(referencedSymbolicName));

        Entity app = createAndStartApplication(
            "services:",
            "- type: "+BasicEntity.class.getName(),
            "  brooklyn.children:",
            "  - type: " + ver(referrerSymbolicName));

        Entity child = Iterables.getOnlyElement(app.getChildren());
        assertEquals(child.getEntityType().getName(), BasicEntity.class.getName());
        Entity grandChild = Iterables.getOnlyElement(child.getChildren());
        assertEquals(grandChild.getEntityType().getName(), BasicEntity.class.getName());
        Entity grandGrandChild = Iterables.getOnlyElement(grandChild.getChildren());
        assertEquals(grandGrandChild.getEntityType().getName(), SIMPLE_ENTITY_TYPE);

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }
    
    @Test
    public void testLaunchApplicationWithTypeUsingJavaColonPrefix() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = SIMPLE_ENTITY_TYPE;
        String serviceName = "java:"+SIMPLE_ENTITY_TYPE;
        registerAndLaunchAndAssertSimpleEntity(symbolicName, serviceName);
    }

    @Test
    public void testLaunchApplicationLoopWithJavaTypeName() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = SIMPLE_ENTITY_TYPE;
        String serviceName = SIMPLE_ENTITY_TYPE;
        registerAndLaunchAndAssertSimpleEntity(symbolicName, serviceName);
    }

    @Test
    public void testReferenceInstalledBundleByName() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String firstItemId = "my.catalog.app.id.register_bundle";
        String secondItemId = "my.catalog.app.id.reference_bundle";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + firstItemId,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    type: " + SIMPLE_ENTITY_TYPE);
        deleteCatalogEntity(firstItemId);

        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + secondItemId,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  libraries:",
            "  - name: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_NAME,
            "    version: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_VERSION,
            "  item:",
            "    type: " + SIMPLE_ENTITY_TYPE);

        deleteCatalogEntity(secondItemId);
    }

    @Test
    public void testReferenceNonInstalledBundledByNameFails() {
        String nonExistentId = "none-existent-id";
        String nonExistentVersion = "9.9.9";
        try {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: my.catalog.app.id.non_existing.ref",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  libraries:",
                "  - name: " + nonExistentId,
                "    version: " + nonExistentVersion,
                "  item:",
                "    type: " + SIMPLE_ENTITY_TYPE);
            fail();
        } catch (IllegalStateException e) {
            Assert.assertEquals(e.getMessage(), "Bundle from null failed to install: Bundle CatalogBundleDto{symbolicName=" + nonExistentId + ", version=" + nonExistentVersion + ", url=null} not previously registered, but URL is empty.");
        }
    }

    @Test
    public void testPartialBundleReferenceFails() {
        try {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: my.catalog.app.id.non_existing.ref",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  libraries:",
                "  - name: io.brooklyn.brooklyn-test-osgi-entities",
                "  item:",
                "    type: " + SIMPLE_ENTITY_TYPE);
            fail();
        } catch (NullPointerException e) {
            Assert.assertEquals(e.getMessage(), "both name and version are required");
        }
        try {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: my.catalog.app.id.non_existing.ref",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  libraries:",
                "  - version: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_VERSION,
                "  item:",
                "    type: " + SIMPLE_ENTITY_TYPE);
            fail();
        } catch (NullPointerException e) {
            Assert.assertEquals(e.getMessage(), "both name and version are required");
        }
    }

    @Test
    public void testFullBundleReference() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String itemId = "my.catalog.app.id.full_ref";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + itemId,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  libraries:",
            "  - name: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_NAME,
            "    version: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_VERSION,
            "    url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    type: " + SIMPLE_ENTITY_TYPE);
        deleteCatalogEntity(itemId);
    }

    /**
     * Test that the name:version contained in the OSGi bundle will
     * override the values supplied in the YAML.
     */
    @Test
    public void testFullBundleReferenceUrlMetaOverridesLocalNameVersion() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String firstItemId = "my.catalog.app.id.register_bundle";
        String nonExistentId = "non_existent_id";
        String nonExistentVersion = "9.9.9";
        try {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + firstItemId,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  libraries:",
                "  - name: " + nonExistentId,
                "    version: " + nonExistentVersion,
                "    url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
                "",
                "  item:",
                "    type: " + SIMPLE_ENTITY_TYPE);
            fail();
        } catch (IllegalStateException e) {
            assertEquals(e.getMessage(), "Bundle from " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL + " failed to install: " +
                    "Bundle already installed as " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_NAME + ":" +
                    OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_VERSION + " but user explicitly requested " +
                    "CatalogBundleDto{symbolicName=" + nonExistentId + ", version=" + nonExistentVersion + ", url=" +
                    OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL + "}");
        }
    }

    @Test
    public void testUpdatingItemAllowedIfSame() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String id = "my.catalog.app.id.duplicate";
        addCatalogOSGiEntity(id);
        addCatalogOSGiEntity(id);
    }
    
    @Test(expectedExceptions = IllegalStateException.class)
    public void testUpdatingItemFailsIfDifferent() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String id = "my.catalog.app.id.duplicate";
        addCatalogOSGiEntity(id);
        addCatalogOSGiEntity(id, SIMPLE_ENTITY_TYPE, true);
    }

    @Test
    public void testForcedUpdatingItem() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String id = "my.catalog.app.id.duplicate";
        addCatalogOSGiEntity(id);
        forceCatalogUpdate();
        addCatalogOSGiEntity(id);
        deleteCatalogEntity(id);
    }

    @Test
    public void testCreateSpecFromCatalogItem() {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String id = "my.catalog.app.id.create_spec";
        addCatalogOSGiEntity(id);
        BrooklynTypeRegistry catalog = mgmt().getTypeRegistry();
        RegisteredType item = catalog.get(id, TEST_VERSION);
        EntitySpec<?> spec = catalog.createSpec(item, null, EntitySpec.class);
        Assert.assertNotNull(spec);
        AbstractBrooklynObjectSpec<?,?> spec2 = catalog.createSpec(item, null, null);
        Assert.assertNotNull(spec2);
    }
    
    @Test
    public void testLoadResourceFromBundle() throws Exception {
        String id = "resource.test";
        addCatalogOSGiEntity(id, SIMPLE_ENTITY_TYPE);
        String yaml =
                "services: \n" +
                "  - serviceType: "+ver(id);
        Entity app = createAndStartApplication(yaml);
        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        InputStream icon = new ResourceUtils(simpleEntity).getResourceFromUrl("classpath:/org/apache/brooklyn/test/osgi/entities/icon.gif");
        assertTrue(icon != null);
        icon.close();
    }
    
    @Test
    public void testMissingTypeDoesNotRecurse() {
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: org.apache.brooklyn.entity.stock.BasicEntity");

        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + symbolicName,
                    "  version: " + TEST_VERSION + "-update",
                    "  itemType: entity",
                    "  item:",
                    "    type: " + symbolicName);
            fail("Catalog addition expected to fail due to non-existent java type " + symbolicName);
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }
    
    @Test
    public void testVersionedTypeDoesNotRecurse() {
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: org.apache.brooklyn.entity.stock.BasicEntity");

        String versionedId = CatalogUtils.getVersionedId(symbolicName, TEST_VERSION);
        try {
            addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION + "-update",
                "  itemType: entity",
                "  item:",
                "    type: " + versionedId);
            fail("Catalog addition expected to fail due to non-existent java type " + versionedId);
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testIndirectRecursionFails() throws Exception {
        String symbolicName = "my.catalog.app.id.basic";
        // Need to have a stand alone caller first so we can create an item to depend on it.
        // After that replace it/insert a new version which completes the cycle
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName + ".caller",
                "  version: " + TEST_VERSION + "pre",
                "  itemType: entity",
                "  item:",
                "    type: "+BasicEntity.class.getName());

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName + ".callee",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + symbolicName + ".caller");

        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + symbolicName + ".caller",
                    "  version: " + TEST_VERSION,
                    "  itemType: entity",
                    "  item:",
                    "    type: " + symbolicName + ".callee");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testChildItemsDoNotRecurse() throws Exception {
        String symbolicName = "my.catalog.app.id.basic";
        // Need to have a stand alone caller first so we can create an item to depend on it.
        // After that replace it/insert a new version which completes the cycle
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName + ".caller",
                "  version: " + TEST_VERSION + "pre",
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.stock.BasicEntity");

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName + ".callee",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + symbolicName + ".caller");

        try {
            // TODO Only passes if include "services:" and if itemType=entity, rather than "template"!
            // Being a child is important, triggers the case where: we allow retrying with other transformers.
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + symbolicName + ".caller",
                    "  version: " + TEST_VERSION,
                    "  itemType: entity",
                    "  item:",
                    "    services:",
                    "    - type: org.apache.brooklyn.entity.stock.BasicEntity",
                    "      brooklyn.children:",
                    "      - type: " + symbolicName + ".callee");
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testRecursiveCheckForDepenentsOnly() throws Exception {
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + symbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: org.apache.brooklyn.entity.stock.BasicEntity");

        createAndStartApplication(
                "services:",
                "- type: " + ver(symbolicName),
                "  brooklyn.children:",
                "  - type: " + ver(symbolicName),
                "- type: " + ver(symbolicName),
                "  brooklyn.children:",
                "  - type: " + ver(symbolicName));
    }

    @Test
    public void testOsgiNotLeakingToParent() {
        addCatalogOSGiEntity(SIMPLE_ENTITY_TYPE);
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + SIMPLE_ENTITY_TYPE,
                    "  version: " + TEST_VERSION + "-update",
                    "  itemType: entity",
                    "  item:",
                    "    type: " + SIMPLE_ENTITY_TYPE);
            fail("Catalog addition expected to fail due to non-existent java type " + SIMPLE_ENTITY_TYPE);
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testConfigAppliedToCatalogItem() throws Exception {
        addCatalogOSGiEntity("test", TestEntity.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.config:",
                "    test.confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }

    @Test
    public void testFlagsAppliesToCatalogItem() throws Exception {
        addCatalogOSGiEntity("test", TestEntity.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }

    @Test
    public void testExplicitFlagsAppliesToCatalogItem() throws Exception {
        addCatalogOSGiEntity("test", TestEntity.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.flags:",
                "    confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }
    

    @Test
    public void testConfigAppliedToCatalogItemImpl() throws Exception {
        addCatalogOSGiEntity("test", TestEntityImpl.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.config:",
                "    test.confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }

    @Test
    public void testFlagsAppliesToCatalogItemImpl() throws Exception {
        addCatalogOSGiEntity("test", TestEntityImpl.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }

    @Test
    public void testExplicitFlagsAppliesToCatalogItemImpl() throws Exception {
        addCatalogOSGiEntity("test", TestEntityImpl.class.getName());
        String testName = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.flags:",
                "    confName: " + testName);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), testName);
    }

    @Test
    public void testHardcodedCatalog() throws Exception {
        createAppEntitySpec(
                "services:",
                "- type: cluster",
                "- type: vanilla");
    }
    
    @Test(groups = "Broken")
    public void testSameCatalogReferences() {
        addCatalogItems(
            "brooklyn.catalog:",
            "  items:",
            "  - id: referenced-entity",
            "    item:",
            "      services:",
            "      - type: " + BasicEntity.class.getName(),
            "  - id: referrer-entity",
            "    item:",
            "      services:",
            "      - type: " + BasicApplication.class.getName(),
            "        brooklyn.children:",
            "        - type: referenced-entity",
            "        brooklyn.config:",
            "          spec: ",
            "            $brooklyn:entitySpec:",
            "              type: referenced-entity");

    }

    @Test
    public void testItemWithBrooklynParameters() throws Exception {
        String id = "inline_version.app";
        String version = TEST_VERSION;
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + id,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    brooklyn.parameters:",
                "    - name: test.myconf",
                "      type:  string",
                "      default: myval",
                "    brooklyn.config:",
                "      myconf2: $brooklyn:config(\"test.myconf\")",
                "      myconf2.from.root: $brooklyn:root().config(\"test.myconf\")",
                "    brooklyn.children:",
                "    - type: "+BasicEntity.class.getName(),
                "      brooklyn.config:",
                "        myconf3: $brooklyn:config(\"test.myconf\")",
                "        myconf3.from.root: $brooklyn:root().config(\"test.myconf\")");

        RegisteredType catalogItem = mgmt().getTypeRegistry().get(id, version);
        assertEquals(catalogItem.getVersion(), version);
        
        String yaml = Joiner.on("\n").join(
                "name: simple-app-yaml",
                "location: localhost",
                "services:",
                "  - type: "+id+":"+version);
        Entity app = createAndStartApplication(yaml);
        Entity child = Iterables.getOnlyElement(app.getChildren());
        ConfigKey<?> configKey = app.getEntityType().getConfigKey("test.myconf");
        assertNotNull(configKey);
        assertEquals(app.config().get(configKey), "myval");
        assertEquals(app.config().get(ConfigKeys.newStringConfigKey("myconf2.from.root")), "myval");
        assertEquals(child.config().get(ConfigKeys.newStringConfigKey("myconf3.from.root")), "myval");
        assertEquals(app.config().get(ConfigKeys.newStringConfigKey("myconf2")), "myval");
        
        // TODO Because of https://issues.apache.org/jira/browse/BROOKLYN-267, the assertion below fails: 
        // assertEquals(child.config().get(ConfigKeys.newStringConfigKey("myconf3")), "myval");
        
        mgmt().getCatalog().deleteCatalogItem(id, version);
    }

    @Test
    public void testCreateOsgiSpecFromRegistry() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicName = "my.catalog.app.id.registry.spec";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  version: " + TEST_VERSION,
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item: " + SIMPLE_ENTITY_TYPE);

        BrooklynTypeRegistry registry = mgmt().getTypeRegistry();
        RegisteredType item = registry.get(symbolicName, TEST_VERSION);
        AbstractBrooklynObjectSpec<?, ?> spec = registry.createSpec(item, null, null);
        assertEquals(spec.getOuterCatalogItemId(), ver(symbolicName));

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testIndirectCatalogItemCanLoadResources() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String symbolicNameInner = "my.catalog.app.id.inner";
        String symbolicNameOuter = "my.catalog.app.id.outer";
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: " + TEST_VERSION,
            "  items:",
            "  - id: " + symbolicNameInner,
            "    name: My Catalog App",
            "    description: My description",
            "    icon_url: classpath://path/to/myicon.jpg",
            "    libraries:",
            "    - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "    item: " + SIMPLE_ENTITY_TYPE,
            "  - id: " + symbolicNameOuter,
            "    item: " + symbolicNameInner);

        String yaml = "name: " + symbolicNameOuter + "\n" +
                "services: \n" +
                "  - serviceType: "+ver(symbolicNameOuter);
        Entity app = createAndStartApplication(yaml);
        Entity entity = app.getChildren().iterator().next();

        ResourceUtils.create(entity).getResourceAsString("classpath://yaml-ref-osgi-entity.yaml");

        deleteCatalogEntity(symbolicNameInner);
        deleteCatalogEntity(symbolicNameOuter);
    }


   @Test
   public void testDeepCatalogItemCanLoadResources() throws Exception {
      TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
      TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_MORE_ENTITIES_0_1_0_PATH);

      String symbolicNameInner = "my.catalog.app.id.inner";
      String symbolicNameFiller = "my.catalog.app.id.filler";
      String symbolicNameOuter = "my.catalog.app.id.outer";
      addCatalogItems(
         "brooklyn.catalog:",
         "  version: " + TEST_VERSION,
         "  items:",
         "  - id: " + symbolicNameInner,
         "    name: My Catalog App",
         "    brooklyn.libraries:",
         "    - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
         "    item: " + SIMPLE_ENTITY_TYPE,
         "  - id: " + symbolicNameFiller,
         "    name: Filler App",
         "    brooklyn.libraries:",
         "    - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_MORE_ENTITIES_0_1_0_URL,
         "    item: " + symbolicNameInner,
         "  - id: " + symbolicNameOuter,
         "    item: " + symbolicNameFiller);

      String yaml = "name: " + symbolicNameOuter + "\n" +
         "services: \n" +
         "  - serviceType: "+ver(symbolicNameOuter);
      Entity app = createAndStartApplication(yaml);
      Entity entity = app.getChildren().iterator().next();

      final String catalogBom = ResourceUtils.create(entity).getResourceAsString("classpath://" + MORE_ENTITIES_POM_PROPERTIES_PATH);
      assertTrue(catalogBom.contains("artifactId=brooklyn-test-osgi-more-entities"));

      deleteCatalogEntity(symbolicNameOuter);
      deleteCatalogEntity(symbolicNameFiller);
      deleteCatalogEntity(symbolicNameInner);
   }

   @Test
   public void testCatalogItemIdInReferencedItems() throws Exception {
      TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

      String symbolicNameInner = "my.catalog.app.id.inner";
      String symbolicNameOuter = "my.catalog.app.id.outer";
      addCatalogItems(
         "brooklyn.catalog:",
         "  version: " + TEST_VERSION,
         "  items:",
         "  - id: " + symbolicNameInner,
         "    name: My Catalog App",
         "    description: My description",
         "    icon_url: classpath://path/to/myicon.jpg",
         "    brooklyn.libraries:",
         "    - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
         "    item: " + SIMPLE_ENTITY_TYPE,
         "  - id: " + symbolicNameOuter,
         "    item: " + symbolicNameInner);

      String yaml = "name: " + symbolicNameOuter + "\n" +
         "services: \n" +
         "  - serviceType: "+ver(symbolicNameOuter);

      Entity app = createAndStartApplication(yaml);

      Entity entity = app.getChildren().iterator().next();
      assertEquals(entity.getCatalogItemId(), ver(symbolicNameOuter));
      assertEquals(entity.getCatalogItemHierarchy().size(), 2);
      assertEquals(entity.getCatalogItemHierarchy().get(0), ver(symbolicNameOuter));
      assertEquals(entity.getCatalogItemHierarchy().get(1), ver(symbolicNameInner));

      deleteCatalogEntity(symbolicNameInner);
      deleteCatalogEntity(symbolicNameOuter);
   }

    private void registerAndLaunchAndAssertSimpleEntity(String symbolicName, String serviceType) throws Exception {
        addCatalogOSGiEntity(symbolicName, serviceType);
        String yaml = "name: simple-app-yaml\n" +
                      "location: localhost\n" +
                      "services: \n" +
                      "  - serviceType: "+ver(symbolicName);
        Entity app = createAndStartApplication(yaml);

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(simpleEntity.getEntityType().getName(), SIMPLE_ENTITY_TYPE);

        deleteCatalogEntity(symbolicName);
    }

    private void addCatalogEntity(String symbolicName, String serviceType) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: " + serviceType);
    }

    private void addCatalogChildEntity(String symbolicName, String serviceType) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: " + BasicEntity.class.getName(),
            "    brooklyn.children:",
            "    - type: " + serviceType);
    }

    private void addCatalogOSGiEntity(String symbolicName) {
        addCatalogOSGiEntity(symbolicName, SIMPLE_ENTITY_TYPE);
    }

    private void addCatalogOSGiEntity(String symbolicName, String serviceType) {
        addCatalogOSGiEntity(symbolicName, serviceType, false);
    }
    
    private void addCatalogOSGiEntity(String symbolicName, String serviceType, boolean extraLib) {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_OSGI_TEST_A_0_1_0_PATH);

        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL +
            (extraLib ? "\n"+"  - url: "+OsgiStandaloneTest.BROOKLYN_OSGI_TEST_A_0_1_0_URL : ""),
            "  item:",
            "    type: " + serviceType);
    }

    private void addCatalogChildOSGiEntityWithServicesBlock(String symbolicName, String serviceType) {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    services:",
            "    - type: " + BasicEntity.class.getName(),
            "      brooklyn.children:",
            "      - type: " + serviceType);
    }
}
