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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.Iterables;


public class CatalogYamlEntityTest extends AbstractYamlTest {
    
    @Test
    public void testAddCatalogItemVerySimple() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), BasicEntity.class.getName());

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        String planYaml = RegisteredTypes.getImplementationDataStringForSpec(item);
        assertTrue(planYaml.indexOf("services:")>=0, "expected 'services:' block: "+item+"\n"+planYaml);

        deleteCatalogEntity(symbolicName);
    }

    // Legacy / backwards compatibility: should always specify itemType
    @Test
    public void testAddCatalogItemAsStringWithoutItemType() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  item: " + BasicEntity.class.getName());

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testAddCatalogItemTypeExplicitTypeAsString() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item: " + BasicEntity.class.getName());

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testAddCatalogItemLegacySyntax() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "",
            "services:",
            "- type: " + BasicEntity.class.getName());

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);

        deleteCatalogEntity(symbolicName);
    }

    // Legacy / backwards compatibility: should use id
    @Test
    public void testAddCatalogItemUsingNameInsteadOfIdWithoutVersion() throws Exception {
        String id = "unversioned.app";
        addCatalogItems(
            "brooklyn.catalog:",
            "  name: " + id,
            "  itemType: entity",
            "  item:",
            "    type: "+ BasicEntity.class.getName());
        RegisteredType catalogItem = mgmt().getTypeRegistry().get(id, BrooklynCatalog.DEFAULT_VERSION);
        assertEquals(catalogItem.getVersion(), "0.0.0.SNAPSHOT");
        mgmt().getCatalog().deleteCatalogItem(id, "0.0.0.SNAPSHOT");
    }

    // Legacy / backwards compatibility: should use id
    @Test
    public void testAddCatalogItemUsingNameInsteadOfIdWithInlinedVersion() throws Exception {
        String id = "inline_version.app";
        addCatalogItems(
            "brooklyn.catalog:",
            "  name: " + id+":"+TEST_VERSION,
            "  itemType: entity",
            "services:",
            "- type: " + BasicEntity.class.getName());
        RegisteredType catalogItem = mgmt().getTypeRegistry().get(id, TEST_VERSION);
        assertEquals(catalogItem.getVersion(), TEST_VERSION);
        mgmt().getCatalog().deleteCatalogItem(id, TEST_VERSION);
    }

    @Test
    public void testLaunchApplicationReferencingCatalog() throws Exception {
        String symbolicName = "myitem";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+ver(symbolicName, TEST_VERSION));

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationUnversionedCatalogReference() throws Exception {
        String symbolicName = "myitem";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationWithCatalogReferencingOtherCatalog() throws Exception {
        String referencedSymbolicName = "my.catalog.app.id.referenced";
        String referrerSymbolicName = "my.catalog.app.id.referring";
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  name: My Catalog App",
                "  description: My description",
                "  icon_url: classpath://path/to/myicon.jpg",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: " + referencedSymbolicName,
                "    item:",
                "      type: " + TestEntity.class.getName(),
                "  - id: " + referrerSymbolicName,
                "    item:",
                "      type: " + ver(referencedSymbolicName, TEST_VERSION));
          
        RegisteredType referrer = mgmt().getTypeRegistry().get(referrerSymbolicName, TEST_VERSION);
        String planYaml = RegisteredTypes.getImplementationDataStringForSpec(referrer);
        Assert.assertTrue(planYaml.indexOf("services")>=0, "expected services in: "+planYaml);
        
        Entity app = createAndStartApplication("services:",
                      "- type: " + ver(referrerSymbolicName, TEST_VERSION));

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }

    @Test
    public void testLaunchApplicationWithCatalogReferencingOtherCatalogInTwoSteps() throws Exception {
        String referencedSymbolicName = "my.catalog.app.id.referenced";
        String referrerSymbolicName = "my.catalog.app.id.referring";

        addCatalogEntity(IdAndVersion.of(referencedSymbolicName, TEST_VERSION), TestEntity.class.getName());
        addCatalogEntity(IdAndVersion.of(referrerSymbolicName, TEST_VERSION), ver(referencedSymbolicName, TEST_VERSION));

        Entity app = createAndStartApplication("services:",
                      "- type: " + ver(referrerSymbolicName, TEST_VERSION));

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }

    @Test
    public void testLaunchApplicationChildWithCatalogReferencingOtherCatalog() throws Exception {
        String referencedSymbolicName = "my.catalog.app.id.child.referenced";
        String referrerSymbolicName = "my.catalog.app.id.child.referring";
        
        addCatalogEntity(IdAndVersion.of(referencedSymbolicName, TEST_VERSION), TestEntity.class.getName());

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + referrerSymbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    services:",
                "    - type: " + BasicEntity.class.getName(),
                "      brooklyn.children:",
                "      - type: " + ver(referencedSymbolicName, TEST_VERSION));
        
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
        assertEquals(grandGrandChild.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }

    @Test
    public void testLaunchApplicationChildWithCatalogReferencingOtherCatalogServicesBlock() throws Exception {
        String referencedSymbolicName = "my.catalog.app.id.child.referenced";
        String referrerSymbolicName = "my.catalog.app.id.child.referring";
        addCatalogEntity(IdAndVersion.of(referencedSymbolicName, TEST_VERSION), TestEntity.class.getName());

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + referrerSymbolicName,
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    services:",
                "    - type: " + BasicEntity.class.getName(),
                "      brooklyn.children:",
                "      - type: " + ver(referencedSymbolicName, TEST_VERSION));

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
        assertEquals(grandGrandChild.getEntityType().getName(), TestEntity.class.getName());

        deleteCatalogEntity(referencedSymbolicName);
        deleteCatalogEntity(referrerSymbolicName);
    }
    
    @Test
    public void testLaunchApplicationWithTypeUsingJavaColonPrefix() throws Exception {
        String symbolicName = "t1";
        String actualType = TestEntity.class.getName();
        String serviceType = "java:"+actualType;
        registerAndLaunchAndAssertSimpleEntity(symbolicName, serviceType, actualType);
    }

    @Test
    public void testLaunchApplicationLoopWithJavaTypeName() throws Exception {
        String symbolicName = TestEntity.class.getName();
        String serviceName = TestEntity.class.getName();
        registerAndLaunchAndAssertSimpleEntity(symbolicName, serviceName);
    }

    @Test
    public void testLaunchApplicationChildLoopCatalogIdFails() throws Exception {
        String referrerSymbolicName = "my.catalog.app.id.child.referring";
        try {
            // TODO only fails if using 'services', because that forces plan parsing; should fail in all cases
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + referrerSymbolicName,
                    "  version: " + TEST_VERSION,
                    "  itemType: entity",
                    "  item:",
                    "    services:",
                    "    - type: " + BasicEntity.class.getName(),
                    "      brooklyn.children:",
                    "      - type: " + ver(referrerSymbolicName, TEST_VERSION));
            fail("Expected to throw");
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            assertTrue(e.getMessage().contains(referrerSymbolicName), "message was: "+e);
        }
    }

    @Test
    public void testUpdatingItemAllowedIfSame() {
        String symbolicName = "my.catalog.app.id.duplicate";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());
    }
    
    @Test(expectedExceptions = IllegalStateException.class)
    public void testUpdatingItemFailsIfDifferent() {
        String symbolicName = "my.catalog.app.id.duplicate";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), BasicEntity.class.getName());
    }

    @Test
    public void testForcedUpdatingItem() {
        String symbolicName = "my.catalog.app.id.duplicate";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());
        forceCatalogUpdate();
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());
        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testCreateSpecFromCatalogItem() {
        String id = "my.catalog.app.id.create_spec";
        addCatalogEntity(IdAndVersion.of(id, TEST_VERSION), TestEntity.class.getName());
        
        BrooklynTypeRegistry catalog = mgmt().getTypeRegistry();
        RegisteredType item = catalog.get(id, TEST_VERSION);
        EntitySpec<?> spec = catalog.createSpec(item, null, EntitySpec.class);
        Assert.assertNotNull(spec);
        AbstractBrooklynObjectSpec<?,?> spec2 = catalog.createSpec(item, null, null);
        Assert.assertNotNull(spec2);
    }
    
    @Test
    public void testMissingTypeDoesNotRecurse() {
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());

        try {
            addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION + "-update"), symbolicName);
            fail("Catalog addition expected to fail due to recursive reference to " + symbolicName);
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }
    
    @Test
    public void testVersionedTypeDoesNotRecurse() throws Exception {
        // Alternatively, we could change this to tell foo:v2 reference foo:v1, but that feels 
        // like a bad idea! 
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), TestEntity.class.getName());

        String versionedId = CatalogUtils.getVersionedId(symbolicName, TEST_VERSION);
        try {
            addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION + "-update"), versionedId);
            fail("Catalog addition expected to fail due to recursive reference to " + versionedId);
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testIndirectRecursionFails() throws Exception {
        String callerSymbolicName = "my.catalog.app.id.caller";
        String calleeSymbolicName = "my.catalog.app.id.callee";
        
        // Need to have a stand alone caller first so we can create an item to depend on it.
        // After that replace it/insert a new version which completes the cycle
        addCatalogEntity(IdAndVersion.of(callerSymbolicName, TEST_VERSION + "-pre"), TestEntity.class.getName());

        addCatalogEntity(IdAndVersion.of(calleeSymbolicName, TEST_VERSION), callerSymbolicName);

        try {
            addCatalogEntity(IdAndVersion.of(callerSymbolicName, TEST_VERSION), calleeSymbolicName);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testChildItemsDoNotRecurse() throws Exception {
        String callerSymbolicName = "my.catalog.app.id.caller";
        String calleeSymbolicName = "my.catalog.app.id.callee";

        // Need to have a stand alone caller first so we can create an item to depend on it.
        // After that replace it/insert a new version which completes the cycle
        
        addCatalogEntity(IdAndVersion.of(callerSymbolicName, TEST_VERSION + "-pre"), TestEntity.class.getName());

        addCatalogEntity(IdAndVersion.of(calleeSymbolicName, TEST_VERSION), callerSymbolicName);

        try {
            // TODO Only passes if include "services:" and if itemType=entity, rather than "template"!
            // Being a child is important, triggers the case where: we allow retrying with other transformers.
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + callerSymbolicName,
                    "  version: " + TEST_VERSION,
                    "  itemType: entity",
                    "  item:",
                    "    services:",
                    "    - type: " + BasicEntity.class.getName(),
                    "      brooklyn.children:",
                    "      - type: " + calleeSymbolicName);
            fail();
        } catch (IllegalStateException e) {
            assertTrue(e.toString().contains("recursive"), "Unexpected error message: "+e);
        }
    }

    @Test
    public void testRecursiveCheckForDepenentsOnly() throws Exception {
        String symbolicName = "my.catalog.app.id.basic";
        addCatalogEntity(symbolicName, TestEntity.class.getName());

        createAndStartApplication(
                "services:",
                "- type: " + symbolicName,
                "  brooklyn.children:",
                "  - type: " + symbolicName,
                "- type: " + symbolicName,
                "  brooklyn.children:",
                "  - type: " + symbolicName);
    }

    @Test
    public void testConfigAppliedToCatalogItem() throws Exception {
        addCatalogEntity("test", TestEntity.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: test",
                "  brooklyn.config:",
                "    test.confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
    }

    @Test
    public void testFlagsAppliesToCatalogItem() throws Exception {
        addCatalogEntity("test", TestEntity.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
    }

    @Test
    public void testExplicitFlagsAppliesToCatalogItem() throws Exception {
        addCatalogEntity("test", TestEntity.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.flags:",
                "    confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
    }
    
    @Test
    public void testConfigAppliedToCatalogItemImpl() throws Exception {
        addCatalogEntity("test", TestEntityImpl.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.config:",
                "    test.confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
    }

    @Test
    public void testFlagsAppliesToCatalogItemImpl() throws Exception {
        addCatalogEntity("test", TestEntityImpl.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
    }

    @Test
    public void testExplicitFlagsAppliesToCatalogItemImpl() throws Exception {
        addCatalogEntity("test", TestEntityImpl.class.getName());
        String val = "test-applies-config-on-catalog-item";
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + ver("test"),
                "  brooklyn.flags:",
                "    confName: " + val);
        Entity testEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(testEntity.config().get(TestEntity.CONF_NAME), val);
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
        
        assertEquals(child.config().get(ConfigKeys.newStringConfigKey("myconf3")), "myval");
        
        mgmt().getCatalog().deleteCatalogItem(id, version);
    }

    // The test is disabled as it fails. The entity will get assigned the outer-most catalog
    // item which doesn't have the necessary libraries with visibility to the entity's classpath
    // When loading resources from inside the entity then we will use the wrong BCLCS. A workaround
    // has been implemented which explicitly adds the entity's class loader to the fallbacks.
    @Test(groups="WIP")
    public void testCatalogItemIdInReferencedItems() throws Exception {
        String symbolicNameInner = "my.catalog.app.id.inner";
        String symbolicNameOuter = "my.catalog.app.id.outer";
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: " + TEST_VERSION,
            "  items:",
            "  - id: " + symbolicNameInner,
            "    item: " + TestEntity.class.getName(),
            "  - id: " + symbolicNameOuter,
            "    item: " + symbolicNameInner);

        String yaml = "name: " + symbolicNameOuter + "\n" +
                "services: \n" +
                "  - serviceType: "+ver(symbolicNameOuter);

        Entity app = createAndStartApplication(yaml);

        Entity entity = app.getChildren().iterator().next();

        assertEquals(entity.getCatalogItemId(), ver(symbolicNameOuter));
        assertEquals(entity.getCatalogItemSuperIds().size(), 2);
        assertEquals(entity.getCatalogItemSuperIds().get(0), ver(symbolicNameOuter));
        assertEquals(entity.getCatalogItemSuperIds().get(1), ver(symbolicNameInner));

        deleteCatalogEntity(symbolicNameInner);
        deleteCatalogEntity(symbolicNameOuter);
    }

    private void registerAndLaunchAndAssertSimpleEntity(String symbolicName, String serviceType) throws Exception {
        registerAndLaunchAndAssertSimpleEntity(symbolicName, serviceType, serviceType);
    }
    
    private void registerAndLaunchAndAssertSimpleEntity(String symbolicName, String serviceType, String expectedType) throws Exception {
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), serviceType);
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: "+ver(symbolicName, TEST_VERSION));

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(simpleEntity.getEntityType().getName(), expectedType);

        deleteCatalogEntity(symbolicName);
    }

    public static class IdAndVersion {
        public final String id;
        public final String version;
        
        public static IdAndVersion of(String id, String version) {
            return new IdAndVersion(id, version);
        }
        
        public IdAndVersion(String id, String version) {
            this.id = checkNotNull(id, "id");
            this.version = checkNotNull(version, "version");
        }
    }
    
    private void addCatalogEntity(String symbolicName, String entityType) {
        addCatalogEntity(IdAndVersion.of(symbolicName, TEST_VERSION), entityType);
    }
    
    private void addCatalogEntity(IdAndVersion idAndVersion, String serviceType) {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + idAndVersion.id,
                "  version: " + idAndVersion.version,
                "  itemType: entity",
                "  item:",
                "    type: " + serviceType);
    }
}
