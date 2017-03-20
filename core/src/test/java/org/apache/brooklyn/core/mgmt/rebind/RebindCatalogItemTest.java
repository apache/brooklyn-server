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
package org.apache.brooklyn.core.mgmt.rebind;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.io.File;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogEntityItemDto;
import org.apache.brooklyn.core.catalog.internal.CatalogItemBuilder;
import org.apache.brooklyn.core.catalog.internal.CatalogLocationItemDto;
import org.apache.brooklyn.core.catalog.internal.CatalogPolicyItemDto;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.server.BrooklynServerConfig;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;

public class RebindCatalogItemTest extends RebindTestFixtureWithApp {

    // FIXME Can we delete all of this?
    // Is testing now sufficiently covered by brooklyn-camp module, such as o.a.b.camp.brooklyn.catalog.CatalogYamlRebindTest?
    //
    // This tests at a slightly different level (calling `mgmt.getCatalog().addItem(item)` directly, and
    // asserting `mgmt.getCatalog().getCatalogItems()` is the same before and after rebind. Therefore
    // keeping it for now.
    
    private static final String TEST_VERSION = "0.0.1";

    private static final Logger LOG = LoggerFactory.getLogger(RebindCatalogItemTest.class);
    public static class MyPolicy extends AbstractPolicy {}
    private boolean catalogPersistenceWasEnabled;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        catalogPersistenceWasEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY);
        BrooklynFeatureEnablement.enable(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY);
        super.setUp();
        origApp.createAndManageChild(EntitySpec.create(TestEntity.class));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        BrooklynFeatureEnablement.setEnablement(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY, catalogPersistenceWasEnabled);
    }

    @Override
    protected LocalManagementContext createOrigManagementContext() {
        BrooklynProperties properties = BrooklynProperties.Factory.newDefault();
        properties.put(BrooklynServerConfig.CATALOG_LOAD_MODE, org.apache.brooklyn.core.catalog.CatalogLoadMode.LOAD_BROOKLYN_CATALOG_URL);
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .properties(properties)
                .persistPeriodMillis(getPersistPeriodMillis())
                .forLive(useLiveManagementContext())
                .buildStarted();
    }

    @Override
    protected LocalManagementContext createNewManagementContext(File mementoDir, HighAvailabilityMode haMode, Map<?, ?> additionalProperties) {
        BrooklynProperties properties = BrooklynProperties.Factory.newDefault();
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .properties(properties)
                .forLive(useLiveManagementContext())
                .haMode(haMode)
                .emptyCatalog(useEmptyCatalog())
                .buildUnstarted();
    }

    @Test
    public void testAddAndRebindEntity() throws Exception {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        CatalogEntityItemDto item =
            CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                .displayName(symbolicName)
                .plan(yaml)
                .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());
        rebindAndAssertCatalogsAreEqual();
    }

    @Test
    public void testAddAndRebindEntityLegacyFormat() throws Exception {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = 
                "name: " + symbolicName + "\n" +
                "brooklyn.catalog:\n" +
                "  version: " + TEST_VERSION + "\n" +
                "services:\n" +
                "- type: io.camp.mock:AppServer";
        CatalogEntityItemDto item =
            CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                .displayName(symbolicName)
                .plan(yaml)
                .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());
        rebindAndAssertCatalogsAreEqual();
    }

    @Test(enabled = false)
    public void testAddAndRebindTemplate() {
        // todo: could use (deprecated, perhaps wrongly) BBC.addItem(Class/CatalogItem)
        fail("Unimplemented because the catalogue does not currently distinguish between application templates and entities");
    }

    @Test
    public void testAddAndRebindPolicy() {
        // Doesn't matter that SamplePolicy doesn't exist
        String symbolicName = "Test Policy";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: policy",
                "    item:",
                "      type: org.apache.brooklyn.core.mgmt.rebind.RebindCatalogItemTest$MyPolicy",
                "      brooklyn.config:",
                "        cfg1: 111",
                "        cfg2: 222");
        CatalogPolicyItemDto item =
                CatalogItemBuilder.newPolicy(symbolicName, TEST_VERSION)
                    .displayName(symbolicName)
                    .plan(yaml)
                    .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());
        rebindAndAssertCatalogsAreEqual();
    }

    @Test
    public void testAddAndRebindAndDeleteLocation() throws Exception {
        doTestAddAndRebindAndDeleteLocation(false);
    }

    @Test
    public void testAddAndRebindAndDeleteLocationWithoutIntermediateDeltaWrite() throws Exception {
        doTestAddAndRebindAndDeleteLocation(true);
    }

    private void doTestAddAndRebindAndDeleteLocation(boolean suppressPeriodicCheckpointing) throws Exception {
        String symbolicName = "sample_location";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: location",
                "    item:",
                "      type: "+LocalhostMachineProvisioningLocation.class.getName(),
                "      brooklyn.config:",
                "        cfg1: 111",
                "        cfg2: 222");
        CatalogLocationItemDto item =
                CatalogItemBuilder.newLocation(symbolicName, TEST_VERSION)
                    .displayName(symbolicName)
                    .plan(yaml)
                    .build();
        origManagementContext.getCatalog().addItem(item);
        assertEquals(item.getCatalogItemType(), CatalogItemType.LOCATION);
        if (!suppressPeriodicCheckpointing) {
            rebindAndAssertCatalogsAreEqual();
        } else {
            LocalManagementContext nmc = RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .forLive(useLiveManagementContext())
                .emptyCatalog(useEmptyCatalog())
                .persistPeriod(Duration.PRACTICALLY_FOREVER)
                .buildUnstarted();
            rebind(RebindOptions.create().newManagementContext(nmc));
        }
        
        deleteItem(newManagementContext, item.getSymbolicName(), item.getVersion());
        
        switchOriginalToNewManagementContext();
        rebindAndAssertCatalogsAreEqual();
    }
    
    @Test(enabled = false)
    public void testAddAndRebindEnricher() {
        // TODO
        fail("unimplemented");
    }

    @Test(invocationCount = 3)
    public void testDeletedCatalogItemIsNotPersisted() {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        CatalogEntityItemDto item =
            CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                .displayName(symbolicName)
                .plan(yaml)
                .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());

        // Must make sure that the original catalogue item is not managed and unmanaged in the same
        // persistence window. Because BrooklynMementoPersisterToObjectStore applies writes/deletes
        // asynchronously the winner is down to a race and the test might pass or fail.
        origManagementContext.getRebindManager().forcePersistNow(false, null);
        
        origManagementContext.getCatalog().deleteCatalogItem(symbolicName, TEST_VERSION);
        
        Iterable<CatalogItem<Object, Object>> items = origManagementContext.getCatalog().getCatalogItems();
        Optional<CatalogItem<Object, Object>> itemPostDelete = Iterables.tryFind(items, CatalogPredicates.symbolicName(Predicates.equalTo(symbolicName)));
        assertFalse(itemPostDelete.isPresent(), "item="+itemPostDelete);
        
        rebindAndAssertCatalogsAreEqual();
        
        Iterable<CatalogItem<Object, Object>> newItems = newManagementContext.getCatalog().getCatalogItems();
        Optional<CatalogItem<Object, Object>> itemPostRebind = Iterables.tryFind(newItems, CatalogPredicates.symbolicName(Predicates.equalTo(symbolicName)));
        assertFalse(itemPostRebind.isPresent(), "item="+itemPostRebind);
    }

    @Test(invocationCount = 3)
    public void testCanTagCatalogItemAfterRebind() {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        CatalogEntityItemDto item =
            CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                .displayName(symbolicName)
                .plan(yaml)
                .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());

        assertEquals(Iterables.size(origManagementContext.getCatalog().getCatalogItems()), 1);
        CatalogItem<?,?> origItem = origManagementContext.getCatalog().getCatalogItem(symbolicName, TEST_VERSION);
        final String tag = "tag1";
        origItem.tags().addTag(tag);
        assertTrue(origItem.tags().containsTag(tag));

        rebindAndAssertCatalogsAreEqual();

        CatalogItem<?, ?> newItem = newManagementContext.getCatalog().getCatalogItem(symbolicName, TEST_VERSION);
        assertTrue(newItem.tags().containsTag(tag));
        newItem.tags().removeTag(tag);
    }

    @Test
    public void testSameCatalogItemIdRemovalAndAdditionRebinds() throws Exception {
        //The test is not reliable on Windows (doesn't catch the pre-fix problem) -
        //the store is unable to delete still locked files so the bug doesn't manifest.
        //TODO investigate if locked files not caused by unclosed streams!
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        BasicBrooklynCatalog catalog = (BasicBrooklynCatalog) origManagementContext.getCatalog();
        CatalogEntityItemDto item =
                CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                    .displayName(symbolicName)
                    .plan(yaml)
                    .build();
        catalog.addItem(item);
        rebindAndAssertCatalogsAreEqual();
    }

    @Test
    public void testRebindAfterItemDeprecated() {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + TEST_VERSION,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        CatalogEntityItemDto item =
                CatalogItemBuilder.newEntity(symbolicName, TEST_VERSION)
                    .displayName(symbolicName)
                    .plan(yaml)
                    .build();
        origManagementContext.getCatalog().addItem(item);
        assertNotNull(item, "catalogItem");
        BasicBrooklynCatalog catalog = (BasicBrooklynCatalog) origManagementContext.getCatalog();
        
        item.setDeprecated(true);
        catalog.persist(item);
        rebindAndAssertCatalogsAreEqual();
        RegisteredType catalogItemAfterRebind = newManagementContext.getTypeRegistry().get("rebind-yaml-catalog-item-test", TEST_VERSION);
        assertTrue(catalogItemAfterRebind.isDeprecated(), "Expected item to be deprecated");
    }

    protected void deleteItem(ManagementContext mgmt, String symbolicName, String version) {
        mgmt.getCatalog().deleteCatalogItem(symbolicName, version);
        LOG.info("Deleted item from catalog: {}:{}", symbolicName, version);
        Assert.assertNull( mgmt.getTypeRegistry().get(symbolicName, version) );
    }
    
    private void rebindAndAssertCatalogsAreEqual() {
        try {
            rebind();
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
        assertCatalogsEqual(newManagementContext.getCatalog(), origManagementContext.getCatalog());
    }
}
