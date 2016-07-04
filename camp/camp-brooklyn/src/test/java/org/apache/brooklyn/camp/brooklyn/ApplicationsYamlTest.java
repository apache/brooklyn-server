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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.TestSensorAndEffectorInitializer.TestConfigurableInitializer;
import org.apache.brooklyn.camp.brooklyn.catalog.CatalogYamlLocationTest;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.policy.TestEnricher;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Test
public class ApplicationsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ApplicationsYamlTest.class);

    @Override
    protected LocalManagementContext newTestManagementContext() {
        // Don't need osgi
        return LocalManagementContextForTests.newInstance();
    }

    @Test
    public void testWrapsEntity() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName());
        assertWrapped(app, BasicEntity.class);
    }

    @Test
    public void testWrapsMultipleApps() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "- type: " + BasicApplication.class.getName());
        assertTrue(app.getConfig(EntityManagementUtils.WRAPPER_APP_MARKER));
        assertTrue(app instanceof BasicApplication);
        assertEquals(app.getChildren().size(), 2);
    }

    @Test
    public void testWrapsWhenEnrichers() throws Exception {
        Entity app = createAndStartApplication(
                "brooklyn.enrichers:",
                "- type: " + TestEnricher.class.getName(),
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertWrapped(app, BasicApplication.class);
    }

    @Test
    public void testWrapsWhenPolicy() throws Exception {
        Entity app = createAndStartApplication(
                "brooklyn.policies:",
                "- type: " + TestPolicy.class.getName(),
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertWrapped(app, BasicApplication.class);
    }

    @Test
    public void testWrapsWhenInitializer() throws Exception {
        Entity app = createAndStartApplication(
                "brooklyn.initializers:",
                "- type: " + TestConfigurableInitializer.class.getName(),
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertWrapped(app, BasicApplication.class);
    }

    @Test
    public void testWrapsAppIfForced() throws Exception {
        Entity app = createAndStartApplication(
                "wrappedApp: true",
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertWrapped(app, BasicApplication.class);
    }

    @Test
    public void testDoesNotWrapApp() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertDoesNotWrap(app, BasicApplication.class, null);
    }

    @Test
    public void testDoesNotWrapAppIfUnforced() throws Exception {
        Entity app = createAndStartApplication(
                "wrappedApp: false",
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertDoesNotWrap(app, BasicApplication.class, null);
    }
    
    @Test
    public void testDoesNotWrapEntityIfDifferentTopLevelName() throws Exception {
        Entity app = createAndStartApplication(
                "name: topLevel",
                "services:",
                "- type: " + BasicApplication.class.getName(),
                "  name: bottomLevel");
        assertDoesNotWrap(app, BasicApplication.class, "topLevel");
    }

    @Test
    public void testDoesNotWrapsEntityIfNoNameOnService() throws Exception {
        Entity app = createAndStartApplication(
                "name: topLevel",
                "services:",
                "- type: " + BasicApplication.class.getName());
        assertDoesNotWrap(app, BasicApplication.class, "topLevel");
    }

    @Test
    public void testDoesNotWrapCatalogItemWithDisplayName() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  displayName: catalogLevel",
                "  item:",
                "    type: " + BasicApplication.class.getName());
        
        Entity app = createAndStartApplication(
                "name: topLevel",
                "services:",
                "- type: simple:" + TEST_VERSION);
        assertDoesNotWrap(app, BasicApplication.class, "topLevel");
    }

    @Test
    public void testDoesNotWrapCatalogItemWithServiceName() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  displayName: catalogLevel",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    defaultDisplayName: defaultServiceName",
                "    displayName: explicitServiceName");
        
        Entity app = createAndStartApplication(
                "name: topLevel",
                "services:",
                "- type: simple:" + TEST_VERSION);
        assertDoesNotWrap(app, BasicApplication.class, "topLevel");
    }

    @Test
    public void testDoesNotWrapCatalogItemAndOverridesName() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  displayName: catalogLevel",
                "  item:",
                "    type: " + BasicApplication.class.getName());
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: simple:" + TEST_VERSION,
                "  name: serviceLevel");
        assertDoesNotWrap(app, BasicApplication.class, "serviceLevel");
    }

    @Test
    public void testDoesNotWrapCatalogItemAndUsesCatalogName() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  displayName: catalogLevel",
                "  item:",
                "    type: " + BasicApplication.class.getName());
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: simple:" + TEST_VERSION);
        assertDoesNotWrap(app, BasicApplication.class, "catalogLevel");
    }

    @Test
    public void testDoesNotWrapCatalogItemAndUsesCatalogServiceName() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  displayName: catalogLevel",
                "  item:",
                "    type: " + BasicApplication.class.getName(),
                "    name: catalogServiceLevel");
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: simple:" + TEST_VERSION);
        assertDoesNotWrap(app, BasicApplication.class, "catalogServiceLevel");
    }

    @Test
    public void testUnwrappedChildNotTagged() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: simple",
                "  version: " + TEST_VERSION,
                "  itemType: entity",
                "  item:",
                "    type: " + BasicEntity.class.getName());

        Entity app = createAndStartApplication(
                "services:",
                "- type: simple:" + TEST_VERSION);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertNull(entity.getConfig(EntityManagementUtils.WRAPPER_APP_MARKER));
        // Note that "brooklyn.wrapper_app" will still make it into 
        //   ((EntityInternal) entity).config().getBag().getAllConfigAsConfigKeyMap();
        // so the UI will still show the marker as inherited by the entity.

    }

    // FIXME Fails with name "My App 1" rather than the overridden value. 
    // See discussion in https://issues.apache.org/jira/browse/BROOKLYN-248
    @Test(groups="WIP")
    public void testTypeInheritance() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  items:",
                "  - id: app1",
                "    name: My App 1",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      brooklyn.config:",
                "        mykey1: myval1",
                "        mykey1b: myval1b",
                "  - id: app2",
                "    name: My App 2",
                "    item:",
                "      type: app1",
                "      brooklyn.config:",
                "        mykey1: myvalOverridden",
                "        mykey2: myval2");
        
        addCatalogItems(yaml);
        Entity app1 = createAndStartApplication("services: [ {type: app1} ]");
        assertDoesNotWrap(app1, BasicApplication.class, "My App 1");
        CatalogYamlLocationTest.assertContainsAll(((EntityInternal)app1).config().getBag().getAllConfig(), ImmutableMap.of("mykey1", "myval1", "mykey1b", "myval1b"));

        Entity app2 = createAndStartApplication("services: [ {type: app2} ]");
        assertDoesNotWrap(app2, BasicApplication.class, "My App 2");
        CatalogYamlLocationTest.assertContainsAll(((EntityInternal)app2).config().getBag().getAllConfig(), ImmutableMap.of("mykey1", "myvalOverridden", "mykey1b", "myval1b", "mykey2", "myval2"));
    }
    
    // FIXME Fails for app3b, which gets the name "My name within item 3a" rather than the 
    // overridden value. See discussion in https://issues.apache.org/jira/browse/BROOKLYN-248
    @Test(groups="WIP")
    public void testNamePrecedence() throws Exception {
        String yaml1 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  name: My name in top-level metadata",
                "  items:",
                "  - id: app1",
                "    name: My name in item metadata",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      name: My name within item");

        String yaml2 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  name: My name in top-level metadata",
                "  items:",
                "  - id: app2",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      name: My name within item");

        String yaml3 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  items:",
                "  - id: app3a",
                "    name: My name in item 3a metadata",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      name: My name within item 3a",
                "  items:",
                "  - id: app3b",
                "    item:",
                "      type: app3a",
                "      name: My name within item 3b");
        
        addCatalogItems(yaml1);
        addCatalogItems(yaml2);
        addCatalogItems(yaml3);

        Entity app1 = createAndStartApplication("services: [ {type: app1} ]");
        assertDoesNotWrap(app1, BasicApplication.class, "My name within item");

        Entity app2 = createAndStartApplication("services: [ {type: app2} ]");
        assertDoesNotWrap(app2, BasicApplication.class, "My name within item");

        Entity app3b = createAndStartApplication("services: [ {type: app3b} ]");
        assertDoesNotWrap(app3b, BasicApplication.class, "My name within item 3b");
    }
    
    @Test
    public void testNameInCatalogMetadata() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  name: My name in top-level",
                "  items:",
                "  - id: app1",
                "    item:",
                "      type: " + BasicApplication.class.getName());
        
        addCatalogItems(yaml);

        Entity app1 = createAndStartApplication("services: [ {type: app1} ]");
        assertDoesNotWrap(app1, BasicApplication.class, "My name in top-level");
    }
    
    @Test
    public void testNameInItemMetadata() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  items:",
                "  - id: app1",
                "    name: My name in item metadata",
                "    item:",
                "      type: " + BasicApplication.class.getName());
        
        addCatalogItems(yaml);

        Entity app1 = createAndStartApplication("services: [ {type: app1} ]");
        assertDoesNotWrap(app1, BasicApplication.class, "My name in item metadata");
    }
    
    @Test
    public void testNameWithinItem() throws Exception {
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  version: 0.1.2",
                "  itemType: entity",
                "  items:",
                "  - id: app1",
                "    item:",
                "      type: " + BasicApplication.class.getName(),
                "      name: My name within item");
        
        addCatalogItems(yaml);

        Entity app1 = createAndStartApplication("services: [ {type: app1} ]");
        assertDoesNotWrap(app1, BasicApplication.class, "My name within item");
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }

    private void assertWrapped(Entity app, Class<? extends Entity> wrappedEntityType) {
        assertTrue(app.getConfig(EntityManagementUtils.WRAPPER_APP_MARKER));
        assertTrue(app instanceof BasicApplication);
        Entity child = Iterables.getOnlyElement(app.getChildren());
        assertTrue(wrappedEntityType.isInstance(child));
        assertTrue(child.getChildren().isEmpty());
    }

    private void assertDoesNotWrap(Entity app, Class<? extends Application> entityType, String displayName) {
        assertNull(app.getConfig(EntityManagementUtils.WRAPPER_APP_MARKER));
        assertTrue(entityType.isInstance(app));
        if (displayName != null) {
            assertEquals(app.getDisplayName(), displayName);
        }
        assertEquals(app.getChildren().size(), 0);
    }
    
}
