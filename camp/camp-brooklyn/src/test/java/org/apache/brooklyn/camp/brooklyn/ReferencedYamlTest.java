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

import java.util.Collection;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class ReferencedYamlTest extends AbstractYamlTest {

    @Override
    protected boolean disableOsgi() {
        return false;
    }

    @Test
    public void testReferenceEntityYamlAsPlatformComponent() throws Exception {
        String entityName = "Reference child name";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityName,
            "  type: classpath://yaml-ref-entity.yaml");
        
        checkChildEntitySpec(app, entityName);
    }

    @Test
    public void testAnonymousReferenceEntityYamlAsPlatformComponent() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: classpath://yaml-ref-entity.yaml");
        
        // the name declared at the root trumps the name on the item itself
        checkChildEntitySpec(app, "Basic entity");
    }

    @Test
    public void testReferenceAppYamlAsPlatformComponent() throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- name: Reference child name",
            "  type: classpath://yaml-ref-app.yaml");
        
        Assert.assertEquals(app.getChildren().size(), 0);
        Assert.assertEquals(app.getDisplayName(), "Reference child name");

        //child is a proxy so equality test won't do
        Assert.assertEquals(app.getEntityType().getName(), BasicApplication.class.getName());
    }

    @Test
    public void testReferenceYamlAsChild() throws Exception {
        String entityName = "Reference child name";
        Entity createAndStartApplication = createAndStartApplication(
            "services:",
            "- type: org.apache.brooklyn.entity.stock.BasicEntity",
            "  brooklyn.children:",
            "  - name: " + entityName,
            "    type: classpath://yaml-ref-entity.yaml");
        
        checkGrandchildEntitySpec(createAndStartApplication, entityName);
    }

    @Test
    public void testAnonymousReferenceYamlAsChild() throws Exception {
        Entity createAndStartApplication = createAndStartApplication(
            "services:",
            "- type: org.apache.brooklyn.entity.stock.BasicEntity",
            "  brooklyn.children:",
            "  - type: classpath://yaml-ref-entity.yaml");
        
        checkGrandchildEntitySpec(createAndStartApplication, "Basic entity");
    }

    @Test
    public void testCatalogReferencingYamlUrl() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: yaml.reference",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: classpath://yaml-ref-entity.yaml");
        
        String entityName = "YAML -> catalog item -> yaml url";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityName,
            "  type: " + ver("yaml.reference"));
        
        checkChildEntitySpec(app, entityName);
    }

    @Test
    public void testCatalogReferencingYamlUrlFromOsgiBundle() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        addCatalogItems(
            "brooklyn.catalog:",
            "  id: yaml.reference",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    type: classpath://yaml-ref-osgi-entity.yaml");
        
        String entityName = "YAML -> catalog item -> yaml url (osgi)";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityName,
            "  type: " + ver("yaml.reference"));
        
        checkChildEntitySpec(app, entityName);
    }

    @Test
    public void testYamlUrlReferencingCatalog() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: yaml.basic",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  item:",
            "    type: org.apache.brooklyn.entity.stock.BasicEntity");
        
        String entityName = "YAML -> yaml url -> catalog item";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityName,
            "  type: classpath://yaml-ref-catalog.yaml");
        
        checkChildEntitySpec(app, entityName);
    }

    @Test(groups="WIP") //Not able to use caller provided catalog items when referencing entity specs (as opposed to catalog meta)
    public void testYamlUrlReferencingCallerCatalogItem() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  itemType: entity",
            "  items:",
            "  - id: yaml.standalone",
            "    version: " + TEST_VERSION,
            "    item:",
            "      services:",
            "      - type: org.apache.brooklyn.entity.stock.BasicApplication",
            "  - id: yaml.reference",
            "    version: " + TEST_VERSION,
            "    item:",
            "      services:",
            "      - type: classpath://yaml-ref-parent-catalog.yaml");

        String entityName = "YAML -> catalog item -> yaml url";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityName,
            "  type: " + ver("yaml.reference"));
        
        checkChildEntitySpec(app, entityName);
    }

    /**
     * Tests that a YAML referenced by URL from a catalog item
     * will have access to the catalog item's bundles.
     */
    @Test
    public void testCatalogLeaksBundlesToReferencedYaml() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        String parentCatalogId = "my.catalog.app.id.url.parent";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + parentCatalogId,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  libraries:",
            "  - url: " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  item:",
            "    type: classpath://yaml-ref-bundle-without-libraries.yaml");

        Entity app = createAndStartApplication(
            "services:",
                "- type: " + ver(parentCatalogId));
        
        Collection<Entity> children = app.getChildren();
        Assert.assertEquals(children.size(), 1);
        Entity child = Iterables.getOnlyElement(children);
        Assert.assertEquals(child.getEntityType().getName(), "org.apache.brooklyn.test.osgi.entities.SimpleEntity");

        deleteCatalogEntity(parentCatalogId);
    }

    @Test
    public void testCatalogReference() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  brooklyn.libraries:",
            "  - " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  items:",
            "  - classpath://yaml-ref-parent-catalog.bom");

        assertCatalogReference();
    }

    @Test
    public void testCatalogReferenceByExplicitUrl() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  brooklyn.libraries:",
            "  - " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  items:",
            "  - include: classpath://yaml-ref-parent-catalog.bom");

        assertCatalogReference();
    }

    @Test
    public void testCatalogReferenceByMultipleUrls() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  items:",
            "  - include: classpath://yaml-ref-simple.bom",
            "  - include: classpath://yaml-ref-more.bom"
        );

        assertCatalogReference();
    }

    @Test
    public void testCatalogReferenceByMultipleUrlsSimplerSyntax() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  items:",
            "  - classpath://yaml-ref-simple.bom",
            "  - classpath://yaml-ref-more.bom"
        );

        assertCatalogReference();
    }


    @Test
    public void testCatalogReferenceSeesPreviousItems() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  brooklyn.libraries:",
            "  - " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  items:",
            "  - id: yaml.nested.catalog.simple",
            "    itemType: entity",
            "    item:",
            "      type: " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY,
            "  - include: classpath://yaml-ref-back-catalog.bom");

        String entityNameSimple = "YAML -> catalog -> catalog (osgi)";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityNameSimple,
            "  type: back-reference");
        
        Collection<Entity> children = app.getChildren();
        Assert.assertEquals(children.size(), 1);
        Entity childSimple = Iterables.getOnlyElement(children);
        Assert.assertEquals(childSimple.getDisplayName(), entityNameSimple);
        Assert.assertEquals(childSimple.getEntityType().getName(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY);
    }

    @Test
    public void testCatalogReferenceMixesMetaAndUrl() throws Exception {
        addCatalogItems(
            "brooklyn.catalog:",
            "  brooklyn.libraries:",
            "  - " + OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL,
            "  items:",
            "  - include: classpath://yaml-ref-parent-catalog.bom",
            "    items:",
            "    - id: yaml.nested.catalog.nested",
            "      itemType: entity",
            "      item:",
            "        type: " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY);

        BrooklynCatalog catalog = mgmt().getCatalog();
        Assert.assertNotNull(catalog.getCatalogItem("yaml.nested.catalog.nested", BrooklynCatalog.DEFAULT_VERSION));
        Assert.assertNotNull(catalog.getCatalogItem("yaml.nested.catalog.simple", BrooklynCatalog.DEFAULT_VERSION));
        Assert.assertNotNull(catalog.getCatalogItem("yaml.nested.catalog.more", BrooklynCatalog.DEFAULT_VERSION));
    }

    protected void assertCatalogReference() throws Exception {
        String entityNameSimple = "YAML -> catalog -> catalog simple (osgi)";
        String entityNameMore = "YAML -> catalog -> catalog more (osgi)";
        Entity app = createAndStartApplication(
            "services:",
            "- name: " + entityNameSimple,
            "  type: yaml.nested.catalog.simple",
            "- name: " + entityNameMore,
            "  type: yaml.nested.catalog.more");
        
        Collection<Entity> children = app.getChildren();
        Assert.assertEquals(children.size(), 2);
        Entity childSimple = Iterables.get(children, 0);
        Assert.assertEquals(childSimple.getDisplayName(), entityNameSimple);
        Assert.assertEquals(childSimple.getEntityType().getName(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY);

        Entity childMore = Iterables.get(children, 1);
        Assert.assertEquals(childMore.getDisplayName(), entityNameMore);
        Assert.assertEquals(childMore.getEntityType().getName(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
    }

    private void checkChildEntitySpec(Entity app, String entityName) {
        Collection<Entity> children = app.getChildren();
        Assert.assertEquals(children.size(), 1);
        Entity child = Iterables.getOnlyElement(children);
        Assert.assertEquals(child.getDisplayName(), entityName);
        Assert.assertEquals(child.getEntityType().getName(), BasicEntity.class.getName());
    }

    private void checkGrandchildEntitySpec(Entity createAndStartApplication, String entityName) {
        Collection<Entity> children = createAndStartApplication.getChildren();
        Assert.assertEquals(children.size(), 1);
        Entity child = Iterables.getOnlyElement(children);
        Collection<Entity> grandChildren = child.getChildren();
        Assert.assertEquals(grandChildren.size(), 1);
        Entity grandChild = Iterables.getOnlyElement(grandChildren);
        Assert.assertEquals(grandChild.getDisplayName(), entityName);
        Assert.assertEquals(grandChild.getEntityType().getName(), BasicEntity.class.getName());
    }
    
}
