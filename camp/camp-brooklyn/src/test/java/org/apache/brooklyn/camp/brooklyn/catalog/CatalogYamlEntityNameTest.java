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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;


public class CatalogYamlEntityNameTest extends AbstractYamlTest {
    
    @Test
    public void testUsesNameInCatalogItemMetadataIfNonSupplied() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  name: nameInItemMetadata",
            "  version: " + TEST_VERSION,
            "  item:",
            "    type: "+ BasicEntity.class.getName());

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "nameInItemMetadata");
        
        Entity app2 = createAndStartApplication(
                "services:",
                "- type: "+symbolicName,
                "  name: nameInEntity");
        BasicEntity entity2 = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app2), BasicEntity.class));
        assertEquals(entity2.getDisplayName(), "nameInEntity");
    }
    
    // TODO Has never worked. See CampResolver.createSpecFromFull, for how it injects the name of the item.
    @Test(groups={"WIP","Broken"})
    public void testUsesNameInSubtypeItemMetadataIfNonSupplied() throws Exception {
        String parentSymbolicName = "mySyperType";
        String symbolicName = "mySubType";
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  items:",
            "  - id: " + parentSymbolicName,
            "    name: nameInSuperItemMetadata",
            "    item:",
            "      type: "+ BasicEntity.class.getName(),
            "  - id: " + symbolicName,
            "    name: nameInItemMetadata",
            "    item:",
            "      type: "+ parentSymbolicName);

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "nameInItemMetadata");
        
        Entity app2 = createAndStartApplication(
                "services:",
                "- type: "+symbolicName,
                "  name: nameInEntity");
        BasicEntity entity2 = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app2), BasicEntity.class));
        assertEquals(entity2.getDisplayName(), "nameInEntity");
    }
    
    @Test
    public void testUsesDefaultNameInCatalogItemRatherThanItemMetadata() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  name: nameInItemMetadata",
            "  version: " + TEST_VERSION,
            "  item:",
            "    type: "+ BasicEntity.class.getName(),
            "    brooklyn.config:",
            "      defaultDisplayName: defaultNameInItemEntity");

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "defaultNameInItemEntity");
        
        Entity app2 = createAndStartApplication(
                "services:",
                "- type: "+symbolicName,
                "  name: nameInEntity");
        BasicEntity entity2 = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app2), BasicEntity.class));
        assertEquals(entity2.getDisplayName(), "nameInEntity");
    }
    
    @Test
    public void testUsesDefaultNameInCatalogItemRatherThanItemOrSupertypeMetadata() throws Exception {
        String parentSymbolicName = "mySyperType";
        String symbolicName = "mySubType";
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  items:",
            "  - id: " + parentSymbolicName,
            "    name: nameInSuperItemMetadata",
            "    item:",
            "      type: "+ BasicEntity.class.getName(),
            "  - id: " + symbolicName,
            "    name: nameInItemMetadata",
            "    item:",
            "      type: "+ parentSymbolicName,
            "      brooklyn.config:",
            "        defaultDisplayName: defaultNameInItemEntity");

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "defaultNameInItemEntity");
    }
    
    @Test
    public void testUsesDefaultNameInSupertypeWhenItemExtendsOtherItem() throws Exception {
        String parentSymbolicName = "mySyperType";
        String symbolicName = "mySubType";
        addCatalogItems(
            "brooklyn.catalog:",
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  items:",
            "  - id: " + parentSymbolicName,
            "    name: nameInSuperItemMetadata",
            "    item:",
            "      type: "+ BasicEntity.class.getName(),
            "      brooklyn.config:",
            "        defaultDisplayName: defaultNameInSuperItemEntity",
            "  - id: " + symbolicName,
            "    name: nameInItemMetadata",
            "    item:",
            "      type: "+ parentSymbolicName);

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName);
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "defaultNameInSuperItemEntity");
    }
    
    @Test
    public void testUsesDefaultNameInEntityRatherThanItemMetadata() throws Exception {
        String symbolicName = "my.catalog.app.id.load";
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  name: nameInItemMetadata",
            "  version: " + TEST_VERSION,
            "  item:",
            "    type: "+ BasicEntity.class.getName());

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+symbolicName,
                "  brooklyn.config:",
                "    defaultDisplayName: defaultNameInEntity");
        BasicEntity entity = Iterables.getOnlyElement(Iterables.filter(Entities.descendants(app), BasicEntity.class));
        assertEquals(entity.getDisplayName(), "defaultNameInEntity");
    }
}
