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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.internal.CatalogEntityItemDto;
import org.apache.brooklyn.core.catalog.internal.CatalogItemBuilder;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;

public class RebindCatalogWhenCatalogPersistenceDisabledTest extends RebindTestFixtureWithApp {

    private static final Logger LOG = LoggerFactory.getLogger(RebindCatalogWhenCatalogPersistenceDisabledTest.class);

    private boolean catalogPersistenceWasPreviouslyEnabled;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        catalogPersistenceWasPreviouslyEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY);
        BrooklynFeatureEnablement.disable(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY);
        super.setUp();
        origApp.createAndManageChild(EntitySpec.create(TestEntity.class));
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        BrooklynFeatureEnablement.setEnablement(BrooklynFeatureEnablement.FEATURE_CATALOG_PERSISTENCE_PROPERTY, catalogPersistenceWasPreviouslyEnabled);
    }

    @Test
    public void testModificationsToCatalogAreNotPersistedWhenCatalogPersistenceFeatureIsDisabled() throws Exception {
        String symbolicName = "rebind-yaml-catalog-item-test";
        String version = "1.2.3";
        String yaml = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  items:",
                "  - id: " + symbolicName,
                "    version: " + version,
                "    itemType: entity",
                "    item:",
                "      type: io.camp.mock:AppServer");
        CatalogEntityItemDto item =
            CatalogItemBuilder.newEntity(symbolicName, version)
                .displayName(symbolicName)
                .plan(yaml)
                .build();
        origManagementContext.getCatalog().addItem(item);
        LOG.info("Added item to catalog: {}, id={}", item, item.getId());

        assertNotNull(origManagementContext.getCatalog().getCatalogItem(symbolicName, version));

        rebind();

        assertNull(newManagementContext.getCatalog().getCatalogItem(symbolicName, version));
    }
}
