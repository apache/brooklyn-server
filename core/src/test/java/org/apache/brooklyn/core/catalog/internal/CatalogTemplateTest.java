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
package org.apache.brooklyn.core.catalog.internal;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

public class CatalogTemplateTest {
    private LocalManagementContext managementContext;
    private BrooklynCatalog catalog;
    
    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        managementContext = LocalManagementContextForTests.newInstance();
        catalog = managementContext.getCatalog();
    }
    
    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (managementContext != null) Entities.destroyAll(managementContext);
    }

    @Test
    public void testMultiLineStringTemplate() {
        catalog.addItems(
            "brooklyn.catalog:\n" + 
            "  version: 1\n" + 
            "  items:\n" + 
            "    - id: test\n" + 
            "      itemType: template\n" + 
            "      item: |\n" + 
            "        # comment\n" + 
            "        type: "+BasicEntity.class.getCanonicalName()+"\n");
        CatalogItem<Object, Object> item = assertSingleCatalogItem("test", "1");
        Asserts.assertTrue(item.getPlanYaml().startsWith("# comment"), "Wrong format: "+item.getPlanYaml());
    }
    
    @Test
    public void testOneLineStringMapTemplate() {
        catalog.addItems(
            "brooklyn.catalog:\n" + 
            "  version: 1\n" + 
            "  items:\n" + 
            "    - id: test\n" + 
            "      itemType: template\n" + 
            "      item: |\n" + 
            "        type: "+BasicEntity.class.getCanonicalName()+"\n");
        CatalogItem<Object, Object> item = assertSingleCatalogItem("test", "1");
        Asserts.assertTrue(item.getPlanYaml().startsWith("type: org"), "Wrong format: "+item.getPlanYaml());
    }
    
    @Test
    public void testOneLineStringTypeTemplate() {
        catalog.addItems(
            "brooklyn.catalog:\n" + 
            "  version: 1\n" + 
            "  items:\n" + 
            "    - id: test\n" + 
            "      itemType: template\n" + 
            "      item: |\n" + 
            "        "+BasicEntity.class.getCanonicalName()+"\n");
        CatalogItem<Object, Object> item = assertSingleCatalogItem("test", "1");
        Asserts.assertTrue(item.getPlanYaml().startsWith("type: |\n  org"), "Wrong format: "+item.getPlanYaml());
    }
    
    @Test
    public void testMapTemplate() {
        catalog.addItems(
            "brooklyn.catalog:\n" + 
            "  version: 1\n" + 
            "  items:\n" + 
            "    - id: test\n" + 
            "      itemType: template\n" + 
            "      item:\n" + 
            "        type: "+BasicEntity.class.getCanonicalName()+"\n");
        CatalogItem<Object, Object> item = assertSingleCatalogItem("test", "1");
        Asserts.assertTrue(item.getPlanYaml().startsWith("type: org"), "Wrong format: "+item.getPlanYaml());
    }
    
    private CatalogItem<Object, Object> assertSingleCatalogItem(String symbolicName, String version) {
        Iterable<CatalogItem<Object, Object>> items = catalog.getCatalogItems(CatalogPredicates.symbolicName(Predicates.equalTo(symbolicName)));
        CatalogItem<Object, Object> item = Iterables.getOnlyElement(items);
        assertEquals(item.getSymbolicName(), symbolicName);
        assertEquals(item.getVersion(), version);
        return item;
    }


}
