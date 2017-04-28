/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.core.mgmt.classloading;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class ClassLoaderFromBrooklynClassLoadingContextTest {

    private LocalManagementContext mgmt;
    private CatalogItem<?, ?> item;
    private ClassLoaderFromBrooklynClassLoadingContext loader;
    
    @BeforeMethod(alwaysRun=true)
    @SuppressWarnings("deprecation")
    public void setUp() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        item = mgmt.getCatalog().addItem(BasicApplication.class);
        
        BrooklynClassLoadingContext clc = new OsgiBrooklynClassLoadingContext(mgmt, item.getCatalogItemId(), ImmutableList.<OsgiBundleWithUrl>of());
        loader = new ClassLoaderFromBrooklynClassLoadingContext(clc);
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (mgmt != null) {
            Entities.destroyAll(mgmt);
            mgmt = null;
        }
    }

    @Test
    public void testLoadNonExistantClassThrowsClassNotFound() throws Exception {
        try {
            loader.loadClass("my.clazz.does.not.Exist");
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassNotFoundException e) {
            // success;
        }
    }
}
