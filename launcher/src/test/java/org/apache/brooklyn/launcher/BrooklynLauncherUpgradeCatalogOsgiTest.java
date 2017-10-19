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
package org.apache.brooklyn.launcher;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.BundleUpgradeParser;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class BrooklynLauncherUpgradeCatalogOsgiTest extends AbstractBrooklynLauncherRebindTest {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    @Override
    protected boolean useOsgi() {
        return true;
    }
    
    @Override
    protected boolean reuseOsgi() {
        return false;
    }

    private BrooklynLauncher newLauncherForTests(String catalogInitial) {
        CatalogInitialization catalogInitialization = new CatalogInitialization(catalogInitial);
        return super.newLauncherForTests()
                .catalogInitialization(catalogInitialization);
    }

    @Test
    public void testRemoveLegacyItems() throws Exception {
        VersionedName one_0_1_0 = VersionedName.fromString("one:0.1.0");
        VersionedName two_0_1_0 = VersionedName.fromString("two:0.1.0");
        VersionedName two_1_0_0 = VersionedName.fromString("two:1.0.0");
        VersionedName three_0_1_0 = VersionedName.fromString("three:0.1.0");
        VersionedName three_0_2_0 = VersionedName.fromString("three:0.2.0");
        VersionedName four_0_1_0 = VersionedName.fromString("four:0.1.0");
        
        initPersistedState(ImmutableMap.<String, String>builder()
                .put("one_0.1.0", createLegacyPersistenceCatalogItem(one_0_1_0))
                .put("two_0.1.0", createLegacyPersistenceCatalogItem(two_0_1_0))
                .put("two_1.0.0", createLegacyPersistenceCatalogItem(two_1_0_0))
                .put("three_0.1.0", createLegacyPersistenceCatalogItem(three_0_1_0))
                .put("three_0.2.0", createLegacyPersistenceCatalogItem(three_0_2_0))
                .put("four_0.1.0", createLegacyPersistenceCatalogItem(four_0_1_0))
                .build());
        
        String bundleBom = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of());
        VersionedName bundleName = new VersionedName("org.example.testRemoveLegacyItems", "1.0.0");
        String removedLegacyItems = "\"one:[0,1.0.0)\",\"two:[0,1.0.0)\",\"three:0.1.0\"";
        Map<String, String> bundleManifest = ImmutableMap.of(BundleUpgradeParser.MANIFEST_HEADER_REMOVE_LEGACY_ITEMS, removedLegacyItems);
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName, bundleManifest);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(two_1_0_0, three_0_2_0, four_0_1_0));
        assertManagedBundle(launcher, bundleName, ImmutableSet.<VersionedName>of());

        launcher.terminate();
    }
}
