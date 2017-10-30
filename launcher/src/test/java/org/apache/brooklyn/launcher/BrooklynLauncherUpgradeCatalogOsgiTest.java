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

import static org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog.CATALOG_BOM;
import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES;
import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS;
import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES;

import java.io.File;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

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
        
        newPersistedStateInitializer()
                .legacyCatalogItems(ImmutableMap.<String, String>builder()
                    .put("one_0.1.0", createLegacyPersistenceCatalogItem(one_0_1_0))
                    .put("two_0.1.0", createLegacyPersistenceCatalogItem(two_0_1_0))
                    .put("two_1.0.0", createLegacyPersistenceCatalogItem(two_1_0_0))
                    .put("three_0.1.0", createLegacyPersistenceCatalogItem(three_0_1_0))
                    .put("three_0.2.0", createLegacyPersistenceCatalogItem(three_0_2_0))
                    .put("four_0.1.0", createLegacyPersistenceCatalogItem(four_0_1_0))
                    .build())
                .initState();
        
        String bundleBom = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of());
        VersionedName bundleName = new VersionedName("org.example.testRemoveLegacyItems", "1.0.0");
        Map<String, String> bundleManifest = ImmutableMap.of(MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"one:[0,1.0.0)\",\"two:[0,1.0.0)\",\"three:0.1.0\"");
        File bundleFile = newTmpBundle(ImmutableMap.of(CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName, bundleManifest);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(two_1_0_0, three_0_2_0, four_0_1_0));
        assertManagedBundle(launcher, bundleName, ImmutableSet.<VersionedName>of());

        launcher.terminate();
    }
    
    @Test
    public void testForceUpgradeBundle() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        String bundleSymbolicName = "org.example.testForceUpgradeBundle";
        VersionedName bundleVersionedName1 = new VersionedName(bundleSymbolicName, "1.0.0");
        String bundleBom1 = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0));
        File bundleFile1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom1.getBytes(StandardCharsets.UTF_8)), bundleVersionedName1);

        newPersistedStateInitializer()
                .bundle(bundleVersionedName1, bundleFile1)
                .initState();
        
        VersionedName bundleVersionedName2 = new VersionedName(bundleSymbolicName, "2.0.0");
        String bundleBom2 = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0));
        Map<String, String> bundleManifest2 = ImmutableMap.of(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\""+bundleSymbolicName+":[0.0.0,2.0.0)\"");
        File bundleFile2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom2.getBytes(StandardCharsets.UTF_8)), bundleVersionedName2, bundleManifest2);

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile2.toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_2_0_0));
        assertManagedBundle(launcher, bundleVersionedName2, ImmutableSet.<VersionedName>of(one_2_0_0));
        assertNotManagedBundle(launcher, bundleVersionedName1);
        launcher.terminate();
    }
    
    // Simple test (no upgrade), important for validating that other tests really do as expected!
    @Test
    public void testLoadsBundleFromPersistedState() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        
        String bundleSymbolicName = "org.example.testForceUpgradeBundle";
        VersionedName bundleVersionedName = new VersionedName(bundleSymbolicName, "1.0.0");
        String bundleBom = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0));
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleVersionedName);

        newPersistedStateInitializer()
                .bundle(bundleVersionedName, bundleFile)
                .initState();
        
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_1_0_0));
        assertManagedBundle(launcher, bundleVersionedName, ImmutableSet.<VersionedName>of(one_1_0_0));
        launcher.terminate();
    }
    
    // removed item with upgrade deployed after rebind
    // TODO WIP
    @Test
    public void testForceUpgradeItemByRemovingBundle() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        String bundleSymbolicName = "org.example.testForceUpgradeBundle";
        VersionedName bundleVersionedName1 = new VersionedName(bundleSymbolicName, "1.0.0");
        String bundleBom1 = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0));
        File bundleFile1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom1.getBytes(StandardCharsets.UTF_8)), bundleVersionedName1);

        newPersistedStateInitializer()
                .bundle(bundleVersionedName1, bundleFile1)
                .initState();
        
        VersionedName bundleVersionedName2 = new VersionedName(bundleSymbolicName, "2.0.0");
        String bundleBom2 = createCatalogYaml(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0));
        Map<String, String> bundleManifest2 = ImmutableMap.of(
            MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\""+bundleSymbolicName+":[0.0.0,2.0.0)\"",
            MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "*");
        File bundleFile2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom2.getBytes(StandardCharsets.UTF_8)), bundleVersionedName2, bundleManifest2);

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile2.toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();

        Application app = createAndStartApplication(launcher.getManagementContext(), 
            "services: [ { type: 'one:1.0.0' } ]");
        Entity one = Iterables.getOnlyElement( app.getChildren() );
        Assert.assertEquals(one.getCatalogItemId(), "one:2.0.0");
        
        launcher.terminate();
    }
        
    // NB other related tests in BrooklynLauncherRebindCatalogOsgiTest:
    // * removed item in deployment fails - rebind and upgrade uses new item
    // * removed item in deployment upgrades - rebind and upgrade uses new item
    // * removed item in spec fails
    // * removed item in spec upgrades
    
}
