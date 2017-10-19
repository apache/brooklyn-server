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
import java.nio.charset.StandardCharsets;
import java.util.Set;

import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class BrooklynLauncherRebindCatalogOsgiTest extends AbstractBrooklynLauncherRebindTest {

    private static final VersionedName COM_EXAMPLE_BUNDLE_ID = new VersionedName(
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL, 
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_VERSION);
    
    private static final Set<VersionedName> COM_EXAMPLE_BUNDLE_CATALOG_IDS = ImmutableSet.of(
            new VersionedName("com.example.simpleTest", "0.0.0-SNAPSHOT"));

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
    public void testRebindGetsInitialOsgiCatalog() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        VersionedName bundleName = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, bundleName, bundleItems);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(initialBomFile.getAbsolutePath());
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, bundleItems);
        assertManagedBundle(newLauncher, bundleName, bundleItems);
    }

    @Test
    public void testRebindUpgradesBundleWithSameItems() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        
        VersionedName bundleNameV1 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFileV1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleNameV1);
        File initialBomFileV1 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV1.toURI()), ImmutableList.of()));

        VersionedName bundleNameV2 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "2.0.0");
        File bundleFileV2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleNameV2);
        File initialBomFileV2 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV2.toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFileV1.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, bundleNameV1, bundleItems);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(initialBomFileV2.getAbsolutePath());
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, bundleItems);
        assertManagedBundle(newLauncher, bundleNameV2, bundleItems);
    }

    @Test
    public void testRebindUpgradesBundleWithNewerItems() throws Exception {
        Set<VersionedName> bundleItemsV1 = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBomV1 = createCatalogYaml(ImmutableList.of(), bundleItemsV1);
        VersionedName bundleNameV1 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFileV1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBomV1.getBytes(StandardCharsets.UTF_8)), bundleNameV1);
        File initialBomFileV1 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV1.toURI()), ImmutableList.of()));

        Set<VersionedName> bundleItemsV2 = ImmutableSet.of(VersionedName.fromString("one:2.0.0"));
        String bundleBomV2 = createCatalogYaml(ImmutableList.of(), bundleItemsV2);
        VersionedName bundleNameV2 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "2.0.0");
        File bundleFileV2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBomV2.getBytes(StandardCharsets.UTF_8)), bundleNameV2);
        File initialBomFileV2 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV2.toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFileV1.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, bundleItemsV1);
        assertManagedBundle(launcher, bundleNameV1, bundleItemsV1);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(initialBomFileV2.getAbsolutePath());
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, Iterables.concat(bundleItemsV1, bundleItemsV2));
        assertManagedBundle(newLauncher, bundleNameV1, bundleItemsV1);
        assertManagedBundle(newLauncher, bundleNameV2, bundleItemsV2);
    }

    // TODO bundle fails to start: 
    //     missing requirement [com.example.brooklyn.test.resources.osgi.brooklyn-test-osgi-com-example-entities [147](R 147.0)] 
    //     osgi.wiring.package; (&(osgi.wiring.package=org.apache.brooklyn.config)(version>=0.12.0)(!(version>=1.0.0)))
    // Presumably because brooklyn-api is just on the classpath rather than in osgi.
    // How did such tests work previously?!
    @Test(groups="Broken")
    public void testRebindGetsInitialOsgiCatalogWithJava() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String initialBom = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  brooklyn.libraries:",
                "    - " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_URL);
        File initialBomFile = newTmpFile(initialBom);
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, COM_EXAMPLE_BUNDLE_CATALOG_IDS);
        assertManagedBundle(launcher, COM_EXAMPLE_BUNDLE_ID, COM_EXAMPLE_BUNDLE_CATALOG_IDS);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(initialBomFile.getAbsolutePath());
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, COM_EXAMPLE_BUNDLE_CATALOG_IDS);
        assertManagedBundle(newLauncher, COM_EXAMPLE_BUNDLE_ID, COM_EXAMPLE_BUNDLE_CATALOG_IDS);
    }
}
