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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Identifiers;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class BrooklynLauncherRebindCatalogOsgiTest extends AbstractBrooklynLauncherRebindTest {

    private static final String CATALOG_EMPTY_INITIAL = "classpath://rebind-test-empty-catalog.bom";

    private static final VersionedName COM_EXAMPLE_BUNDLE_ID = new VersionedName(
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL, 
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_VERSION);
    
    private static final Set<VersionedName> COM_EXAMPLE_BUNDLE_CATALOG_IDS = ImmutableSet.of(
            new VersionedName("com.example.simpleTest", "0.0.0-SNAPSHOT"));

    /**
     * Whether we reuse OSGi Framework depends if we want it to feel like rebinding on a new machine 
     * (so no cached bundles), or a local restart. We can also use {@code reuseOsgi = true} to emulate
     * system bundles (by pre-installing them into the reused framework at the start of the test).
     */
    private boolean reuseOsgi = false;
    
    private List<Bundle> reusedBundles = new ArrayList<>();
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            reuseOsgi = false;
            for (Bundle bundle : reusedBundles) {
                if (bundle != null && bundle.getState() != Bundle.UNINSTALLED) {
                    bundle.uninstall();
                }
            }
            reusedBundles.clear();
        } finally {
            super.tearDown();
        }
    }
    
    @Override
    protected boolean useOsgi() {
        return true;
    }
    
    @Override
    protected boolean reuseOsgi() {
        return reuseOsgi;
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

    /**
     * See https://issues.apache.org/jira/browse/BROOKLYN-546.
     * 
     * We built up to launcher2, which will have three things:
     *  1. a pre-installed "system bundle"
     *  2. an initial catalog that references this same system bundle (hence the bundle will be "managed")
     *  3. persisted state that references this same system bundle.
     *
     * At the end of this, we want only one version of that "system bundle" to be installed, but also for
     * it to be a "brooklyn managed bundle".
     *
     * This scenario isn't quite the same as BROOKLYN-546. To make it fail, we'd need to have bundle URLs like:
     *     "mvn:org.apache.brooklyn/brooklyn-software-cm-ansible/1.0.0-SNAPSHOT"
     * (as is used in brooklyn-library/karaf/catalog/target/classes/catalog.bom).
     *
     * When that is used, the "osgi unique url" is the same as for the system library, so when it tries
     * to replace the library by calling "installBundle" then it fails.
     *
     * We ensure this isn't happening by asserting that our manually installed "system bundle" is still the same.
     */
    @Test
    public void testRebindWithSystemBundleInCatalog() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0-SNAPSHOT"));
        VersionedName bundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest."+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
        File bundleFile = newTmpBundle(bundleItems, bundleName);
        File bundleFileCopy = newTmpCopy(bundleFile);

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));

        reuseOsgi = true;
        
        // Add our bundle, so it feels for all intents and purposes like a "system bundle"
        Framework reusedFramework = initReusableOsgiFramework();
        Bundle pseudoSystemBundle = installBundle(reusedFramework, bundleFileCopy);
        reusedBundles.add(pseudoSystemBundle);
        
        // Launch brooklyn, where initial catalog includes a duplicate of the system bundle
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, bundleName, bundleItems);
        launcher.terminate();

        // Launch brooklyn, where persisted state now includes the initial catalog's bundle
        BrooklynLauncher launcher2 = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher2.start();
        assertCatalogConsistsOfIds(launcher2, bundleItems);
        assertManagedBundle(launcher2, bundleName, bundleItems);
        
        // Should not have replaced the original "system bundle"
        assertOnlyBundle(reusedFramework, bundleName, pseudoSystemBundle);

        launcher2.terminate();
    }

    @Test
    public void testInstallPreexistingBundle() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0-SNAPSHOT"));
        VersionedName bundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest."+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
        File bundleFile = newTmpBundle(bundleItems, bundleName);
        File bundleFileCopy = newTmpCopy(bundleFile);

        reuseOsgi = true;
        
        // Add our bundle, so it feels for all intents and purposes like a "system bundle"
        Framework reusedFramework = initReusableOsgiFramework();
        Bundle pseudoSystemBundle = installBundle(reusedFramework, bundleFileCopy);
        reusedBundles.add(pseudoSystemBundle);
        
        // Launch brooklyn, and explicitly install pre-existing bundle.
        // Should bring it under brooklyn-management (should not re-install it).
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher.start();
        installBrooklynBundle(launcher, bundleFile, false).getWithError();
        
        assertOnlyBundle(launcher, bundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, bundleName, bundleItems);
        launcher.terminate();

        // Launch brooklyn again (because will have persisted the pre-installed bundle)
        BrooklynLauncher launcher2 = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher2.start();
        assertOnlyBundle(reusedFramework, bundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher2, bundleItems);
        assertManagedBundle(launcher2, bundleName, bundleItems);
        launcher2.terminate();
    }

    @Test
    public void testInstallPreexistingBundleViaIndirectBrooklynLibrariesReference() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0-SNAPSHOT"));
        VersionedName systemBundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest.system"+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
        File systemBundleFile = newTmpBundle(bundleItems, systemBundleName);

        String bundleBom = createCatalogYaml(ImmutableList.of(), ImmutableList.of(systemBundleName), ImmutableList.of());
        VersionedName bundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest.initial"+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));
        
        reuseOsgi = true;
        
        // Add our bundle, so it feels for all intents and purposes like a "system bundle"
        Framework reusedFramework = initReusableOsgiFramework();
        Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
        reusedBundles.add(pseudoSystemBundle);
        
        // Launch brooklyn, with initial catalog pointing at bundle that points at system bundle.
        // Should bring it under brooklyn-management (without re-installing it).
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertOnlyBundle(launcher, systemBundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, systemBundleName, bundleItems);
        assertManagedBundle(launcher, bundleName, ImmutableSet.of());
        launcher.terminate();

        // Launch brooklyn again (because will have persisted both those bundles)
        BrooklynLauncher launcher2 = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher2.start();
        assertOnlyBundle(launcher2, systemBundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher2, bundleItems);
        assertManagedBundle(launcher2, systemBundleName, bundleItems);
        assertManagedBundle(launcher2, bundleName, ImmutableSet.of());
        launcher2.terminate();
    }

    @Test
    public void testInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference() throws Exception {
        runInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference(false);
    }
    
    // Aled thought we supported version ranges in 'brooklyn.libraries', but doesn't work here.
    @Test(groups="Broken")
    public void testInstallPreexistingBundleViaInitialBomBrooklynLibrariesReferenceWithVersionRange() throws Exception {
        runInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference(true);
    }
    
    protected void runInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference(boolean useVersionRange) throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        VersionedName systemBundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest.system"+Identifiers.makeRandomId(4), "1.0.0");
        File systemBundleFile = newTmpBundle(bundleItems, systemBundleName);

        VersionedName systemBundleNameRef;
        if (useVersionRange) {
            systemBundleNameRef = new VersionedName(systemBundleName.getSymbolicName(), "[1,2)");
        } else {
            systemBundleNameRef = systemBundleName;
        }
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(), ImmutableList.of(systemBundleNameRef), ImmutableList.of()));
        
        reuseOsgi = true;
        
        // Add our bundle, so it feels for all intents and purposes like a "system bundle"
        Framework reusedFramework = initReusableOsgiFramework();
        Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
        reusedBundles.add(pseudoSystemBundle);
        
        // Launch brooklyn, with initial catalog pointing at system bundle.
        // Should bring it under brooklyn-management (without re-installing it).
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertOnlyBundle(launcher, systemBundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, systemBundleName, bundleItems);
        launcher.terminate();

        // Launch brooklyn again (because will have persisted both those bundles)
        BrooklynLauncher launcher2 = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher2.start();
        assertOnlyBundle(launcher2, systemBundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher2, bundleItems);
        assertManagedBundle(launcher2, systemBundleName, bundleItems);
        launcher2.terminate();
    }

    @Test
    public void testInstallReplacesPreexistingBundleWithoutForce() throws Exception {
        runInstallReplacesPreexistingBundle(false);
    }
    
    @Test
    public void testInstallReplacesPreexistingBundleWithForce() throws Exception {
        runInstallReplacesPreexistingBundle(true);
    }
    
    protected void runInstallReplacesPreexistingBundle(boolean force) throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0-SNAPSHOT"));
        VersionedName bundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest."+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
        File systemBundleFile = newTmpBundle(bundleItems, bundleName);
        File replacementBundleFile = newTmpBundle(bundleItems, bundleName, "randomDifference"+Identifiers.makeRandomId(4));

        reuseOsgi = true;
        
        // Add our bundle, so it feels for all intents and purposes like a "system bundle"
        Framework reusedFramework = initReusableOsgiFramework();
        Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
        reusedBundles.add(pseudoSystemBundle);
        
        // Launch brooklyn, and explicitly install pre-existing bundle.
        // Should bring it under brooklyn-management (should not re-install it).
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher.start();
        installBrooklynBundle(launcher, replacementBundleFile, force).getWithError();
        
        assertOnlyBundleReplaced(launcher, bundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher, bundleItems);
        assertManagedBundle(launcher, bundleName, bundleItems);
        launcher.terminate();

        // Launch brooklyn again (because will have persisted the pre-installed bundle)
        BrooklynLauncher launcher2 = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher2.start();
        assertOnlyBundleReplaced(reusedFramework, bundleName, pseudoSystemBundle);
        assertCatalogConsistsOfIds(launcher2, bundleItems);
        assertManagedBundle(launcher2, bundleName, bundleItems);
        launcher2.terminate();
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
    
    @Test
    public void testPersistsSingleCopyOfInitialCatalog() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        VersionedName bundleName = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog"+Identifiers.makeRandomId(4), "1.0.0");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));

        // First launcher should persist the bundle
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        String bundlePersistenceId = findManagedBundle(launcher, bundleName).getId();
        launcher.terminate();
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId));

        // Second launcher should read from initial catalog and persisted state. Both those bundles have different ids.
        // Should only end up with one of them in persisted state.
        // (Current impl is that it will be the "initial catalog" version, discarding the previously persisted bundle).
        BrooklynLauncher launcher2 = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher2.start();
        String bundlePersistenceId2 = findManagedBundle(launcher2, bundleName).getId();
        launcher2.terminate();
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId2));
    }

    /**
     * It is vital that the brooklyn-managed-bundle ids match those in persisted state. If they do not, 
     * then deletion of a brooklyn-managed-bundle will not actually delete it from persisted state.
     * 
     * Under the covers, the scenario of HOT_STANDBY promoted to MASTER is very different from when 
     * it starts as master:
     * <ol>
     *   <li> We become hot-standby; we call RebindManager.startReaOnly (so PeriodicDeltaChangeListener 
     *        discards changes).
     *   <li>We repeatedly call rebindManager.rebind():
     *     <ol>
     *       <li>Each time, we populate the catalog from the initial and persisted state
     *       <li>The OsgiManager.ManagedBundleRecords is populated the first time; subsequent times we see it is
     *           already a managed bundle so do nothing.
     *       <li>The first time, it calls to the PeriodicDeltaChangeListener about the managed bundle, but the
     *           change is discarded (because we are read-only, and PeriodicDeltaChangeListener is 'STOPPED').
     *     </ol>
     *   <li>On HighAvailabilityManagerImpl promoting us from HOT_STANDBY to MASTER:
     *     <ol>
     *       <li>Calls rebindManager.stopReadOnly; this resets the PeriodicDeltaChangeListener
     *           (so that it's in the INIT state, ready for recording whatever happens while we're promoting to MASTER)
     *       <li>Clears our cache of brooklyn-managed-bundles. This is important so that we record
     *           (and thus update persistence) for the "initial catalog" bundles being managed.
     *       <li>Calls rebindManager.rebind(); we are in a good state to do this, having cleared out whatever
     *           was done previously while we were hot-standby.
     *         <ol>
     *           <li>The new ManagedBundle instances from the "initial catalog" are recorded in the 
     *               PeriodicDeltaChangeListener.
     *           <li>For persisted bundles, if they were duplicates of existing brooklyn-managed-bundles then they
     *               are recorded as deleted (in PeriodicDeltaChangeListener).
     *         </ol>
     *       <li>HighAvailabilityManagerImpl.promoteToMaster() then calls rebindManager.start(), which switches us
     *           into writable mode. The changes recorded in PeriodicDeltaChangeListener  are applied to the
     *           persisted state.
     *     </ol>
     * </ol>
     * 
     * In contrast, when we start as MASTER:
     * <ol>
     *   <li>We call rebindManager.setPersister(); the PeriodicDeltaChangeListener is in 'INIT' state 
     *       so will record any changes. 
     *   <li>We call rebindManager.rebind(); it populates the catalog from the initial and persisted state; 
     *     <ol>
     *       <li>The new ManagedBundle instances from the "initial catalog" are recorded in the PeriodicDeltaChangeListener.
     *       <li>For persisted bundles, if they were duplicates of existing brooklyn-managed-bundles then they
     *           are recorded as deleted (in PeriodicDeltaChangeListener).
     *     </ol>
     *   <li>We call rebindManager.startPersistence(); this enables write-access to the persistence store,
     *       and starts the PeriodicDeltaChangeListener (so all recorded changes are applied).
     * </ol>
     */
    @Test
    public void testPersistsSingleCopyOfInitialCatalogOnHotStandbyPromotion() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        VersionedName bundleName = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog"+Identifiers.makeRandomId(4), "1.0.0");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));

        // First launcher should persist the bundle
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath())
                .highAvailabilityMode(HighAvailabilityMode.MASTER);
        launcher.start();
        String bundlePersistenceId = findManagedBundle(launcher, bundleName).getId();
        RebindTestUtils.waitForPersisted(launcher.getManagementContext());
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId));

        // Second launcher goes into hot-standby, and will thus rebind periodically.
        // When we terminate the first launcher, it will be promoted to master automatically.
        BrooklynLauncher launcher2 = newLauncherForTests(initialBomFile.getAbsolutePath())
                .highAvailabilityMode(HighAvailabilityMode.HOT_STANDBY);
        launcher2.start();
        assertHotStandbyEventually(launcher2);
        
        launcher.terminate();
        assertMasterEventually(launcher2);
        String bundlePersistenceId2 = findManagedBundle(launcher2, bundleName).getId();
        launcher2.terminate();
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId2));
    }

    private void assertHotStandbyEventually(BrooklynLauncher launcher) {
        ManagementContext mgmt = launcher.getManagementContext();
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertTrue(mgmt.isStartupComplete());
                assertTrue(mgmt.isRunning());
                assertEquals(mgmt.getHighAvailabilityManager().getNodeState(), ManagementNodeState.HOT_STANDBY);
            }});
    }
    
    private void assertMasterEventually(BrooklynLauncher launcher) {
        ManagementContext mgmt = launcher.getManagementContext();
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertTrue(mgmt.isStartupComplete());
                assertTrue(mgmt.isRunning());
                assertEquals(mgmt.getHighAvailabilityManager().getNodeState(), ManagementNodeState.MASTER);
            }});
    }
    
    private Bundle installBundle(Framework framework, File bundle) throws Exception {
        try (FileInputStream stream = new FileInputStream(bundle)) {
            return framework.getBundleContext().installBundle(bundle.toURI().toString(), stream);
        }
    }
    
    private ReferenceWithError<OsgiBundleInstallationResult> installBrooklynBundle(BrooklynLauncher launcher, File bundleFile, boolean force) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get();
        try (FileInputStream bundleStream = new FileInputStream(bundleFile)) {
            return osgiManager.install(null, bundleStream, true, true, force);
        }
    }
    
    private void assertOnlyBundle(BrooklynLauncher launcher, VersionedName bundleName, Bundle expectedBundle) {
        Framework framework = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get().getFramework();
        assertOnlyBundle(framework, bundleName, expectedBundle);
    }
    
    private void assertOnlyBundleReplaced(BrooklynLauncher launcher, VersionedName bundleName, Bundle expectedBundle) {
        Framework framework = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get().getFramework();
        assertOnlyBundleReplaced(framework, bundleName, expectedBundle);
    }
    
    private void assertOnlyBundle(Framework framework, VersionedName bundleName, Bundle expectedBundle) {
        List<Bundle> matchingBundles = Osgis.bundleFinder(framework).symbolicName(bundleName.getSymbolicName()).version(bundleName.getOsgiVersionString()).findAll();
        assertTrue(matchingBundles.contains(expectedBundle), "Bundle missing; matching="+matchingBundles);
        assertEquals(matchingBundles.size(), 1, "Extra bundles; matching="+matchingBundles);
    }
    
    private void assertOnlyBundleReplaced(Framework framework, VersionedName bundleName, Bundle expectedBundle) {
        List<Bundle> matchingBundles = Osgis.bundleFinder(framework).symbolicName(bundleName.getSymbolicName()).version(bundleName.getOsgiVersionString()).findAll();
        assertFalse(matchingBundles.contains(expectedBundle), "Bundle still present; matching="+matchingBundles);
        assertEquals(matchingBundles.size(), 1, "Extra bundles; matching="+matchingBundles);
    }
    
    private Framework initReusableOsgiFramework() {
        if (!reuseOsgi) throw new IllegalStateException("Must first set reuseOsgi");
        
        if (OsgiManager.tryPeekFrameworkForReuse().isAbsent()) {
            BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
            launcher.start();
            launcher.terminate();
            Os.deleteRecursively(persistenceDir);
        }
        return OsgiManager.tryPeekFrameworkForReuse().get();
    }

    private File newTmpBundle(Set<VersionedName> catalogItems, VersionedName bundleName) {
        return newTmpBundle(catalogItems, bundleName, null);
    }
    
    private File newTmpBundle(Set<VersionedName> catalogItems, VersionedName bundleName, String randomNoise) {
        String bundleBom = createCatalogYaml(ImmutableList.of(), ImmutableList.of(), catalogItems, randomNoise);
        return newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
    }
}
