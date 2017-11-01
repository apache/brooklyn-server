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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.text.Identifiers;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public abstract class BrooklynLauncherRebindCatalogOsgiTest extends AbstractBrooklynLauncherRebindTest {

    private static final String CATALOG_EMPTY_INITIAL = "classpath://rebind-test-empty-catalog.bom";

    private static final VersionedName COM_EXAMPLE_BUNDLE_ID = new VersionedName(
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL, 
            OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_VERSION);
    
    private static final Set<VersionedName> COM_EXAMPLE_BUNDLE_CATALOG_IDS = ImmutableSet.of(
            new VersionedName("com.example.simpleTest", "0.1.0"));

    /**
     * Whether we reuse OSGi Framework depends if we want it to feel like rebinding on a new machine 
     * (so no cached bundles), or a local restart. We can also use {@code reuseOsgi = true} to emulate
     * system bundles (by pre-installing them into the reused framework at the start of the test).
     * 
     * Default true because this speeds things up, but some fixtures run with it false.
     */
    protected boolean reuseOsgi = true;
    
    protected List<Bundle> manuallyInsertedBundles = new ArrayList<>();
    
    BrooklynLauncher launcherT1, launcherT2, launcherLast;
    Runnable startupAssertions;

    protected abstract boolean isT1KeptRunningWhenT2Starts();
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            launcherT1 = null; launcherT2 = null; launcherLast = null;
            startupAssertions = null;
            reuseOsgi = true;
            
            // OSGi reuse system will clear cache on framework no longer being used,
            // but we've installed out of band so need to clean it up ourselves if the test doesn't actually use it!
            for (Bundle bundle : manuallyInsertedBundles) {
                if (bundle != null && bundle.getState() != Bundle.UNINSTALLED) {
                    bundle.uninstall();
                }
            }
            manuallyInsertedBundles.clear();
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

    protected BrooklynLauncher newLauncherForTests(String catalogInitial) {
        CatalogInitialization catalogInitialization = new CatalogInitialization(catalogInitial);
        return super.newLauncherForTests()
                .catalogInitialization(catalogInitialization);
    }        
    
    protected void startT1(BrooklynLauncher l) {
        if (launcherT1!=null) throw new IllegalStateException("Already started T1 launcher");
        
        if (isT1KeptRunningWhenT2Starts()) {
            l.highAvailabilityMode(HighAvailabilityMode.MASTER);
        }
        l.start();
        launcherLast = launcherT1 = l;
        assertMasterEventually(launcherT1);
        
        if (startupAssertions!=null) startupAssertions.run();
    }
    
    protected void startT2(BrooklynLauncher l) {
        startT2(l, true);
    }
    
    protected void startT2(BrooklynLauncher l, boolean expectSuccess) {
        if (launcherT2!=null) throw new IllegalStateException("Already started T2 launcher");
        
        try {
            RebindTestUtils.waitForPersisted(launcherT1.getManagementContext());
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
        if (!isT1KeptRunningWhenT2Starts()) {
            launcherT1.terminate();
        } else {
            l.highAvailabilityMode(HighAvailabilityMode.HOT_STANDBY);
        }
        l.start();
        launcherLast = launcherT2 = l;
        if (expectSuccess) {
            if (isT1KeptRunningWhenT2Starts()) { 
                assertHotStandbyNow(launcherT2);
            } else {
                assertMasterEventually(launcherT2);
            }
        } else {
            assertFailsEventually(launcherLast);
        }
        
        if (startupAssertions!=null) startupAssertions.run();
    }
    protected void promoteT2IfStandby() {
        promoteT2IfStandby(true);
    }
    protected void promoteT2IfStandby(boolean expectHealthy) {
        if (isT1KeptRunningWhenT2Starts()) {
            launcherT1.terminate();
            if (expectHealthy) {
                assertMasterEventually(launcherT2);
            } else {
                assertFailsEventually(launcherT2);
            }
            
            if (startupAssertions!=null) startupAssertions.run();
        }
    }
    
    @Test
    public static class LauncherRebindSubTests extends BrooklynLauncherRebindCatalogOsgiTest {
        @Override protected boolean isT1KeptRunningWhenT2Starts() { return false; }
        
        // tests here should run only for straight rebind as they assume a reused framework
        
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

            // Add our bundle, so it feels for all intents and purposes like a "system bundle"
            Framework reusedFramework = initReusableOsgiFramework();
            Bundle pseudoSystemBundle = installBundle(reusedFramework, bundleFileCopy);
            manuallyInsertedBundles.add(pseudoSystemBundle);
            
            startupAssertions = () -> {
                assertCatalogConsistsOfIds(launcherLast, bundleItems);
                assertManagedBundle(launcherLast, bundleName, bundleItems);
            };
            
            // Launch brooklyn, where initial catalog includes a duplicate of the system bundle
            startT1(newLauncherForTests(initialBomFile.getAbsolutePath()));

            // Launch brooklyn, where persisted state now includes the initial catalog's bundle
            startT2(newLauncherForTests(initialBomFile.getAbsolutePath()));
            
            // Should not have replaced the original "system bundle"
            assertOnlyBundle(reusedFramework, bundleName, pseudoSystemBundle);

            launcherT2.terminate();
        }
        
        @Test
        public void testInstallPreexistingBundle() throws Exception {
            Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0-SNAPSHOT"));
            VersionedName bundleName = new VersionedName("org.example.brooklynLauncherRebindCatalogOsgiTest."+Identifiers.makeRandomId(4), "1.0.0.SNAPSHOT");
            File bundleFile = newTmpBundle(bundleItems, bundleName);
            File bundleFileCopy = newTmpCopy(bundleFile);

            // Add our bundle, so it feels for all intents and purposes like a "system bundle"
            Framework reusedFramework = initReusableOsgiFramework();
            Bundle pseudoSystemBundle = installBundle(reusedFramework, bundleFileCopy);
            manuallyInsertedBundles.add(pseudoSystemBundle);
            
            // Launch brooklyn, and explicitly install pre-existing bundle.
            // Should bring it under brooklyn-management (should not re-install it).
            startT1(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            installBrooklynBundle(launcherT1, bundleFile, false).getWithError();
            
            assertOnlyBundle(launcherT1, bundleName, pseudoSystemBundle);
            startupAssertions = () -> {
                assertCatalogConsistsOfIds(launcherLast, bundleItems);
                assertManagedBundle(launcherLast, bundleName, bundleItems);
            };
            startupAssertions.run();

            // Launch brooklyn again (because will have persisted the pre-installed bundle)
            startT2(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            assertOnlyBundle(reusedFramework, bundleName, pseudoSystemBundle);
            
            launcherT2.terminate();
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
            
            Preconditions.checkArgument(reuseOsgi, "Should be reusing OSGi; test did not correctly reset it.");
            // Add our bundle, so it feels for all intents and purposes like a "system bundle"
            Framework reusedFramework = initReusableOsgiFramework();
            Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
            manuallyInsertedBundles.add(pseudoSystemBundle);
            
            startupAssertions = () -> {
                assertOnlyBundle(launcherLast, systemBundleName, pseudoSystemBundle);
                assertCatalogConsistsOfIds(launcherLast, bundleItems);
                assertManagedBundle(launcherLast, systemBundleName, bundleItems);
                assertManagedBundle(launcherLast, bundleName, ImmutableSet.of());
            };
            
            // Launch brooklyn, with initial catalog pointing at bundle that points at system bundle.
            // Should bring it under brooklyn-management (without re-installing it).
            startT1(newLauncherForTests(initialBomFile.getAbsolutePath()));

            // Launch brooklyn again (because will have persisted both those bundles)
            startT2(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            
            launcherT2.terminate();
        }

        @Test
        public void testInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference() throws Exception {
            runInstallPreexistingBundleViaInitialBomBrooklynLibrariesReference(false);
        }
        
        // Aled thought we supported version ranges in 'brooklyn.libraries', but doesn't work here.
        // Alex confirms nope, not supported there yet (2017-10).
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
            
            // Add our bundle, so it feels for all intents and purposes like a "system bundle"
            Framework reusedFramework = initReusableOsgiFramework();
            Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
            manuallyInsertedBundles.add(pseudoSystemBundle);
            
            startupAssertions = () -> {
                assertOnlyBundle(launcherLast, systemBundleName, pseudoSystemBundle);
                assertCatalogConsistsOfIds(launcherLast, bundleItems);
                assertManagedBundle(launcherLast, systemBundleName, bundleItems);
            };
            
            // Launch brooklyn, with initial catalog pointing at system bundle.
            // Should bring it under brooklyn-management (without re-installing it).
            startT1(newLauncherForTests(initialBomFile.getAbsolutePath()));

            // Launch brooklyn again (because will have persisted both those bundles)
            startT2(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            launcherT2.terminate();
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

            // Add our bundle, so it feels for all intents and purposes like a "system bundle"
            Framework reusedFramework = initReusableOsgiFramework();
            Bundle pseudoSystemBundle = installBundle(reusedFramework, systemBundleFile);
            manuallyInsertedBundles.add(pseudoSystemBundle);
            
            // Launch brooklyn, and explicitly install pre-existing bundle.
            // Should bring it under brooklyn-management (should not re-install it).
            startT1(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            installBrooklynBundle(launcherT1, replacementBundleFile, force).getWithError();
            assertOnlyBundleReplaced(launcherLast, bundleName, pseudoSystemBundle);
            startupAssertions = () -> {
                assertCatalogConsistsOfIds(launcherLast, bundleItems);
                assertManagedBundle(launcherLast, bundleName, bundleItems);
            };
            startupAssertions.run();
            
            // Launch brooklyn again (because will have persisted the pre-installed bundle)
            startT2(newLauncherForTests(CATALOG_EMPTY_INITIAL));
            assertOnlyBundleReplaced(reusedFramework, bundleName, pseudoSystemBundle);
            launcherT2.terminate();
        }
    }
   
    @Test
    public static class HotStandbyRebindSubTests extends BrooklynLauncherRebindCatalogOsgiTest {
        @Override protected boolean isT1KeptRunningWhenT2Starts() { return true; }
    }

    public abstract static class AbstractNonReuseRebindSubTests extends BrooklynLauncherRebindCatalogOsgiTest {
        @BeforeMethod(alwaysRun=true)
        @Override
        public void setUp() throws Exception {
            super.setUp();
            //reuseOsgi = false;
        }
    }
    @Test(groups="Integration")
    public static class NonReuseRebindSubTests extends AbstractNonReuseRebindSubTests {
        @Override protected boolean isT1KeptRunningWhenT2Starts() { return false; }
    }
    @Test(groups="Integration")
    public static class HotStandbyNonReuseRebindSubTests extends AbstractNonReuseRebindSubTests {
        @Override protected boolean isT1KeptRunningWhenT2Starts() { return false; }
    }
    
    @Test
    public void testRebindGetsInitialOsgiCatalog() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        VersionedName bundleName = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));
        
        startupAssertions = () -> {
            assertCatalogConsistsOfIds(launcherLast, bundleItems);
            assertManagedBundle(launcherLast, bundleName, bundleItems);
        };
        startT1( newLauncherForTests(initialBomFile.getAbsolutePath()) );
        startT2(newLauncherForTests(initialBomFile.getAbsolutePath()));
        promoteT2IfStandby();
    }
    
    @Test
    public void testRestartWithNewBundleWithSameItemsReplacesItems() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        
        VersionedName bundleNameV1 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFileV1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleNameV1);
        File initialBomFileV1 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV1.toURI()), ImmutableList.of()));

        VersionedName bundleNameV2 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "2.0.0");
        File bundleFileV2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleNameV2);
        File initialBomFileV2 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV2.toURI()), ImmutableList.of()));
        
        startupAssertions = () -> {
            assertCatalogConsistsOfIds(launcherLast, bundleItems);
            assertManagedBundle(launcherLast, bundleNameV1, bundleItems);
            
            String bundleVersionSupplyingType = getBundleSupplyingFirstType(bundleItems);
            if (launcherT2==null) {
                Assert.assertEquals(bundleVersionSupplyingType, "1.0.0");
                
            } else {
                assertManagedBundle(launcherLast, bundleNameV2, bundleItems);
                getBundleSupplyingFirstType(bundleItems);
                Assert.assertEquals(bundleVersionSupplyingType, "2.0.0");
            }
        };
        startT1(newLauncherForTests(initialBomFileV1.getAbsolutePath()));
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()));
        assertManagedBundle(launcherLast, bundleNameV2, bundleItems);
        promoteT2IfStandby();
    }

    protected String getBundleSupplyingFirstType(Set<VersionedName> bundleItems) {
        RegisteredType type = launcherLast.getManagementContext().getTypeRegistry().get(
            bundleItems.iterator().next().toString() );
        if (type==null) return null;
        return VersionedName.fromString( type.getContainingBundle() ).getVersionString();
    }

    @Test
    public void testRestartWithNewBundleWithNewItemsAddsItems() throws Exception {
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
        
        startupAssertions = () -> {
            assertManagedBundle(launcherLast, bundleNameV1, bundleItemsV1);
            Assert.assertEquals(getBundleSupplyingFirstType(bundleItemsV1), "1.0.0");
            if (launcherT2==null) {
                assertCatalogConsistsOfIds(launcherLast, bundleItemsV1);
                
            } else {
                assertCatalogConsistsOfIds(launcherLast, Iterables.concat(bundleItemsV1, bundleItemsV2));
                
                assertManagedBundle(launcherLast, bundleNameV2, bundleItemsV2);
                Assert.assertEquals(getBundleSupplyingFirstType(bundleItemsV2), "2.0.0");
            }
        };
        startT1(newLauncherForTests(initialBomFileV1.getAbsolutePath()));
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()));
        promoteT2IfStandby();
    }

    // if fails with wiring error you might need to rebuild items in utils/common/dependencies/osgi/
    @Test
    public void testRebindGetsInitialOsgiCatalogWithJava() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String initialBom = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  brooklyn.libraries:",
                "    - " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_URL);
        File initialBomFile = newTmpFile(initialBom);
        
        startupAssertions = () -> {
            assertCatalogConsistsOfIds(launcherLast, COM_EXAMPLE_BUNDLE_CATALOG_IDS);
            assertManagedBundle(launcherLast, COM_EXAMPLE_BUNDLE_ID, COM_EXAMPLE_BUNDLE_CATALOG_IDS);
        };
        startT1(newLauncherForTests(initialBomFile.getAbsolutePath()));
        startT2(newLauncherForTests(initialBomFile.getAbsolutePath()));
        promoteT2IfStandby();
    }
    
    /**
     * It is vital that the brooklyn-managed-bundle ids match those in persisted state. If they do not, 
     * then deletion of a brooklyn-managed-bundle will not actually delete it from persisted state.
     * 
     * This test (like many here) asserts for simple terminate-rebind and for hot-standby and promotion,
     * but here note the paths are vbery different. 
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
    public void testPersistsSingleCopyOfInitialCatalog() throws Exception {
        Set<VersionedName> bundleItems = ImmutableSet.of(VersionedName.fromString("one:1.0.0"));
        String bundleBom = createCatalogYaml(ImmutableList.of(), bundleItems);
        VersionedName bundleName = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog"+Identifiers.makeRandomId(4), "1.0.0");
        File bundleFile = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFile.toURI()), ImmutableList.of()));

        // First launcher should persist the bundle
        startT1(newLauncherForTests(initialBomFile.getAbsolutePath()));
        String bundlePersistenceId1 = findManagedBundle(launcherT1, bundleName).getId();
        
        if (!isT1KeptRunningWhenT2Starts()) {
            launcherT1.terminate();
        } else {
            RebindTestUtils.waitForPersisted(launcherT1.getManagementContext());
        }
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId1));

        // Second launcher should read from initial catalog and persisted state. Both those bundles have different ids.
        // Should only end up with one of them in persisted state.
        // (Current impl is that it will be the "initial catalog" version, discarding the previously persisted bundle).
        startT2(newLauncherForTests(initialBomFile.getAbsolutePath()));
        if (isT1KeptRunningWhenT2Starts()) {
            assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId1));
        }

        promoteT2IfStandby();
        
        String bundlePersistenceId2 = findManagedBundle(launcherT2, bundleName).getId();
        launcherT2.terminate();
        
        // ID2 only persisted after promotion - checks above are for ID1
        assertEquals(getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE), ImmutableSet.of(bundlePersistenceId2));

        if (isT1KeptRunningWhenT2Starts()) {
            // would like if we could make them the same but currently code should _always_ change
            Assert.assertNotEquals(bundlePersistenceId1, bundlePersistenceId2);
        }
    }
    
    @Test
    public void testRebindRemovedItemButLeavingJavaSucceeds() throws Exception {
        File initialBomFileV2 = prepForRebindRemovedItemTestReturningBomV2(false, false);
        createAndStartApplication(launcherLast.getManagementContext(), 
            "services: [ { type: 'simple-entity:1' } ]");
        
        // should start and promote fine, even though original catalog item ID not available
        
        // when we switch to loading from type registry types instead of persisted java type (see RebindIteration.load)
        // T2 startup may fail like testRebindRemovedItemIAlsoRemovingJavaDependencyCausesFailure does,
        // or it may fall back to the java type and succeed (but note this test does NOT allow the type to be upgraded)
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()));
        promoteT2IfStandby();
        
        Entity entity = Iterables.getOnlyElement( Iterables.getOnlyElement(launcherLast.getManagementContext().getApplications()).getChildren() );
        Assert.assertEquals(entity.getCatalogItemId(), "simple-entity:1.0.0");
    }
    
    @Test
    public void testRebindRemovedItemAndRemovingJavaDependencyCausesFailure() throws Exception {
        File initialBomFileV2 = prepForRebindRemovedItemTestReturningBomV2(true, false);
        createAndStartApplication(launcherLast.getManagementContext(), 
            "services: [ { type: 'simple-entity:1.0' } ]");
        
        // errors on promotion because the SimpleEntity.class isn't available

        if (isT1KeptRunningWhenT2Starts()) {
            // if starting hot-standby, HA manager will clear all state and set it to FAILED state;
            // for master/standalone/single it treats it as a startup subsystem error and does not
            // so for hot-standby we should assert everything is cleared
            startupAssertions = () -> {
                Asserts.assertSize(launcherLast.getManagementContext().getTypeRegistry().getAll(), 0);
                Asserts.assertSize(launcherLast.getManagementContext().getEntityManager().getEntities(), 0);
            };
        }
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()), false);
        Assert.assertFalse( ((ManagementContextInternal)launcherLast.getManagementContext()).errors().isEmpty() );
        
        promoteT2IfStandby(false);
        Assert.assertFalse( ((ManagementContextInternal)launcherLast.getManagementContext()).errors().isEmpty() );

        if (!isT1KeptRunningWhenT2Starts()) {
            // if t2 not running as standby then it should start, but with errors,
            // including entity shouldn't be loaded (but app is)
            Asserts.assertSize( launcherLast.getManagementContext().getApplications(), 1 );
            Asserts.assertSize( Iterables.getOnlyElement(launcherLast.getManagementContext().getApplications()).getChildren(), 0 );
        }
    }
    
    private File prepForRebindRemovedItemTestReturningBomV2(boolean removeSourceJavaBundle, boolean upgradeEntity) throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String initialBomV1 = Joiner.on("\n").join(
                "brooklyn.catalog:",
                "  brooklyn.libraries:",
                "    - " + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_URL,
                "  items:",
                "    - id: simple-entity",
                "      item:",
                "        type: com.example.brooklyn.test.osgi.entities.SimpleEntity");
        VersionedName bundleNameV1 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "1.0.0");
        File bundleFileV1 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, initialBomV1.getBytes()), bundleNameV1);
        File initialBomFileV1 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV1.toURI()), ImmutableList.of()));

        String initialBomV2 = Joiner.on("\n").join(
            "brooklyn.catalog:",
            "  items:",
            "    - id: simple-entity",
            "      item:",
            "        type: "+BasicEntity.class.getName());
        VersionedName bundleNameV2 = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", "2.0.0");
        Map<String,String> headers = MutableMap.of(
            BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "*"
                + (removeSourceJavaBundle ? ","+"'"+OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL+":"+OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_VERSION+"'"
                    : ""));
        if (upgradeEntity) {
            headers.put(BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "*");
        }
        File bundleFileV2 = newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, initialBomV2.getBytes()), bundleNameV2, headers);
        File initialBomFileV2 = newTmpFile(createCatalogYaml(ImmutableList.of(bundleFileV2.toURI()), ImmutableList.of()));

        startupAssertions = () -> {
            String v = launcherT2==null ? "1.0.0" : "2.0.0";
            VersionedName bv = new VersionedName("org.example.testRebindGetsInitialOsgiCatalog", v);
            VersionedName iv = new VersionedName("simple-entity", v);
            assertManagedBundle(launcherLast, bv, MutableSet.of(iv));
            if (launcherT2==null || !removeSourceJavaBundle) {
                assertCatalogConsistsOfIds(launcherLast, MutableList.copyOf(COM_EXAMPLE_BUNDLE_CATALOG_IDS).append(iv));
            } else {
                assertCatalogConsistsOfIds(launcherLast, MutableList.of(iv));
            }
        };
        
        startT1(newLauncherForTests(initialBomFileV1.getAbsolutePath()));
        return initialBomFileV2;
    }
    
    @Test
    public void testRebindUpgradeEntity() throws Exception {
        File initialBomFileV2 = prepForRebindRemovedItemTestReturningBomV2(false, true);
        createAndStartApplication(launcherLast.getManagementContext(), 
            "services: [ { type: 'simple-entity:1' } ]");
        
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()));
        promoteT2IfStandby();
        
        Entity entity = Iterables.getOnlyElement( Iterables.getOnlyElement(launcherLast.getManagementContext().getApplications()).getChildren() );
        
        // assert it was updated
        
        Assert.assertEquals(entity.getCatalogItemId(), "simple-entity:1.0.0");
        // TODO when upgrades work at level of catalog item ID and bundle ID, do below instead of above
//        Assert.assertEquals(entity.getCatalogItemId(), "simple-entity:2.0.0");
        
        Assert.assertEquals(Entities.deproxy(entity).getClass().getName(), "com.example.brooklyn.test.osgi.entities.SimpleEntityImpl");
        // TODO when registered types are used to create, instead of java type,
        // upgrade should cause the line below instead of the line above (see RebindIteration.load)
//        Assert.assertEquals(Entities.deproxy(entity).getClass().getName(), BasicEntityImpl.class.getName());
    }
    
    @Test(groups="WIP")
    public void testRebindUpgradeSpec() throws Exception {
        File initialBomFileV2 = prepForRebindRemovedItemTestReturningBomV2(true, true);
        Application app = createAndStartApplication(launcherLast.getManagementContext(), 
            Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.group.DynamicCluster",
                "  cluster.initial.size: 1",
                "  dynamiccluster.memberspec:",
                "    $brooklyn:entitySpec:",
                "      type: simple-entity:1") );
        Entity cluster = Iterables.getOnlyElement( app.getChildren() );
        ((DynamicCluster)cluster).resize(0);
        // at size 0 it should always persist and rebind, even with the dependent java removed (args to prep method above)
              
        startT2(newLauncherForTests(initialBomFileV2.getAbsolutePath()));
        promoteT2IfStandby();
        
        cluster = Iterables.getOnlyElement( Iterables.getOnlyElement(launcherLast.getManagementContext().getApplications()).getChildren() );
        ((DynamicCluster)cluster).resize(1);
        
        Entity entity = Iterables.getOnlyElement( ((DynamicCluster)cluster).getMembers() );
        
        // assert it uses the new spec
        
        Assert.assertEquals(entity.getCatalogItemId(), "simple-entity:1.0.0");
        // TODO when upgrades work at level of catalog item ID and bundle ID, do below instead of above
//        Assert.assertEquals(entity.getCatalogItemId(), "simple-entity:2.0.0");
        
        Assert.assertEquals(Entities.deproxy(entity).getClass().getName(), "com.example.brooklyn.test.osgi.entities.SimpleEntityImpl");
        // TODO when registered types are used to create, instead of java type,
        // upgrade should cause the line below instead of the line above (see RebindIteration.load)
//        Assert.assertEquals(Entities.deproxy(entity).getClass().getName(), BasicEntityImpl.class.getName());
    }
    
    protected void assertPersistedBundleListingEqualsEventually(BrooklynLauncher launcher, Set<VersionedName> bundles) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                try {
                    Set<String> expected = new LinkedHashSet<>();
                    for (VersionedName bundle : bundles) {
                        String bundleUid = findManagedBundle(launcher, bundle).getId();
                        expected.add(bundleUid);
                    }
                    Set<String> persisted = getPersistenceListing(BrooklynObjectType.MANAGED_BUNDLE); 
                    assertEquals(persisted, expected);
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }});
    }

    protected void assertHotStandbyNow(BrooklynLauncher launcher) {
        ManagementContext mgmt = launcher.getManagementContext();
        assertTrue(mgmt.isStartupComplete());
        assertTrue(mgmt.isRunning());
        assertEquals(mgmt.getNodeState(), ManagementNodeState.HOT_STANDBY);
        Asserts.assertSize(((ManagementContextInternal)mgmt).errors(), 0);
    }
    
    protected void assertMasterEventually(BrooklynLauncher launcher) {
        ManagementContext mgmt = launcher.getManagementContext();
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertTrue(mgmt.isStartupComplete());
                assertTrue(mgmt.isRunning());
                assertEquals(mgmt.getNodeState(), ManagementNodeState.MASTER);
                Asserts.assertSize(((ManagementContextInternal)mgmt).errors(), 0);
            }});
    }
    
    protected void assertFailsEventually(BrooklynLauncher launcher) {
        Asserts.succeedsEventually(() -> assertEquals(launcher.getManagementContext().getNodeState(), ManagementNodeState.FAILED));
    }
    
    protected Bundle installBundle(Framework framework, File bundle) throws Exception {
        try (FileInputStream stream = new FileInputStream(bundle)) {
            return framework.getBundleContext().installBundle(bundle.toURI().toString(), stream);
        }
    }
    
    protected ReferenceWithError<OsgiBundleInstallationResult> installBrooklynBundle(BrooklynLauncher launcher, File bundleFile, boolean force) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get();
        try (FileInputStream bundleStream = new FileInputStream(bundleFile)) {
            return osgiManager.install(null, bundleStream, true, true, force);
        }
    }
    
    protected void assertOnlyBundle(BrooklynLauncher launcher, VersionedName bundleName, Bundle expectedBundle) {
        Framework framework = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get().getFramework();
        assertOnlyBundle(framework, bundleName, expectedBundle);
    }
    
    protected void assertOnlyBundleReplaced(BrooklynLauncher launcher, VersionedName bundleName, Bundle expectedBundle) {
        Framework framework = ((ManagementContextInternal)launcher.getManagementContext()).getOsgiManager().get().getFramework();
        assertOnlyBundleReplaced(framework, bundleName, expectedBundle);
    }
    
    protected void assertOnlyBundle(Framework framework, VersionedName bundleName, Bundle expectedBundle) {
        List<Bundle> matchingBundles = Osgis.bundleFinder(framework).symbolicName(bundleName.getSymbolicName()).version(bundleName.getOsgiVersionString()).findAll();
        assertTrue(matchingBundles.contains(expectedBundle), "Bundle missing; matching="+matchingBundles);
        assertEquals(matchingBundles.size(), 1, "Extra bundles; matching="+matchingBundles);
    }
    
    protected void assertOnlyBundleReplaced(Framework framework, VersionedName bundleName, Bundle expectedBundle) {
        List<Bundle> matchingBundles = Osgis.bundleFinder(framework).symbolicName(bundleName.getSymbolicName()).version(bundleName.getOsgiVersionString()).findAll();
        assertFalse(matchingBundles.contains(expectedBundle), "Bundle still present; matching="+matchingBundles);
        assertEquals(matchingBundles.size(), 1, "Extra bundles; matching="+matchingBundles);
    }
    
    protected Framework initReusableOsgiFramework() {
        if (!reuseOsgi) throw new IllegalStateException("Must first set reuseOsgi");
        
        if (OsgiManager.tryPeekFrameworkForReuse().isAbsent()) {
            BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
            launcher.start();
            launcher.terminate();
            Os.deleteRecursively(persistenceDir);
        }
        return OsgiManager.tryPeekFrameworkForReuse().get();
    }

    protected File newTmpBundle(Set<VersionedName> catalogItems, VersionedName bundleName) {
        return newTmpBundle(catalogItems, bundleName, null);
    }
    
    protected File newTmpBundle(Set<VersionedName> catalogItems, VersionedName bundleName, String randomNoise) {
        String bundleBom = createCatalogYaml(ImmutableList.of(), ImmutableList.of(), catalogItems, randomNoise);
        return newTmpBundle(ImmutableMap.of(BasicBrooklynCatalog.CATALOG_BOM, bundleBom.getBytes(StandardCharsets.UTF_8)), bundleName);
    }

    // convenience for testing just a single test (TestNG plugin otherwise runs lots of them)
    public static void main(String[] args) throws Exception {
        try {
            BrooklynLauncherRebindCatalogOsgiTest fixture = new LauncherRebindSubTests();
            fixture.setUp();
            fixture.testRebindUpgradeEntity();
            fixture.tearDown();
        } catch (Throwable e) {
            e.printStackTrace();
            System.exit(1);
        } 
        System.exit(0);
    }
}
