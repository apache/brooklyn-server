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

import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_BUNDLES;
import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS;
import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.MANIFEST_HEADER_UPGRADE_FOR_BUNDLES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.net.URI;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult.ResultCode;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
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
        
        BundleFile bundleRemovingItems = bundleBuilder()
                .name("org.example.testRemoveLegacyItems", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of())
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_LEGACY_ITEMS, "\"one:[0,1.0.0)\",\"two:[0,1.0.0)\",\"three:0.1.0\"")
                        .build())
                .build();
        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleRemovingItems.getFile().toURI()), ImmutableList.of()));
        
        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(two_1_0_0, three_0_2_0, four_0_1_0));
        assertManagedBundle(launcher, bundleRemovingItems.getVersionedName(), ImmutableSet.<VersionedName>of());

        launcher.terminate();
    }
    
    @Test
    public void testForceUpgradeBundle() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.testForceUpgradeBundle", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .initState();
        
        BundleFile bundleV2 = bundleBuilder()
                .name(bundleV1.getVersionedName().getSymbolicName(), "2.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0))
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"*\"")
                        .put(MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"*\"")
                        .build())
                .build();

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleV2.getFile().toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_2_0_0));
        assertManagedBundle(launcher, bundleV2.getVersionedName(), ImmutableSet.<VersionedName>of(one_2_0_0));
        assertNotManagedBundle(launcher, bundleV1.getVersionedName());
        launcher.terminate();
    }
    
    // Simple test (no upgrade), important for validating that other tests really do as expected!
    @Test
    public void testLoadsBundleFromPersistedState() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.testForceUpgradeBundle", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .initState();
        
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_1_0_0));
        assertManagedBundle(launcher, bundleV1.getVersionedName(), ImmutableSet.<VersionedName>of(one_1_0_0));
        launcher.terminate();
    }
    
    // remove+upgrade v1, then try deploying v1, get v2
    // see also BrooklynLauncherRebindCatalogOsgiTest for more variants
    // (this case is a bit simpler however as it prepares persisted state without a full launch) 
    @Test
    public void testDeployRemovedUpgradedItemWorks() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.testForceUpgradeBundle", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .initState();
        
        BundleFile bundleV2 = bundleBuilder()
                .name(bundleV1.getVersionedName().getSymbolicName(), "2.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0))
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"*\"")
                        .put(MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"*\"")
                        .build())
                .build();

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleV2.getFile().toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();

        Application app = createAndStartApplication(launcher.getManagementContext(), 
            "services: [ { type: 'one:1.0.0' } ]");
        Entity one = Iterables.getOnlyElement( app.getChildren() );
        Assert.assertEquals(one.getCatalogItemId(), "one:2.0.0");

        app = createAndStartApplication(launcher.getManagementContext(), 
            Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.entity.group.DynamicCluster",
                "  cluster.initial.size: 1",
                "  dynamiccluster.memberspec:",
                "    $brooklyn:entitySpec:",
                "      type: one:1") );
        Entity cluster = Iterables.getOnlyElement( app.getChildren() );
        one = Iterables.getOnlyElement( ((Group)cluster).getMembers() );
        Assert.assertEquals(one.getCatalogItemId(), "one:2.0.0");
        
        launcher.terminate();
    }

    @Test
    public void testForciblyRemovedBundleNotAdded() throws Exception {
        runForciblyRemovedBundleNotAdded(true);
    }
    
    @Test
    public void testForciblyRemovedBundleNotAddedWithNoUpgradeTarget() throws Exception {
        runForciblyRemovedBundleNotAdded(false);
    }
    
    protected void runForciblyRemovedBundleNotAdded(boolean hasUpgradeForBundle) throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.bundleBeingUpgraded", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        // A bundle that references the forcibly-replaced bundle via url;
        // (add this to persisted state).
        BundleFile bundleWithReferenceUrl = bundleBuilder()
                .name("org.example.bundleWithReferenceUrl", "1.0.0")
                .catalogBom(Joiner.on("\n").join(
                        "brooklyn.catalog:",
                        "  version: 1.0.0",
                        "  brooklyn.libraries:",
                        "  - "+bundleV1.getFile().toURI()))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .bundle(bundleWithReferenceUrl)
                .initState();
        
        BundleFile bundleV2 = bundleBuilder()
                .name(bundleV1.getVersionedName().getSymbolicName(), "2.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0))
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"*\"")
                        .put(hasUpgradeForBundle ? MANIFEST_HEADER_UPGRADE_FOR_BUNDLES : "disabled-upgrade", "\"*\"")
                        .build())
                .build();

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleV2.getFile().toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertHealthyMaster(launcher);
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_2_0_0));
        assertManagedBundle(launcher, bundleV2.getVersionedName(), ImmutableSet.<VersionedName>of(one_2_0_0));
        assertNotManagedBundle(launcher, bundleV1.getVersionedName());

        // Try installing the removed bundle explicitly (force=false)
        ReferenceWithError<OsgiBundleInstallationResult> resultWithoutForce = installBundle(launcher, bundleV1.getFile(), false);
        resultWithoutForce.checkNoError();
        ResultCode resultWithoutForceCode = resultWithoutForce.get().getCode();
        String resultWithoutForceMessage = resultWithoutForce.get().getMessage();
        assertEquals(resultWithoutForceCode, ResultCode.IGNORING_BUNDLE_FORCIBLY_REMOVED);
        if (hasUpgradeForBundle) {
            assertEquals(resultWithoutForce.get().getMetadata().getVersionedName(), bundleV2.getVersionedName());
            assertTrue(resultWithoutForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, upgraded to 2.0.0"), "msg="+resultWithoutForceMessage);
        } else {
            assertNull(resultWithoutForce.get().getMetadata());
            assertTrue(resultWithoutForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, no upgrade defined"), "msg="+resultWithoutForceMessage);
        }

        // Try installing the removed bundle explicitly (force=true)
        ReferenceWithError<OsgiBundleInstallationResult> resultWithForce = installBundle(launcher, bundleV1.getFile(), true);
        resultWithForce.checkNoError();
        ResultCode resultWithForceCode = resultWithForce.get().getCode();
        String resultWithForceMessage = resultWithForce.get().getMessage();
        assertEquals(resultWithForceCode, ResultCode.IGNORING_BUNDLE_FORCIBLY_REMOVED);
        if (hasUpgradeForBundle) {
            assertEquals(resultWithForce.get().getMetadata().getVersionedName(), bundleV2.getVersionedName());
            assertTrue(resultWithForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, upgraded to 2.0.0"), "msg="+resultWithForceMessage);
        } else {
            assertNull(resultWithoutForce.get().getMetadata());
            assertTrue(resultWithForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, no upgrade defined"), "msg="+resultWithForceMessage);
        }

        launcher.terminate();
    }
    
    @Test
    public void testForciblyRemovedBundleNotAddedWhenReferencedByName() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.bundleBeingUpgraded", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        // A bundle that references the forcibly-replaced bundle via name:version;
        // (add this to persisted state).
        BundleFile bundleWithReferenceByNameVersionName = bundleBuilder()
                .name("org.example.bundleWithReferenceByName", "1.0.0")
                .catalogBom(Joiner.on("\n").join(
                        "brooklyn.catalog:",
                        "  version: 1.0.0",
                        "  brooklyn.libraries:",
                        "  - name: "+bundleV1.getVersionedName().getSymbolicName(),
                        "    version: 1.0.0"))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .bundle(bundleWithReferenceByNameVersionName)
                .initState();
        
        BundleFile bundleV2 = bundleBuilder()
                .name(bundleV1.getVersionedName().getSymbolicName(), "2.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0))
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"*\"")
                        .put(MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"*\"")
                        .build())
                .build();

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleV2.getFile().toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertHealthyMaster(launcher);
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_2_0_0));
        assertManagedBundle(launcher, bundleV2.getVersionedName(), ImmutableSet.<VersionedName>of(one_2_0_0));
        assertNotManagedBundle(launcher, bundleV1.getVersionedName());

        // Try installing the removed bundle explicitly (force=false)
        ReferenceWithError<OsgiBundleInstallationResult> resultWithoutForce = installBundle(launcher, bundleV1.getFile(), false);
        resultWithoutForce.checkNoError();
        ResultCode resultWithoutForceCode = resultWithoutForce.get().getCode();
        String resultWithoutForceMessage = resultWithoutForce.get().getMessage();
        assertEquals(resultWithoutForceCode, ResultCode.IGNORING_BUNDLE_FORCIBLY_REMOVED);
        assertEquals(resultWithoutForce.get().getMetadata().getVersionedName(), bundleV2.getVersionedName());
        assertTrue(resultWithoutForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, upgraded to 2.0.0"), "msg="+resultWithoutForceMessage);
        
        launcher.terminate();
    }
    
    @Test
    public void testForciblyRemovedBundleNotAddedWhenReferencedByMvnUrl() throws Exception {
        VersionedName one_1_0_0 = VersionedName.fromString("one:1.0.0");
        VersionedName one_2_0_0 = VersionedName.fromString("one:2.0.0");
        
        BundleFile bundleV1 = bundleBuilder()
                .name("org.example.bundleBeingUpgraded", "1.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_1_0_0))
                .build();

        // A bundle that references the forcibly-replaced bundle via name:version;
        // (add this to persisted state).
        BundleFile bundleWithReferenceByNameVersionName = bundleBuilder()
                .name("org.example.bundleWithReferenceByName", "1.0.0")
                .catalogBom(Joiner.on("\n").join(
                        "brooklyn.catalog:",
                        "  version: 1.0.0",
                        "  brooklyn.libraries:",
                        "  - mvn:org.example/bundleWithReferenceByName/1.0.0"))
                .build();

        newPersistedStateInitializer()
                .bundle(bundleV1)
                .bundle(bundleWithReferenceByNameVersionName)
                .initState();
        
        BundleFile bundleV2 = bundleBuilder()
                .name(bundleV1.getVersionedName().getSymbolicName(), "2.0.0")
                .catalogBom(ImmutableList.<URI>of(), ImmutableSet.<VersionedName>of(one_2_0_0))
                .manifestLines(ImmutableMap.<String, String>builder()
                        .put(MANIFEST_HEADER_FORCE_REMOVE_BUNDLES, "\"*\"")
                        .put(MANIFEST_HEADER_UPGRADE_FOR_BUNDLES, "\"*\"")
                        .build())
                .build();

        File initialBomFile = newTmpFile(createCatalogYaml(ImmutableList.of(bundleV2.getFile().toURI()), ImmutableList.of()));

        BrooklynLauncher launcher = newLauncherForTests(initialBomFile.getAbsolutePath());
        launcher.start();
        assertHealthyMaster(launcher);
        assertCatalogConsistsOfIds(launcher, ImmutableList.of(one_2_0_0));
        assertManagedBundle(launcher, bundleV2.getVersionedName(), ImmutableSet.<VersionedName>of(one_2_0_0));
        assertNotManagedBundle(launcher, bundleV1.getVersionedName());

        // Try installing the removed bundle explicitly (force=false)
        ReferenceWithError<OsgiBundleInstallationResult> resultWithoutForce = installBundle(launcher, bundleV1.getFile(), false);
        resultWithoutForce.checkNoError();
        ResultCode resultWithoutForceCode = resultWithoutForce.get().getCode();
        String resultWithoutForceMessage = resultWithoutForce.get().getMessage();
        assertEquals(resultWithoutForceCode, ResultCode.IGNORING_BUNDLE_FORCIBLY_REMOVED);
        assertEquals(resultWithoutForce.get().getMetadata().getVersionedName(), bundleV2.getVersionedName());
        assertTrue(resultWithoutForceMessage.contains("Bundle "+bundleV1.getVersionedName()+" forcibly removed, upgraded to 2.0.0"), "msg="+resultWithoutForceMessage);
        
        launcher.terminate();
    }
}
