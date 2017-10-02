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
import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.core.mgmt.persist.PersistMode;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.os.Os;
import org.apache.commons.collections.IteratorUtils;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

public class BrooklynLauncherRebindCatalogTest {

    private static final String TEST_VERSION = "test-version";
    private static final String CATALOG_INITIAL = "classpath://rebind-test-catalog.bom";
    private static final String CATALOG_EMPTY_INITIAL = "classpath://rebind-test-empty-catalog.bom";
    private static final String CATALOG_ADDITIONS = "rebind-test-catalog-additions.bom";
    private static final Set<String> EXPECTED_DEFAULT_IDS = ImmutableSet.of("one:" + TEST_VERSION, "two:" + TEST_VERSION);
    private static final Set<String> EXPECTED_ADDED_IDS = ImmutableSet.of("three:" + TEST_VERSION, "four:" + TEST_VERSION);

    private List<BrooklynLauncher> launchers = Lists.newCopyOnWriteArrayList();
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        for (BrooklynLauncher launcher : launchers) {
            launcher.terminate();
        }
        launchers.clear();
    }

    private BrooklynLauncher newLauncherForTests(String persistenceDir) {
        return newLauncherForTests(persistenceDir, CATALOG_INITIAL);
    }
    
    private BrooklynLauncher newLauncherForTests(String persistenceDir, String catalogInitial) {
        CatalogInitialization catalogInitialization = new CatalogInitialization(catalogInitial);
        BrooklynLauncher launcher = BrooklynLauncher.newInstance()
                .brooklynProperties(LocalManagementContextForTests.builder(true).buildProperties())
                .catalogInitialization(catalogInitialization)
                .persistMode(PersistMode.AUTO)
                .persistenceDir(persistenceDir)
                .webconsole(false);
        launchers.add(launcher);
        return launcher;
    }

    @Test
    public void testRebindGetsInitialCatalog() {
        String persistenceDir = newTempPersistenceContainerName();

        BrooklynLauncher launcher = newLauncherForTests(persistenceDir);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(persistenceDir);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS);
    }

    @Test
    public void testRebindPersistsInitialCatalog() {
        String persistenceDir = newTempPersistenceContainerName();

        BrooklynLauncher launcher = newLauncherForTests(persistenceDir, CATALOG_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(persistenceDir, CATALOG_EMPTY_INITIAL);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS);
    }

    @Test
    public void testRebindGetsUnionOfInitialAndPersisted() {
        String persistenceDir = newTempPersistenceContainerName();

        BrooklynLauncher launcher = newLauncherForTests(persistenceDir);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        catalog.addItems(new ResourceUtils(this).getResourceAsString(CATALOG_ADDITIONS));
        assertCatalogConsistsOfIds(launcher, Iterables.concat(EXPECTED_DEFAULT_IDS, EXPECTED_ADDED_IDS));

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(persistenceDir);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, Iterables.concat(EXPECTED_DEFAULT_IDS, EXPECTED_ADDED_IDS));
    }

    // In CatalogInitialization, we take the union of the initial catalog and the persisted state catalog.
    // Therefore removals from the original catalog do not take effect.
    // That is acceptable - better than previously where, after upgrading Brooklyn, one had to run
    // `br catalog add `${BROOKLYN_HOME}/catalog/catalog.bom` to add the new catalog items to the existing
    // persisted state.
    @Test(groups="Broken")
    public void testRemovedInitialItemStillRemovedAfterRebind() {
        Set<String> EXPECTED_DEFAULT_IDS_WITHOUT_ONE = MutableSet.<String>builder()
                .addAll(EXPECTED_DEFAULT_IDS)
                .remove("one:" + TEST_VERSION).build();
        
        String persistenceDir = newTempPersistenceContainerName();

        BrooklynLauncher launcher = newLauncherForTests(persistenceDir);
        launcher.start();

        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        catalog.deleteCatalogItem("one", TEST_VERSION);
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS_WITHOUT_ONE);
        
        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(persistenceDir);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS_WITHOUT_ONE);
    }

    private void assertCatalogConsistsOfIds(BrooklynLauncher launcher, Iterable<String> ids) {
        BrooklynTypeRegistry typeRegistry = launcher.getServerDetails().getManagementContext().getTypeRegistry();
        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        assertTypeRegistryConsistsOfIds(typeRegistry.getAll(), ids);
        assertCatalogConsistsOfIds(catalog.getCatalogItems(), ids);
    }

    private void assertCatalogConsistsOfIds(Iterable<CatalogItem<Object, Object>> catalogItems, Iterable<String> ids) {
        Iterable<String> idsFromItems = Iterables.transform(catalogItems, new Function<CatalogItem<?,?>, String>() {
            @Nullable
            @Override
            public String apply(CatalogItem<?, ?> input) {
                return input.getCatalogItemId();
            }
        });
        Assert.assertTrue(compareIterablesWithoutOrderMatters(ids, idsFromItems), String.format("Expected %s, found %s", ids, idsFromItems));
    }

    private void assertTypeRegistryConsistsOfIds(Iterable<RegisteredType> types, Iterable<String> ids) {
        Iterable<String> idsFromItems = Iterables.transform(types, new Function<RegisteredType, String>() {
            @Nullable
            @Override
            public String apply(RegisteredType input) {
                return input.getId();
            }
        });
        Assert.assertTrue(compareIterablesWithoutOrderMatters(ids, idsFromItems), String.format("Expected %s, found %s", ids, idsFromItems));
    }

    protected String newTempPersistenceContainerName() {
        File persistenceDirF = Files.createTempDir();
        Os.deleteOnExitRecursively(persistenceDirF);
        return persistenceDirF.getAbsolutePath();
    }

    private static <T> boolean compareIterablesWithoutOrderMatters(Iterable<T> a, Iterable<T> b) {
        List<T> aList = IteratorUtils.toList(a.iterator());
        List<T> bList = IteratorUtils.toList(b.iterator());

        return aList.containsAll(bList) && bList.containsAll(aList);
    }
}
