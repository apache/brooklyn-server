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

import java.util.List;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.CatalogInitialization;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.commons.collections.IteratorUtils;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class BrooklynLauncherRebindCatalogTest extends AbstractBrooklynLauncherRebindTest {

    private static final String TEST_VERSION = "test-version";
    private static final String CATALOG_INITIAL = "classpath://rebind-test-catalog.bom";
    private static final String CATALOG_EMPTY_INITIAL = "classpath://rebind-test-empty-catalog.bom";
    private static final String CATALOG_ADDITIONS = "rebind-test-catalog-additions.bom";
    private static final Set<VersionedName> EXPECTED_DEFAULT_IDS = ImmutableSet.of(new VersionedName("one", TEST_VERSION), new VersionedName("two", TEST_VERSION));
    private static final Set<VersionedName> EXPECTED_ADDED_IDS = ImmutableSet.of(new VersionedName("three", TEST_VERSION), new VersionedName("four", TEST_VERSION));

    private BrooklynLauncher newLauncherForTests(String catalogInitial) {
        CatalogInitialization catalogInitialization = new CatalogInitialization(catalogInitial);
        return super.newLauncherForTests()
                .catalogInitialization(catalogInitialization);
    }

    @Test
    public void testRebindGetsInitialCatalog() {
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(CATALOG_INITIAL);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS);
    }

    @Test
    public void testRebindPersistsInitialCatalog() {
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(CATALOG_EMPTY_INITIAL);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS);
    }

    @Test
    public void testRebindGetsUnionOfInitialAndPersisted() {
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_INITIAL);
        launcher.start();
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS);

        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        catalog.addItems(new ResourceUtils(this).getResourceAsString(CATALOG_ADDITIONS));
        assertCatalogConsistsOfIds(launcher, Iterables.concat(EXPECTED_DEFAULT_IDS, EXPECTED_ADDED_IDS));

        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(CATALOG_INITIAL);
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
        Set<VersionedName> EXPECTED_DEFAULT_IDS_WITHOUT_ONE = MutableSet.<VersionedName>builder()
                .addAll(EXPECTED_DEFAULT_IDS)
                .remove(new VersionedName("one", TEST_VERSION))
                .build();
        
        BrooklynLauncher launcher = newLauncherForTests(CATALOG_INITIAL);
        launcher.start();

        BrooklynCatalog catalog = launcher.getServerDetails().getManagementContext().getCatalog();
        catalog.deleteCatalogItem("one", TEST_VERSION);
        assertCatalogConsistsOfIds(launcher, EXPECTED_DEFAULT_IDS_WITHOUT_ONE);
        
        launcher.terminate();

        BrooklynLauncher newLauncher = newLauncherForTests(CATALOG_INITIAL);
        newLauncher.start();
        assertCatalogConsistsOfIds(newLauncher, EXPECTED_DEFAULT_IDS_WITHOUT_ONE);
    }
}
