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
package org.apache.brooklyn.core.typereg;

import com.google.common.collect.ImmutableListMultimap;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.assertj.core.api.WithAssertions;
import org.mockito.Mockito;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.UUID;

import static org.apache.brooklyn.core.typereg.BundleUpgradeParser.*;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class CatalogUpgradesGetBundleUpgradedIfNecessaryTest implements WithAssertions {

    private final ManagementContextInternal managementContext = mock(ManagementContextInternal.class);
    private final OsgiManager osgiManager = mock(OsgiManager.class);
    private final ManagedBundle managedBundle = mock(ManagedBundle.class);
    private final BasicBrooklynTypeRegistry typeRegistry = mock(BasicBrooklynTypeRegistry.class);

    private final String originalName = createAName("original", "1.0.0");
    private final String updatedName = createAName("updated", "2.0.0");
    private final VersionedName originalVersionedName = VersionedName.fromString(originalName);
    private final VersionedName updatedVersionedName = VersionedName.fromString(updatedName);
    private final VersionRangedName originalVersionRangedName =
            VersionRangedName.fromString(originalName, true);

    private CatalogUpgrades.Builder catalogUpgradesBuilder;

    @BeforeMethod
    public void setUp() {
        Mockito.reset(managementContext, osgiManager, managedBundle, typeRegistry);
        catalogUpgradesBuilder = CatalogUpgrades.builder();
    }

    @Test
    public void whenContextIsNullThenNull() {
        //given
        final ManagementContext managementContext = null;
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isNull();
    }

    @Test
    public void whenNameIsNullThenNull() {
        //given
        final String originalName = null;
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isNull();
    }

    @Test
    public void whenNonOSGIEnvironmentThenOriginalName() {
        //given
        givenANonOsgiEnvironment(managementContext);
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void whenAlreadyInstalledThenOriginalName() {
        //given
        givenAnOsgiEnvironment(managementContext, osgiManager);
        givenBundleIsAlreadyInstalled(osgiManager, originalVersionedName, managedBundle);
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void whenNotInstalledAndNoRenameFoundThenOriginalName() {
        //given
        givenAnOsgiEnvironment(managementContext, osgiManager);
        givenBundleIsNotAlreadyInstalled(osgiManager, originalVersionedName);
        givenNoRenameFound(managementContext, typeRegistry, catalogUpgradesBuilder);
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void whenNotInstalledAndNoCatalogUpgradesThenOriginalName() {
        //given
        givenAnOsgiEnvironment(managementContext, osgiManager);
        givenBundleIsNotAlreadyInstalled(osgiManager, originalVersionedName);
        givenNoCatalogUpgrades(managementContext, typeRegistry);
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isEqualTo(originalName);
    }

    @Test
    public void whenNotAlreadyInstalledAndRenameIsFoundThenUpdatedName() {
        //given
        givenAnOsgiEnvironment(managementContext, osgiManager);
        givenBundleIsNotAlreadyInstalled(osgiManager, originalVersionedName);
        givenRenameIsFound(managementContext, typeRegistry, catalogUpgradesBuilder,
                updatedVersionedName, originalVersionRangedName);
        //when
        final String result = CatalogUpgrades.getBundleUpgradedIfNecessary(managementContext, originalName);
        //then
        assertThat(result).isEqualTo(updatedName);
    }

    private static void givenANonOsgiEnvironment(
            final ManagementContextInternal managementContext
    ) {
        given(managementContext.getOsgiManager()).willReturn(Maybe.absent());
    }

    private static void givenNoRenameFound(
            final ManagementContextInternal managementContext,
            final BasicBrooklynTypeRegistry typeRegistry,
            final CatalogUpgrades.Builder catalogUpgradesBuilder
    ) {
        givenCatalogUpgrades(managementContext, typeRegistry, catalogUpgradesBuilder);
    }

    private static void givenRenameIsFound(
            final ManagementContextInternal managementContext,
            final BasicBrooklynTypeRegistry typeRegistry,
            final CatalogUpgrades.Builder catalogUpgradesBuilder,
            final VersionedName updatedVersionedName,
            final VersionRangedName originalVersionRangedName
    ) {
        catalogUpgradesBuilder.upgradeBundles(
                ImmutableListMultimap.of(updatedVersionedName, originalVersionRangedName));
        givenCatalogUpgrades(managementContext, typeRegistry, catalogUpgradesBuilder);
    }

    private static void givenNoCatalogUpgrades(
            final ManagementContextInternal managementContext,
            final BasicBrooklynTypeRegistry typeRegistry
    ) {
        givenManagementContextHasTypeRegistry(managementContext, typeRegistry);
        given(typeRegistry.getCatalogUpgradesInstructions()).willReturn(null);
    }

    private static void givenCatalogUpgrades(
            final ManagementContextInternal managementContext,
            final BasicBrooklynTypeRegistry typeRegistry,
            final CatalogUpgrades.Builder catalogUpgradesBuilder
    ) {
        givenManagementContextHasTypeRegistry(managementContext, typeRegistry);
        given(typeRegistry.getCatalogUpgradesInstructions()).willReturn(catalogUpgradesBuilder.build());
    }

    private static void givenManagementContextHasTypeRegistry(
            final ManagementContextInternal managementContext,
            final BrooklynTypeRegistry typeRegistry
    ) {
        given(managementContext.getTypeRegistry()).willReturn(typeRegistry);
    }

    private static void givenBundleIsAlreadyInstalled(
            final OsgiManager osgiManager,
            final VersionedName originalVersionedName,
            final ManagedBundle managedBundle
    ) {
        given(osgiManager.getManagedBundle(originalVersionedName)).willReturn(managedBundle);
    }

    private static void givenBundleIsNotAlreadyInstalled(
            final OsgiManager osgiManager,
            final VersionedName originalVersionedName
    ) {
        given(osgiManager.getManagedBundle(originalVersionedName)).willReturn(null);
    }

    private static void givenAnOsgiEnvironment(
            final ManagementContextInternal managementContext,
            final OsgiManager osgiManager
    ) {
        given(managementContext.getOsgiManager()).willReturn(Maybe.of(osgiManager));
    }

    private static String createAName(
            final String prefix,
            final String version
    ) {
        return prefix + ":" + version;
    }

}