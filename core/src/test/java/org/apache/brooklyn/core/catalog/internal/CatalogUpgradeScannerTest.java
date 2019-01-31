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
package org.apache.brooklyn.core.catalog.internal;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.VersionRangedName;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.assertj.core.api.WithAssertions;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Collections;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.brooklyn.core.catalog.internal.CatalogUpgradeScannerTest.Givens.*;
import static org.apache.brooklyn.core.catalog.internal.CatalogUpgradeScannerTest.Utils.*;
import static org.apache.brooklyn.core.typereg.BundleTestUtil.newMockBundle;
import static org.assertj.core.api.SoftAssertions.assertSoftly;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;

public class CatalogUpgradeScannerTest implements WithAssertions {

    // collaborators
    private final ManagementContextInternal managementContext = new LocalManagementContext();
    @Mock
    private BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser;
    @Mock
    private Function<OsgiBundleWithUrl, Predicate<? super RegisteredType>> managedBundlePredicateSupplier;
    @Mock
    private Function<String, Predicate<? super RegisteredType>> unmanagedBundlePredicateSupplier;

    // subject under test
    private CatalogUpgradeScanner scanner;

    // parameters
    private final OsgiManager osgiManager = mock(OsgiManager.class);
    private final CatalogInitialization.RebindLogger rebindLogger = mock(CatalogInitialization.RebindLogger.class);
    private final BundleContext bundleContext = mock(BundleContext.class);

    @BeforeClass
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        scanner = new CatalogUpgradeScanner(managementContext, bundleUpgradeParser, managedBundlePredicateSupplier,
                        unmanagedBundlePredicateSupplier);
    }

    private CatalogUpgrades invoke() {
        return scanner.scan(osgiManager, bundleContext, rebindLogger);
    }

    @Test
    public void whenNoUnmanagedOrManagedBundlesThenNoUpgrades() {
        //given
        givenNoManagedBundles(osgiManager);
        givenNoUnmanagedBundles(bundleContext);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatThereAreNoUpgrades(result);
    }

    @Test
    public void whenUnmanagedAndManagedBundlesWithNoUpgradesThenNoUpgrades() {
        //given
        givenManagedBundlesWithNoUpgrades(osgiManager, bundleUpgradeParser);
        givenUnmanagedBundlesWithNoUpgrades(bundleContext, bundleUpgradeParser);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatThereAreNoUpgrades(result);
    }

    @Test
    public void whenOnlyManagedBundleHasUpgradesThenUpgradesForManagedBundle() {
        //given
        final String upgradeFrom = bundleName("managed", "1.0.0");
        final String upgradeTo = bundleName("managed", "2.0.0");
        final CatalogUpgrades catalogUpgrades = CatalogUpgrades.builder()
                .upgradeBundles(upgradeMapping(upgradeFrom, upgradeTo))
                .build();
        givenManagedBundlesWithUpgrades(catalogUpgrades, osgiManager, bundleUpgradeParser);
        givenUnmanagedBundlesWithNoUpgrades(bundleContext, bundleUpgradeParser);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatBundleHasUpgrades(result, upgradeFrom, upgradeTo);
    }

    @Test
    public void whenOnlyManagedBundleHasUpgradesThenNoUpgradesForUnmanagedBundles() {
        //given
        final String upgradeFrom = bundleName("managed", "1.0.0");
        final String upgradeTo = bundleName("managed", "2.0.0");
        final CatalogUpgrades catalogUpgrades = CatalogUpgrades.builder()
                .upgradeBundles(upgradeMapping(upgradeFrom, upgradeTo))
                .build();
        givenManagedBundlesWithUpgrades(catalogUpgrades, osgiManager, bundleUpgradeParser);
        final String unmanagedBundle = bundleName("unmanaged", "1.1.0");
        final String location = "mvn:groupId/unmanagedId/2.0.0";
        givenUnmanagedBundleWithNoUpgrades(unmanagedBundle, location, bundleContext, bundleUpgradeParser);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatBundleHasNoUpgrades(result, unmanagedBundle);
    }

    @Test
    public void whenOnlyUnmanagedBundleHasUpgradesThenUpgradesForUnmanagedBundle() {
        //given
        final String upgradeFrom = bundleName("unmanaged", "1.0.0");
        final String upgradeTo = bundleName("unmanaged", "2.0.0");
        final String location = "mvn:groupId/unmanagedId/2.0.0";
        final CatalogUpgrades catalogUpgrades = CatalogUpgrades.builder()
                .upgradeBundles(upgradeMapping(upgradeFrom, upgradeTo))
                .build();
        givenManagedBundlesWithNoUpgrades(osgiManager, bundleUpgradeParser);
        givenUnmanagedBundleWithUpgrades(upgradeTo, location, catalogUpgrades, bundleContext, bundleUpgradeParser);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatBundleHasUpgrades(result, upgradeFrom, upgradeTo);
    }

    @Test
    public void whenOnlyUnmanagedBundleHasUpgradesThenNoUpgradesForManagedBundles() {
        //given
        final String upgradeFrom = bundleName("unmanaged", "1.0.0");
        final String upgradeTo = bundleName("unmanaged", "2.0.0");
        final String location = "mvn:groupId/unmanaged/2.0.0";
        final CatalogUpgrades catalogUpgrades = CatalogUpgrades.builder()
                .upgradeBundles(upgradeMapping(upgradeFrom, upgradeTo))
                .build();
        final String managedBundle = bundleName("managed", "1.1.0");
        givenManagedBundleWithNoUpgrades(managedBundle, osgiManager, bundleUpgradeParser);
        givenUnmanagedBundleWithUpgrades(upgradeTo, location, catalogUpgrades, bundleContext, bundleUpgradeParser);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatBundleHasNoUpgrades(result, managedBundle);
    }

    @Test
    public void whenUnmanagedBundleWithNoSymbolicNameHasUpgradeThenUpgradesForUnmanagedBundle() {
        //given
        final String upgradeFrom = bundleName("unmanaged", "1.0.0");
        final String upgradeTo = bundleName("unmanaged", "2.0.0");
        final String location = "mvn:groupId/unmanaged/2.0.0";
        final CatalogUpgrades catalogUpgrades = CatalogUpgrades.builder()
                .upgradeBundles(upgradeMapping(upgradeFrom, upgradeTo))
                .build();
        final String managedBundle = bundleName("managed", "1.1.0");
        givenManagedBundleWithNoUpgrades(managedBundle, osgiManager, bundleUpgradeParser);
        givenUnmanagedBundleWithUpgrades(upgradeTo, location, catalogUpgrades, bundleContext, bundleUpgradeParser);
        givenAllUnmanagedBundlesHaveNoSymbolicName(upgradeTo, bundleContext);
        givenSymbolicNamesAreRequired(unmanagedBundlePredicateSupplier);
        //when
        final CatalogUpgrades result = invoke();
        //then
        assertThatBundleHasNoUpgrades(result, managedBundle);
    }

    static class Givens {

        static void givenNoManagedBundles(final OsgiManager osgiManager) {
            final Map<String, ManagedBundle> noBundles = Collections.emptyMap();
            given(osgiManager.getManagedBundles()).willReturn(noBundles);
        }

        static void givenNoUnmanagedBundles(BundleContext bundleContext) {
            final Bundle[] noBundles = new Bundle[0];
            given(bundleContext.getBundles()).willReturn(noBundles);
        }

        static void givenManagedBundlesWithNoUpgrades(
                final OsgiManager osgiManager,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Map<String, ManagedBundle> bundles =
                    ImmutableMap.of(
                            "managed1", findableManagedBundle(osgiManager, CatalogUpgrades.EMPTY, bundleUpgradeParser),
                            "managed2", findableManagedBundle(osgiManager, CatalogUpgrades.EMPTY, bundleUpgradeParser)
                    );
            doReturn(bundles).when(osgiManager).getManagedBundles();
        }

        static void givenManagedBundlesWithUpgrades(
                final CatalogUpgrades upgrades,
                final OsgiManager osgiManager,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Map<String, ManagedBundle> bundles =
                    ImmutableMap.of("managed", findableManagedBundle(osgiManager, upgrades, bundleUpgradeParser));
            doReturn(bundles).when(osgiManager).getManagedBundles();
        }

        static void givenManagedBundleWithNoUpgrades(
                final String bundleName,
                final OsgiManager osgiManager,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final ManagedBundle managedBundle =
                    findableManagedBundle(osgiManager, CatalogUpgrades.EMPTY, bundleUpgradeParser);
            final Map<String, ManagedBundle> bundles =
                    ImmutableMap.of(bundleName, managedBundle);
            doReturn(bundles).when(osgiManager).getManagedBundles();
        }

        static void givenUnmanagedBundlesWithNoUpgrades(
                final BundleContext bundleContext,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Bundle[] bundles = new Bundle[]{
                    unmanagedBundle(bundleName("unmanaged1", "1.1.0"), "mvn:groupId/artifactId/1.0.0", CatalogUpgrades.EMPTY, bundleUpgradeParser),
                    unmanagedBundle(bundleName("unmanaged2", "1.1.0"), "mvn:groupId/artifactId/1.0.0", CatalogUpgrades.EMPTY, bundleUpgradeParser)
            };
            doReturn(bundles).when(bundleContext).getBundles();
        }

        static void givenUnmanagedBundleWithNoUpgrades(
                final String bundleName,
                final String location,
                final BundleContext bundleContext,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Bundle[] bundles = new Bundle[]{
                    unmanagedBundle(bundleName, location, CatalogUpgrades.EMPTY, bundleUpgradeParser)
            };
            doReturn(bundles).when(bundleContext).getBundles();
        }

        static void givenUnmanagedBundleWithUpgrades(
                final String bundleName,
                final String location,
                final CatalogUpgrades upgrades,
                final BundleContext bundleContext,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Bundle[] bundles = new Bundle[]{
                    unmanagedBundle(bundleName, location, upgrades, bundleUpgradeParser)
            };
            doReturn(bundles).when(bundleContext).getBundles();
        }

        static void givenAllUnmanagedBundlesHaveNoSymbolicName(
                final String upgradeTo,
                final BundleContext bundleContext
        ) {
            for (Bundle unmanagedBundle : bundleContext.getBundles()) {
                given(unmanagedBundle.getSymbolicName()).willReturn("");
            }
        }

        static void givenSymbolicNamesAreRequired(
                final Function<String, Predicate<? super RegisteredType>> unmanagedBundlePredicateSupplier
        ) {
            given(unmanagedBundlePredicateSupplier.apply("")).willThrow(IllegalStateException.class);
        }

    }

    void assertThatThereAreNoUpgrades(final CatalogUpgrades result) {
        assertSoftly(s -> {
            s.assertThat(result.getUpgradesProvidedByBundles().size()).isZero();
            s.assertThat(result.getUpgradesProvidedByTypes().size()).isZero();
        });
    }

    void assertThatBundleHasUpgrades(
            final CatalogUpgrades result,
            final String upgradeFrom,
            final String upgradeTo
    ) {
        assertThat(result.getUpgradesForBundle(VersionedName.fromString(upgradeFrom)))
                .contains(VersionedName.fromString(upgradeTo));
    }

    void assertThatBundleHasNoUpgrades(
            final CatalogUpgrades result,
            final String unmanagedBundle
    ) {
        assertThat(result.getUpgradesForBundle(VersionedName.fromString(unmanagedBundle))).isEmpty();
    }

    static class Utils {

        static Bundle unmanagedBundle(
                final String bundleName,
                final String location,
                final CatalogUpgrades catalogUpgrades,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final Bundle bundle = newMockBundle(VersionedName.fromString(bundleName), ImmutableMap.of());
            given(bundle.getLocation()).willReturn(location);
            given(bundleUpgradeParser.apply(eq(bundle), any())).willReturn(catalogUpgrades);
            return bundle;
        }

        static ManagedBundle findableManagedBundle(
                final OsgiManager osgiManager,
                final CatalogUpgrades catalogUpgrades,
                final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser
        ) {
            final ManagedBundle managedBundle = new BasicManagedBundle();
            final Bundle bundle = mock(Bundle.class);
            given(osgiManager.findBundle(managedBundle)).willReturn(Maybe.of(bundle));
            given(bundleUpgradeParser.apply(eq(bundle), any())).willReturn(catalogUpgrades);
            return managedBundle;
        }

        static String bundleName(
                final String name,
                final String version
        ) {
            return name + ":" + version;
        }

        static Multimap<VersionedName, VersionRangedName> upgradeMapping(
                final String upgradeFrom,
                final String upgradeTo
        ) {
            return ImmutableMultimap.of(
                    VersionedName.fromString(upgradeTo),
                    VersionRangedName.fromString(upgradeFrom, true));
        }

    }

}