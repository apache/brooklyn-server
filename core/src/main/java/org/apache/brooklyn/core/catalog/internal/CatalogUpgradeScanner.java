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
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.OsgiBundleWithUrl;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.util.guava.Maybe;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;

import java.util.Collection;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

/**
 * Scans managed bundles and other jar bundles to find upgrades for installed bundles.
 */
class CatalogUpgradeScanner {

    private static final Pattern FALLBACK_SYMBOLICNAME_PATTERN =
            Pattern.compile(":(?<groupId>.+)/(?<artifactId>.+)/(?<version>.+)$");

    private final ManagementContextInternal managementContext;
    private final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser;
    private final Function<OsgiBundleWithUrl, Predicate<? super RegisteredType>> managedBundlePredicateSupplier;
    private final Function<String, Predicate<? super RegisteredType>> unmanagedBundlePredicateSupplier;

    CatalogUpgradeScanner(
            final ManagementContextInternal managementContext,
            final BiFunction<Bundle, RegisteredTypesSupplier, CatalogUpgrades> bundleUpgradeParser,
            final Function<OsgiBundleWithUrl, Predicate<? super RegisteredType>> managedBundlePredicateSupplier,
            final Function<String, Predicate<? super RegisteredType>> unmanagedBundlePredicateSupplier
    ) {
        this.managementContext = requireNonNull(managementContext, "managementContext");
        this.bundleUpgradeParser = requireNonNull(bundleUpgradeParser, "bundleUpgradeParser");
        this.managedBundlePredicateSupplier =
                requireNonNull(managedBundlePredicateSupplier, "managedBundlePredicateSupplier");
        this.unmanagedBundlePredicateSupplier =
                requireNonNull(unmanagedBundlePredicateSupplier, "unmanagedBundlePredicateSupplier");
    }

    public CatalogUpgrades scan(
            final OsgiManager osgiManager,
            final BundleContext bundleContext,
            final CatalogInitialization.RebindLogger rebindLogger
    ) {
        final CatalogUpgrades.Builder catalogUpgradesBuilder = CatalogUpgrades.builder();
        scanManagedBundles(osgiManager, catalogUpgradesBuilder, rebindLogger);
        scanAllBundles(catalogUpgradesBuilder, bundleContext);
        return catalogUpgradesBuilder.build();
    }

    private void scanManagedBundles(
            final OsgiManager osgiManager,
            final CatalogUpgrades.Builder catalogUpgradesBuilder,
            final CatalogInitialization.RebindLogger rebindLogger
    ) {
        Collection<ManagedBundle> managedBundles = osgiManager.getManagedBundles().values();
        for (ManagedBundle managedBundle : managedBundles) {
            Maybe<Bundle> bundle = osgiManager.findBundle(managedBundle);
            if (bundle.isPresent()) {
                CatalogUpgrades catalogUpgrades = bundleUpgradeParser.apply(bundle.get(), typeSupplier(managedBundle));
                catalogUpgradesBuilder.addAll(catalogUpgrades);
            } else {
                rebindLogger.info("Managed bundle "+managedBundle.getId()+" not found by OSGi Manager; "
                        + "ignoring when calculating persisted state catalog upgrades");
            }
        }
    }

    private void scanAllBundles(
            final CatalogUpgrades.Builder catalogUpgradesBuilder,
            final BundleContext bundleContext
    ) {
        for (Bundle bundle : bundleContext.getBundles()) {
            final CatalogUpgrades catalogUpgrades = bundleUpgradeParser.apply(bundle, typeSupplier(bundle));
            catalogUpgradesBuilder.addAll(catalogUpgrades);
        }
    }

    private RegisteredTypesSupplier typeSupplier(final ManagedBundle managedBundle) {
        return new RegisteredTypesSupplier(managementContext, managedBundlePredicateSupplier.apply(managedBundle));
    }

    private RegisteredTypesSupplier typeSupplier(final Bundle bundle) {
        return new RegisteredTypesSupplier(managementContext,
                unmanagedBundlePredicateSupplier.apply(symbolicName(bundle)));
    }

    private String symbolicName(final Bundle bundle) {
        final String symbolicName = bundle.getSymbolicName();
        if (symbolicName.length() == 0) {
            return fallbackSymbolicName(bundle);
        }
        return symbolicName;
    }

    private String fallbackSymbolicName(final Bundle bundle) {
        final Matcher matcher = FALLBACK_SYMBOLICNAME_PATTERN.matcher(bundle.getLocation());
        if (matcher.matches()) {
            final String group = matcher.group("groupId");
            final String artifact = matcher.group("artifactId");
            return String.format("%s-%s", group, artifact);
        } else {
            throw new IllegalStateException("Bundle: no SymbolicName and Location not matched");
        }
    }

}
