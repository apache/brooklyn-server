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

import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser;
import org.apache.brooklyn.core.typereg.BundleUpgradeParser.CatalogUpgrades;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.util.guava.Maybe;
import org.osgi.framework.Bundle;

import java.util.Collection;

/**
 * Scans managed bundles and other jar bundles to find upgrades for installed bundles.
 */
class CatalogUpgradeScanner {

    private final ManagementContextInternal managementContext;

    CatalogUpgradeScanner(
            final ManagementContextInternal managementContext
    ) {
        this.managementContext = managementContext;
    }

    public CatalogUpgrades scan(final CatalogInitialization.RebindLogger rebindLogger) {
        Maybe<OsgiManager> osgiManager = managementContext.getOsgiManager();
        if (osgiManager.isAbsent()) {
            // Can't find any bundles to tell if there are upgrades. Could be running tests; do no filtering.
            return CatalogUpgrades.EMPTY;
        }
        final CatalogUpgrades.Builder catalogUpgradesBuilder = CatalogUpgrades.builder();
        scanManagedBundles(osgiManager.get(), catalogUpgradesBuilder, rebindLogger);
        scanAllBundles(osgiManager.get(), catalogUpgradesBuilder);
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
                CatalogUpgrades catalogUpgrades = BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(
                        bundle.get(), typeSupplier(managedBundle));
                catalogUpgradesBuilder.addAll(catalogUpgrades);
            } else {
                rebindLogger.info("Managed bundle "+managedBundle.getId()+" not found by OSGi Manager; "
                        + "ignoring when calculating persisted state catalog upgrades");
            }
        }
    }

    private void scanAllBundles(
            final OsgiManager osgiManager,
            final CatalogUpgrades.Builder catalogUpgradesBuilder
    ) {
        for (Bundle bundle : osgiManager.getFramework().getBundleContext().getBundles()) {
            final CatalogUpgrades catalogUpgrades =
                    BundleUpgradeParser.parseBundleManifestForCatalogUpgrades(bundle, typeSupplier(bundle));
            catalogUpgradesBuilder.addAll(catalogUpgrades);
        }
    }

    private RegisteredTypesSupplier typeSupplier(final ManagedBundle managedBundle) {
        return new RegisteredTypesSupplier(managementContext,
                RegisteredTypePredicates.containingBundle(managedBundle));
    }

    private RegisteredTypesSupplier typeSupplier(final Bundle bundle) {
        return new RegisteredTypesSupplier(managementContext,
                RegisteredTypePredicates.containingBundle(bundle.getSymbolicName()));
    }

}
