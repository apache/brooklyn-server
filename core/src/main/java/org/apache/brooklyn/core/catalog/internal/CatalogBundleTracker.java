/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.core.catalog.internal;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.BundleTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;

@Beta
public class CatalogBundleTracker extends BundleTracker<Iterable<? extends CatalogItem<?, ?>>> {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBundleTracker.class);

    private ServiceReference<ManagementContext> mgmtContextReference;
    private ManagementContext managementContext;

    private CatalogBomScanner catalogBomScanner;
    private CatalogBundleLoader catalogBundleLoader;

    public CatalogBundleTracker(CatalogBomScanner catalogBomScanner, ServiceReference<ManagementContext> serviceReference) {
        super(serviceReference.getBundle().getBundleContext(), Bundle.ACTIVE, null);
        this.mgmtContextReference = serviceReference;
        this.catalogBomScanner = catalogBomScanner;
        open();
    }

    @Override
    public void open() {
        managementContext = mgmtContextReference.getBundle().getBundleContext().getService(mgmtContextReference);
        catalogBundleLoader = new CatalogBundleLoader(catalogBomScanner, managementContext);
        super.open();
    }

    @Override
    public void close() {
        super.close();
        managementContext = null;
        mgmtContextReference.getBundle().getBundleContext().ungetService(mgmtContextReference);
        catalogBundleLoader = null;
    }

    public ManagementContext getManagementContext() {
        return managementContext;
    }

    /**
     * Scans the bundle being added for a catalog.bom file and adds any entries in it to the catalog.
     *
     * @param bundle      The bundle being added to the bundle context.
     * @param bundleEvent The event of the addition.
     * @return The items added to the catalog; these will be tracked by the {@link BundleTracker} mechanism
     * and supplied to the {@link #removedBundle(Bundle, BundleEvent, Iterable)} method.
     * @throws RuntimeException if the catalog items failed to be added to the catalog
     */
    @Override
    public Iterable<? extends CatalogItem<?, ?>> addingBundle(Bundle bundle, BundleEvent bundleEvent) {
        return catalogBundleLoader.scanForCatalog(bundle);
    }

    /**
     * Remove the given entries from the catalog, related to the given bundle.
     *
     * @param bundle      The bundle being removed to the bundle context.
     * @param bundleEvent The event of the removal.
     * @param items       The items being removed
     * @throws RuntimeException if the catalog items failed to be added to the catalog
     */
    @Override
    public void removedBundle(Bundle bundle, BundleEvent bundleEvent, Iterable<? extends CatalogItem<?, ?>> items) {
        if (!items.iterator().hasNext()) {
            return;
        }
        LOG.debug("Unloading catalog BOM entries from {} {} {}", catalogBomScanner.bundleIds(bundle));
        for (CatalogItem<?, ?> item : items) {
            LOG.debug("Unloading {} {} from catalog", item.getSymbolicName(), item.getVersion());

            catalogBundleLoader.removeFromCatalog(item);
        }
    }
}
