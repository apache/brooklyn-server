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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.catalog.BrooklynCatalog;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.yaml.Yamls;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleContext;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.FrameworkUtil;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.BundleTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.net.URL;
import java.util.List;
import java.util.Map;

public class CatalogBomScanner {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBomScanner.class);
    private static final String CATALOG_BOM_URL = "catalog.bom";
    private static final String BROOKLYN_CATALOG = "brooklyn.catalog";
    private static final String BROOKLYN_LIBRARIES = "brooklyn.libraries";

    private CatalogPopulator catalogTracker;

    public void bind(ServiceReference<ManagementContext> managementContext) throws Exception {
        LOG.debug("Binding management context");
        catalogTracker = new CatalogPopulator(managementContext);
    }

    public void unbind(ServiceReference<ManagementContext> managementContext) throws Exception {
        LOG.debug("Unbinding management context");
        if (null != catalogTracker) {
            CatalogPopulator temp = catalogTracker;
            catalogTracker = null;
            temp.close();
        }
    }

    private String[] bundleIds(Bundle bundle) {
        return new String[] {
            String.valueOf(bundle.getBundleId()), bundle.getSymbolicName(), bundle.getVersion().toString()
        };
    }


    public class CatalogPopulator extends BundleTracker<Iterable<? extends CatalogItem<?, ?>>> {

        private ServiceReference<ManagementContext> mgmtContextReference;
        private ManagementContext managementContext;

        public CatalogPopulator(ServiceReference<ManagementContext> serviceReference) {
            super(serviceReference.getBundle().getBundleContext(), Bundle.ACTIVE, null);
            this.mgmtContextReference = serviceReference;
            open();
        }

        @Override
        public void open() {
            managementContext = mgmtContextReference.getBundle().getBundleContext().getService(mgmtContextReference);
            super.open();
        }

        @Override
        public void close() {
            super.close();
            managementContext = null;
            mgmtContextReference.getBundle().getBundleContext().ungetService(mgmtContextReference);
        }

        public ManagementContext getManagementContext() {
            return managementContext;
        }

        /**
         * Scans the bundle being added for a catalog.bom file and adds any entries in it to the catalog.
         *
         * @param bundle The bundle being added to the bundle context.
         * @param bundleEvent The event of the addition.
         *
         * @return The items added to the catalog; these will be tracked by the {@link BundleTracker} mechanism
         *         and supplied to the {@link #removedBundle(Bundle, BundleEvent, Iterable)} method.
         */
        @Override
        public Iterable<? extends CatalogItem<?, ?>> addingBundle(Bundle bundle, BundleEvent bundleEvent) {
            return scanForCatalog(bundle);
        }


        @Override
        public void removedBundle(Bundle bundle, BundleEvent bundleEvent, Iterable<? extends CatalogItem<?, ?>> items) {
            LOG.debug("Unloading catalog BOM entries from {} {} {}", bundleIds(bundle));
            List<Exception> exceptions = MutableList.of();
            final BrooklynCatalog catalog = getManagementContext().getCatalog();
            for (CatalogItem<?, ?> item : items) {
                LOG.debug("Unloading {} {} from catalog", item.getSymbolicName(), item.getVersion());

                try {
                    catalog.deleteCatalogItem(item.getSymbolicName(), item.getVersion());
                } catch (Exception e) {
                    LOG.warn("Caught {} unloading {} {} from catalog", new String [] {
                        e.getMessage(), item.getSymbolicName(), item.getVersion()
                    });
                    exceptions.add(e);
                }
            }

            if (0 < exceptions.size()) {
                throw new CompoundRuntimeException(
                    "Caught exceptions unloading catalog from bundle " + bundle.getBundleId(),
                    exceptions);
            }
        }

        private Iterable<? extends CatalogItem<?, ?>> scanForCatalog(Bundle bundle) {
            LOG.debug("Scanning for catalog items in bundle {} {} {}", bundleIds(bundle));
            final URL bom = bundle.getResource(CATALOG_BOM_URL);

            if (null != bom) {
                LOG.debug("Found catalog BOM in {} {} {}", bundleIds(bundle));
                String bomText = readBom(bom);
                String bomWithLibraryPath = addLibraryDetails(bundle, bomText);
                final Iterable<? extends CatalogItem<?, ?>> catalogItems =
                    getManagementContext().getCatalog().addItems(bomWithLibraryPath);
                for (CatalogItem<?, ?> item : catalogItems) {
                    LOG.debug("Added to catalog: {}, {}", item.getSymbolicName(), item.getVersion());
                }

                return catalogItems;
            }
            return ImmutableList.of();
        }

        private String addLibraryDetails(Bundle bundle, String bomText) {
            final Map<String, Object> bom = (Map<String, Object>)Iterables.getOnlyElement(Yamls.parseAll(bomText));
            final Object catalog = bom.get(BROOKLYN_CATALOG);
            if (null != catalog && catalog instanceof Map<?, ?>) {
                addLibraryDetails(bundle, (Map<String, Object>) catalog);
            }
            final String updatedBom = new Yaml().dump(bom);
            LOG.debug("Updated catalog bom:\n{}", updatedBom);
            return updatedBom;
        }

        private void addLibraryDetails(Bundle bundle, Map<String, Object> catalog) {
            if (!catalog.containsKey(BROOKLYN_LIBRARIES)) {
                catalog.put(BROOKLYN_LIBRARIES, MutableList.of());
            }
            final Object librarySpec = catalog.get(BROOKLYN_LIBRARIES);
            if (!(librarySpec instanceof List)) {
                throw new RuntimeException("expected " + BROOKLYN_LIBRARIES + " to be a list");
            }
            List libraries = (List)librarySpec;
            libraries.add(ImmutableMap.of(
                "name", bundle.getSymbolicName(),
                "version", bundle.getVersion().toString()));
            LOG.debug("library spec is {}", librarySpec);
        }

        private String readBom(URL bom) {
            try {
                return Streams.readFullyString(bom.openStream());
            } catch (IOException e) {
                throw Exceptions.propagate("Error loading Catalog BOM from " + bom, e);
            }
        }

    }

}
