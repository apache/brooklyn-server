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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
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
            String.valueOf(bundle.getBundleId()), String.valueOf(bundle.getState()), bundle.getSymbolicName()
        };
    }


    public class CatalogPopulator extends BundleTracker<Long> {

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

        @Override
        public Long addingBundle(Bundle bundle, BundleEvent bundleEvent) {

            final BundleContext bundleContext = FrameworkUtil.getBundle(CatalogBomScanner.class).getBundleContext();
            if (bundleContext == null) {
                LOG.info("Bundle context not yet established for bundle {} {} {}", bundleIds(bundle));
                return null;
            }

            scanForCatalog(bundle);

            return bundle.getBundleId();
        }

        @Override
        public void modifiedBundle(Bundle bundle, BundleEvent event, Long bundleId) {
            sanityCheck(bundle, bundleId);
            LOG.info("Modified bundle {} {} {}", bundleIds(bundle));
        }

        @Override
        public void removedBundle(Bundle bundle, BundleEvent bundleEvent, Long bundleId) {
            sanityCheck(bundle, bundleId);
            LOG.info("Unloading catalog BOM from {} {} {}", bundleIds(bundle));
        }

        private void scanForCatalog(Bundle bundle) {
            LOG.info("Scanning for catalog items in bundle {} {} {}", bundleIds(bundle));
            final URL bom = bundle.getResource(CATALOG_BOM_URL);

            if (null != bom) {
                LOG.info("Found catalog BOM in {} {} {}", bundleIds(bundle));
                String bomText = readBom(bom);
                String bomWithLibraryPath = addLibraryDetails(bundle, bomText);
                for (CatalogItem<?, ?> item : getManagementContext().getCatalog().addItems(bomWithLibraryPath)) {
                    LOG.debug("Added to catalog: {}", item.getSymbolicName());
                }
            }
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

        private void sanityCheck(Bundle bundle, Long bundleId) {
            if (bundleId != bundle.getBundleId()) {
                throw new RuntimeException("Unexpected ID supplied for bundle " + bundle + " (" + bundleId + ")");
            }
        }

    }

}
