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
import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleEvent;
import org.osgi.framework.ServiceReference;
import org.osgi.util.tracker.BundleTracker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import static org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType.TEMPLATE;

public class CatalogBomScanner {

    private final String ACCEPT_ALL_BY_DEFAULT = ".*";

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBomScanner.class);
    private static final String CATALOG_BOM_URL = "catalog.bom";
    private static final String BROOKLYN_CATALOG = "brooklyn.catalog";
    private static final String BROOKLYN_LIBRARIES = "brooklyn.libraries";

    private List<String> whiteList = ImmutableList.of(ACCEPT_ALL_BY_DEFAULT);
    private List<String> blackList = ImmutableList.of();

    private CatalogPopulator catalogTracker;

    public void bind(ServiceReference<ManagementContext> managementContext) throws Exception {
        if (isEnabled()) {
            LOG.debug("Binding management context with whiteList [{}] and blacklist [{}]",
                Strings.join(getWhiteList(), "; "),
                Strings.join(getBlackList(), "; "));
            catalogTracker = new CatalogPopulator(managementContext);
        }
    }

    public void unbind(ServiceReference<ManagementContext> managementContext) throws Exception {
        if (isEnabled()) {
            LOG.debug("Unbinding management context");
            if (null != catalogTracker) {
                CatalogPopulator temp = catalogTracker;
                catalogTracker = null;
                temp.close();
            }
        }
    }

    private boolean isEnabled() {
        return BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_LOAD_BUNDLE_CATALOG_BOM);
    }

    private String[] bundleIds(Bundle bundle) {
        return new String[] {
            String.valueOf(bundle.getBundleId()), bundle.getSymbolicName(), bundle.getVersion().toString()
        };
    }

    public List<String> getWhiteList() {
        return whiteList;
    }

    public void setWhiteList(List<String> whiteList) {
        this.whiteList = whiteList;
    }

    public void setWhiteList(String whiteListText) {
        LOG.debug("Setting whiteList to ", whiteListText);
        this.whiteList = Strings.parseCsv(whiteListText);
    }

    public List<String> getBlackList() {
        return blackList;
    }

    public void setBlackList(List<String> blackList) {
        this.blackList = blackList;
    }

    public void setBlackList(String blackListText) {
        LOG.debug("Setting blackList to ", blackListText);
        this.blackList = Strings.parseCsv(blackListText);
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
            if (!items.iterator().hasNext()) {
                return;
            }
            LOG.debug("Unloading catalog BOM entries from {} {} {}", bundleIds(bundle));
            for (CatalogItem<?, ?> item : items) {
                LOG.debug("Unloading {} {} from catalog", item.getSymbolicName(), item.getVersion());

                removeFromCatalog(item);
            }
        }

        private void removeFromCatalog(CatalogItem<?, ?> item) {
            try {
                getManagementContext().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                LOG.warn(Strings.join(new String[] {
                    "Failed to remove", item.getSymbolicName(), item.getVersion(), "from catalog"
                }, " "), e);
            }
        }

        private Iterable<? extends CatalogItem<?, ?>> scanForCatalog(Bundle bundle) {

            Iterable<? extends CatalogItem<?, ?>> catalogItems = MutableList.of();

            final URL bom = bundle.getResource(CATALOG_BOM_URL);
            if (null != bom) {
                LOG.debug("Found catalog BOM in {} {} {}", bundleIds(bundle));
                String bomText = readBom(bom);
                String bomWithLibraryPath = addLibraryDetails(bundle, bomText);
                catalogItems = getManagementContext().getCatalog().addItems(bomWithLibraryPath);
                for (CatalogItem<?, ?> item : catalogItems) {
                    LOG.debug("Added to catalog: {}, {}", item.getSymbolicName(), item.getVersion());
                }

            } else {
                LOG.debug("No BOM found in {} {} {}", bundleIds(bundle));
            }

            if (!passesWhiteAndBlacklists(bundle)) {
                catalogItems = removeAnyApplications(catalogItems);
            }

            return catalogItems;
        }

        private Iterable<? extends CatalogItem<?, ?>> removeAnyApplications(
            Iterable<? extends CatalogItem<?, ?>> catalogItems) {

            List<CatalogItem<?, ?>> result = MutableList.of();

            for (CatalogItem<?, ?> item: catalogItems) {
                if (TEMPLATE.equals(item.getCatalogItemType())) {
                    removeFromCatalog(item);
                } else {
                    result.add(item);
                }
            }
            return result;
        }

        private boolean passesWhiteAndBlacklists(Bundle bundle) {
            return on(bundle, getWhiteList()) && !on(bundle, getBlackList());
        }

        private boolean on(Bundle bundle, List<String> list) {
            for (String candidate : list) {
                final String symbolicName = bundle.getSymbolicName();
                if (symbolicName.matches(candidate.trim())) {
                    return true;
                }
            }
            return false;
        }

        private String addLibraryDetails(Bundle bundle, String bomText) {
            final Map<String, Object> bom = (Map<String, Object>)Iterables.getOnlyElement(Yamls.parseAll(bomText));
            final Object catalog = bom.get(BROOKLYN_CATALOG);
            if (null != catalog) {
                if (catalog instanceof Map<?, ?>) {
                    addLibraryDetails(bundle, (Map<String, Object>) catalog);
                } else {
                    LOG.warn("Unexpected syntax for {} (expected Map), ignoring", BROOKLYN_CATALOG);
                }
            }
            final String updatedBom = backToYaml(bom);
            LOG.trace("Updated catalog bom:\n{}", updatedBom);
            return updatedBom;
        }

        private String backToYaml(Map<String, Object> bom) {
            final DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setPrettyFlow(true);
            return new Yaml(options).dump(bom);
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
            try (final InputStream ins = bom.openStream()) {
                return Streams.readFullyString(ins);
            } catch (IOException e) {
                throw Exceptions.propagate("Error loading Catalog BOM from " + bom, e);
            }
        }

    }


}
