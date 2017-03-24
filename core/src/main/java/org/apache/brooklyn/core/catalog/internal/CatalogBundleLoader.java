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

import static org.apache.brooklyn.api.catalog.CatalogItem.CatalogItemType.TEMPLATE;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import com.google.common.annotations.Beta;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

@Beta
public class CatalogBundleLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBundleLoader.class);
    private static final String CATALOG_BOM_URL = "catalog.bom";
    private static final String BROOKLYN_CATALOG = "brooklyn.catalog";
    private static final String BROOKLYN_LIBRARIES = "brooklyn.libraries";

    private CatalogBomScanner catalogBomScanner;
    private ManagementContext managementContext;

    public CatalogBundleLoader(CatalogBomScanner catalogBomScanner, ManagementContext managementContext) {
        this.catalogBomScanner = catalogBomScanner;
        this.managementContext = managementContext;
    }

    /**
     * Scan the given bundle for a catalog.bom and adds it to the catalog.
     *
     * @param bundle The bundle to add
     * @return A list of items added to the catalog
     * @throws RuntimeException if the catalog items failed to be added to the catalog
     */
    public Iterable<? extends CatalogItem<?, ?>> scanForCatalog(Bundle bundle) {

        Iterable<? extends CatalogItem<?, ?>> catalogItems = MutableList.of();

        final URL bom = bundle.getResource(CatalogBundleLoader.CATALOG_BOM_URL);
        if (null != bom) {
            LOG.debug("Found catalog BOM in {} {} {}", catalogBomScanner.bundleIds(bundle));
            String bomText = readBom(bom);
            String bomWithLibraryPath = addLibraryDetails(bundle, bomText);
            catalogItems = this.managementContext.getCatalog().addItems(bomWithLibraryPath);
            for (CatalogItem<?, ?> item : catalogItems) {
                LOG.debug("Added to catalog: {}, {}", item.getSymbolicName(), item.getVersion());
            }
        } else {
            LOG.debug("No BOM found in {} {} {}", catalogBomScanner.bundleIds(bundle));
        }

        if (!passesWhiteAndBlacklists(bundle)) {
            catalogItems = removeAnyApplications(catalogItems);
        }

        return catalogItems;
    }

    /**
     * Remove the given items from the catalog.
     *
     * @param item Catalog items to remove
     */
    public void removeFromCatalog(CatalogItem<?, ?> item) {
        try {
            this.managementContext.getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            LOG.warn(Strings.join(new String[]{
                    "Failed to remove", item.getSymbolicName(), item.getVersion(), "from catalog"
            }, " "), e);
        }
    }

    private String readBom(URL bom) {
        try (final InputStream ins = bom.openStream()) {
            return Streams.readFullyString(ins);
        } catch (IOException e) {
            throw Exceptions.propagate("Error loading Catalog BOM from " + bom, e);
        }
    }

    private String addLibraryDetails(Bundle bundle, String bomText) {
        @SuppressWarnings("unchecked")
        final Map<String, Object> bom = (Map<String, Object>) Iterables.getOnlyElement(Yamls.parseAll(bomText));
        final Object catalog = bom.get(CatalogBundleLoader.BROOKLYN_CATALOG);
        if (null != catalog) {
            if (catalog instanceof Map<?, ?>) {
                @SuppressWarnings("unchecked")
                Map<String, Object> catalogMap = (Map<String, Object>) catalog;
                addLibraryDetails(bundle, catalogMap);
            } else {
                LOG.warn("Unexpected syntax for {} (expected Map, but got {}), ignoring", CatalogBundleLoader.BROOKLYN_CATALOG, catalog.getClass().getName());
            }
        }
        final String updatedBom = backToYaml(bom);
        LOG.trace("Updated catalog bom:\n{}", updatedBom);
        return updatedBom;
    }

    private void addLibraryDetails(Bundle bundle, Map<String, Object> catalog) {
        if (!catalog.containsKey(CatalogBundleLoader.BROOKLYN_LIBRARIES)) {
            catalog.put(CatalogBundleLoader.BROOKLYN_LIBRARIES, MutableList.of());
        }
        final Object librarySpec = catalog.get(CatalogBundleLoader.BROOKLYN_LIBRARIES);
        if (!(librarySpec instanceof List)) {
            throw new RuntimeException("expected " + CatalogBundleLoader.BROOKLYN_LIBRARIES + " to be a list, but got "
                    + (librarySpec == null ? "null" : librarySpec.getClass().getName()));
        }
        @SuppressWarnings("unchecked")
        List<Map<String, String>> libraries = (List<Map<String, String>>) librarySpec;
        if (bundle.getSymbolicName() == null || bundle.getVersion() == null) {
            throw new IllegalStateException("Cannot scan " + bundle + " for catalog files: name or version is null");
        }
        libraries.add(ImmutableMap.of(
                "name", bundle.getSymbolicName(),
                "version", bundle.getVersion().toString()));
        LOG.debug("library spec is {}", librarySpec);
    }

    private String backToYaml(Map<String, Object> bom) {
        final DumperOptions options = new DumperOptions();
        options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
        options.setPrettyFlow(true);
        return new Yaml(options).dump(bom);
    }

    private Iterable<? extends CatalogItem<?, ?>> removeAnyApplications(
            Iterable<? extends CatalogItem<?, ?>> catalogItems) {

        List<CatalogItem<?, ?>> result = MutableList.of();

        for (CatalogItem<?, ?> item : catalogItems) {
            if (TEMPLATE.equals(item.getCatalogItemType())) {
                removeFromCatalog(item);
            } else {
                result.add(item);
            }
        }
        return result;
    }

    private boolean passesWhiteAndBlacklists(Bundle bundle) {
        return on(bundle, catalogBomScanner.getWhiteList()) && !on(bundle, catalogBomScanner.getBlackList());
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
}
