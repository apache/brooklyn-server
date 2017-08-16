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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Iterables;

@Beta
public class CatalogBundleLoader {

    private static final Logger LOG = LoggerFactory.getLogger(CatalogBundleLoader.class);
    private static final String CATALOG_BOM_URL = "catalog.bom";

    private ManagementContext managementContext;

    public CatalogBundleLoader(ManagementContext managementContext) {
        this.managementContext = managementContext;
    }

    /** @deprecated since 0.12.0 use {@link #scanForCatalog(Bundle, boolean, boolean, Map)} */
    public void scanForCatalog(Bundle bundle, boolean force, boolean validate) {
        scanForCatalog(bundle, force, validate, null);
    }
    
    public void scanForCatalog(Bundle bundle, boolean force, boolean validate, Map<RegisteredType, RegisteredType> result) {
        scanForCatalogInternal(bundle, force, validate, false, result);
    }

    /** @deprecated since 0.12.0 */
    @Deprecated // scans a bundle which is installed but Brooklyn isn't managing (will probably remove)
    public Iterable<? extends CatalogItem<?, ?>> scanForCatalogLegacy(Bundle bundle, boolean force) {
        LOG.warn("Bundle "+bundle+" being loaded with deprecated legacy loader");
        return scanForCatalogInternal(bundle, force, true, true, null).legacyResult;
    }
    
    private static class TemporaryInternalScanResult {
        Iterable<? extends CatalogItem<?, ?>> legacyResult;
        Map<RegisteredType, RegisteredType> mapOfNewToReplaced;
        
    }
    private TemporaryInternalScanResult scanForCatalogInternal(Bundle bundle, boolean force, boolean validate, boolean legacy, Map<RegisteredType, RegisteredType> resultNewFormat) {
        ManagedBundle mb = ((ManagementContextInternal)managementContext).getOsgiManager().get().getManagedBundle(
            new VersionedName(bundle));

        TemporaryInternalScanResult result = new TemporaryInternalScanResult();
        result.legacyResult = MutableList.of();
        result.mapOfNewToReplaced = resultNewFormat;

        final URL bom = bundle.getResource(CatalogBundleLoader.CATALOG_BOM_URL);
        if (null != bom) {
            LOG.debug("Found catalog BOM in {} {} {}", CatalogUtils.bundleIds(bundle));
            String bomText = readBom(bom);
            if (mb==null) {
                LOG.warn("Bundle "+bundle+" containing BOM is not managed by Brooklyn; using legacy item installation");
                legacy = true;
            }
            if (legacy) {
                result.legacyResult = this.managementContext.getCatalog().addItems(bomText, mb, force);
                for (CatalogItem<?, ?> item : result.legacyResult) {
                    LOG.debug("Added to catalog: {}, {}", item.getSymbolicName(), item.getVersion());
                }
            } else {
                this.managementContext.getCatalog().addTypesFromBundleBom(bomText, mb, force, result.mapOfNewToReplaced);
                if (validate) {
                    Set<RegisteredType> matches = MutableSet.copyOf(this.managementContext.getTypeRegistry().getMatching(RegisteredTypePredicates.containingBundle(mb.getVersionedName())));
                    if (!matches.equals(result.mapOfNewToReplaced.keySet())) {
                        // sanity check
                        LOG.warn("Discrepancy in list of Brooklyn items found for "+mb.getVersionedName()+": "+
                            "installer said "+result.mapOfNewToReplaced+" but registry looking found "+matches);
                    }
                    Map<RegisteredType, Collection<Throwable>> validationErrors = this.managementContext.getCatalog().validateTypes( matches );
                    if (!validationErrors.isEmpty()) {
                        throw Exceptions.propagate("Failed to install "+mb.getVersionedName()+", types "+validationErrors.keySet()+" gave errors",
                            Iterables.concat(validationErrors.values()));
                    }
                }
            }
            
            if (!legacy && BasicBrooklynCatalog.isNoBundleOrSimpleWrappingBundle(managementContext, mb)) {
                ((ManagementContextInternal)managementContext).getOsgiManager().get().addInstalledWrapperBundle(mb);
            }
        } else {
            LOG.debug("No BOM found in {} {} {}", CatalogUtils.bundleIds(bundle));
        }

        return result;
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
    
    public void removeFromCatalog(VersionedName n) {
        ((ManagementContextInternal)managementContext).getOsgiManager().get().uninstallCatalogItemsFromBundle(n);
    }

    private String readBom(URL bom) {
        try (final InputStream ins = bom.openStream()) {
            return Streams.readFullyString(ins);
        } catch (IOException e) {
            throw Exceptions.propagateAnnotated("Error loading Catalog BOM from " + bom, e);
        }
    }

    private Iterable<? extends CatalogItem<?, ?>> removeApplications(Iterable<? extends CatalogItem<?, ?>> catalogItems) {

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

}
