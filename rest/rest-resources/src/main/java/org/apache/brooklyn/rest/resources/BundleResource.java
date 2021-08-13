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
package org.apache.brooklyn.rest.resources;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.BrooklynBomBundleCatalogBundleResolver;
import org.apache.brooklyn.core.typereg.BrooklynBomYamlCatalogBundleResolver;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.rest.api.BundleApi;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.ApiError.Builder;
import org.apache.brooklyn.rest.domain.BundleInstallationRestResult;
import org.apache.brooklyn.rest.domain.BundleSummary;
import org.apache.brooklyn.rest.domain.TypeDetail;
import org.apache.brooklyn.rest.domain.TypeSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.TypeTransformer;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.io.FileUtil;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.osgi.VersionedName.VersionedNameComparator;
import org.apache.brooklyn.util.stream.InputStreamSource;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

@HaHotStateRequired
public class BundleResource extends AbstractBrooklynRestResource implements BundleApi {

    private static final Logger log = LoggerFactory.getLogger(BundleResource.class);
    private static final String LATEST = "latest";

    @Override
    public List<BundleSummary> list(String versions, boolean detail) {
        return list(Predicates.alwaysTrue(), TypeResource.isLatestOnly(versions, true), detail);
    }
    
    private List<BundleSummary> list(Predicate<String> symbolicNameFilter, boolean onlyLatest, boolean detail) {
        
        Map<VersionedName,ManagedBundle> bundles = new TreeMap<>(VersionedNameComparator.INSTANCE);
        for (ManagedBundle b: ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundles().values()) {
            if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, b.getId())) {
                continue;
            }
            if (symbolicNameFilter.apply(b.getSymbolicName())) {
                VersionedName key = onlyLatest ? new VersionedName(b.getSymbolicName(), LATEST) : b.getVersionedName();
                ManagedBundle oldBundle = bundles.get(key);
                if (oldBundle==null || oldBundle.getVersionedName().compareTo(b.getVersionedName()) > 0) {
                    bundles.put(key, b);
                }
            }
        }
        return toBundleSummary(bundles.values(), detail);
    }

    private List<BundleSummary> toBundleSummary(Iterable<ManagedBundle> sortedItems, boolean detail) {
        List<BundleSummary> result = MutableList.of();
        for (ManagedBundle t: sortedItems) {
            result.add(TypeTransformer.bundleSummary(brooklyn(), t, ui.getBaseUriBuilder(), mgmt(), detail));
        }
        return result;
    }

    @Override
    public List<BundleSummary> listVersions(String symbolicName, boolean detail) {
        return list(Predicates.equalTo(symbolicName), false, detail);
    }

    @Override
    public BundleSummary detail(String symbolicName, String version) {
        ManagedBundle b = lookup(symbolicName, version);
        return TypeTransformer.bundleDetails(brooklyn(), b, ui.getBaseUriBuilder(), mgmt());
    }

    protected ManagedBundle lookup(String symbolicName, String version) {
        ManagedBundle b = ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundle(new VersionedName(symbolicName, version));
        if (b==null) {
            throw WebResourceUtils.notFound("Bundle with id '%s:%s' not found", symbolicName, version);
        }
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, b.getId())) {
            throw WebResourceUtils.notFound("Bundle with id '%s:%s' not found", symbolicName, version);
        }
        return b;
    }

    public Response download(String symbolicName, String version) {
        ManagedBundle managedBundle = lookup(symbolicName, version);
        File bundleFile = ((ManagementContextInternal) mgmt()).getOsgiManager().get().getBundleFile(managedBundle);
        if (bundleFile == null || !bundleFile.exists()) {
            throw WebResourceUtils.notFound("Bundle with id '%s:%s' doesn't have a ZIP archive found", symbolicName, version);
        }

        try {
            return Response
                    .ok(FileUtils.readFileToByteArray(bundleFile), "application/zip")
                    .header("Content-Disposition", "attachment; filename=" + symbolicName + "-" + version + ".zip")
                    .build();
        } catch (IOException e) {
            throw WebResourceUtils.serverError("Cannot read the ZIP archive for bundle '%s:%s'", symbolicName, version);
        }
    }

    @Override
    public List<TypeSummary> getTypes(String symbolicName, String version) {
        ManagedBundle b = lookup(symbolicName, version);
        return TypeTransformer.bundleDetails(brooklyn(), b, ui.getBaseUriBuilder(), mgmt()).getTypes();
    }

    @Override
    public TypeDetail getType(String symbolicName, String version, String typeSymbolicName) {
        return getTypeExplicitVersion(symbolicName, version, typeSymbolicName, version);
    }

    @Override
    public TypeDetail getTypeExplicitVersion(String symbolicName, String version, String typeSymbolicName, String typeVersion) {
        RegisteredType item = getRegisteredType(symbolicName, version, typeSymbolicName, typeVersion);
        return TypeTransformer.detail(brooklyn(), item, ui.getBaseUriBuilder());
    }

    private RegisteredType getRegisteredType(String symbolicName, String version, String typeSymbolicName, String typeVersion) {
        ManagedBundle b = lookup(symbolicName, version);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, typeSymbolicName + ":" + typeVersion)) {
            // TODO best to default to "not found" - unless maybe they have permission to "see null"
            throw WebResourceUtils.forbidden("User '%s' not permitted to see info on this type (including whether or not installed)",
                    Entitlements.getEntitlementContext().user());
        }

        Predicate<RegisteredType> pred = RegisteredTypePredicates.nameOrAlias(typeSymbolicName);
        pred = Predicates.and(pred, RegisteredTypePredicates.containingBundle(b.getVersionedName()));
        if (!LATEST.equalsIgnoreCase(typeVersion)) {
            pred = Predicates.and(pred, RegisteredTypePredicates.version(typeVersion));
        }
        Iterable<RegisteredType> items = mgmt().getTypeRegistry().getMatching(pred);

        if (Iterables.isEmpty(items)) {
            throw WebResourceUtils.notFound("Entity with id '%s:%s' not found", typeSymbolicName, typeVersion);
        }

        return RegisteredTypes.getBestVersion(items);
    }

    @Override
    public Response getTypeExplicitVersionIcon(String symbolicName, String version, String typeSymbolicName, String typeVersion) {
        RegisteredType item = getRegisteredType(symbolicName, version, typeSymbolicName, typeVersion);
        return TypeResource.produceIcon(mgmt(), brooklyn(), item);
    }

    @Override
    public BundleInstallationRestResult remove(String symbolicName, String version, Boolean force) {
        ManagedBundle b = lookup(symbolicName, version);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM,  Entitlements.StringAndArgument.of(symbolicName+(Strings.isBlank(version) ? "" : ":"+version), "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to remove catalog item '%s:%s'",
                    Entitlements.getEntitlementContext().user(),symbolicName,version);
        }
        log.info("REST removing "+symbolicName+":"+version);
        if (force==null) force = false;
        ReferenceWithError<OsgiBundleInstallationResult> r = ((ManagementContextInternal)mgmt()).getOsgiManager().get().uninstallUploadedBundle(b, force);
        return TypeTransformer.bundleInstallationResult(r.getWithoutError(), mgmt(), brooklyn(), ui);
    }


    @Override @Deprecated
    public Response createFromYaml(String yaml, Boolean force) {
        return create(yaml.getBytes(), BrooklynBomYamlCatalogBundleResolver.FORMAT, force);
    }
    
    @Override @Deprecated
    public Response createFromArchive(byte[] zipInput, Boolean force) {
        return create(zipInput, BrooklynBomBundleCatalogBundleResolver.FORMAT, force);
    }

    @Override
    public Response create(byte[] contents, String format, Boolean force) {
        InputStreamSource source = InputStreamSource.of("REST bundle upload", contents);
        if(!BrooklynBomYamlCatalogBundleResolver.FORMAT.equals(format) && FileUtil.doesZipContainJavaBinaries(source)){
            if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ADD_JAVA, null)) {
                throw WebResourceUtils.forbidden("User '%s' is not authorized to add catalog item containing java classes",
                        Entitlements.getEntitlementContext().user());
            }
        }
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ADD_CATALOG_ITEM, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to add catalog item",
                    Entitlements.getEntitlementContext().user());
        }
        if (force==null) force = false;

        ReferenceWithError<OsgiBundleInstallationResult> result = ((ManagementContextInternal)mgmt()).getOsgiManager().get()
                .install(source, format, force);

        if (result.hasError()) {
            // (rollback already done as part of install, if necessary)
            if (log.isTraceEnabled()) {
                log.trace("Unable to create from archive, returning 400: "+result.getError().getMessage(), result.getError());
            }
            Builder error = ApiError.builder().errorCode(Status.BAD_REQUEST);
            if (result.getWithoutError()!=null) {
                error = error.message(result.getWithoutError().getMessage())
                        .data(TypeTransformer.bundleInstallationResult(result.getWithoutError(), mgmt(), brooklyn(), ui));
            } else {
                error.message(Strings.isNonBlank(result.getError().getMessage()) ? result.getError().getMessage() : result.getError().toString());
            }
            return error.build().asJsonResponse();
        }

        BundleInstallationRestResult resultR = TypeTransformer.bundleInstallationResult(result.get(), mgmt(), brooklyn(), ui);
        Status status;
        switch (result.get().getCode()) {
            case IGNORING_BUNDLE_AREADY_INSTALLED:
            case IGNORING_BUNDLE_FORCIBLY_REMOVED:
                status = Status.OK;
                break;
            default:
                // already checked that it was not an error; anything else means we created it.
                status = Status.CREATED;
                break;
        }
        return Response.status(status).entity(resultR).build();
    }
    
}
