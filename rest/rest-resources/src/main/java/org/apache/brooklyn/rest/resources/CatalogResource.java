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

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import javax.annotation.Nullable;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.catalog.internal.CatalogBundleLoader;
import org.apache.brooklyn.core.catalog.internal.CatalogDto;
import org.apache.brooklyn.core.catalog.internal.CatalogItemComparator;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.StringAndArgument;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.rest.api.CatalogApi;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.domain.CatalogItemSummary;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.CatalogPolicySummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.CatalogTransformer;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.osgi.BundleMaker;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.commons.compress.archivers.zip.ZipArchiveEntry;
import org.apache.commons.compress.archivers.zip.ZipFile;
import org.osgi.framework.Bundle;
import org.osgi.framework.BundleException;
import org.osgi.framework.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@HaHotStateRequired
public class CatalogResource extends AbstractBrooklynRestResource implements CatalogApi {

    private static final Logger log = LoggerFactory.getLogger(CatalogResource.class);
    
    @SuppressWarnings("rawtypes")
    private Function<CatalogItem, CatalogItemSummary> toCatalogItemSummary(final UriInfo ui) {
        return new Function<CatalogItem, CatalogItemSummary>() {
            @Override
            public CatalogItemSummary apply(@Nullable CatalogItem input) {
                return CatalogTransformer.catalogItemSummary(brooklyn(), input, ui.getBaseUriBuilder());
            }
        };
    };

    static Set<String> missingIcons = MutableSet.of();

    @Override
    @Beta
    public Response createFromUpload(byte[] item) {
        Throwable yamlException = null;
        try {
            MutableList.copyOf( Yamls.parseAll(new InputStreamReader(new ByteArrayInputStream(item))) );
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            yamlException = e;
        }
        
        if (yamlException==null) {
            // treat as yaml if it parsed
            return createFromYaml(new String(item));
        }
        
        return createFromArchive(item);
    }
    
    @Override
    @Deprecated
    public Response create(String yaml) {
        return createFromYaml(yaml);
    }
    
    @Override
    public Response createFromYaml(String yaml) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ADD_CATALOG_ITEM, yaml)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to add catalog item",
                Entitlements.getEntitlementContext().user());
        }

        try {
            final Iterable<? extends CatalogItem<?, ?>> items = brooklyn().getCatalog().addItems(yaml);
            return buildCreateResponse(items);
        } catch (Exception e) {
            e.printStackTrace();
            Exceptions.propagateIfFatal(e);
            return ApiError.of(e).asBadRequestResponseJson();
        }
    }

    @Override
    @Beta
    public Response createFromArchive(byte[] zipInput) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ROOT, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to add catalog item",
                Entitlements.getEntitlementContext().user());
        }

        BundleMaker bm = new BundleMaker(mgmtInternal());
        File f=null, f2=null;
        try {
            f = Os.newTempFile("brooklyn-posted-archive", "zip");
            try {
                Files.write(zipInput, f);
            } catch (IOException e) {
                Exceptions.propagate(e);
            }
            
            ZipFile zf;
            try {
                zf = new ZipFile(f);
            } catch (IOException e) {
                throw new IllegalArgumentException("Invalid ZIP/JAR archive: "+e);
            }
            ZipArchiveEntry bom = zf.getEntry("catalog.bom");
            if (bom==null) {
                bom = zf.getEntry("/catalog.bom");
            }
            if (bom==null) {
                throw new IllegalArgumentException("Archive must contain a catalog.bom file in the root");
            }
            String bomS;
            try {
                bomS = Streams.readFullyString(zf.getInputStream(bom));
            } catch (IOException e) {
                throw new IllegalArgumentException("Error reading catalog.bom from ZIP/JAR archive: "+e);
            }
            VersionedName vn = BasicBrooklynCatalog.getVersionedName( BasicBrooklynCatalog.getCatalogMetadata(bomS) );
            
            Manifest mf = bm.getManifest(f);
            if (mf==null) {
                mf = new Manifest();
            }
            String bundleNameInMF = mf.getMainAttributes().getValue(Constants.BUNDLE_SYMBOLICNAME);
            if (Strings.isNonBlank(bundleNameInMF)) {
                if (!bundleNameInMF.equals(vn.getSymbolicName())) {
                    throw new IllegalArgumentException("JAR MANIFEST symbolic-name '"+bundleNameInMF+"' does not match '"+vn.getSymbolicName()+"' defined in BOM");
                }
            } else {
                mf.getMainAttributes().putValue(Constants.BUNDLE_SYMBOLICNAME, vn.getSymbolicName());
            }
            
            String bundleVersionInMF = mf.getMainAttributes().getValue(Constants.BUNDLE_VERSION);
            if (Strings.isNonBlank(bundleVersionInMF)) {
                if (!bundleVersionInMF.equals(vn.getVersion().toString())) {
                    throw new IllegalArgumentException("JAR MANIFEST version '"+bundleVersionInMF+"' does not match '"+vn.getVersion()+"' defined in BOM");
                }
            } else {
                mf.getMainAttributes().putValue(Constants.BUNDLE_VERSION, vn.getVersion().toString());
            }
            if (mf.getMainAttributes().getValue(Attributes.Name.MANIFEST_VERSION)==null) {
                mf.getMainAttributes().putValue(Attributes.Name.MANIFEST_VERSION.toString(), "1.0");
            }
            
            f2 = bm.copyAddingManifest(f, mf);
            
            Bundle bundle = bm.installBundle(f2, true);

            Iterable<? extends CatalogItem<?, ?>> catalogItems = MutableList.of();
            
            if (!BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_LOAD_BUNDLE_CATALOG_BOM)) {
                // if the above feature is not enabled, let's do it manually (as a contract of this method)
                try {
                    // TODO improve on this - it ignores the configuration of whitelists, see CatalogBomScanner.
                    // One way would be to add the CatalogBomScanner to the new Scratchpad area, then retrieving the singleton
                    // here to get back the predicate from it.
                    final Predicate<Bundle> applicationsPermitted = Predicates.<Bundle>alwaysTrue();

                    catalogItems = new CatalogBundleLoader(applicationsPermitted, mgmt()).scanForCatalog(bundle);
                } catch (RuntimeException ex) {
                    try {
                        bundle.uninstall();
                    } catch (BundleException e) {
                        log.error("Cannot uninstall bundle " + bundle.getSymbolicName() + ":" + bundle.getVersion(), e);
                    }
                    throw new IllegalArgumentException("Error installing catalog items", ex);
                }
            }

            // TODO improve on this - If the FEATURE_LOAD_BUNDLE_CATALOG_BOM is enabled, the REST API won't return the
            // added items which breaks the contract on the API endpoint.
            return buildCreateResponse(catalogItems);
        } catch (RuntimeException ex) {
            throw WebResourceUtils.badRequest(ex);
        } finally {
            if (f!=null) f.delete();
            if (f2!=null) f2.delete();
        }
    }

    private Response buildCreateResponse(Iterable<? extends CatalogItem<?, ?>> catalogItems) {
        log.info("REST created catalog items: "+catalogItems);

        Map<String,Object> result = MutableMap.of();

        for (CatalogItem<?,?> catalogItem: catalogItems) {
            try {
                result.put(catalogItem.getId(), CatalogTransformer.catalogItemSummary(brooklyn(), catalogItem, ui.getBaseUriBuilder()));
            } catch (Throwable t) {
                log.warn("Error loading catalog item '"+catalogItem+"' (rethrowing): "+t);
                // unfortunately items are already added to the catalog and hard to remove,
                // but at least let the user know;
                // happens eg if a class refers to a missing class, like
                // loading nosql items including mongo without the mongo bson class on the classpath
                throw Exceptions.propagateAnnotated("At least one unusable item was added ("+catalogItem.getId()+")", t);
            }
        }
        return Response.status(Status.CREATED).entity(result).build();
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public Response resetXml(String xml, boolean ignoreErrors) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, null) ||
            !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ADD_CATALOG_ITEM, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                Entitlements.getEntitlementContext().user());
        }

        ((BasicBrooklynCatalog)mgmt().getCatalog()).reset(CatalogDto.newDtoFromXmlContents(xml, "REST reset"), !ignoreErrors);
        return Response.ok().build();
    }
    
    @Override
    @Deprecated
    public void deleteEntity_0_7_0(String entityId) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(entityId, "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                Entitlements.getEntitlementContext().user());
        }
        try {
            Maybe<RegisteredType> item = RegisteredTypes.tryValidate(mgmt().getTypeRegistry().get(entityId), RegisteredTypeLoadingContexts.spec(Entity.class));
            if (item.isNull()) {
                throw WebResourceUtils.notFound("Entity with id '%s' not found", entityId);
            }
            if (item.isAbsent()) {
                throw WebResourceUtils.notFound("Item with id '%s' is not an entity", entityId);
            }

            brooklyn().getCatalog().deleteCatalogItem(item.get().getSymbolicName(), item.get().getVersion());

        } catch (NoSuchElementException e) {
            // shouldn't come here
            throw WebResourceUtils.notFound("Entity with id '%s' could not be deleted", entityId);

        }
    }

    @Override
    public void deleteApplication(String symbolicName, String version) throws Exception {
        deleteEntity(symbolicName, version);
    }

    @Override
    public void deleteEntity(String symbolicName, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(symbolicName+(Strings.isBlank(version) ? "" : ":"+version), "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                Entitlements.getEntitlementContext().user());
        }
        
        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, version);
        if (item == null) {
            throw WebResourceUtils.notFound("Entity with id '%s:%s' not found", symbolicName, version);
        } else if (!RegisteredTypePredicates.IS_ENTITY.apply(item) && !RegisteredTypePredicates.IS_APPLICATION.apply(item)) {
            throw WebResourceUtils.preconditionFailed("Item with id '%s:%s' not an entity", symbolicName, version);
        } else {
            brooklyn().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
        }
    }

    @Override
    public void deletePolicy(String policyId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(policyId+(Strings.isBlank(version) ? "" : ":"+version), "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                Entitlements.getEntitlementContext().user());
        }
        
        RegisteredType item = mgmt().getTypeRegistry().get(policyId, version);
        if (item == null) {
            throw WebResourceUtils.notFound("Policy with id '%s:%s' not found", policyId, version);
        } else if (!RegisteredTypePredicates.IS_POLICY.apply(item)) {
            throw WebResourceUtils.preconditionFailed("Item with id '%s:%s' not a policy", policyId, version);
        } else {
            brooklyn().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
        }
    }

    @Override
    public void deleteLocation(String locationId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(locationId+(Strings.isBlank(version) ? "" : ":"+version), "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                Entitlements.getEntitlementContext().user());
        }
        
        RegisteredType item = mgmt().getTypeRegistry().get(locationId, version);
        if (item == null) {
            throw WebResourceUtils.notFound("Location with id '%s:%s' not found", locationId, version);
        } else if (!RegisteredTypePredicates.IS_LOCATION.apply(item)) {
            throw WebResourceUtils.preconditionFailed("Item with id '%s:%s' not a location", locationId, version);
        } else {
            brooklyn().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
        }
    }

    @Override
    public List<CatalogEntitySummary> listEntities(String regex, String fragment, boolean allVersions) {
        Predicate<CatalogItem<Entity, EntitySpec<?>>> filter =
                Predicates.and(
                        CatalogPredicates.IS_ENTITY,
                        CatalogPredicates.<Entity, EntitySpec<?>>disabled(false));
        List<CatalogItemSummary> result = getCatalogItemSummariesMatchingRegexFragment(filter, regex, fragment, allVersions);
        return castList(result, CatalogEntitySummary.class);
    }

    @Override
    public List<CatalogItemSummary> listApplications(String regex, String fragment, boolean allVersions) {
        @SuppressWarnings("unchecked")
        Predicate<CatalogItem<Application, EntitySpec<? extends Application>>> filter =
                Predicates.and(
                        CatalogPredicates.IS_TEMPLATE,
                        CatalogPredicates.<Application,EntitySpec<? extends Application>>deprecated(false),
                        CatalogPredicates.<Application,EntitySpec<? extends Application>>disabled(false));
        return getCatalogItemSummariesMatchingRegexFragment(filter, regex, fragment, allVersions);
    }

    @Override
    @Deprecated
    public CatalogEntitySummary getEntity_0_7_0(String entityId) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, entityId)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        CatalogItem<Entity,EntitySpec<?>> result =
                CatalogUtils.getCatalogItemOptionalVersion(mgmt(), Entity.class, entityId);

        if (result==null) {
            throw WebResourceUtils.notFound("Entity with id '%s' not found", entityId);
        }

        return CatalogTransformer.catalogEntitySummary(brooklyn(), result, ui.getBaseUriBuilder());
    }
    
    @Override
    public CatalogEntitySummary getEntity(String symbolicName, String version) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, symbolicName+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        //TODO These casts are not pretty, we could just provide separate get methods for the different types?
        //Or we could provide asEntity/asPolicy cast methods on the CataloItem doing a safety check internally
        @SuppressWarnings("unchecked")
        CatalogItem<Entity, EntitySpec<?>> result =
              (CatalogItem<Entity, EntitySpec<?>>) brooklyn().getCatalog().getCatalogItem(symbolicName, version);

        if (result==null) {
            throw WebResourceUtils.notFound("Entity with id '%s:%s' not found", symbolicName, version);
        }

        return CatalogTransformer.catalogEntitySummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @Override
    @Deprecated
    public CatalogEntitySummary getApplication_0_7_0(String applicationId) throws Exception {
        return getEntity_0_7_0(applicationId);
    }

    @Override
    public CatalogEntitySummary getApplication(String symbolicName, String version) {
        return getEntity(symbolicName, version);
    }

    @Override
    public List<CatalogPolicySummary> listPolicies(String regex, String fragment, boolean allVersions) {
        Predicate<CatalogItem<Policy, PolicySpec<?>>> filter =
                Predicates.and(
                        CatalogPredicates.IS_POLICY,
                        CatalogPredicates.<Policy, PolicySpec<?>>disabled(false));
        List<CatalogItemSummary> result = getCatalogItemSummariesMatchingRegexFragment(filter, regex, fragment, allVersions);
        return castList(result, CatalogPolicySummary.class);
    }

    @Override
    @Deprecated
    public CatalogPolicySummary getPolicy_0_7_0(String policyId) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, policyId)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        CatalogItem<? extends Policy, PolicySpec<?>> result =
            CatalogUtils.getCatalogItemOptionalVersion(mgmt(), Policy.class, policyId);

        if (result==null) {
            throw WebResourceUtils.notFound("Policy with id '%s' not found", policyId);
        }

        return CatalogTransformer.catalogPolicySummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @Override
    public CatalogPolicySummary getPolicy(String policyId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, policyId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        @SuppressWarnings("unchecked")
        CatalogItem<? extends Policy, PolicySpec<?>> result =
                (CatalogItem<? extends Policy, PolicySpec<?>>)brooklyn().getCatalog().getCatalogItem(policyId, version);

        if (result==null) {
          throw WebResourceUtils.notFound("Policy with id '%s:%s' not found", policyId, version);
        }

        return CatalogTransformer.catalogPolicySummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @Override
    public List<CatalogLocationSummary> listLocations(String regex, String fragment, boolean allVersions) {
        Predicate<CatalogItem<Location, LocationSpec<?>>> filter =
                Predicates.and(
                        CatalogPredicates.IS_LOCATION,
                        CatalogPredicates.<Location, LocationSpec<?>>disabled(false));
        List<CatalogItemSummary> result = getCatalogItemSummariesMatchingRegexFragment(filter, regex, fragment, allVersions);
        return castList(result, CatalogLocationSummary.class);
    }

    @Override
    @Deprecated
    public CatalogLocationSummary getLocation_0_7_0(String locationId) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, locationId)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        CatalogItem<? extends Location, LocationSpec<?>> result =
            CatalogUtils.getCatalogItemOptionalVersion(mgmt(), Location.class, locationId);

        if (result==null) {
            throw WebResourceUtils.notFound("Location with id '%s' not found", locationId);
        }

        return CatalogTransformer.catalogLocationSummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @Override
    public CatalogLocationSummary getLocation(String locationId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, locationId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        @SuppressWarnings("unchecked")
        CatalogItem<? extends Location, LocationSpec<?>> result =
                (CatalogItem<? extends Location, LocationSpec<?>>)brooklyn().getCatalog().getCatalogItem(locationId, version);

        if (result==null) {
          throw WebResourceUtils.notFound("Location with id '%s:%s' not found", locationId, version);
        }

        return CatalogTransformer.catalogLocationSummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    private <T,SpecT> List<CatalogItemSummary> getCatalogItemSummariesMatchingRegexFragment(Predicate<? super CatalogItem<T,SpecT>> type, String regex, String fragment, boolean allVersions) {
        List filters = new ArrayList();
        filters.add(type);
        if (Strings.isNonEmpty(regex))
            filters.add(CatalogPredicates.xml(StringPredicates.containsRegex(regex)));
        if (Strings.isNonEmpty(fragment))
            filters.add(CatalogPredicates.xml(StringPredicates.containsLiteralIgnoreCase(fragment)));
        if (!allVersions)
            filters.add(CatalogPredicates.isBestVersion(mgmt()));
        
        filters.add(CatalogPredicates.entitledToSee(mgmt()));

        ImmutableList<CatalogItem<Object, Object>> sortedItems =
                FluentIterable.from(brooklyn().getCatalog().getCatalogItems())
                    .filter(Predicates.and(filters))
                    .toSortedList(CatalogItemComparator.getInstance());
        return Lists.transform(sortedItems, toCatalogItemSummary(ui));
    }

    @Override
    @Deprecated
    public Response getIcon_0_7_0(String itemId) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, itemId)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        return getCatalogItemIcon( mgmt().getTypeRegistry().get(itemId) );
    }

    @Override
    public Response getIcon(String itemId, String version) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, itemId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }
        
        return getCatalogItemIcon(mgmt().getTypeRegistry().get(itemId, version));
    }

    @Override
    public void setDeprecatedLegacy(String itemId, boolean deprecated) {
        log.warn("Use of deprecated \"/catalog/entities/{itemId}/deprecated/{deprecated}\" for "+itemId
                +"; use \"/catalog/entities/{itemId}/deprecated\" with request body");
        setDeprecated(itemId, deprecated);
    }
    
    @SuppressWarnings("deprecation")
    @Override
    public void setDeprecated(String itemId, boolean deprecated) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(itemId, "deprecated"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                    Entitlements.getEntitlementContext().user());
        }
        CatalogUtils.setDeprecated(mgmt(), itemId, deprecated);
    }

    @SuppressWarnings("deprecation")
    @Override
    public void setDisabled(String itemId, boolean disabled) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(itemId, "disabled"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                    Entitlements.getEntitlementContext().user());
        }
        CatalogUtils.setDisabled(mgmt(), itemId, disabled);
    }

    private Response getCatalogItemIcon(RegisteredType result) {
        String url = result.getIconUrl();
        if (url==null) {
            log.debug("No icon available for "+result+"; returning "+Status.NO_CONTENT);
            return Response.status(Status.NO_CONTENT).build();
        }
        
        if (brooklyn().isUrlServerSideAndSafe(url)) {
            // classpath URL's we will serve IF they end with a recognised image format;
            // paths (ie non-protocol) and 
            // NB, for security, file URL's are NOT served
            log.debug("Loading and returning "+url+" as icon for "+result);
            
            MediaType mime = WebResourceUtils.getImageMediaTypeFromExtension(Files.getFileExtension(url));
            try {
                Object content = ResourceUtils.create(CatalogUtils.newClassLoadingContext(mgmt(), result)).getResourceFromUrl(url);
                return Response.ok(content, mime).build();
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                synchronized (missingIcons) {
                    if (missingIcons.add(url)) {
                        // note: this can be quite common when running from an IDE, as resources may not be copied;
                        // a mvn build should sort it out (the IDE will then find the resources, until you clean or maybe refresh...)
                        log.warn("Missing icon data for "+result.getId()+", expected at: "+url+" (subsequent messages will log debug only)");
                        log.debug("Trace for missing icon data at "+url+": "+e, e);
                    } else {
                        log.debug("Missing icon data for "+result.getId()+", expected at: "+url+" (already logged WARN and error details)");
                    }
                }
                throw WebResourceUtils.notFound("Icon unavailable for %s", result.getId());
            }
        }
        
        log.debug("Returning redirect to "+url+" as icon for "+result);
        
        // for anything else we do a redirect (e.g. http / https; perhaps ftp)
        return Response.temporaryRedirect(URI.create(url)).build();
    }

    // TODO Move to an appropriate utility class?
    @SuppressWarnings("unchecked")
    private static <T> List<T> castList(List<? super T> list, Class<T> elementType) {
        List<T> result = Lists.newArrayList();
        Iterator<? super T> li = list.iterator();
        while (li.hasNext()) {
            try {
                result.add((T) li.next());
            } catch (Throwable throwable) {
                if (throwable instanceof NoClassDefFoundError) {
                    // happens if class cannot be loaded for any reason during transformation - don't treat as fatal
                } else {
                    Exceptions.propagateIfFatal(throwable);
                }
                
                // item cannot be transformed; we will have logged a warning earlier
                log.debug("Ignoring invalid catalog item: "+throwable);
            }
        }
        return result;
    }
}
 