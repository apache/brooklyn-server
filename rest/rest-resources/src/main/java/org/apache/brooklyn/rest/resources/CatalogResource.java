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
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;
import javax.ws.rs.DefaultValue;
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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.CatalogPredicates;
import org.apache.brooklyn.core.catalog.internal.CatalogItemComparator;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.StringAndArgument;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.rest.api.CatalogApi;
import org.apache.brooklyn.rest.domain.ApiError;
import org.apache.brooklyn.rest.domain.CatalogEnricherSummary;
import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.domain.CatalogItemSummary;
import org.apache.brooklyn.rest.domain.CatalogLocationSummary;
import org.apache.brooklyn.rest.domain.CatalogPolicySummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.CatalogTransformer;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.ReferenceWithError;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
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

import io.swagger.annotations.ApiParam;

@HaHotStateRequired
public class CatalogResource extends AbstractBrooklynRestResource implements CatalogApi {

    private static final Logger log = LoggerFactory.getLogger(CatalogResource.class);
    private static final String LATEST = "latest";
    
    @SuppressWarnings("rawtypes")
    private Function<CatalogItem, CatalogItemSummary> toCatalogItemSummary(final UriInfo ui) {
        return new Function<CatalogItem, CatalogItemSummary>() {
            @Override
            public CatalogItemSummary apply(@Nullable CatalogItem input) {
                return CatalogTransformer.catalogItemSummary(brooklyn(), input, ui.getBaseUriBuilder());
            }
        };
    };

    private String processVersion(String version) {
        if (version != null && LATEST.equals(version.toLowerCase())) {
            version = null;
        }
        return version;
    }

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
            Exceptions.propagateIfFatal(e);
            return badRequest(e);
        }
    }

    public static class BundleInstallationRestResult {
        // as Osgi result, but without bundle, and with maps of catalog items installed
        
        String message;
        ManagedBundle metadata;
        OsgiBundleInstallationResult.ResultCode code;
        
        Map<String,Object> types;
        
        public String getMessage() {
            return message;
        }
        
        @SuppressWarnings("deprecation")
        public static BundleInstallationRestResult of(OsgiBundleInstallationResult in, ManagementContext mgmt, BrooklynRestResourceUtils brooklynU, UriInfo ui) {
            BundleInstallationRestResult result = new BundleInstallationRestResult();
            result.message = in.getMessage();
            result.metadata = in.getMetadata();
            result.code = in.getCode();
            if (in.getCatalogItemsInstalled()!=null) {
                result.types = MutableMap.of();
                for (String id: in.getCatalogItemsInstalled()) {
                    // TODO prefer to use RegisteredType, but we need transformer for those in REST
                    //RegisteredType ci = mgmt.getTypeRegistry().get(id);
                    
                    CatalogItem<?, ?> ci = CatalogUtils.getCatalogItemOptionalVersion(mgmt, id);
                    CatalogItemSummary summary = CatalogTransformer.catalogItemSummary(brooklynU, ci, ui.getBaseUriBuilder());
                    result.types.put(id, summary);
                }
            }
            return result;
        }
    }
    
    @Override
    @Beta
    public Response createFromArchive(byte[] zipInput) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.ROOT, null)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to add catalog item",
                Entitlements.getEntitlementContext().user());
        }

        ReferenceWithError<OsgiBundleInstallationResult> result = ((ManagementContextInternal)mgmt()).getOsgiManager().get()
            .install(new ByteArrayInputStream(zipInput));
        
        if (result.hasError()) {
            return ApiError.builder().errorCode(Status.BAD_REQUEST).message(result.getWithoutError().getMessage())
                .data(BundleInstallationRestResult.of(result.getWithoutError(), mgmt(), brooklyn(), ui)).build().asJsonResponse();
        }

        return Response.status(Status.CREATED).entity( BundleInstallationRestResult.of(result.get(), mgmt(), brooklyn(), ui) ).build();
    }

    private Response buildCreateResponse(Iterable<? extends CatalogItem<?, ?>> catalogItems) {
        log.info("REST created catalog items: "+catalogItems);

        Map<String,Object> result = MutableMap.of();

        for (CatalogItem<?,?> catalogItem: catalogItems) {
            try {
                result.put(
                        catalogItem.getId(),
                        CatalogTransformer.catalogItemSummary(brooklyn(), catalogItem, ui.getBaseUriBuilder()));
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

        version = processVersion(version);
        
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

        version = processVersion(version);
        
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

        version = processVersion(version);
        
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
    public CatalogEntitySummary getEntity(String symbolicName, String version) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, symbolicName+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        version = processVersion(version);

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
    public CatalogPolicySummary getPolicy(String policyId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, policyId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        version = processVersion(version);

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
    public CatalogLocationSummary getLocation(String locationId, String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, locationId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        version = processVersion(version);

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
            filters.add(CatalogPredicates.stringRepresentationMatches(StringPredicates.containsRegex(regex)));
        if (Strings.isNonEmpty(fragment))
            filters.add(CatalogPredicates.stringRepresentationMatches(StringPredicates.containsLiteralIgnoreCase(fragment)));
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
    public Response getIcon(String itemId, String version) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, itemId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                Entitlements.getEntitlementContext().user());
        }

        version = processVersion(version);
        
        return getCatalogItemIcon(mgmt().getTypeRegistry().get(itemId, version));
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

    @Override
    public List<CatalogEnricherSummary> listEnrichers(@ApiParam(name = "regex", value = "Regular expression to search for") @DefaultValue("") String regex, @ApiParam(name = "fragment", value = "Substring case-insensitive to search for") @DefaultValue("") String fragment, @ApiParam(name = "allVersions", value = "Include all versions (defaults false, only returning the best version)") @DefaultValue("false") boolean includeAllVersions) {
        Predicate<CatalogItem<Enricher, EnricherSpec<?>>> filter =
                Predicates.and(
                        CatalogPredicates.IS_ENRICHER,
                        CatalogPredicates.<Enricher, EnricherSpec<?>>disabled(false));
        List<CatalogItemSummary> result = getCatalogItemSummariesMatchingRegexFragment(filter, regex, fragment, includeAllVersions);
        return castList(result, CatalogEnricherSummary.class);
    }

    @Override
    public CatalogEnricherSummary getEnricher(@ApiParam(name = "enricherId", value = "The ID of the enricher to retrieve", required = true) String enricherId, @ApiParam(name = "version", value = "The version identifier of the enricher to retrieve", required = true) String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, enricherId+(Strings.isBlank(version)?"":":"+version))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see catalog entry",
                    Entitlements.getEntitlementContext().user());
        }

        version = processVersion(version);

        @SuppressWarnings("unchecked")
        CatalogItem<? extends Enricher, EnricherSpec<?>> result =
                (CatalogItem<? extends Enricher, EnricherSpec<?>>)brooklyn().getCatalog().getCatalogItem(enricherId, version);

        if (result==null) {
            throw WebResourceUtils.notFound("Enricher with id '%s:%s' not found", enricherId, version);
        }

        return CatalogTransformer.catalogEnricherSummary(brooklyn(), result, ui.getBaseUriBuilder());
    }

    @Override
    public void deleteEnricher(@ApiParam(name = "enricherId", value = "The ID of the enricher to delete", required = true) String enricherId, @ApiParam(name = "version", value = "The version identifier of the enricher to delete", required = true) String version) throws Exception {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_CATALOG_ITEM, StringAndArgument.of(enricherId+(Strings.isBlank(version) ? "" : ":"+version), "delete"))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify catalog",
                    Entitlements.getEntitlementContext().user());
        }

        RegisteredType item = mgmt().getTypeRegistry().get(enricherId, version);
        if (item == null) {
            throw WebResourceUtils.notFound("Enricher with id '%s:%s' not found", enricherId, version);
        } else if (!RegisteredTypePredicates.IS_ENRICHER.apply(item)) {
            throw WebResourceUtils.preconditionFailed("Item with id '%s:%s' not an enricher", enricherId, version);
        } else {
            brooklyn().getCatalog().deleteCatalogItem(item.getSymbolicName(), item.getVersion());
        }
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

