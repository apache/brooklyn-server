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

import java.net.URI;
import java.util.List;
import java.util.Set;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriBuilder;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.typereg.RegisteredTypePredicates;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.rest.api.TypeApi;
import org.apache.brooklyn.rest.domain.TypeDetail;
import org.apache.brooklyn.rest.domain.TypeSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.TypeTransformer;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Files;

@HaHotStateRequired
@Beta
public class TypeResource extends AbstractBrooklynRestResource implements TypeApi {

    private static final Logger log = LoggerFactory.getLogger(TypeResource.class);
    private static final String LATEST = "latest";
    private static final String ALL = "all";

    private static Set<String> missingIcons = MutableSet.of();

    static boolean isLatestOnly(String versions, boolean defaultValue) {
        if (ALL.equalsIgnoreCase(versions)) return false;
        if (LATEST.equalsIgnoreCase(versions)) return true;
        if (Strings.isNonBlank(versions)) {
            log.warn("Invalid 'versions' argument '"+versions+"' when listing types; should be 'all' or 'latest'");
        }
        return defaultValue;
    }
    
    @Override
    public List<TypeSummary> list(String supertype, String versions, String regex, String fragment) {
        List<Predicate<RegisteredType>> filters = MutableList.<Predicate<RegisteredType>>of()
            .append(RegisteredTypePredicates.entitledToSee(mgmt()));
        if (Strings.isNonBlank(supertype)) {
            // rewrite certain well known ones
            // (in future this should happen automatically as Entity.class should be known as user-friendly name 'entity') 
            if ("entity".equals(supertype)) supertype = Entity.class.getName();
            else if ("enricher".equals(supertype)) supertype = Enricher.class.getName();
            else if ("policy".equals(supertype)) supertype = Policy.class.getName();
            else if ("location".equals(supertype)) supertype = Location.class.getName();
            // TODO application probably isn't at all interesting; keep it for backward compatibility,
            // and meanwhile sort out things like "template" vs "quick launch"
            // (probably adding tags on the API)
            else if ("application".equals(supertype)) supertype = Application.class.getName();
            
            filters.add(RegisteredTypePredicates.subtypeOf(supertype));
        }
        if (TypeResource.isLatestOnly(versions, true)) {
            // TODO inefficient - does n^2 comparisons where n is sufficient
            // create RegisteredTypes.filterBestVersions to do a list after the initial parse
            // (and javadoc in predicate method below)
            filters.add(RegisteredTypePredicates.isBestVersion(mgmt()));
        }
        if (Strings.isNonEmpty(regex)) {
            filters.add(RegisteredTypePredicates.nameOrAlias(StringPredicates.containsRegex(regex)));
        }
        if (Strings.isNonEmpty(fragment)) {
            filters.add(RegisteredTypePredicates.nameOrAlias(StringPredicates.containsLiteralIgnoreCase(fragment)));
        }
        Predicate<RegisteredType> filter = Predicates.and(filters);

        ImmutableList<RegisteredType> sortedItems =
            FluentIterable.from(brooklyn().getTypeRegistry().getMatching(filter))
                .toSortedList(RegisteredTypes.RegisteredTypeNameThenBestFirstComparator.INSTANCE);
        return toTypeSummary(brooklyn(), sortedItems, ui.getBaseUriBuilder());
    }

    static List<TypeSummary> toTypeSummary(BrooklynRestResourceUtils brooklyn, Iterable<RegisteredType> sortedItems, UriBuilder uriBuilder) {
        List<TypeSummary> result = MutableList.of();
        for (RegisteredType t: sortedItems) {
            result.add(TypeTransformer.summary(brooklyn, t, uriBuilder));
        }
        return result;
    }

    @Override
    public List<TypeSummary> listVersions(String nameOrAlias) {
        Predicate<RegisteredType> filter = Predicates.and(RegisteredTypePredicates.entitledToSee(mgmt()), 
            RegisteredTypePredicates.nameOrAlias(nameOrAlias));
        ImmutableList<RegisteredType> sortedItems =
            FluentIterable.from(brooklyn().getTypeRegistry().getMatching(filter))
                .toSortedList(RegisteredTypes.RegisteredTypeNameThenBestFirstComparator.INSTANCE);
        return toTypeSummary(brooklyn(), sortedItems, ui.getBaseUriBuilder());
    }

    @Override
    public TypeDetail detail(String symbolicName, String version) {
        RegisteredType item = lookup(symbolicName, version);
        return TypeTransformer.detail(brooklyn(), item, ui.getBaseUriBuilder());
    }

    protected RegisteredType lookup(String symbolicName, String version) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CATALOG_ITEM, symbolicName+":"+version)) {
            // TODO best to default to "not found" - unless maybe they have permission to "see null"
            throw WebResourceUtils.forbidden("User '%s' not permitted to see info on this type (including whether or not installed)",
                Entitlements.getEntitlementContext().user());
        }
        RegisteredType item;
        if (LATEST.equalsIgnoreCase(version)) {
            item = brooklyn().getTypeRegistry().get(symbolicName);
        } else {
            item = brooklyn().getTypeRegistry().get(symbolicName, version);
        }
        if (item==null) {
            throw WebResourceUtils.notFound("Entity with id '%s:%s' not found", symbolicName, version);
        }
        return item;
    }

    @Override
    public Response icon(String symbolicName, String version) {
        RegisteredType item = lookup(symbolicName, version);
        return produceIcon(item);
    }

    private Response produceIcon(RegisteredType result) {
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
    
}
