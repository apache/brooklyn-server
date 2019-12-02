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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.rest.api.AdjunctApi;
import org.apache.brooklyn.rest.domain.AdjunctDetail;
import org.apache.brooklyn.rest.domain.AdjunctSummary;
import org.apache.brooklyn.rest.domain.ConfigSummary;
import org.apache.brooklyn.rest.domain.Status;
import org.apache.brooklyn.rest.domain.SummaryComparators;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.AdjunctTransformer;
import org.apache.brooklyn.rest.transform.ConfigTransformer;
import org.apache.brooklyn.rest.transform.EntityTransformer;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;

@HaHotStateRequired
public class AdjunctResource extends AbstractBrooklynRestResource implements AdjunctApi {

    private static final Logger log = LoggerFactory.getLogger(AdjunctResource.class);

    private @Context UriInfo ui;

    @Override
    public List<AdjunctSummary> list(String application, String entityToken, String adjunctType) {
        final Entity entity = brooklyn().getEntity(application, entityToken);
        Iterable<? extends EntityAdjunct> source = Collections.emptyList();
        boolean all = Strings.isBlank(adjunctType);
        boolean any = false;
        if (all || adjunctType.equalsIgnoreCase(BrooklynObjectType.POLICY.name())) {
            any = true;
            source = Iterables.concat(source, entity.policies());
        }
        if (all || adjunctType.equalsIgnoreCase(BrooklynObjectType.ENRICHER.name())) {
            any = true;
            source = Iterables.concat(source, entity.enrichers());
        }
        if (all || adjunctType.equalsIgnoreCase(BrooklynObjectType.FEED.name())) {
            any = true;
            source = Iterables.concat(source, ((EntityInternal)entity).feeds());
        }
        if (!any) {
            throw WebResourceUtils.badRequest("Unknown adjunct type '%s'; use 'policy', 'enricher', or 'feed'", adjunctType);
        }
        return FluentIterable.from(source)
            .transform(new Function<EntityAdjunct, AdjunctSummary>() {
                @Override
                public AdjunctSummary apply(EntityAdjunct adjunct) {
                    return AdjunctTransformer.adjunctSummary(entity, adjunct, ui.getBaseUriBuilder(), brooklyn());
                }
            })
            .toSortedList(SummaryComparators.nameComparator());
    }

    // TODO would like to make 'config' arg optional but jersey complains if we do
    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public AdjunctDetail addAdjunct(String application, String entityToken, String adjunctTypeName, Map<String, String> config) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
        
        RegisteredType rt = brooklyn().getTypeRegistry().get(adjunctTypeName);
        AbstractBrooklynObjectSpec<?, ?> spec;
        if (rt!=null) {
            spec = brooklyn().getTypeRegistry().createSpec(rt, null, null);
        } else {
            try {
                Class<?> type = new ClassLoaderUtils(this, mgmt()).loadClass(adjunctTypeName);
                if (Policy.class.isAssignableFrom(type)) {
                    spec = PolicySpec.create((Class) type);
                } else if (Enricher.class.isAssignableFrom(type)) {
                    spec = EnricherSpec.create((Class) type);
                } else if (Feed.class.isAssignableFrom(type)) {
                    // TODO add FeedSpec ?  would be needed even if using the type registry
                    throw WebResourceUtils.badRequest("Creation of feeds from java type (%s) not supported", adjunctTypeName);
                } else {
                    throw WebResourceUtils.badRequest("Invalid type %s; not a support adjunct type", adjunctTypeName);
                }
            } catch (ClassNotFoundException e) {
                throw WebResourceUtils.badRequest("No adjunct with type %s found", adjunctTypeName);
            } catch (ClassCastException e) {
                throw WebResourceUtils.badRequest("No adjunct with type %s found", adjunctTypeName);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        spec.configure(config);

        EntityAdjunct instance;
        if (spec instanceof PolicySpec) {
            instance = entity.policies().add((PolicySpec)spec);
        } else if (spec instanceof EnricherSpec) {
            instance = entity.enrichers().add((EnricherSpec)spec);
        } else {
            // TODO add FeedSpec
            throw WebResourceUtils.badRequest("Unexpected spec type %s", spec);
        }

        log.debug("REST API added adjunct " + instance + " to " + entity);

        return AdjunctTransformer.adjunctDetail(entity, instance, ui.getBaseUriBuilder(), brooklyn());
    }

    @Override
    public AdjunctDetail get(String application, String entityToken, String adjunctId) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctId);

        return AdjunctTransformer.adjunctDetail(entity, adjunct, ui.getBaseUriBuilder(), brooklyn());
    }
    
    @Override
    public Status getStatus(String application, String entityToken, String adjunctId) {
        return AdjunctTransformer.inferStatus( brooklyn().getAdjunct(application, entityToken, adjunctId) );
    }

    @Override
    public Response start(String application, String entityToken, String adjunctId) {
        EntityAdjunct adjunct = brooklyn().getAdjunct(application, entityToken, adjunctId);
        if (adjunct instanceof Policy) {
            ((Policy)adjunct).resume();
        } else if (adjunct instanceof Feed) {
            ((Feed)adjunct).resume();
        } else {
            throw WebResourceUtils.badRequest("%s does not support start/resume", adjunct);
        }
        
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    @Override
    public Response stop(String application, String entityToken, String adjunctId) {
        EntityAdjunct adjunct = brooklyn().getAdjunct(application, entityToken, adjunctId);
        if (adjunct instanceof Policy) {
            ((Policy)adjunct).suspend();
        } else if (adjunct instanceof Feed) {
            ((Feed)adjunct).suspend();
        } else {
            throw WebResourceUtils.badRequest("%s does not support suspend", adjunct);
        }
        
        return Response.status(Response.Status.NO_CONTENT).build();
    }

    @Override
    public Response destroy(String application, String entityToken, String adjunctId) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctId);

        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.DELETE_ADJUNCT, adjunct)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to delete adjuncts '%s'",
                    Entitlements.getEntitlementContext().user(), adjunct);
        }

        if (adjunct instanceof Policy) {
            ((Policy)adjunct).suspend();
            entity.policies().remove((Policy) adjunct);
        } else if (adjunct instanceof Enricher) {
            entity.enrichers().remove((Enricher) adjunct);
        } else if (adjunct instanceof Feed) {
            ((Feed)adjunct).suspend();
            ((EntityInternal)entity).feeds().remove((Feed) adjunct);
        } else {
            // shouldn't come here
            throw WebResourceUtils.badRequest("Unexpected adjunct type %s", adjunct);
        }
        
        return Response.status(Response.Status.NO_CONTENT).build();
    }
    
    // ---- config ----
    
    @Override
    public List<ConfigSummary> listConfig(
            final String application, final String entityToken, final String adjunctToken) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctToken);

        List<ConfigSummary> result = Lists.newArrayList();
        for (ConfigKey<?> key : adjunct.config().findKeysDeclared(Predicates.alwaysTrue())) {
            result.add(ConfigTransformer.of(key).on(entity, adjunct).includeLinks(ui.getBaseUriBuilder(), false, true).transform());
        }
        return result;
    }

    // TODO support parameters  ?show=value,summary&name=xxx &format={string,json,xml}
    // (and in sensors class)
    @Override
    public Map<String, Object> batchConfigRead(String application, String entityToken, String adjunctToken) {
        return EntityTransformer.getConfigValues(brooklyn(), brooklyn().getAdjunct(application, entityToken, adjunctToken) );
    }

    @Override
    public String getConfig(String application, String entityToken, String adjunctToken, String configKeyName) {
        EntityAdjunct adjunct = brooklyn().getAdjunct(application, entityToken, adjunctToken);
        Set<ConfigKey<?>> cki = adjunct.config().findKeysDeclared(ConfigPredicates.nameSatisfies(Predicates.equalTo(configKeyName)));
        // TODO try deprecated names?
        if (cki.isEmpty()) throw WebResourceUtils.notFound("Cannot find config key '%s' in adjunct '%s' of entity '%s'", configKeyName, adjunctToken, entityToken);

        return brooklyn().getStringValueForDisplay(adjunct.config().get(cki.iterator().next()));
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public Response setConfig(String application, String entityToken, String adjunctToken, String configKeyName, Object value) {
        Entity entity = brooklyn().getEntity(application, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }

        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctToken);
        Set<ConfigKey<?>> cki = adjunct.config().findKeysDeclared(ConfigPredicates.nameSatisfies(Predicates.equalTo(configKeyName)));
        // TODO try deprecated names?
        if (cki.isEmpty()) throw WebResourceUtils.notFound("Cannot find config key '%s' in adjunct '%s' of entity '%s'", configKeyName, adjunctToken, entityToken);
        ConfigKey<?> ck = cki.iterator().next();
        
        adjunct.config().set((ConfigKey) ck, TypeCoercions.coerce(value, ck.getTypeToken()));

        return Response.status(Response.Status.OK).build();
    }

    @Override
    public List<TaskSummary> listTasks(String applicationId, String entityId, String adjunctToken, int limit, Boolean recurse) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctToken);
        return TaskTransformer.fromTasks(MutableList.copyOf(BrooklynTaskTags.getTasksInAdjunctContext(mgmt().getExecutionManager(), adjunct)),
            limit, recurse, entity, ui);
    }

    @Override
    public Response getIcon(String applicationId, String entityId, String adjunctToken) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        EntityAdjunct adjunct = brooklyn().getAdjunct(entity, adjunctToken);
        String url = RegisteredTypes.getIconUrl(adjunct);
        if (url == null)
            return Response.status(javax.ws.rs.core.Response.Status.NO_CONTENT).build();

        if (brooklyn().isUrlServerSideAndSafe(url)) {
            // classpath URL's we will serve IF they end with a recognised image format;
            // paths (ie non-protocol) and
            // NB, for security, file URL's are NOT served
            MediaType mime = WebResourceUtils.getImageMediaTypeFromExtension(Files.getFileExtension(url));
            Object content = ResourceUtils.create(entity).getResourceFromUrl(url);
            return Response.ok(content, mime).build();
        }

        // for anything else we do a redirect (e.g. http / https; perhaps ftp)
        return Response.temporaryRedirect(URI.create(url)).build();
    }
    
}
