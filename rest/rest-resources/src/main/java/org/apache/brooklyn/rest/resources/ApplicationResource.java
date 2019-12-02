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

import static com.google.common.base.Preconditions.checkNotNull;
import static javax.ws.rs.core.Response.created;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.Status.ACCEPTED;
import static org.apache.brooklyn.rest.util.WebResourceUtils.serviceAbsoluteUriBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.ConstraintViolationException;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils.CreationResult;
import org.apache.brooklyn.core.mgmt.entitlement.EntitlementPredicates;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.EntityAndItem;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.StringAndArgument;
import org.apache.brooklyn.core.mgmt.internal.IdAlreadyExistsException;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.entity.group.AbstractGroup;
import org.apache.brooklyn.rest.api.ApplicationApi;
import org.apache.brooklyn.rest.domain.ApplicationSpec;
import org.apache.brooklyn.rest.domain.ApplicationSummary;
import org.apache.brooklyn.rest.domain.EntityDetail;
import org.apache.brooklyn.rest.domain.EntitySummary;
import org.apache.brooklyn.rest.domain.TaskSummary;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.ApplicationTransformer;
import org.apache.brooklyn.rest.transform.EntityTransformer;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.rest.util.BrooklynRestResourceUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

@HaHotStateRequired
public class ApplicationResource extends AbstractBrooklynRestResource implements ApplicationApi {

    private static final Logger log = LoggerFactory.getLogger(ApplicationResource.class);

    @Context
    private UriInfo uriInfo;

    /** depth 0 means no detail even at root; negative means infinite; positive means include details for that many levels 
     * (ie 1 means this entity but no details of descendants) */
    private EntitySummary fromEntity(Entity entity, boolean includeTags, int detailDepth, List<String> extraSensorGlobs, List<String> extraConfigGlobs) {
        if (detailDepth==0) {
            return new EntitySummary(
                entity.getId(), 
                entity.getDisplayName(),
                entity.getEntityType().getName(),
                entity.getCatalogItemId(),
                MutableMap.of("self", EntityTransformer.entityUri(entity, ui.getBaseUriBuilder())) );
        }

        Boolean serviceUp = entity.getAttribute(Attributes.SERVICE_UP);

        Lifecycle serviceState = entity.getAttribute(Attributes.SERVICE_STATE_ACTUAL);

        String iconUrl = RegisteredTypes.getIconUrl(entity);
        if (iconUrl!=null) {
            if (brooklyn().isUrlServerSideAndSafe(iconUrl))
                // route to server if it is a server-side url
                iconUrl = EntityTransformer.entityUri(entity, ui.getBaseUriBuilder())+"/icon";
        }

        List<EntitySummary> children = Lists.newArrayList();
        if (!entity.getChildren().isEmpty()) {
            for (Entity child : entity.getChildren()) {
                if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, child)) {
                    children.add(fromEntity(child, includeTags, detailDepth-1, extraSensorGlobs, extraConfigGlobs));
                }
            }
        }

        String parentId = null;
        if (entity.getParent()!= null) {
            parentId = entity.getParent().getId();
        }

        List<String> groupIds = Lists.newArrayList();
        if (!entity.groups().isEmpty()) {
            groupIds.addAll(entitiesIdAsArray(entity.groups()));
        }

        List<Map<String, String>> members = Lists.newArrayList();
        if (entity instanceof Group) {
            // use attribute instead of method in case it is read-only
            Collection<Entity> memberEntities = entity.getAttribute(AbstractGroup.GROUP_MEMBERS);
            if (memberEntities != null && !memberEntities.isEmpty())
                members.addAll(entitiesIdAndNameAsList(memberEntities));
        }

        EntityDetail result = new EntityDetail(
                entity.getApplicationId(),
                entity.getId(),
                parentId,
                entity.getDisplayName(),
                entity.getEntityType().getName(),
                serviceUp,
                serviceState,
                iconUrl,
                entity.getCatalogItemId(),
                children,
                groupIds,
                members,
                MutableMap.of("self", EntityTransformer.entityUri(entity, ui.getBaseUriBuilder())) );
        
        if (includeTags) {
            result.setExtraField("tags", resolving(MutableList.copyOf(entity.tags().getTags())).preferJson(true).resolve() );
        }
        
        addSensorsByGlobs(result, entity, extraSensorGlobs);
        addConfigByGlobs(result, entity, extraConfigGlobs);
        
        return result;
    }

    private List<Map<String, String>> entitiesIdAndNameAsList(Collection<? extends Entity> entities) {
        List<Map<String, String>> members = Lists.newArrayList();
        for (Entity entity : entities) {
            if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
                members.add(ImmutableMap.of("id", entity.getId(), "name", entity.getDisplayName()));
            }
        }
        return members;
    }

    private List<String> entitiesIdAsArray(Iterable<? extends Entity> entities) {
        List<String> ids = Lists.newArrayList();
        for (Entity entity : entities) {
            if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
                ids.add(entity.getId());
            }
        }
        return ids;
    }

    @Override
    public List<EntityDetail> fetch(String entityIds, String extraSensorsS) {
        List<String> extraSensorNames = JavaStringEscapes.unwrapOptionallyQuotedJavaStringList(extraSensorsS);
        List<AttributeSensor<?>> extraSensors = extraSensorNames.stream().map((s) -> Sensors.newSensor(Object.class, s)).collect(Collectors.toList());
        
        List<EntityDetail> entitySummaries = Lists.newArrayList();
        for (Entity application : mgmt().getApplications()) {
            entitySummaries.add(addSensorsByName((EntityDetail)fromEntity(application, false, -1, null, null), application, extraSensors));
        }

        if (Strings.isNonBlank(entityIds)) {
            List<String> extraEntities = JavaStringEscapes.unwrapOptionallyQuotedJavaStringList(entityIds);
            for (String entityId: extraEntities) {
                Entity entity = mgmt().getEntityManager().getEntity(entityId.trim());
                while (entity != null && entity.getParent() != null) {
                    if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
                        entitySummaries.add(addSensorsByName((EntityDetail)fromEntity(entity, false, -1, null, null), entity, extraSensors));
                    }
                    entity = entity.getParent();
                }
            }
        }
        return entitySummaries;
    }
    
    @Override
    public List<EntitySummary> details(String entityIds, boolean includeAllApps, String extraSensorsGlobsS, String extraConfigGlobsS, int depth) {
        List<String> extraSensorGlobs = JavaStringEscapes.unwrapOptionallyQuotedJavaStringList(extraSensorsGlobsS);
        List<String> extraConfigGlobs = JavaStringEscapes.unwrapOptionallyQuotedJavaStringList(extraConfigGlobsS);

        Map<String, EntitySummary> entitySummaries = MutableMap.of();

        if (includeAllApps) {
            for (Entity application : mgmt().getApplications()) {
                if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, application)) {
                    entitySummaries.put(application.getId(), fromEntity(application, true, depth, extraSensorGlobs, extraConfigGlobs));
                }
            }
        }

        if (Strings.isNonBlank(entityIds)) {
            List<String> extraEntities = JavaStringEscapes.unwrapOptionallyQuotedJavaStringList(entityIds);
            for (String entityId: extraEntities) {
                Entity entity = mgmt().getEntityManager().getEntity(entityId.trim());
                while (entity != null && !entitySummaries.containsKey(entity.getId())) {
                    if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
                        entitySummaries.put(entity.getId(), fromEntity(entity, true, depth, extraSensorGlobs, extraConfigGlobs));
                    }
                    entity = entity.getParent();
                }
            }
        }
        return MutableList.copyOf(entitySummaries.values());
    }

    private EntityDetail addSensorsByName(EntityDetail result, Entity entity, List<AttributeSensor<?>> extraSensors) {
        if (extraSensors!=null && !extraSensors.isEmpty()) {
            Object sensorsO = result.getExtraFields().get("sensors");
            if (sensorsO==null) {
                sensorsO = MutableMap.of();
                result.setExtraField("sensors", sensorsO);
            }
            if (!(sensorsO instanceof Map)) {
                throw new IllegalStateException("sensors field in result for "+entity+" should be a Map; found: "+sensorsO);
            }
            @SuppressWarnings("unchecked")
            Map<String,Object> sensors = (Map<String, Object>) sensorsO;
            
            for (AttributeSensor<?> s: extraSensors) {
                if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_SENSOR, new EntityAndItem<String>(entity, s.getName()))) {
                    Object sv = entity.sensors().get(s);
                    if (sv!=null) {
                        sv = resolving(sv).preferJson(true).asJerseyOutermostReturnValue(false).raw(true).context(entity).immediately(true).timeout(Duration.ZERO).resolve();
                        sensors.put(s.getName(), sv);
                    }
                }
            }
        }
        return result;
    }
    
    private EntitySummary addSensorsByGlobs(EntitySummary result, Entity entity, List<String> extraSensorGlobs) {
        if (extraSensorGlobs!=null && !extraSensorGlobs.isEmpty()) {
            Object sensorsO = result.getExtraFields().get("sensors");
            if (sensorsO==null) {
                sensorsO = MutableMap.of();
                result.setExtraField("sensors", sensorsO);
            }
            if (!(sensorsO instanceof Map)) {
                throw new IllegalStateException("sensors field in result for "+entity+" should be a Map; found: "+sensorsO);
            }
            @SuppressWarnings("unchecked")
            Map<String,Object> sensors = (Map<String, Object>) sensorsO;
            
            Map<AttributeSensor<?>, Object> svs = entity.sensors().getAll();
            svs.entrySet().stream().forEach(kv -> {
                Object sv = kv.getValue();
                if (sv!=null) {
                    String name = kv.getKey().getName();
                    if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_SENSOR, new EntityAndItem<String>(entity, name))) {
                        if (extraSensorGlobs.stream().anyMatch(sn -> WildcardGlobs.isGlobMatched(sn, name))) {
                            sv = resolving(sv).preferJson(true).asJerseyOutermostReturnValue(false).raw(true).context(entity).immediately(true).timeout(Duration.ZERO).resolve();
                            sensors.put(name, sv);
                        }
                    }
                }
            });
        }
        return result;
    }

    private EntitySummary addConfigByGlobs(EntitySummary result, Entity entity, List<String> extraConfigGlobs) {
        if (extraConfigGlobs!=null && !extraConfigGlobs.isEmpty()) {
            Object configO = result.getExtraFields().get("config");
            if (configO==null) {
                configO = MutableMap.of();
                result.setExtraField("config", configO);
            }
            if (!(configO instanceof Map)) {
                throw new IllegalStateException("config field in result for "+entity+" should be a Map; found: "+configO);
            }
            @SuppressWarnings("unchecked")
            Map<String,Object> configs = (Map<String, Object>) configO;
            
            List<Predicate<ConfigKey<?>>> extraConfigPreds = MutableList.of();
            extraConfigGlobs.stream().forEach(g -> { extraConfigPreds.add( ConfigPredicates.nameMatchesGlob(g) ); });
            entity.config().findKeysDeclared(Predicates.or(extraConfigPreds)).forEach(key -> {
                if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_CONFIG, new EntityAndItem<String>(entity, key.getName()))) {
                    Maybe<Object> vRaw = ((EntityInternal)entity).config().getRaw(key);
                    Object v = vRaw.isPresent() ? vRaw.get() : entity.config().get(key);
                    if (v!=null) {
                        v = resolving(v, mgmt()).preferJson(true).asJerseyOutermostReturnValue(false).raw(true).context(entity).immediately(true).timeout(Duration.ZERO).resolve();
                        configs.put(key.getName(), v);
                    }
                }
            });
        }
        return result;
    }

    @Override
    public List<ApplicationSummary> list(String typeRegex) {
        if (Strings.isBlank(typeRegex)) {
            typeRegex = ".*";
        }
        return FluentIterable
                .from(mgmt().getApplications())
                .filter(EntitlementPredicates.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY))
                .filter(EntityPredicates.hasInterfaceMatching(typeRegex))
                .transform(ApplicationTransformer.fromApplication(ui.getBaseUriBuilder()))
                .toList();
    }

    @Override
    public ApplicationSummary get(String application) {
        return ApplicationTransformer.summaryFromApplication(brooklyn().getApplication(application), ui.getBaseUriBuilder());
    }

    /** @deprecated since 0.7.0 see #create */ @Deprecated
    protected Response createFromAppSpec(ApplicationSpec applicationSpec) {
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.DEPLOY_APPLICATION, applicationSpec)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to start application %s",
                Entitlements.getEntitlementContext().user(), applicationSpec);
        }

        checkApplicationTypesAreValid(applicationSpec);
        checkLocationsAreValid(applicationSpec);
        List<Location> locations = brooklyn().getLocationsManaged(applicationSpec);
        Application app = brooklyn().create(applicationSpec);
        Task<?> t = brooklyn().start(app, locations);
        waitForStart(app, Duration.millis(100));
        TaskSummary ts = TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(t);
        URI ref = serviceAbsoluteUriBuilder(uriInfo.getBaseUriBuilder(), ApplicationApi.class, "get")
                .build(app.getApplicationId());
        return created(ref).entity(ts).build();
    }

    @Override
    public Response createFromYaml(String yaml) {
        return createFromYaml(yaml, Optional.absent());
    }
    
    @Override
    public Response createFromYamlWithAppId(String yaml, String appId) {
        return createFromYaml(yaml, Optional.of(appId));
    }
    
    protected Response createFromYaml(String yaml, Optional<String> appId) {
        // First of all, see if it's a URL
        Preconditions.checkNotNull(yaml, "Blueprint must not be null");
        URI uri = null;
        try {
            String yamlUrl = yaml.trim();
            if (Urls.isUrlWithProtocol(yamlUrl)) {
                uri = new URI(yamlUrl);
            }
        } catch (URISyntaxException e) {
            // It's not a URI then...
            uri = null;
        }
        if (uri != null) {
            log.debug("Create app called with URI; retrieving contents: {}", uri);
            try {
                yaml = ResourceUtils.create(mgmt()).getResourceAsString(uri.toString());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new UserFacingException("Cannot resolve URL: "+uri, e);
            }
        }

        log.debug("Creating app from yaml:\n{}", yaml);

        EntitySpec<? extends Application> spec;
        try {
            spec = createEntitySpecForApplication(yaml);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Failed REST deployment, could not create spec: "+e);
            UserFacingException userFacing = Exceptions.getFirstThrowableOfType(e, UserFacingException.class);
            if (userFacing!=null) {
                log.debug("Throwing "+userFacing+", wrapped in "+e);
                throw userFacing;
            }
            throw WebResourceUtils.badRequest(e, "Error in blueprint");
        }
        
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.DEPLOY_APPLICATION, spec)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to start application %s",
                Entitlements.getEntitlementContext().user(), yaml);
        }

        try {
            return launch(yaml, spec, appId);
        } catch (IdAlreadyExistsException e) {
            log.warn("Failed REST deployment launching "+spec+": "+e);
            throw WebResourceUtils.throwWebApplicationException(Response.Status.CONFLICT, e, "Error launching blueprint, id already exists");
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Failed REST deployment launching "+spec+": "+e);
            throw WebResourceUtils.badRequest(e, "Error launching blueprint");
        }
    }

    private Response launch(String yaml, EntitySpec<? extends Application> spec, Optional<String> entityId) {
        try {
            Application app = EntityManagementUtils.createUnstarted(mgmt(), spec, entityId);

            boolean isEntitled = Entitlements.isEntitled(
                    mgmt().getEntitlementManager(),
                    Entitlements.INVOKE_EFFECTOR,
                    EntityAndItem.of(app, StringAndArgument.of(Startable.START.getName(), null)));

            if (!isEntitled) {
                throw WebResourceUtils.forbidden("User '%s' is not authorized to start application %s",
                    Entitlements.getEntitlementContext().user(), spec.getType());
            }

            CreationResult<Application,Void> result = EntityManagementUtils.start(app);
            waitForStart(app, Duration.millis(100));

            log.info("Launched from YAML: " + yaml + " -> " + app + " (" + result.task() + ")");

            URI ref = serviceAbsoluteUriBuilder(ui.getBaseUriBuilder(), ApplicationApi.class, "get").build(app.getApplicationId());
            ResponseBuilder response = created(ref);
            if (result.task() != null)
                response.entity(TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(result.task()));
            return response.build();
        } catch (ConstraintViolationException e) {
            throw new UserFacingException(e);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    private void waitForStart(final Application app, Duration timeout) {
        // without this UI shows "stopped" sometimes esp if brooklyn has just started,
        // because app goes to stopped state if it sees unstarted children,
        // and that gets returned too quickly, before the start has taken effect
        Repeater.create("wait a bit for app start").every(Duration.millis(10)).limitTimeTo(timeout).until(
            new Callable<Boolean>() {
                @Override
                public Boolean call() throws Exception {
                    Lifecycle state = app.getAttribute(Attributes.SERVICE_STATE_ACTUAL);
                    if (state==Lifecycle.CREATED || state==Lifecycle.STOPPED) return false;
                    return true;
                }
            }).run();
    }

    @Override
    public Response createPoly(byte[] inputToAutodetectType) {
        log.debug("Creating app from autodetecting input");

        boolean looksLikeLegacy = false;
        Exception legacyFormatException = null;
        // attempt legacy format
        try {
            ApplicationSpec appSpec = mapper().readValue(inputToAutodetectType, ApplicationSpec.class);
            if (appSpec.getType() != null || appSpec.getEntities() != null) {
                looksLikeLegacy = true;
            }
            return createFromAppSpec(appSpec);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            legacyFormatException = e;
            log.debug("Input is not legacy ApplicationSpec JSON (will try others): "+e, e);
        }

        //TODO infer encoding from request
        String potentialYaml = new String(inputToAutodetectType);
        EntitySpec<? extends Application> spec;
        try {
            spec = createEntitySpecForApplication(potentialYaml);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Failed REST deployment, could not create spec (autodetecting): "+e);
            
            // TODO if not yaml/json - try ZIP, etc
            
            throw WebResourceUtils.badRequest(e, "Error in blueprint");
        }


        if (spec != null) {
            try {
                return launch(potentialYaml, spec, Optional.absent());
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.warn("Failed REST deployment launching "+spec+": "+e);
                throw WebResourceUtils.badRequest(e, "Error launching blueprint (autodetecting)");
            }
        } else if (looksLikeLegacy) {
            throw Exceptions.propagate(legacyFormatException);
        } else {
            return Response.serverError().entity("Unsupported format; not able to autodetect.").build();
        }
    }

    @Override
    public Response createFromForm(String contents) {
        log.debug("Creating app from form");
        return createPoly(contents.getBytes());
    }

    @Override
    public Response delete(String application) {
        Application app = brooklyn().getApplication(application);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.INVOKE_EFFECTOR, Entitlements.EntityAndItem.of(app,
            StringAndArgument.of(Entitlements.LifecycleEffectors.DELETE, null)))) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to delete application %s",
                Entitlements.getEntitlementContext().user(), app);
        }
        Task<?> t = brooklyn().destroy(app);
        TaskSummary ts = TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(t);
        return status(ACCEPTED).entity(ts).build();
    }

    private EntitySpec<? extends Application> createEntitySpecForApplication(String potentialYaml) {
        return EntityManagementUtils.createEntitySpecForApplication(mgmt(), potentialYaml);
    }
    
    private void checkApplicationTypesAreValid(ApplicationSpec applicationSpec) {
        String appType = applicationSpec.getType();
        if (appType != null) {
            checkEntityTypeIsValid(appType);

            if (applicationSpec.getEntities() != null) {
                throw WebResourceUtils.preconditionFailed("Application given explicit type '%s' must not define entities", appType);
            }
            return;
        }

        for (org.apache.brooklyn.rest.domain.EntitySpec entitySpec : applicationSpec.getEntities()) {
            String entityType = entitySpec.getType();
            checkEntityTypeIsValid(checkNotNull(entityType, "entityType"));
        }
    }

    private void checkSpecTypeIsValid(String type, Class<? extends BrooklynObject> subType) {
        Maybe<RegisteredType> typeV = RegisteredTypes.tryValidate(mgmt().getTypeRegistry().get(type), RegisteredTypeLoadingContexts.spec(subType));
        if (!typeV.isNull()) {
            // found, throw if any problem
            typeV.get();
            return;
        }

        // not found, try classloading
        try {
            brooklyn().getCatalogClassLoader().loadClass(type);
        } catch (ClassNotFoundException e) {
            log.debug("Class not found for type '" + type + "'; reporting 404", e);
            throw WebResourceUtils.notFound("Undefined type '%s'", type);
        }
        log.info(JavaClassNames.simpleClassName(subType)+" type '{}' not defined in catalog but is on classpath; continuing", type);
    }

    private void checkEntityTypeIsValid(String type) {
        checkSpecTypeIsValid(type, Entity.class);
    }

    @SuppressWarnings("deprecation")
    private void checkLocationsAreValid(ApplicationSpec applicationSpec) {
        for (String locationId : applicationSpec.getLocations()) {
            locationId = BrooklynRestResourceUtils.fixLocation(locationId);
            if (brooklyn().getLocationRegistry().getLocationSpec(locationId).isAbsent() && brooklyn().getLocationRegistry().getDefinedLocationById(locationId)==null) {
                throw WebResourceUtils.notFound("Undefined location '%s'", locationId);
            }
        }
    }

    @Override
    public List<EntitySummary> getDescendants(String application, String typeRegex) {
        Entity entity =brooklyn().getApplication(application);
        if (entity != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see the descendants of entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
        return EntityTransformer.entitySummaries(brooklyn().descendantsOfType(application, application, typeRegex), ui.getBaseUriBuilder());
    }

    @Override
    public Map<String, Object> getDescendantsSensor(String application, String sensor, String typeRegex) {
        Entity entity =brooklyn().getApplication(application);
        if (entity != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see the value of sensor '%s' for descendants of entity '%s'",
                    Entitlements.getEntitlementContext().user(), sensor, entity);
        }
        Iterable<Entity> descs = brooklyn().descendantsOfType(application, application, typeRegex);
        return getSensorMap(sensor, descs);
    }

    public static Map<String, Object> getSensorMap(String sensor, Iterable<Entity> descs) {
        if (Iterables.isEmpty(descs))
            return Collections.emptyMap();
        Map<String, Object> result = MutableMap.of();
        Iterator<Entity> di = descs.iterator();
        Sensor<?> s = null;
        while (di.hasNext()) {
            Entity potentialSource = di.next();
            s = potentialSource.getEntityType().getSensor(sensor);
            if (s!=null) break;
        }
        if (s==null)
            s = Sensors.newSensor(Object.class, sensor);
        if (!(s instanceof AttributeSensor<?>)) {
            log.warn("Cannot retrieve non-attribute sensor "+s+" for entities; returning empty map");
            return result;
        }
        for (Entity e: descs) {
            Object v = null;
            try {
                v = e.getAttribute((AttributeSensor<?>)s);
            } catch (Exception exc) {
                Exceptions.propagateIfFatal(exc);
                log.warn("Error retrieving sensor "+s+" for "+e+" (ignoring): "+exc);
            }
            if (v!=null)
                result.put(e.getId(), v);
        }
        return result;
    }

}
