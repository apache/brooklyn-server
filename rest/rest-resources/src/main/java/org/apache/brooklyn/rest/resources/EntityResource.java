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

import static javax.ws.rs.core.Response.created;
import static javax.ws.rs.core.Response.status;
import static javax.ws.rs.core.Response.Status.ACCEPTED;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTags.SpecSummary;
import static org.apache.brooklyn.rest.util.WebResourceUtils.serviceAbsoluteUriBuilder;

import java.net.URI;
import java.util.*;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.Response.Status;
import javax.ws.rs.core.UriInfo;

import com.google.common.collect.*;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils.CreationResult;
import org.apache.brooklyn.core.mgmt.entitlement.EntitlementPredicates;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.rest.api.EntityApi;
import org.apache.brooklyn.rest.domain.*;
import org.apache.brooklyn.rest.filter.HaHotStateRequired;
import org.apache.brooklyn.rest.transform.EntityTransformer;
import org.apache.brooklyn.rest.transform.LocationTransformer;
import org.apache.brooklyn.rest.transform.LocationTransformer.LocationDetailLevel;
import org.apache.brooklyn.rest.transform.TaskTransformer;
import org.apache.brooklyn.rest.util.EntityRelationUtils;
import org.apache.brooklyn.rest.util.WebResourceUtils;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.ClassLoadingContext;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.io.Files;

@HaHotStateRequired
public class EntityResource extends AbstractBrooklynRestResource implements EntityApi {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(EntityResource.class);

    @Context
    private UriInfo uriInfo;
    
    @Override
    public List<EntitySummary> list(final String application) {
        return FluentIterable
                .from(brooklyn().getApplication(application).getChildren())
                .filter(EntitlementPredicates.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY))
                .transform(EntityTransformer.fromEntity(ui.getBaseUriBuilder()))
                .toList();
    }

    @Override
    public EntitySummary get(String application, String entityName) {
        Entity entity = brooklyn().getEntity(application, entityName);
        if (Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            return EntityTransformer.entitySummary(entity, ui.getBaseUriBuilder());
        }
        throw WebResourceUtils.forbidden("User '%s' is not authorized to get entity '%s'",
                Entitlements.getEntitlementContext().user(), entity);
    }

    @Override
    public List<EntitySummary> getChildren(final String application, final String entity) {
        return FluentIterable
                .from(brooklyn().getEntity(application, entity).getChildren())
                .filter(EntitlementPredicates.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY))
                .transform(EntityTransformer.fromEntity(ui.getBaseUriBuilder()))
                .toList();
    }

    @Override
    public List<RelationSummary> getRelations(final String applicationId, final String entityId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        if (entity != null) {
            return EntityRelationUtils.getRelations(entity);
        }
        return Collections.emptyList();
    }

    @Override
    public Response addChildren(String applicationToken, String entityToken, Boolean start, String timeoutS, String yaml) {
        final Entity parent = brooklyn().getEntity(applicationToken, entityToken);
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, parent)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entityToken);
        }
        CreationResult<List<Entity>, List<String>> added = EntityManagementUtils.addChildren(parent, yaml, start)
                .blockUntilComplete(timeoutS==null ? Duration.millis(20) : Duration.of(timeoutS));
        ResponseBuilder response;
        
        if (added.get().size()==1) {
            Entity child = Iterables.getOnlyElement(added.get());
            URI ref = serviceAbsoluteUriBuilder(uriInfo.getBaseUriBuilder(), EntityApi.class, "get")
                    .build(child.getApplicationId(), child.getId());
            response = created(ref);
        } else {
            response = Response.status(Status.CREATED);
        }
        return response.entity(TaskTransformer.taskSummary(added.task(), ui.getBaseUriBuilder())).build();
    }

    @Override
    public List<TaskSummary> listTasks(String applicationId, String entityId, int limit, Boolean recurse) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        return TaskTransformer.fromTasks(MutableList.copyOf(BrooklynTaskTags.getTasksInEntityContext(mgmt().getExecutionManager(), entity)),
            limit, recurse, entity, ui);
    }

    /** API does not guarantee order, but this is a the one we use (when there are lots of tasks):
     * prefer top-level tasks and to recent tasks, 
     * balanced such that the following are equal:
     * <li>something manually submitted here, submitted two hours ago
     * <li>something submitted from another entity, submitted ten minutes ago
     * <li>anything in progress, submitted one minute ago
     * <li>anything not started, submitted ten seconds ago
     * <li>anything completed, submitted one second ago
     * <p>
     * So if there was a manual "foo" effector invoked via REST on this entity a day ago,
     * a "bar" effector invoked from a parent effector would only be preferred
     * if invoked in the last two hours;
     * active subtasks of "bar" would be preferred if submitted within the last 12 minutes,
     * unstarted subtasks if submitted within 2 minutes,
     * and completed subtasks if within the last 12 seconds.
     * Thus there is a heavy bias over time to show the top-level tasks,
     * but there is also a bias for detail for very recent activity.
     * <p>
     * It's far from perfect but provides a way -- when there are lots of tasks --
     * that we can show important things, where important things are the top level
     * and very recent.
     */
    @Beta  
    public static class InterestingTasksFirstComparator implements Comparator<Task<?>> {
        Entity context;
        public InterestingTasksFirstComparator() { this(null); }
        public InterestingTasksFirstComparator(Entity entity) { this.context = entity; }
        @Override
        public int compare(Task<?> o1, Task<?> o2) {
            // absolute pref for submitted items
            if (!Objects.equal(o1.isSubmitted(), o2.isSubmitted())) {
                return o1.isSubmitted() ? -1 : 1;
            }
            // followed by absolute pref for active items
            if (!Objects.equal(o1.isDone(), o2.isDone())) {
                return !o1.isDone() ? -1 : 1;
            }

            // followed by absolute pref for things not started yet
            if (!Objects.equal(o1.isBegun(), o2.isBegun())) {
                return !o1.isBegun() ? -1 : 1;
            }

            // we no longer do this analysis, but instead we first take at least one of each task with a distinct name

//            if (!o1.isDone()) {
//                // among active items, scheduled ones
//                if (!Objects.equal(o1 instanceof ScheduledTask, o2 instanceof ScheduledTask)) {
//                    return !(o1 instanceof ScheduledTask) ? -1 : 1;
//                }
//            }

//            // if all else is equal:
//            // big pref for top-level tasks (manual operations), where submitter null, or submitted by other entities
//            int weight = 0;
//            Task<?> o1s = o1.getSubmittedByTask();
//            Task<?> o2s = o2.getSubmittedByTask();
//            if (!Objects.equal(o1s==null, o2s==null)) {
//                weight += 20 * 60 * (o1s == null ? -1 : 1);
//            }
//            // then pref for things invoked by other entities
//            if (context!=null && o1s!=null && o2s!=null) {
//                boolean o1se = context.equals(BrooklynTaskTags.getContextEntity(o1s));
//                boolean o2se = context.equals(BrooklynTaskTags.getContextEntity(o2s));
//                if (!Objects.equal(o1se, o2se)) {
//                    weight += 10 * 60 * (o2se ? -1 : 1);
//                }
//            }

            // then sort based on how recently the task changed state
            long now = System.currentTimeMillis();
            long t1 = o1.isDone() ? o1.getEndTimeUtc() : o1.isBegun() ? o1.getStartTimeUtc() : o1.getSubmitTimeUtc();
            long t2 = o2.isDone() ? o2.getEndTimeUtc() : o2.isBegun() ? o2.getStartTimeUtc() : o2.getSubmitTimeUtc();
            long u1 = now - t1;
            long u2 = now - t2;
//            // so smaller = more recent
//            // and if there is a weight, increase the other side so it is de-emphasised
//            // IE if weight was -10 that means T1 is "10 times more interesting"
//            // or more precisely, a task T1 from 10 mins ago equals a task T2 from 1 min ago
//            if (weight<0) u2 *= -weight;
//            else if (weight>0) u1 *= weight;
            if (u1!=u2) return u1 > u2 ? 1 : -1;

//            // if equal under mapping, use weight
//            if (weight!=0) return weight < 0 ? -1 : 1;

            // lastly use ID to ensure canonical order
            return o1.getId().compareTo(o2.getId());
        }
    }
    
    @Override @Deprecated
    public List<TaskSummary> listTasks(String applicationId, String entityId) {
        return listTasks(applicationId, entityId, -1, false);
    }

    @Override
    public TaskSummary getTask(final String application, final String entityToken, String taskId) {
        // TODO deprecate in favour of ActivityApi.get ?
        Entity entity =brooklyn().getApplication(application);
        if (entity != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.SEE_ENTITY, entity)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to see the task '%s' for the entity '%s'",
                    Entitlements.getEntitlementContext().user(), taskId, entity);
        }
        Task<?> t = mgmt().getExecutionManager().getTask(taskId);
        if (t == null)
            throw WebResourceUtils.notFound("Cannot find task '%s'", taskId);
        return TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(t);
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<Object> listTags(String applicationId, String entityId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        return (List<Object>) resolving(MutableList.copyOf(entity.tags().getTags())).preferJson(true).resolve();
    }

    @Override
    public void addTag(String applicationId, String entityId, Object tag) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        entity.tags().addTag(tag);
    }

    @Override
    public boolean deleteTag(String applicationId, String entityId, Object tag) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        return entity.tags().removeTag(tag);
    }

    @Override
    public void upsertTag(String applicationId, String entityId, String tagKey, Object tagValue) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        BrooklynTags.upsertSingleKeyMapValueTag(entity.tags(), tagKey, tagValue);
    }

    @Override
    public Object getTag(String applicationId, String entityId, String tagKey) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        return BrooklynTags.findSingleKeyMapValue(tagKey, Object.class, entity.tags().getTags());
    }

    @Override
    public Response getIcon(String applicationId, String entityId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        String url = RegisteredTypes.getIconUrl(entity);
        if (url == null)
            return Response.status(Status.NO_CONTENT).build();

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

    @Override
    public Response rename(String application, String entity, String newName) {
        Entity instance = brooklyn().getEntity(application, entity);
        if (instance != null && !Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.RENAME_ENTITY, instance)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to rename the entity '%s'",
                    Entitlements.getEntitlementContext().user(), entity);
        }
        instance.setDisplayName(newName);
        return status(Response.Status.OK).build();
    }

    @Override
    public Response expunge(String application, String entity, boolean release) {
        Entity instance = brooklyn().getEntity(application, entity);
        Task<?> task = brooklyn().expunge(instance, release);
        TaskSummary summary = TaskTransformer.fromTask(ui.getBaseUriBuilder()).apply(task);
        return status(ACCEPTED).entity(summary).build();
    }

    @Override
    public List<EntitySummary> getDescendants(String application, String entity, String typeRegex) {
        return EntityTransformer.entitySummaries(brooklyn().descendantsOfType(application, entity, typeRegex), ui.getBaseUriBuilder());
    }

    @Override
    public Map<String, Object> getDescendantsSensor(String application, String entity, String sensor, String typeRegex) {
        Iterable<Entity> descs = brooklyn().descendantsOfType(application, entity, typeRegex);
        return ApplicationResource.getSensorMap(sensor, descs);
    }

    @Override
    public List<LocationSummary> getLocations(String application, String entity) {
        List<LocationSummary> result = Lists.newArrayList();
        Entity e = brooklyn().getEntity(application, entity);
        for (Location l : e.getLocations()) {
            result.add(LocationTransformer.newInstance(mgmt(), l, LocationDetailLevel.NONE, ui.getBaseUriBuilder()));
        }
        return result;
    }

    @Override
    public String getSpec(String applicationToken, String entityToken) {
        Entity entity = brooklyn().getEntity(applicationToken, entityToken);
        NamedStringTag spec = BrooklynTags.findFirstNamedStringTag(BrooklynTags.YAML_SPEC_KIND, entity.tags().getTags());
        if (spec == null)
            return null;
        return (String) WebResourceUtils.getValueForDisplay(spec.getContents(), false, true);
    }

    @Override
    public List<Object>  getSpecList(String applicationId, String entityId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        List<SpecSummary> specTag = BrooklynTags.findSpecHierarchyTag(entity.tags().getTags());
        return (List<Object>) resolving(specTag).preferJson(true).resolve();
    }

    @Override
    public List<WorkflowExecutionContext> getWorkflows(String applicationId, String entityId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        return MutableList.copyOf(new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(entity).values());
    }

    @Override
    public WorkflowExecutionContext getWorkflow(String applicationId, String entityId, String workflowId) {
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        WorkflowExecutionContext w = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(entity).get(workflowId);
        if (w==null) throw WebResourceUtils.notFound("Cannot find workflow with ID '%s'", workflowId);
        return w;
    }

    @Override
    public String replayWorkflow(String applicationId, String entityId, String workflowId, String step, String reason, Boolean force) {
        WorkflowExecutionContext w = getWorkflow(applicationId, entityId, workflowId);
        Entity entity = brooklyn().getEntity(applicationId, entityId);
        boolean forced = Boolean.TRUE.equals(force);
        String msg = "Replaying workflow "+workflowId+" on "+entity+", from step '"+step+"', reason '"+reason+"'";
        if (forced) log.warn(msg); else log.debug(msg);
        if (reason==null) reason = "manually requested";
        Task<Object> t = null;
        if ("end".equalsIgnoreCase(step)) t = w.createTaskReplaying(w.makeInstructionsForReplayingLast(reason, forced));
        else if ("start".equalsIgnoreCase(step)) w.createTaskReplaying(w.makeInstructionsForReplayingFromStart(reason, forced));
        else {
            Maybe<Integer> stepNumberRequested = TypeCoercions.tryCoerce(step, Integer.class);
            if (stepNumberRequested.isPresent()) t = w.createTaskReplaying(w.makeInstructionsForReplayingFromStep(stepNumberRequested.get(), reason, forced));
            else {
                // could support resuming from a step ID, but not so important; UI can find that
                throw new IllegalStateException("Unsupported to resume from '"+step+"'");
            }
        }
        DynamicTasks.submit(t, entity);
        return (String) WebResourceUtils.getValueForDisplay(mapper(), t.getId(), true, true);
    }

    @Override
    public Response runWorkflow(String applicationToken, String entityToken, String timeoutS, String yaml) {
        final Entity target = brooklyn().getEntity(applicationToken, entityToken);
        // TODO new entitlement, here and above
        if (!Entitlements.isEntitled(mgmt().getEntitlementManager(), Entitlements.MODIFY_ENTITY, target)) {
            throw WebResourceUtils.forbidden("User '%s' is not authorized to modify entity '%s'",
                    Entitlements.getEntitlementContext().user(), entityToken);
        }
        CustomWorkflowStep workflow;
        try {
            workflow = BeanWithTypeUtils.newYamlMapper(mgmt(), true, RegisteredTypes.getClassLoadingContext(target), true)
                    .readerFor(CustomWorkflowStep.class).readValue(yaml);
        } catch (JsonProcessingException e) {
            return ApiError.of(e).asBadRequestResponseJson();
        }

        WorkflowExecutionContext execution = workflow.newWorkflowExecution(target,
                Strings.firstNonBlank(workflow.getName(), workflow.getId(), "API workflow invocation"),
                null,
                MutableMap.of("tags", MutableMap.of("workflow_yaml", yaml)));

        Task<Object> task = Entities.submit(target, execution.getTask(true).get());
        task.blockUntilEnded(timeoutS==null ? Duration.millis(20) : Duration.of(timeoutS));

        URI ref = serviceAbsoluteUriBuilder(uriInfo.getBaseUriBuilder(), EntityApi.class, "getWorkflow")
                .build(target.getApplicationId(), target.getId(), execution.getWorkflowId());
        ResponseBuilder response = created(ref);
        return response.entity(TaskTransformer.taskSummary(task, ui.getBaseUriBuilder())).build();
    }
}
