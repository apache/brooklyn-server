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
package org.apache.brooklyn.core.workflow.steps.appmodel;

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.ForeachWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UpdateChildrenWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep, WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateChildrenWorkflowStep.class);

    public static final String SHORTHAND = "[ \" of \" ${parent} ] [ \" type \" ${type} ] [ \" id \" ${identifier_expression} ] [ \" from \" ${items} ]";

    public static final ConfigKey<Object> PARENT = ConfigKeys.newConfigKey(Object.class, "parent",
            "the entity or entity ID whose children are to be updated, defaulting to the current entity; " +
                    "any children which do not match something in `items` may be removed");

    public static final ConfigKey<String> IDENTIFIER_EXRPESSION = ConfigKeys.newStringConfigKey("identifier_expression", "an expression in terms of a local variable `item` to use to identify the same child; " +
                    "e.g. if the `items` is of the form `[{ field_id: 1, name: \"Ticket 1\" },...]` then " +
                    "`identifier_expression: ticket_${item.field_id}` will create/update/delete a child whose ID is `ticket_1`; " +
                    "ignored unless `item_check_workflow` is specified");

    public static final ConfigKey<List> ITEMS = ConfigKeys.newConfigKey(List.class, "items",
                    "the list of items to be used to create/update/delete the children");

    public static final ConfigKey<CustomWorkflowStep> MATCH_CHECK_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "match_check").build();
    public static final ConfigKey<CustomWorkflowStep> CREATION_CHECK_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "creation_check").build();
    public static final ConfigKey<CustomWorkflowStep> DELETION_CHECK_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "deletion_check").build();
    public static final ConfigKey<CustomWorkflowStep> ON_CREATE_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "on_create").build();
    public static final ConfigKey<CustomWorkflowStep> ON_UPDATE_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "on_update").build();
    public static final ConfigKey<CustomWorkflowStep> ON_DELETE_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "on_delete").build();

    /*
        * `on_create`: an optionally supplied workflow to run at any newly created child, where no pre-existing child was found
          corresponding to an item in `items` and `update-children` created it from `blueprint`,
          passed the `item` (and all inputs to the `update-children` step)
          and typically doing initial setup as required on the child;
          the default behavior is to invoke an `on_create` effector at the child (if there is such an effector, otherwise do nothing), passing `item`;
          this is invoked prior to `on_update` so if there is nothing special needed for creation this can be omitted

        * `on_update`: a workflow to run on each child which has a corresponding item,
          passed the `item` (and all inputs to the `update-children` step),
          and typically updating config and sensors as appropriate on the child;
          the default behavior is to invoke an `on_update` effector at the child (if there is such an effector, otherwise do nothing), passing `item`;
          if the name or any policies may need to change on update, that should be considered in this workflow;
          if the `update-children` is performed frequently, it might be efficient in this method to check whether the `item` has changed

        * `on_delete`: a workflow to run on each child which no longer has a corresponding item prior to its being deleted;
          the default behavior is to invoke an `on_delete` effector at the child (if there is such an effector, or nothing);
          if this workflow returns `false` the framework will not delete it;
          this workflow may reparent or delete the entity, although if deletion is desired there is no need as that will be done after this workflow

        * `match_check`:

        * `creation_check`:
          this optionally supplied workflow allows filtering and custom creation;
          it will be invoked for each item in `items` for which the `match_check` returned `null` or `true`;
          the workflow is passed input variable `item` (and all inputs to the `update-children` step)
          and can return the same semantics as `match_check`, with the difference that this method can create and return a custom entity;
          if `null` or `true` is returned, the child will be created and `on_create` called;
          and if an entity is returned or created (i.e. this workflow does not return `false`) then `on_update` will be called;
          the default implementation of this workflow is to return `true`;
          this workflow may, for example, check whether an entity that exists elsewhere (e.g. something previously created then reparented away in the `deletion_check`)
          should be reparented again under the `parent` and re-connected to this synchronization process

        * `deletion_check`:
          this optionally supplied workflow allows customizing pre-deletion activities and/or the deletion itself;
          it will be invoked for each pre-existing child which was not
          returned by the `item_check_workflow`,
          with each such entity passed as an input variable `child` (along with all inputs to the `update-children` step);
          it can then return `true` or `false` to specify whether the child should be deleted
          (with `on_delete` called prior to any deletion);
          this workflow may reparent the entity and return `false` if it is intended to keep the entity but
          disconnected from this synchronization process,
          or may even `delete-entity ${child}` (although that is not usually necessary)
     */

    // see WorkflowCommonConfig.LOCK
    // TODO obtain lock on parent -> "update-children"
    protected Object lock;

    // usually a string; see utils/WorkflowConcurrency
    protected Object concurrency;

    @Override
    public Logger logger() {
        return LOG;
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        validateStepBlueprint(mgmt, workflow);
    }

    @Override
    public List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly, boolean allowInternallyEvenIfDisabled) {
        //return WorkflowReplayUtils.getSubWorkflowsForReplay(context, forced, peekingOnly, allowInternallyEvenIfDisabled);

        UpdateChildrenStepState stepState = getStepState(context);
        if (stepState.matchCheck!=null && stepState.matchCheck.workflowTag!=null) return MutableList.of(retrieveSubWorkflow(context, stepState.matchCheck.workflowTag.getWorkflowId()));
        if (stepState.creationCheck!=null && stepState.creationCheck.workflowTag!=null) return MutableList.of(retrieveSubWorkflow(context, stepState.creationCheck.workflowTag.getWorkflowId()));
        if (stepState.deletionCheck!=null && stepState.deletionCheck.workflowTag!=null) return MutableList.of(retrieveSubWorkflow(context, stepState.deletionCheck.workflowTag.getWorkflowId()));
        return Collections.emptyList();
    }

    private WorkflowExecutionContext retrieveSubWorkflow(WorkflowStepInstanceExecutionContext context, String workflowId) {
        return new WorkflowStatePersistenceViaSensors(context.getManagementContext()).getWorkflows(context.getEntity()).get(workflowId);
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        WorkflowReplayUtils.markSubWorkflowsSupersededByTask(context, Tasks.current().getId());
        return doTaskBodyPossiblyResuming(context, instructions, subworkflows.isEmpty() ? null : Iterables.getOnlyElement(subworkflows));
    }

    static class UpdateChildrenStepState {
        Entity parent;
        String identifier_expression;
        List items;

        WorkflowTagWithResult<List> matchCheck = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<List<Map>> creationCheck = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<Object> onCreation = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<Object> onUpdate = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<List> deletionCheck = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<Object> onDelete = new WorkflowTagWithResult<>();
    }

    static class WorkflowTagWithResult<T> {
        BrooklynTaskTags.WorkflowTaskTag workflowTag;
        T result;
    }

    static class TransientEntityReference {
        transient Entity entity;
        String entityId;
        public TransientEntityReference(Entity entity) {
            this.entity = entity;
            this.entityId = entity==null ? null : entity.getId();
        }
        public Entity getEntity(ManagementContext mgmt) {
            if (entityId==null) return null;
            if (entity!=null && Entities.isManagedActiveOrComingUp(entity)) return entity;
            entity = mgmt.lookup(entityId, Entity.class);
            if (entity!=null && Entities.isManagedActiveOrComingUp(entity)) return entity;
            return null;
        }
    }

    @Override
    protected UpdateChildrenStepState getStepState(WorkflowStepInstanceExecutionContext context) {
        return (UpdateChildrenStepState) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, UpdateChildrenStepState stepState) {
        context.setStepState(stepState, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        return doTaskBodyPossiblyResuming(context, null, null);
    }

    protected Object doTaskBodyPossiblyResuming(WorkflowStepInstanceExecutionContext context, ReplayContinuationInstructions instructionsForResuming, WorkflowExecutionContext subworkflowTargetForResuming) {
        ManagementContext mgmt = context.getManagementContext();
        UpdateChildrenStepState stepStateO = getStepState(context);
        UpdateChildrenStepState stepState;

        if (stepStateO==null) {
            stepState = new UpdateChildrenStepState();

            Object parentId = context.getInput(PARENT);
            stepState.parent = parentId!=null ? WorkflowStepResolution.findEntity(context, parentId).get() : context.getEntity();

            stepState.identifier_expression = TypeCoercions.coerce(context.getInputRaw(IDENTIFIER_EXRPESSION.getName()), String.class);
            stepState.items = context.getInput(ITEMS);
            if (stepState.items==null) throw new IllegalStateException("Items cannot be null");
            setStepState(context, stepState);
        } else {
            stepState = stepStateO;
        }

        Object blueprintNotYetInterpolated = resolveBlueprint(context, () -> {
            String type = context.getInput(TYPE);
            if (Strings.isBlank(type)) throw new IllegalStateException("blueprint or type must be supplied"); // should've been caught earlier but check again for good measure
            return "type: " + StringEscapes.JavaStringEscapes.wrapJavaString(type);
        }, SetVariableWorkflowStep.InterpolationMode.DISABLED, TemplateProcessor.InterpolationErrorMode.FAIL);

        BiFunction<CustomWorkflowStep, Consumer<ForeachWorkflowStep>,ConfigBag> outerWorkflowCustomers = (checkWorkflow, foreachCustom) -> {
            ForeachWorkflowStep foreach = new ForeachWorkflowStep(checkWorkflow);
            foreach.getInput().put("parent", stepState.parent);
            foreach.getInput().put("identifier_expression", stepState.identifier_expression);
            foreach.getInput().put("blueprint", blueprintNotYetInterpolated);
            // TODO other vars?
            if (foreach.getIdempotent() == null) foreach.setIdempotent(idempotent);
            if (foreach.getConcurrency() == null) foreach.setConcurrency(concurrency);
            foreachCustom.accept(foreach);

            return ConfigBag.newInstance()
                    .configure(WorkflowCommonConfig.STEPS,
                            MutableList.of(foreach))
                    .configure(WorkflowCommonConfig.IDEMPOTENT, idempotent);
        };


        List matches = runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Matching items against children", stepState.matchCheck, MATCH_CHECK_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                        "transform identifier_expression | resolve_expression | set id",
                        MutableMap.of("step", "fail message identifier_expression should be a non-static expression including an interpolated reference to item",
                            "condition", MutableMap.of("target", "${identifier_expression}", "equals", "${id}")),
                        "let child_or_id = ${parent.children[id]} ?? ${id}",
                        "transform child_tostring = ${child_or_id} | to_string",
                        "transform parent_tostring = ${parent} | to_string",
                        "return ${child_or_id}")),
                checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                    foreach -> {
                        foreach.setTarget(stepState.items);
                        foreach.setTargetVarName("item");
                    }),
                list -> (List) list.stream().map(m -> m instanceof Entity ? new TransientEntityReference((Entity)m) : m).collect(Collectors.toList()) );

        List<Map<String,Object>> stringMatchesToCreate = MutableList.of();
        for (int i=0; i<matches.size(); i++) {
            Object m = matches.get(i);
            if (m instanceof String) {
                stringMatchesToCreate.add(MutableMap.of("match", m, "item", stepState.items.get(i), "index", i));
            }
        }
        List<Map> addedChildren = runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Creating new children ("+stringMatchesToCreate.size()+")", stepState.creationCheck, CREATION_CHECK_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                                        "transform blueprint_resolved = ${blueprint} | resolve_expression",
                                        MutableMap.of(
                                                "step", "add-entity",
                                                "parent", "${parent}",
                                                "blueprint", "${blueprint_resolved}"
                                        ),
                                        "let result = ${output.entity}",
                                        MutableMap.of("step", "set-config",
                                            "config", MutableMap.of("entity", "${result}", "name", BrooklynConfigKeys.PLAN_ID.getName()),
                                            "value", "${match}"),
                                        "return ${result}")),
                        checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                                foreach -> {
                                    foreach.setTarget(stringMatchesToCreate);
                                    foreach.setTargetVarName("{match,item,index}");
                                    foreach.setWorkflowOutput(MutableMap.of("index", "${index}", "match", "${match}", "child", "${output}", "item", "${item}"));
                                }),
                        list -> (List) list.stream().map(x -> MutableMap.copyOf( ((Map)x) ).add("child", new TransientEntityReference((Entity) ((Map)x).get("child")))).collect(Collectors.toList()) );

        List<Map<String,Object>> onCreateTargets = (List) addedChildren.stream().map(x ->
                MutableMap.copyOf(x).add("child", ((TransientEntityReference) x.get("child")).getEntity(mgmt))
        ).collect(Collectors.toList());
        runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Calling onCreate on newly created children ("+stringMatchesToCreate.size()+")", stepState.onCreation, ON_CREATE_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                        MutableMap.of(
                                "step", "invoke-effector on_create",
                                "entity", "${child}",
                                "args", MutableMap.of("item", "${item}"),
                                "condition", MutableMap.of("target", "${child.effector.on_create}")
                        )) ),
                checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                        foreach -> {
                            foreach.setTarget(onCreateTargets);
                            foreach.setTargetVarName("{child,item,index}");
                        }),
                list -> list.size());

        List<Map<String,Object>> onUpdateTargets = MutableList.copyOf(onCreateTargets);
        for (int i=0; i<matches.size(); i++) {
            Object m = matches.get(i);
            if (m instanceof TransientEntityReference) {
                m = ((TransientEntityReference)m).getEntity(mgmt);
            }
            if (m instanceof Entity) {
                onUpdateTargets.add(MutableMap.of("child", m, "item", stepState.items.get(i), "index", i));
            }
        }
        runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Calling onCreate on newly created children ("+stringMatchesToCreate.size()+")", stepState.onUpdate, ON_UPDATE_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                        MutableMap.of(
                                "step", "invoke-effector on_update",
                                "entity", "${child}",
                                "args", MutableMap.of("item", "${item}"),
                                "condition", MutableMap.of("target", "${child.effector.on_update}")
                        )) ),
                checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                        foreach -> {
                            foreach.setTarget(onUpdateTargets);
                            foreach.setTargetVarName("{child,item,index}");
                        }),
                list -> list.size());

        Map<String,Entity> oldChildren = MutableMap.of();
        stepState.parent.getChildren().forEach(c -> oldChildren.put(c.getId(), c));
        onUpdateTargets.forEach(c -> oldChildren.remove( ((Entity)c.get("child")).getId()) );

        List<Entity> entitiesToPossiblyDelete = MutableList.copyOf(oldChildren.values());
        List<TransientEntityReference> deletionChecks = runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Creating new children ("+stringMatchesToCreate.size()+")", stepState.deletionCheck, DELETION_CHECK_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of("return true")),
                checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                        foreach -> {
                            foreach.setTarget(entitiesToPossiblyDelete);
                            foreach.setTargetVarName("child");
                            foreach.setWorkflowOutput(MutableMap.of("delete", "${output}", "child", "${child}"));
                        }),
                list -> (List) list.stream().map(x -> {
                            Object check = ((Map) x).get("delete");
                            Entity child = (Entity) ((Map) x).get("child");
                            check = TypeCoercions.coerce(check, Boolean.class);
                            if (check==null) throw new IllegalStateException("Invalid deletion check result for "+child+": "+check);
                            if (!Boolean.TRUE.equals(check)) return null;
                            return new TransientEntityReference(child);
                        }).filter(x -> x!=null).collect(Collectors.toList()) );

        List<Map<String,Object>> onDeleteTargets = (List) deletionChecks.stream().map(t -> t.getEntity(mgmt)).filter(x -> x!=null).map(x -> MutableMap.of("child", x)).collect(Collectors.toList());
        runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(context, instructionsForResuming, subworkflowTargetForResuming,
                "Calling onCreate on newly created children ("+stringMatchesToCreate.size()+")", stepState.onDelete, ON_DELETE_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                        MutableMap.of(
                                "step", "invoke-effector on_delete",
                                "entity", "${child}",
                                "condition", MutableMap.of("target", "${child.effector.on_delete}")
                        )) ),
                checkWorkflow -> outerWorkflowCustomers.apply(checkWorkflow,
                        foreach -> {
                            foreach.setTarget(onDeleteTargets);
                            foreach.setTargetVarName("{child}");
                        }),
                list -> list.size());

        for (TransientEntityReference entityToDelete: deletionChecks) {
            Entity entity = entityToDelete.getEntity(mgmt);
            if (entity!=null && Entities.isManagedActiveOrComingUp(entity)) Entities.unmanage(entity);
        }

        return context.getPreviousStepOutput();
    }

    protected <T> T runOrResumeSubWorkflowForPhaseOrReturnPreviousIfCompleted(WorkflowStepInstanceExecutionContext context, ReplayContinuationInstructions instructionsForResuming, WorkflowExecutionContext subworkflowTargetForResuming,
                      String name,
                      WorkflowTagWithResult<T> stepSubState, ConfigKey<CustomWorkflowStep> key,
                      Supplier<CustomWorkflowStep> defaultWorkflow, Function<CustomWorkflowStep, ConfigBag> outerWorkflowConfigFn,
                      Function<List,T> postprocess) {
        if (stepSubState.result==null) {
            ManagementContext mgmt = context.getManagementContext();
            UpdateChildrenStepState stepState = getStepState(context);

            if (stepSubState.workflowTag ==null) {
                CustomWorkflowStep checkWorkflow = context.getWorkflowExectionContext().resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING,
                        context.getInputRaw(key.getName()), TypeToken.of(CustomWorkflowStep.class));
                if (checkWorkflow == null) {
                    checkWorkflow = defaultWorkflow.get();
                }
                ConfigBag outerWorkflowConfig = outerWorkflowConfigFn.apply(checkWorkflow);

                WorkflowExecutionContext matchWorkflow = WorkflowExecutionContext.newInstanceUnpersistedWithParent(
                        context.getEntity(), context.getWorkflowExectionContext(), WorkflowExecutionContext.WorkflowContextType.NESTED_WORKFLOW,
                        name,
                        outerWorkflowConfig, null, null, null);
                stepSubState.workflowTag = BrooklynTaskTags.tagForWorkflow(matchWorkflow);
                WorkflowReplayUtils.addNewSubWorkflow(context, stepState.matchCheck.workflowTag);
                setStepState(context, stepState);

                stepSubState.result = postprocess.apply((List) DynamicTasks.queue(matchWorkflow.getTask(true).get()).getUnchecked());
            } else {
                stepSubState.result = postprocess.apply((List) WorkflowReplayUtils.replayResumingInSubWorkflow("workflow effector", context, subworkflowTargetForResuming, instructionsForResuming,
                        (w, e)-> {
                            LOG.debug("Sub workflow "+w+" is not replayable; running anew ("+ Exceptions.collapseText(e)+")");
                            return doTaskBody(context);
                        }, true));
            }
            stepSubState.workflowTag = null;
            setStepState(context, stepState);
        }
        return stepSubState.result;
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
