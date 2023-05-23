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
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
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
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class UpdateChildrenWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep, WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateChildrenWorkflowStep.class);

    public static final String SHORTHAND = "[ \"of \" ${parent} \" \" ] \"type \" ${type} \" id \" ${identifier} [ \" from \" ${items} ]";

    public static final ConfigKey<Object> PARENT = ConfigKeys.newConfigKey(Object.class, "parent",
            "the entity or entity ID whose children are to be updated, defaulting to the current entity; " +
                    "any children which do not match something in `items` may be removed");

    public static final ConfigKey<String> IDENTIFIER_EXRPESSION = ConfigKeys.newStringConfigKey("identifier_expression", "an expression in terms of a local variable `item` to use to identify the same child; " +
                    "e.g. if the `items` is of the form `[{ field_id: 1, name: \"Ticket 1\" },...]` then " +
                    "`identifier_expression: ticket_${item.field_id}` will create/update/delete a child whose ID is `ticket_1`; " +
                    "ignored unless `item_check_workflow` is specified");

    public static final ConfigKey<List> ITEMS = ConfigKeys.newConfigKey(List.class, "items",
                    "the list of items to be used to create/update/delete the children");

    public static final ConfigKey<CustomWorkflowStep> MATCH_CHECK_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "match_check")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

    public static final ConfigKey<CustomWorkflowStep> CREATION_CHECK_WORKFLOW = ConfigKeys.builder(CustomWorkflowStep.class, "creation_check")
            .runtimeInheritance(ConfigInheritance.NONE)
            .build();

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
          (with `on_delete` called prior to deletion if `true` is returned);
          this workflow may reparent the entity and return `false` if it is intended to keep the entity but
          disconnected from this synchronization process,
          or may even `delete-entity ${child}` (although that is not usually necessary)

 - step: foreach item in ${items}
   reducing:
     result: {}
   steps:
   - match-check
   x
     */
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
        WorkflowTagWithResult<List> creationCheck = new WorkflowTagWithResult<>();
        WorkflowTagWithResult<List> deletionCheck = new WorkflowTagWithResult<>();
    }

    static class WorkflowTagWithResult<T> {
        BrooklynTaskTags.WorkflowTaskTag workflowTag;
        T result;
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

        List matches = runSubWorkflowForPhase(context, instructionsForResuming, subworkflowTargetForResuming,
                "Matching items against children", stepState.matchCheck, MATCH_CHECK_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                        "let id = ${${identifier}}",
                        "let child_or_id = ${parent.child[${id}]} ?? ${id}",
                        "return ${child_or_id}")),
                checkWorkflow -> ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.STEPS,
                                MutableList.of(
                                        MutableMap.of("step", "foreach item",
                                                "target", stepState.items,
                                                "steps", MutableList.of(checkWorkflow),
                                                "input", MutableMap.of(
                                                        "parent", stepState.parent,
                                                        "identifier_expression", stepState.identifier_expression
                                                        // TODO others?
                                                ),
                                                "idempotent", idempotent
                                                // TODO concurrency?
                                        )))
                        .configure(WorkflowCommonConfig.IDEMPOTENT, idempotent) );

        List<Map<String,Object>> stringMatches = MutableList.of();
        for (int i=0; i<matches.size(); i++) {
            Object m = matches.get(i);
            if (m instanceof String) {
                stringMatches.add(MutableMap.of("match", m, "item", stepState.items.get(i)));
            }
        }
        List<Map<String,Object>> entityMatches = MutableList.of();
        for (int i=0; i<matches.size(); i++) {
            Object m = matches.get(i);
            if (m instanceof Entity) {
                stringMatches.add(MutableMap.of("match", m, "item", stepState.items.get(i)));
            }
        }

        Object blueprintNotYetInterpolated = resolveBlueprint(context, () -> {
            String type = context.getInput(TYPE);
            if (Strings.isBlank(type)) throw new IllegalStateException("blueprint or type must be supplied"); // should've been caught earlier but check again for good measure
            return "type: " + StringEscapes.JavaStringEscapes.wrapJavaString(type);
        }, SetVariableWorkflowStep.InterpolationMode.DISABLED, TemplateProcessor.InterpolationErrorMode.FAIL);

        Set<Entity> addedChildren = MutableSet.copyOf(runSubWorkflowForPhase(context, instructionsForResuming, subworkflowTargetForResuming,
                "Creating new children ("+stringMatches.size()+")", stepState.creationCheck, CREATION_CHECK_WORKFLOW,
                () -> new CustomWorkflowStep(MutableList.of(
                                        MutableMap.of(
                                                "step", "add-entity",
                                                "parent", stepState.parent,
                                                "blueprint", blueprintNotYetInterpolated
                                        ),
                                        "let result = ${output.entity}",
                                        MutableMap.of("step", "set-config",
                                            "config", MutableMap.of("entity", "${result}", "name", BrooklynConfigKeys.PLAN_ID.getName()),
                                            "value", "${match}"),
                                        "return ${result}")),
                checkWorkflow -> ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.STEPS,
                                MutableList.of(
                                        MutableMap.of("step", "foreach {match,item}",
                                                "target", stringMatches,
                                                "steps", MutableList.of(checkWorkflow),
                                                "input", MutableMap.of(
                                                        "parent", stepState.parent,
                                                        "identifier_expression", stepState.identifier_expression
                                                        // TODO others?
                                                ),
                                                "idempotent", idempotent
                                                // TODO concurrency?
                                        )))
                        .configure(WorkflowCommonConfig.IDEMPOTENT, idempotent) ));

        // TODO default lock on parent -> "update-children"

        Set<Entity> updatedChildren = (Set) entityMatches.stream().map(m -> m.get("match")).collect(Collectors.toSet());

//        Object oldItem = context.getWorkflowExectionContext().getWorkflowScratchVariables().get("item");
//        for (int i = 0; i<stepState.items.size(); i++) {
//            Object item = stepState.items.get(i);
//            Object create = stepState.creationChecks.get(i);
//            if (create==null || Boolean.TRUE.equals(create) || "true".equals(create)) {
//                context.getWorkflowExectionContext().getWorkflowScratchVariables().put("item", item);
//                Object blueprint = resolveBlueprint(context);
//                try {
//                    List<? extends EntitySpec> specs = blueprint instanceof EntitySpec ? MutableList.of((EntitySpec) blueprint)
//                            : EntityManagementUtils.getAddChildrenSpecs(mgmt,
//                            blueprint instanceof String ? (String) blueprint :
//                                    BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writeValueAsString(blueprint));
//                    if (specs.size()!=1) {
//                        throw new IllegalStateException("Wrong number of specs returned: "+specs);
//                    }
//                    EntitySpec spec = Iterables.getOnlyElement(specs);
//                    spec.parent(Entities.proxy(stepState.parent));
//                    if (Strings.isBlank(stepState.identifier_expression)) throw new IllegalStateException("'identifier' expression is required");
//
//                    String identifier = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, stepState.identifier_expression, String.class);
//                    if (stepState.identifier_expression.equals(identifier)) throw new IllegalStateException("'identifier' must be an expression, e.g. '${item.id_field}' not the static value '"+stepState.identifier_expression +"'");
//                    if (Strings.isBlank(identifier)) throw new IllegalStateException("'identifier' must not resolve to an empty string");
//                    spec.configure(BrooklynConfigKeys.PLAN_ID, identifier);
//
//                    Entity child = (Entity) ((EntityInternal) context.getEntity()).getExecutionContext().get(Tasks.<Entity>builder().dynamic(false)
//                            .displayName("Creating entity " +
//                                    (Strings.isNonBlank(spec.getDisplayName()) ? spec.getDisplayName() : spec.getType().getName()))
//                            .body(() -> mgmt.getEntityManager().createEntity(spec /* TODO , idempotentIdentifier */))
//                            .build());
//
//                    addedChildren.add(child);
//
//                    // TODO on_create
//
//                } catch (JsonProcessingException e) {
//                    throw new RuntimeException(e);
//                }
//
//            } else if (Boolean.FALSE.equals(create) || "false".equals(create)) {
//                // ignore
//
//            } else if (create instanceof Entity) {
//                updatedChildren.add( (Entity)create );
//
//                // TODO on_update
//
//            } else {
//                throw new IllegalStateException("Invalid result from match/creation check for item '"+item+"': "+create);
//            }
//        }
//        context.getWorkflowExectionContext().getWorkflowScratchVariables().put("item", oldItem);

        Map<String,Entity> oldChildren = MutableMap.of();
        stepState.parent.getChildren().forEach(c -> oldChildren.put(c.getId(), c));
        addedChildren.forEach(c -> oldChildren.remove(c.getId()));
        updatedChildren.forEach(c -> oldChildren.remove(c.getId()));

        // TODO deletion_check, on_delete
        for (Entity oldChild: oldChildren.values()) {
            stepState.parent.removeChild(oldChild);
        }

        return context.getPreviousStepOutput();
    }

    protected <T> T runSubWorkflowForPhase(WorkflowStepInstanceExecutionContext context, ReplayContinuationInstructions instructionsForResuming, WorkflowExecutionContext subworkflowTargetForResuming,
                                              String name,
                                              WorkflowTagWithResult<T> stepSubState, ConfigKey<CustomWorkflowStep> key,
                                                Supplier<CustomWorkflowStep> defaultWorkflow, Function<CustomWorkflowStep, ConfigBag> outerWorkflowConfigFn) {
        if (stepSubState.result==null) {
            ManagementContext mgmt = context.getManagementContext();
            UpdateChildrenStepState stepState = getStepState(context);

            if (stepSubState.workflowTag ==null) {
                CustomWorkflowStep checkWorkflow = context.getInput(key);
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

                stepSubState.result = (T) DynamicTasks.queue(matchWorkflow.getTask(true).get()).getUnchecked();
            } else {
                stepSubState.result = (T) WorkflowReplayUtils.replayResumingInSubWorkflow("workflow effector", context, subworkflowTargetForResuming, instructionsForResuming,
                        (w, e)-> {
                            LOG.debug("Sub workflow "+w+" is not replayable; running anew ("+ Exceptions.collapseText(e)+")");
                            return doTaskBody(context);
                        }, true);
            }
            stepSubState.workflowTag = null;
            setStepState(context, stepState);
        }
        return stepSubState.result;
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
