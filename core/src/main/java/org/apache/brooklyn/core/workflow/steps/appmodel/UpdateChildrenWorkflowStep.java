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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.mgmt.internal.EntityManagerInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class UpdateChildrenWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep {

    private static final Logger LOG = LoggerFactory.getLogger(UpdateChildrenWorkflowStep.class);

    public static final String SHORTHAND = "[ \"of \" ${parent} \" \" ] \"type \" ${type} \" id \" ${identifier} [ \" from \" ${items} ]";

    public static final ConfigKey<Object> PARENT = ConfigKeys.newConfigKey(Object.class, "parent",
            "the entity or entity ID whose children are to be updated, defaulting to the current entity; " +
                    "any children which do not match something in `items` may be removed");

    public static final ConfigKey<String> IDENTIFIER = ConfigKeys.newStringConfigKey("identifier", "an expression in terms of a local variable `item` to use to identify the same child; " +
                    "e.g. if the `items` is of the form `[{ field_id: ticket1, name: \"Ticket 1\" },...]` then " +
                    "and `identifier: ${item.field_id}` will create/update/delete a child whose ID is `ticket1`; " +
                    "ignored unless `item_check_workflow` is specified");

    public static final ConfigKey<List> ITEMS = ConfigKeys.newConfigKey(List.class, "items",
                    "the list of items to be used to create/update/delete the children");

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
          this optionally supplied workflow allows the matching process to be customized;
          it will be invoked for each item in `items` to find a matching child/entity if one is already present;
          the workflow is passed input variable `item` (and all inputs to the `update-children` step)
          and should return the existing child that corresponds to it,
          or `true` or `null` if there is none and the item should be passed to the `creation_check`,
          or `false` if the `item` should be ignored (no child created or updated, identical to the `creation_check` but in some cases it may be easier to test in this workflow);
          the default implementation of this workflow is to compute and return `${parent.child[${${identifier}}]} ?? true`

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

    static class UpdateChildrenStepState {
        Entity parent;
        String identifier;
        List items;

        WorkflowExecutionContext matchWorkflow;
        List matchChecks;
        List creationChecks;
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
        ManagementContext mgmt = context.getManagementContext();
        UpdateChildrenStepState stepState = getStepState(context);

        if (stepState==null) {
            stepState = new UpdateChildrenStepState();

            Object parentId = context.getInput(PARENT);
            stepState.parent = parentId!=null ? WorkflowStepResolution.findEntity(context, parentId).get() : context.getEntity();

            stepState.identifier = TypeCoercions.coerce(context.getInputRaw(IDENTIFIER.getName()), String.class);
            stepState.items = context.getInput(ITEMS);
            if (stepState.items==null) throw new IllegalStateException("Items cannot be null");
            setStepState(context, stepState);
        }

        if (stepState.matchChecks==null) {
            if (stepState.matchWorkflow==null) {
                CustomWorkflowStep matchCheck = null;  // TODO getInput(MATCH_CHECL);
                if (matchCheck == null) {
                    try {
                        matchCheck =
                                BeanWithTypeUtils.convert(mgmt,
                                    MutableList.of("let match_id_or_create = ${parent.child[${${identifier}}]} ?? true",
                                        "return ${match_id_or_create}"),
                                    TypeToken.of(CustomWorkflowStep.class), true, null, true);
                    } catch (JsonProcessingException e) {
                        throw Exceptions.propagate(e);
                    }
                }
                ConfigBag matchConfig = ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.STEPS,
                                MutableList.of(
                                        MutableMap.of("step", "foreach item",
                                                "target", stepState.items,
                                                "steps", MutableList.of(matchCheck),
                                            "input", MutableMap.of(
                                                    "parent", stepState.parent,
                                                    "identifier", stepState.identifier
                                                    // TODO others?
                                            ),
                                            "idempotent", idempotent
                                            // TODO concurrency?
                                )))
                        .configure(WorkflowCommonConfig.IDEMPOTENT, idempotent);

                stepState.matchWorkflow = WorkflowExecutionContext.newInstanceUnpersistedWithParent(
                        context.getEntity(), context.getWorkflowExectionContext(), WorkflowExecutionContext.WorkflowContextType.NESTED_WORKFLOW,
                        "Matching items against children",
                        matchConfig, null, null, null);
                setStepState(context, stepState);
                stepState.matchChecks = (List) DynamicTasks.queue(stepState.matchWorkflow.getTask(true).get()).getUnchecked();
            } else {
                // TODO replay matchWorkflow
                throw new IllegalStateException("TODO replay matching");
//                stepState.matchChecks = ...
            }
            setStepState(context, stepState);
        }

        if (stepState.creationChecks==null) {
            // TODO creation check
            stepState.creationChecks = stepState.matchChecks;
            setStepState(context, stepState);
        }

        // TODO default lock on parent -> "update-children"

        Set<Entity> addedChildren = MutableSet.of();
        Set<Entity> updatedChildren = MutableSet.of();

        Object oldItem = context.getWorkflowExectionContext().getWorkflowScratchVariables().get("item");
        for (int i = 0; i<stepState.items.size(); i++) {
            Object item = stepState.items.get(i);
            Object create = stepState.creationChecks.get(i);
            if (create==null || Boolean.TRUE.equals(create) || "true".equals(create)) {
                context.getWorkflowExectionContext().getWorkflowScratchVariables().put("item", item);
                Object blueprint = resolveBlueprint(context);
                try {
                    List<? extends EntitySpec> specs = blueprint instanceof EntitySpec ? MutableList.of((EntitySpec) blueprint)
                            : EntityManagementUtils.getAddChildrenSpecs(mgmt,
                            blueprint instanceof String ? (String) blueprint :
                                    BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writeValueAsString(blueprint));
                    if (specs.size()!=1) {
                        throw new IllegalStateException("Wrong number of specs returned: "+specs);
                    }
                    EntitySpec spec = Iterables.getOnlyElement(specs);
                    spec.parent(Entities.proxy(stepState.parent));
                    if (Strings.isBlank(stepState.identifier)) throw new IllegalStateException("'identifier' expression is required");

                    String identifier = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, stepState.identifier, String.class);
                    if (stepState.identifier.equals(identifier)) throw new IllegalStateException("'identifier' must be an expression, e.g. '${item.id_field}' not the static value '"+stepState.identifier+"'");
                    if (Strings.isBlank(identifier)) throw new IllegalStateException("'identifier' must not resolve to an empty string");
                    spec.configure(BrooklynConfigKeys.PLAN_ID, identifier);

                    Entity child = (Entity) ((EntityInternal) context.getEntity()).getExecutionContext().get(Tasks.<Entity>builder().dynamic(false)
                            .displayName("Creating entity " +
                                    (Strings.isNonBlank(spec.getDisplayName()) ? spec.getDisplayName() : spec.getType().getName()))
                            .body(() -> mgmt.getEntityManager().createEntity(spec /* TODO , idempotentIdentifier */))
                            .build());

                    addedChildren.add(child);

                    // TODO on_create

                } catch (JsonProcessingException e) {
                    throw new RuntimeException(e);
                }

            } else if (Boolean.FALSE.equals(create) || "false".equals(create)) {
                // ignore

            } else if (create instanceof Entity) {
                updatedChildren.add( (Entity)create );

                // TODO on_update

            } else {
                throw new IllegalStateException("Invalid result from match/creation check for item '"+item+"': "+create);
            }
        }
        context.getWorkflowExectionContext().getWorkflowScratchVariables().put("item", oldItem);

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

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
