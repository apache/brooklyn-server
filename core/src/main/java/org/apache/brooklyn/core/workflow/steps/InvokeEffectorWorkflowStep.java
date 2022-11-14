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
package org.apache.brooklyn.core.workflow.steps;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Predicates;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowReplayUtils;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;

public class InvokeEffectorWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(InvokeEffectorWorkflowStep.class);

    public static final String SHORTHAND = "${effector}";

    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");
    public static final ConfigKey<String> EFFECTOR = ConfigKeys.newStringConfigKey("effector");
    public static final ConfigKey<Map<String,Object>> ARGS = new MapConfigKey.Builder<>(Object.class, "args").build();

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep() {
        if (!getInput().containsKey(EFFECTOR.getName())) throw new IllegalArgumentException("Missing required input: "+EFFECTOR.getName());
        super.validateStep();
    }


    @Override @JsonIgnore
    public List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context) {
        if (context.getStepState()!=null) {
            BrooklynTaskTags.WorkflowTaskTag nestedWorkflowTag = (BrooklynTaskTags.WorkflowTaskTag) context.getStepState();
            Maybe<WorkflowExecutionContext> nestedWorkflow = new WorkflowStatePersistenceViaSensors(context.getManagementContext()).getFromTag(nestedWorkflowTag);

            if (nestedWorkflow.isPresent()) {
                return MutableList.of(nestedWorkflow.get());
            } else {
                // entity or workflow no longer available
                LOG.warn("Step " + context.getWorkflowStepReference() + " cannot access nested workflow " + context.getStepState() + " for effector, "+nestedWorkflowTag+": "+Maybe.Absent.getException(nestedWorkflow));
                context.setStepState(null, false);
                // fall through to below
            }
        }
        return null;
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        return WorkflowReplayUtils.replayInSubWorkflow("workflow effector", context, subworkflows, instructions, ()->doTaskBody(context));
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object te = context.getInput(ENTITY);
        if (te==null) te = context.getEntity();
        if (te instanceof String) {
            String desiredComponentId = (String) te;
            List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                    Predicates.and(EntityPredicates.configEqualTo(BrooklynConfigKeys.PLAN_ID, desiredComponentId), x->true)::apply);
            if (firstGroupOfMatches.isEmpty()) {
                firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                        Predicates.and(EntityPredicates.idEqualTo(desiredComponentId), x->true)::apply);
            }
            if (!firstGroupOfMatches.isEmpty()) {
                te = firstGroupOfMatches.get(0);
            } else {
                throw new IllegalStateException("Cannot find entity with ID '"+desiredComponentId+"'");
            }
        }
        if (!(te instanceof Entity)) {
            throw new IllegalStateException("Unsupported object for entity '"+te+"' ("+te.getClass()+")");
        }

        Effector<Object> effector = (Effector) ((Entity) te).getEntityType().getEffectorByName(context.getInput(EFFECTOR)).get();
        TaskAdaptable<Object> invocation = Effectors.invocationPossiblySubWorkflow((Entity) te, effector, context.getInput(ARGS), context.getWorkflowExectionContext(), workflowTag -> {
            // make sure parent knows about child before child workflow is persisted, otherwise there is a chance the child workflow gets orphaned (if interrupted before parent persists)
            context.getSubWorkflows().forEach(tag -> tag.setSupersededByTaskId(workflowTag.getWorkflowId()));
            context.getSubWorkflows().add(workflowTag);
            context.setStepState(workflowTag, true);
        });

        return DynamicTasks.queue(invocation).asTask().getUnchecked();
    }

}
