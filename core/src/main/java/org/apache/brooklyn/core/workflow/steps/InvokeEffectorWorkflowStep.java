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

import com.google.common.base.Predicates;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.effector.EffectorTasks;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

public class InvokeEffectorWorkflowStep extends WorkflowStepDefinition {

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
        super.validateStep();
        if (!getInput().containsKey(EFFECTOR.getName())) throw new IllegalArgumentException("Missing required input: "+EFFECTOR.getName());
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        if (context.getStepState()!=null) {
            // replaying
            BrooklynTaskTags.WorkflowTaskTag nestedWorkflowTag = (BrooklynTaskTags.WorkflowTaskTag) context.getStepState();
            Entity targetEntity = context.getManagementContext().lookup(nestedWorkflowTag.getEntityId(), Entity.class);
            if (targetEntity==null) {
                // entity has gone away
                LOG.warn("Step " + context.getWorkflowStepReference() + " cannot access nested workflow " + context.getStepState() + " for effector, entity "+nestedWorkflowTag.getEntityId()+" no longer present, so replaying from start");
                context.setStepState(null, false);
                // fall through to below
            } else {
                WorkflowExecutionContext nestedWorkflowToReplay = new WorkflowStatePersistenceViaSensors(context.getManagementContext()).getWorkflows(targetEntity).get(nestedWorkflowTag.getWorkflowId());
                if (nestedWorkflowToReplay == null) {
                    // shouldn't happen unless workflow was expired, as workflow will be saved before resumption
                    LOG.warn("Step " + context.getWorkflowStepReference() + " cannot access nested workflow " + context.getStepState() + " for effector, possibly expired, so replaying from start");
                    context.setStepState(null, false);
                    // fall through to below
                } else {
                    Task<Object> t = nestedWorkflowToReplay.getTaskReplayingCurrentStep(false);
                    LOG.debug("Step " + context.getWorkflowStepReference() + " resuming workflow effector " + nestedWorkflowToReplay.getWorkflowId() + " in task " + t.getId());
                    return DynamicTasks.queue(t).getUnchecked();
                }
            }
        }

        Object te = context.getInput(ENTITY);
        if (te==null) te = context.getEntity();
        if (te instanceof String) {
            String desiredComponentId = (String) te;
            List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(),
                    Predicates.and(EntityPredicates.configEqualTo(BrooklynConfigKeys.PLAN_ID, desiredComponentId), x->true)::apply);
            if (firstGroupOfMatches.isEmpty()) {
                firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(),
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

        Effector<?> effector = ((Entity) te).getEntityType().getEffectorByName(context.getInput(EFFECTOR)).get();
        TaskAdaptable<?> invocation = Effectors.invocation((Entity) te, effector, context.getInput(ARGS));

        BrooklynTaskTags.WorkflowTaskTag workflowTag = BrooklynTaskTags.getWorkflowTaskTag(invocation.asTask(), false);
        if (workflowTag!=null) {
            // effector is also workflow, so store a link to be able to resume that process
            context.setStepState(workflowTag, true);
        }

        return DynamicTasks.queue(invocation).asTask().getUnchecked();
    }

}
