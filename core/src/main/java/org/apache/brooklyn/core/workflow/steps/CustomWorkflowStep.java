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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CustomWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.WorkflowStepDefinitionWithSpecialDeserialization, WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger LOG = LoggerFactory.getLogger(CustomWorkflowStep.class);

    String shorthand;
    @Override
    public void populateFromShorthand(String value) {
        if (shorthand==null) throw new IllegalStateException("Shorthand not supported for "+getNameOrDefault());
        if (input==null) input = MutableMap.of();
        populateFromShorthandTemplate(shorthand, value);
    }

    /** What to run this set of steps against, either an entity to run in that context, 'children' or 'members' to run over those, a range eg 1..10,  or a list (often in a variable) to run over elements of the list */
    Object target;

    Map<String,Object> parameters;

    // should be treated as raw json
    @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
    List<Object> steps;

    Object workflowOutput;

    @Override
    public void validateStep() {
        if (steps==null) throw new IllegalStateException("No steps defined for "+getName());
        super.validateStep();
    }

    @Override
    protected boolean isOutputHandledByTask() { return true; }

    @Override @JsonIgnore
    public List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context) {
        if (context.getStepState()!=null) {
            // replaying

            List<String> ids = (List<String>) context.getStepState();
            List<WorkflowExecutionContext> nestedWorkflowsToReplay = ids.stream().map(id ->
                    new WorkflowStatePersistenceViaSensors(context.getManagementContext()).getWorkflows(context.getEntity()).get(id)).collect(Collectors.toList());

            if (nestedWorkflowsToReplay.contains(null)) {
                LOG.debug("Step "+context.getWorkflowStepReference()+" missing sub workflows ("+ids+"->"+nestedWorkflowsToReplay+"); replaying from start");
                context.setStepState(null, false);
            } else {
                return nestedWorkflowsToReplay;
            }
        }
        return null;
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        return WorkflowReplayUtils.replayInSubWorkflow("nested workflow", context, subworkflows, instructions, ()->doTaskBody(context));
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object targetR = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, target, Object.class);

        if (targetR instanceof String) {
            targetR = getTargetFromString(context, (String) targetR);
        }

        List<WorkflowExecutionContext> nestedWorkflowContext = MutableList.of();

        boolean wasList = targetR instanceof Iterable;
        if (!wasList) {
            if (targetR != null) {
                throw new IllegalArgumentException("Target of workflow must be a list or an expression that resolves to a list");
            } else {
                targetR = MutableList.of(targetR);
            }
        }

        ((Iterable<?>) targetR).forEach(t -> {
            WorkflowExecutionContext nw = newWorkflow(context, t);
            Maybe<Task<Object>> mt = nw.getTask(true);

            String targetS = wasList || t !=null ? " for target '"+t+"'" : "";
            if (mt.isAbsent()) {
                LOG.debug("Step " + context.getWorkflowStepReference() + " skipping nested workflow " + nw.getWorkflowId() + targetS + "; condition not met");
            } else {
                LOG.debug("Step " + context.getWorkflowStepReference() + " launching nested workflow " + nw.getWorkflowId() + targetS + " in task " + nw.getTaskId());
                nestedWorkflowContext.add(nw);
            }
        });

        context.getSubWorkflows().forEach(tag -> tag.setSupersededByTaskId(Tasks.current().getId()));

        nestedWorkflowContext.forEach(nw -> {
            context.getSubWorkflows().add(BrooklynTaskTags.tagForWorkflow(nw));
        });

        // save the sub-workflow ID before submitting it, and before child knows about parent, per invoke effector step notes
        context.setStepState(MutableList.copyOf(nestedWorkflowContext), true);
        nestedWorkflowContext.forEach(n -> n.persist());

        if (wasList) {
            // TODO concurrency
            List result = MutableList.of();
            nestedWorkflowContext.forEach(nw ->
                    result.add(DynamicTasks.queue(nw.getTask(false).get()).getUnchecked())
            );
            return result;
        } else if (!nestedWorkflowContext.isEmpty()) {
            return DynamicTasks.queue(Iterables.getOnlyElement(nestedWorkflowContext).getTask(false).get()).getUnchecked();
        } else {
            // no steps matched condition
            return null;
        }
    }

    protected Object getTargetFromString(WorkflowStepInstanceExecutionContext context, String target) {
        if ("children".equals(target)) return context.getEntity().getChildren();

        if ("members".equals(target)) {
            if (!(context.getEntity() instanceof Group))
                throw new IllegalArgumentException("Cannot specify target 'members' when not on a group");
            return ((Group) context.getEntity()).getMembers();
        }

        if (target.matches("-?[0-9]+\\.\\.-?[0-9]+")) {
            String[] numbers = target.split("\\.\\.");
            if (numbers.length==2) {
                int min = Integer.parseInt(numbers[0]);
                int max = Integer.parseInt(numbers[1]);
                if (min>max) throw new IllegalArgumentException("Invalid target range "+min+".."+max);
                List<Integer> result = MutableList.of();
                while (min<=max) result.add(min++);
                return result;
            }
        }

        throw new IllegalArgumentException("Invalid target '"+target+"'; if a string, it must match a known keyword ('children' or 'members') or pattern (a range, '1..10')");
    }

    private WorkflowExecutionContext newWorkflow(WorkflowStepInstanceExecutionContext context, Object target) {
        WorkflowExecutionContext nestedWorkflowContext = WorkflowExecutionContext.newInstanceUnpersistedWithParent(
                target instanceof BrooklynObject ? (BrooklynObject) target : context.getEntity(),
                context.getWorkflowExectionContext(), "Workflow for " + getNameOrDefault(),
                ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                        .configure(WorkflowCommonConfig.STEPS, steps)
                        .configure(WorkflowCommonConfig.OUTPUT, workflowOutput),
                null,
                ConfigBag.newInstance(getInput()), null);
        if (target!=null) {
            nestedWorkflowContext.getWorkflowScratchVariables().put("target", target);
        }
        return nestedWorkflowContext;
    }

    public String getNameOrDefault() {
        return (Strings.isNonBlank(getName()) ? getName() : "custom step");
    }

    @Override
    public WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, WorkflowStepDefinitionWithSpecialDeserialization firstParse) {
        // if we've resolved a custom workflow step, we need to make sure that the map supplied here
        // - doesn't set parameters
        // - doesn't set steps unless it is a simple `workflow` step (not a custom step)
        // - (also caller must not override shorthand definition, but that is explicitly removed by WorkflowStepResolution)
        // - has its output treated specially (workflow from output vs output when using this)
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        if (typeBestGuess==null || !(definition instanceof Map)) {
            throw new IllegalStateException("Should not be able to create a custom workflow definition from anything other than a map with a type");
        }
        CustomWorkflowStep result = (CustomWorkflowStep) firstParse;
        Map m = (Map)definition;
        for (String forbiddenKey: new String[] { "parameters" }) {
            if (m.containsKey(forbiddenKey)) {
                throw new IllegalArgumentException("Not permitted to override '" + forbiddenKey + "' when using a workflow step");
            }
        }
        if (!"workflow".equals(typeBestGuess)) {
            // custom workflow step
            for (String forbiddenKey : new String[]{"steps"}) {
                if (m.containsKey(forbiddenKey)) {
                    throw new IllegalArgumentException("Not permitted to override '" + forbiddenKey + "' when using a custom workflow step");
                }
            }
        }
        if (m.containsKey("output")) {
            // need to restore the workflow output from the base definition
            try {
                CustomWorkflowStep base = BeanWithTypeUtils.convert(mgmt, MutableMap.of("type", typeBestGuess), TypeToken.of(CustomWorkflowStep.class), true, loader, false);
                result.workflowOutput = base.output;
            } catch (JsonProcessingException e) {
                throw Exceptions.propagate(e);
            }
        } else {
            // if not specified in the definition, the output should be the workflow output
            result.workflowOutput = result.output;
            result.output = null;
        }
        return result;
    }

    /** Returns a top-level workflow running the workflow defined here */
    public WorkflowExecutionContext newWorkflowExecution(Entity entity, String name, ConfigBag extraConfig) {
        return newWorkflowExecution(entity, name, extraConfig, null);
    }
    public WorkflowExecutionContext newWorkflowExecution(Entity entity, String name, ConfigBag extraConfig, Map extraTaskFlags) {
        return WorkflowExecutionContext.newInstancePersisted(entity, name,
                ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                        .configure(WorkflowCommonConfig.STEPS, steps)
                        .configure(WorkflowCommonConfig.INPUT, input)
                        .configure(WorkflowCommonConfig.OUTPUT, workflowOutput)
                        .configure(WorkflowCommonConfig.REPLAYABLE, replayable)
                        .configure(WorkflowCommonConfig.ON_ERROR, onError)
                        .configure(WorkflowCommonConfig.TIMEOUT, timeout)
                        .configure((ConfigKey) WorkflowCommonConfig.CONDITION, condition),
                null,
                ConfigBag.newInstance(getInput()).putAll(extraConfig), extraTaskFlags);
    }

    @VisibleForTesting
    public List<Object> peekSteps() {
        return steps;
    }
}
