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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.function.Function;

public class SwitchWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.WorkflowStepDefinitionWithSubWorkflow {

    private static final Logger log = LoggerFactory.getLogger(SwitchWorkflowStep.class);

    public static final String SHORTHAND = "[ ${value} ]";

    public static final ConfigKey<List> CASES = ConfigKeys.newConfigKey(List.class, "cases");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @JsonDeserialize(contentUsing = JsonPassThroughDeserializer.class)
    List<Object> cases;

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        if (cases==null) throw new IllegalStateException("No cases defined for "+Strings.firstNonBlank(getName(), "switch"));
        List<WorkflowStepDefinition> stepsResolved = WorkflowStepResolution.resolveSubSteps(mgmt, Strings.firstNonBlank(getName(), "switch"), cases);
        if (stepsResolved.size()>1) {
            for (int i = 0; i < stepsResolved.size()-1; i++) {
                if (stepsResolved.get(i).getConditionRaw() == null) {
                    throw new IllegalStateException("All but the last case to a switch block must specify a condition; case "+(i+1)+" does not");
                }
            }
        }
    }

    static class StepState {
        WorkflowStepDefinition selectedStepDefinition;
        WorkflowStepInstanceExecutionContext selectedStepContext;
    }

    @Override
    protected StepState getStepState(WorkflowStepInstanceExecutionContext context) {
        return (StepState) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, boolean persist, WorkflowStepDefinition selectedStepDefinition, WorkflowStepInstanceExecutionContext selectedStepContext) {
        StepState state = new StepState();
        state.selectedStepDefinition = selectedStepDefinition;
        state.selectedStepContext = selectedStepContext;
        context.setStepState(context, persist);
    }
    protected <T> Maybe<T> runOnStepStateIfHasSubWorkflows(WorkflowStepInstanceExecutionContext context, Function<WorkflowStepDefinitionWithSubWorkflow,T> fn) {
        StepState state = getStepState(context);
        if (state!=null && state.selectedStepDefinition instanceof WorkflowStepDefinitionWithSubWorkflow) {
            return Maybe.of( fn.apply((WorkflowStepDefinitionWithSubWorkflow) state.selectedStepDefinition) );
        }
        return Maybe.absent(state==null ? "no state" : "not a subworkflow-enabled substep");
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {

        List<WorkflowStepDefinition> stepsResolved = WorkflowStepResolution.resolveSubSteps(context.getManagementContext(), getName(), cases);
        Object valueResolved = context.getInput(VALUE);

        for (int i = 0; i<stepsResolved.size(); i++) {
            // go through steps, find first that matches

            WorkflowStepDefinition subStep = stepsResolved.get(i);

            WorkflowStepInstanceExecutionContext subStepContext = new WorkflowStepInstanceExecutionContext(
                /** use same step index */ context.getStepIndex(), subStep, context.getWorkflowExectionContext());

            // might want to record sub-step context somewhere; but for now we don't

            String potentialTaskName = Tasks.current().getDisplayName()+"-"+(i+1);

            DslPredicates.DslPredicate condition = subStep.getConditionResolved(subStepContext);

            if (condition!=null) {
                if (log.isTraceEnabled()) log.trace("Considering condition " + condition + " for " + potentialTaskName);
                boolean conditionMet = DslPredicates.evaluateDslPredicateWithBrooklynObjectContext(condition, valueResolved, subStepContext.getEntity());
                if (log.isTraceEnabled()) log.trace("Considered condition " + condition + " for " + potentialTaskName + ": " + conditionMet);
                if (!conditionMet) continue;
            }

            setStepState(context, true, subStep, subStepContext);  // persist this, so when we resume we can pick up the same one
            Task<?> handlerI = subStep.newTaskAsSubTask(subStepContext,
                    potentialTaskName, BrooklynTaskTags.tagForWorkflowSubStep(context, i));

            log.debug("Switch matched at substep "+i+", running " + potentialTaskName + " '" + subStep.computeName(subStepContext, false)+"' in task "+handlerI.getId());

            Object result = DynamicTasks.queue(handlerI).getUnchecked();
            context.next = WorkflowReplayUtils.getNext(subStepContext, subStep, context, this);

            // provide some details of other step
            context.noteOtherMetadata("Switch match", "Case "+(i+1)+": "+Strings.firstNonBlank(subStepContext.getName(), subStepContext.getWorkflowStepReference()));
            context.otherMetadata.putAll(subStepContext.otherMetadata);

            return result;
        }

        // if none apply
        throw new IllegalStateException("No cases match switch statement; include a final `no-op` case if this is intentional");
    }

    @Override
    public List<WorkflowExecutionContext> getSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, boolean forced, boolean peekingOnly) {
        return runOnStepStateIfHasSubWorkflows(context, s -> s.getSubWorkflowsForReplay(context, forced, peekingOnly)).get();
    }

    @Override
    public Object doTaskBodyWithSubWorkflowsForReplay(WorkflowStepInstanceExecutionContext context, @Nonnull List<WorkflowExecutionContext> subworkflows, ReplayContinuationInstructions instructions) {
        return runOnStepStateIfHasSubWorkflows(context, s -> s.doTaskBodyWithSubWorkflowsForReplay(context, subworkflows, instructions)).get();
    }
}
