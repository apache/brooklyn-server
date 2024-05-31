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
package org.apache.brooklyn.core.workflow.steps.flow;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext.SubworkflowLocality;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.javalang.Reflections;

public class SubWorkflowStep extends CustomWorkflowStep {

    public static final String SHORTHAND_TYPE_NAME_DEFAULT = "subworkflow";

    protected static MutableSet<String> X = MutableSet.of("target", "concurrency");
    protected static final Set<String> FORBIDDEN_ON_SUBWORKFLOW_STEP_FIELDS = MutableSet.<String>of()
            .putAll(X).asUnmodifiable();
    protected static final Set<String> FORBIDDEN_ON_SUBWORKFLOW_STEP_MAP = MutableSet.copyOf(FORBIDDEN_ON_NORMAL_WORKFLOW_STEP_MAP)
                .putAll(FORBIDDEN_ON_ALL_WORKFLOW_STEP_TYPES_MAP)
                .putAll(X).asUnmodifiable();

    public SubWorkflowStep() {}

    public SubWorkflowStep(CustomWorkflowStep base) {
        super(base);
    }

    protected boolean isInternalClassNotExtendedAndUserAllowedToSetMostThings(String typeBestGuess) {
        return !isRegisteredTypeExtensionToClass(SubWorkflowStep.class, SHORTHAND_TYPE_NAME_DEFAULT, typeBestGuess);
    }

    protected SubworkflowLocality getSubworkflowLocality() {
        if (subworkflowLocality!=null) return subworkflowLocality;
        return SubworkflowLocality.LOCAL_STEPS_SHARED_CONTEXT;
    }

    protected void checkCallerSuppliedDefinition(String typeBestGuess, Map m) {
        if (!isInternalClassNotExtendedAndUserAllowedToSetMostThings(typeBestGuess)) {
            throw new IllegalArgumentException("Not permitted to define a custom subworkflow step with this supertype");
        }
        // these can't be set by user or registered type for subworkflow
        FORBIDDEN_ON_SUBWORKFLOW_STEP_MAP.stream().filter(m::containsKey).forEach(forbiddenKey -> {
            throw new IllegalArgumentException("Not permitted to set '" + forbiddenKey + "' when using a subworkflow step");
        });
        FORBIDDEN_ON_SUBWORKFLOW_STEP_FIELDS.stream().filter(k -> (Reflections.getFieldValueMaybe(this, k).isPresentAndNonNull())).forEach(forbiddenKey -> {
            throw new IllegalArgumentException("Not permitted for a subworkflow step to use '" + forbiddenKey + "'");
        });
    }

    public WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, WorkflowStepDefinitionWithSpecialDeserialization firstParse) {
        // allow null guesses and other types to instantiate this
        if (typeBestGuess==null || !(definition instanceof Map)) return this;
        return super.applySpecialDefinition(mgmt, definition, typeBestGuess, firstParse);
    }

    @Override
    protected Map initializeReducingVariables(WorkflowStepInstanceExecutionContext context, Map<String, Object> reducing) {
        MutableMap<String, Object> allVarsInScope = MutableMap.copyOf(context.getWorkflowExectionContext().getWorkflowScratchVariables());
        // make output visible
        allVarsInScope.add("output", context.getWorkflowExectionContext().getPreviousStepOutput());
        if (context.getWorkflowExectionContext().getPreviousStepOutput() instanceof Map) {
            allVarsInScope.add((Map)context.getWorkflowExectionContext().getPreviousStepOutput());
        }
        allVarsInScope.add(reducing);
        // but see getReducingWorkflowVarsFromLastStep -- note that for a subworkflow, reduced vars are not returned
        return super.initializeReducingVariables(context, allVarsInScope);
    }

    @Override
    protected void initializeNestedWorkflows(WorkflowStepInstanceExecutionContext outerContext, List<WorkflowExecutionContext> nestedWorkflowContext) {
        // wouldn't work if we iterated in the sub-workflow; but it doesn't allow an iterable target
        outerContext.getWorkflowExectionContext().getAllInput().forEach( (k,v) -> {
            if (!outerContext.hasInput(k)) {
                nestedWorkflowContext.forEach((c -> c.getAllInput().put(k, v)));
            }
        });
    }

    @Override
    protected Map<String, Object> getReducingWorkflowVarsFromLastStep(WorkflowStepInstanceExecutionContext outerContext, WorkflowExecutionContext lastRun, Map<String, Object> prevWorkflowVars) {
        // wouldn't work if we iterated in the sub-workflow; but it doesn't allow an iterable target
        lastRun.getAllInputResolved().forEach( (k,v) -> {
            if (outerContext.getWorkflowExectionContext().hasInput(k)) outerContext.getWorkflowExectionContext().noteInputResolved(k, v);
        });

        outerContext.getWorkflowExectionContext().updateWorkflowScratchVariables(lastRun.getWorkflowScratchVariables());

        // output should just be last step, not the reduced vars
        return null;
    }
}
