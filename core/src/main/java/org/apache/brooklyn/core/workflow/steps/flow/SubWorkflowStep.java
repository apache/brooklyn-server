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

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.workflow.WorkflowCommonConfig;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.javalang.Reflections;

public class SubWorkflowStep extends CustomWorkflowStep {

    public static final String SHORTHAND_TYPE_NAME_DEFAULT = "subworkflow";

    protected static final Set<String> FORBIDDEN_IN_SUBWORKFLOW_STEP_ALWAYS = MutableSet.copyOf(FORBIDDEN_IN_REGISTERED_TYPE_EXTENSIONS)
            .putAll(MutableSet.of("target", "concurrency")).asUnmodifiable();

    public SubWorkflowStep() {}

    public SubWorkflowStep(CustomWorkflowStep base) {
        super(base);
    }

    protected boolean isInternalClassNotExtendedAndUserAllowedToSetMostThings(String typeBestGuess) {
        return !isRegisteredTypeExtensionToClass(SubWorkflowStep.class, SHORTHAND_TYPE_NAME_DEFAULT, typeBestGuess);
    }

    protected void checkCallerSuppliedDefinition(String typeBestGuess, Map m) {
        if (!isInternalClassNotExtendedAndUserAllowedToSetMostThings(typeBestGuess)) {
            throw new IllegalArgumentException("Not permitted to define a custom subworkflow step with this supertype");
        }
        // these can't be set by user or registered type for subworkflow
        FORBIDDEN_IN_SUBWORKFLOW_STEP_ALWAYS.stream().filter(m::containsKey).forEach(forbiddenKey -> {
            throw new IllegalArgumentException("Not permitted to set '" + forbiddenKey + "' when using a subworkflow step");
        });
        FORBIDDEN_IN_SUBWORKFLOW_STEP_ALWAYS.stream().filter(k -> (Reflections.getFieldValueMaybe(this, k).isPresentAndNonNull())).forEach(forbiddenKey -> {
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
        context.isLocalSubworkflow = true;
        return super.initializeReducingVariables(context, context.getWorkflowExectionContext().getWorkflowScratchVariables());
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
