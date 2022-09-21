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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomWorkflowStep extends WorkflowStepDefinition implements WorkflowStepDefinition.SpecialWorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(CustomWorkflowStep.class);

    String shorthand;
    @Override
    public void populateFromShorthand(String value) {
        if (shorthand==null) throw new IllegalStateException("Shorthand not (yet) supported for "+getNameOrDefault());
        if (input==null) input = MutableMap.of();
        input.putAll(new ShorthandProcessor(shorthand).process(value).get());
    }

    Map<String,Object> parameters;
    List<Object> steps;

    Object workflowOutput;

    @Override
    public void validateStep() {
        if (steps==null) throw new IllegalStateException("No steps defined for "+getName());
    }

    @Override
    protected boolean isOutputHandledByTask() { return true; }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        WorkflowExecutionContext nestedWorkflowContext = WorkflowExecutionContext.of(context.getEntity(), "Workflow for " + getNameOrDefault(),
                ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                        .configure(WorkflowCommonConfig.STEPS, steps)
                        .configure(WorkflowCommonConfig.OUTPUT, workflowOutput),
                null,
                ConfigBag.newInstance(getInput()));

        nestedWorkflowContext.getOrCreateTask();
        LOG.debug("Step "+context.getWorkflowStepReference()+" launching nested workflow "+nestedWorkflowContext.getTaskId());

        return DynamicTasks.queue( nestedWorkflowContext.getOrCreateTask().get() ).getUnchecked();
    }

    public String getNameOrDefault() {
        return (Strings.isNonBlank(getName()) ? getName() : "custom step");
    }

    @Override
    public WorkflowStepDefinition applySpecialDefinition(ManagementContext mgmt, Object definition, String typeBestGuess, SpecialWorkflowStepDefinition firstParse) {
        // if we've resolved a custom workflow step, we need to make sure that the map supplied here
        // - does not override steps or parameters
        // - (also caller must not override shorthand definition, but that is explicitly removed by WorkflowStepResolution)
        // - has its output treated specially (workflow from output vs output when using this)
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        if (typeBestGuess==null || !(definition instanceof Map)) {
            throw new IllegalStateException("Should not be able to create a custom workflow definition from anything other than a map with a type");
        }
        CustomWorkflowStep result = (CustomWorkflowStep) firstParse;
        Map m = (Map)definition;
        for (String forbiddenKey: new String[] { "steps", "parameters" }) {
            if (m.containsKey(forbiddenKey)) throw new IllegalArgumentException("Not permitted to override '"+forbiddenKey+"' when using a custom workflow step");
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
}
