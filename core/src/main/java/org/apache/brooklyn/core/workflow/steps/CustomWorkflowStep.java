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

import org.apache.brooklyn.core.workflow.*;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

public class CustomWorkflowStep extends WorkflowStepDefinition {

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
    Object output;

    @Override
    public void validateStep() {
        if (steps==null) throw new IllegalStateException("No steps defined for "+getName());
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        return DynamicTasks.queue( WorkflowExecutionContext.of(context.getEntity(), "Workflow for "+getNameOrDefault(),
                ConfigBag.newInstance()
                        .configure(WorkflowCommonConfig.PARAMETER_DEFS, parameters)
                        .configure(WorkflowCommonConfig.STEPS, steps)
                        .configure(WorkflowCommonConfig.OUTPUT, output),
                null,
                ConfigBag.newInstance(getInput())).getOrCreateTask().get() ).getUnchecked();
    }

    public String getNameOrDefault() {
        return (Strings.isNonBlank(getName()) ? getName() : "custom step");
    }
}
