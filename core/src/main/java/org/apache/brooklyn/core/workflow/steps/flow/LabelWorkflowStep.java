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

import java.util.Objects;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.util.text.Strings;

public class LabelWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "${id}";

    public static final ConfigKey<String> ID = ConfigKeys.newStringConfigKey("id");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
        if (!(input.get(ID.getName()) instanceof String)) throw new IllegalArgumentException("ID is required and must be a string");
    }

    @Override
    public void validateStep(WorkflowStepResolution workflowStepResolution) {
        super.validateStep(workflowStepResolution);
        String newId = (String) input.get(ID.getName());
        if (Strings.isNonBlank(newId)) {
            if (Strings.isNonBlank(id)) {
                if (!Objects.equals(id, newId)) throw new IllegalArgumentException("Incompatible ID's set on step");
            } else {
                id = newId;
            }
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        //// don't allow resolving dynamically; it doesn't make sense to have multiple labels for the same step,
        //// as the goto won't go to that specific instance anyway, and it causes a lot of complexity
        // this.id = context.getInput(ID);
        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
