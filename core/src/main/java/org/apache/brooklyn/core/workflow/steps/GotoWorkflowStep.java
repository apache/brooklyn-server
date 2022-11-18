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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;

import javax.annotation.Nullable;

public class GotoWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "${next}";

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
        Object next = input.remove("next");
        if (next instanceof String) { this.next = (String)next; }
        else throw new IllegalArgumentException("Shorthand should point to the next step");
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        if (next==null) throw new IllegalStateException("next is required for goto step");
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        context.next = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, next);
        return context.getPreviousStepOutput();
    }

}
