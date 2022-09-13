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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;

public class SetVariableWorkflowStep extends WorkflowStepDefinition {

    TypedValueToSet variable;
    Object value;

    public TypedValueToSet getVariable() {
        return variable;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    @Override
    public void setShorthand(String expression) {
        variable = TypedValueToSet.parseFromShorthand(expression, this::setValue);
    }

    @Override
    protected Task<?> newTask(String stepId, WorkflowExecutionContext workflowExecutionContext) {
        return Tasks.create(getDefaultTaskName(workflowExecutionContext), () -> {
            if (variable ==null) throw new IllegalArgumentException("Variable name is required");
            String name = workflowExecutionContext.resolve(variable.name, String.class);
            if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
            TypeToken<?> type = workflowExecutionContext.lookupType(variable.type, () -> TypeToken.of(Object.class));
            Object resolvedValue = workflowExecutionContext.resolve(value, type);
            workflowExecutionContext.getWorkflowScratchVariables().put(name, resolvedValue);
        });
    }

}
