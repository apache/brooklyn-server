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
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.text.Strings;

public class SetVariableWorkflowStep extends WorkflowStepDefinition {

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "sensor");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void setShorthand(String expression) {
        setInput(VARIABLE, TypedValueToSet.parseFromShorthand(expression, v -> setInput(VALUE, v)));
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
        TypeToken<?> type = context.lookupType(variable.type, () -> TypeToken.of(Object.class));
        Object resolvedValue = context.getInput(VALUE.getName(), type);
        context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);
        return resolvedValue;
    }

}
