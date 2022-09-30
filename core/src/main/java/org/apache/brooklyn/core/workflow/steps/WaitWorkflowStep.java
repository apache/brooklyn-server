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
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

public class WaitWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(WaitWorkflowStep.class);

    public static final String SHORTHAND = "[ [ ${variable.type} ] ${variable.name} \"=\" ] [ ${mode} ] ${value}";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<WaitWorkflowStepMode> MODE = ConfigKeys.newConfigKey(WaitWorkflowStepMode.class, "mode");

    public enum WaitWorkflowStepMode {
        EXPRESSION, TASK
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        String name = null;
        TypeToken<?> type = TypeToken.of(Object.class);

        if (variable!=null) {
            name = context.resolve(variable.name, String.class);
            if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
            type = context.lookupType(variable.type, () -> TypeToken.of(Object.class));
        }

        WaitWorkflowStepMode mode = context.getInput(MODE);

        Object resolvedValue = context.resolveWaiting(input.get(VALUE.getName()), type);
        if (WaitWorkflowStepMode.TASK == mode) {
            if (resolvedValue instanceof String) {
                resolvedValue = ((ManagementContextInternal) ((EntityInternal) context.getWorkflowExectionContext().getEntity())).getExecutionManager().getTask((String) resolvedValue);
            }
            if (resolvedValue !=null) {
                if (resolvedValue instanceof TaskAdaptable) {
                    resolvedValue = ((TaskAdaptable) resolvedValue).asTask().getUnchecked();
                } else {
                    throw new IllegalArgumentException("Argument cannot be interpreted as a task: " + resolvedValue);
                }
            }
        }

        if (name!=null) {
            context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);
            return context.getPreviousStepOutput();
        } else {
            return resolvedValue;
        }
    }

}
