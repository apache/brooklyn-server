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
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;

public class SetConfigWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ ${config.type} ] ${config.name} \"=\" ${value...}";

    public static final ConfigKey<EntityValueToSet> CONFIG = ConfigKeys.newConfigKey(EntityValueToSet.class, "config");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        EntityValueToSet config = context.getInput(CONFIG);
        if (config ==null) throw new IllegalArgumentException("Config key name is required");
        String configName = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, config.name, String.class);
        if (Strings.isBlank(configName)) throw new IllegalArgumentException("Config key name is required");
        TypeToken<?> type = context.lookupType(config.type, () -> TypeToken.of(Object.class));
        Object resolvedValue = context.getInput(VALUE.getName(), type);
        Entity entity = config.entity;
        if (entity==null) entity = context.getEntity();
        Object oldValue = entity.config().set((ConfigKey<Object>) ConfigKeys.newConfigKey(type, configName), resolvedValue);

        // see note on type in SetSensorWorkflowStep
        context.noteOtherMetadata("Value set", resolvedValue);
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);

        return context.getPreviousStepOutput();
    }

}
