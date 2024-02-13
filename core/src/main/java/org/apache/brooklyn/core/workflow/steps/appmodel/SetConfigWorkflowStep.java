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
package org.apache.brooklyn.core.workflow.steps.appmodel;

import java.util.List;
import java.util.Objects;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.core.workflow.utils.WorkflowSettingItemsUtils;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.tuple.Pair;

public class SetConfigWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "[ ${config.type} ] ${config.name} [ \" on \" ${config.entity} ] [ \"=\" ${value...} ]";

    public static final ConfigKey<EntityValueToSet> CONFIG = ConfigKeys.newConfigKey(EntityValueToSet.class, "config");
    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        EntityValueToSet config = context.getInput(CONFIG);
        if (config ==null) throw new IllegalArgumentException("Config key name is required");

        Pair<String, List<Object>> nameAndIndices = WorkflowSettingItemsUtils.resolveNameAndBracketedIndices(context, config.name, false);
        if (nameAndIndices==null) throw new IllegalArgumentException("Config key name is required");

        // see note on type in SetSensorWorkflowStep
        TypeToken<?> type = context.lookupType(config.type, () -> TypeToken.of(Object.class));
        Object resolvedValue = context.getInput(VALUE.getName(), type);
        Object entityO1 = context.getInput(ENTITY);
        if (entityO1!=null && config.entity!=null && !Objects.equals(entityO1, config.entity))
            throw new IllegalArgumentException("Cannot specify different entities in 'entity' and 'config.entity' when setting config");
        Object entityO2 = ObjectUtils.firstNonNull(config.entity, entityO1, context.getEntity());
        final Entity entity = WorkflowStepResolution.findEntity(context, entityO2).get();

        Pair<Object, Object> oldValues = WorkflowSettingItemsUtils.setAtIndex(nameAndIndices, true, (_oldValue) -> resolvedValue,
                name -> entity.config().get((ConfigKey<Object>) ConfigKeys.newConfigKey(type, name)),
                (name, value) -> entity.config().set((ConfigKey<Object>) ConfigKeys.newConfigKey(type, name), value));
        WorkflowSettingItemsUtils.noteValueSetNestedMetadata(context, nameAndIndices, resolvedValue, oldValues);

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
