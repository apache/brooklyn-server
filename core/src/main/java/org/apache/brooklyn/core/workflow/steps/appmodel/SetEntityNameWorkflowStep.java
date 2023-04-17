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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;

/**
 * shorthandTypeName = set-entity-name
 */
public class SetEntityNameWorkflowStep extends WorkflowStepDefinition {

    public static final String SHORTHAND = "${value...}";

    // decide config keys here
    public static final ConfigKey<String> VALUE = ConfigKeys.newConfigKey(String.class, "value");
    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");

    @Override
    public void populateFromShorthand(String value) {
        populateFromShorthandTemplate(SHORTHAND, value);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object entityToFind = context.getInput(ENTITY);
        Entity entity = entityToFind != null ? WorkflowStepResolution.findEntity(context, entityToFind).get() : context.getEntity();
        entity.setDisplayName(context.getInput(VALUE));
        return context.getPreviousStepOutput();
    }

    @Override
    protected Boolean isDefaultIdempotent() {
        return true;
    }
}
