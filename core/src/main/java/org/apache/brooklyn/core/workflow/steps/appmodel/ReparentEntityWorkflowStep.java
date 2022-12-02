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

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Objects;

public class ReparentEntityWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(ReparentEntityWorkflowStep.class);

    public static final String SHORTHAND = "[ \" child \" ${child} ] [ \" under \" ${parent} ]";

    public static final ConfigKey<Object> CHILD = ConfigKeys.newConfigKey(Object.class, "child");
    public static final ConfigKey<Object> PARENT = ConfigKeys.newConfigKey(Object.class, "parent");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (!getInput().containsKey(CHILD.getName())) throw new IllegalArgumentException("Missing required argument: "+CHILD.getName());
        if (!getInput().containsKey(PARENT.getName())) throw new IllegalArgumentException("Missing required argument: "+PARENT.getName());
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Entity child = DeleteEntityWorkflowStep.findEntity(context, context.getInput(CHILD)).get();
        Entity parent = DeleteEntityWorkflowStep.findEntity(context, context.getInput(PARENT)).get();

        Entity oldParent = child.getParent();
        if (!Objects.equals(oldParent, parent)) {
            oldParent.removeChild(child);
            parent.addChild(child);
        }

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
