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

import com.google.common.base.Predicates;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class DeletePolicyWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DeletePolicyWorkflowStep.class);

    public static final String SHORTHAND = "[ ${policy} [ \" at \" ${entity} ] ]";

    public static final ConfigKey<Object> POLICY = ConfigKeys.newConfigKey(Object.class, "policy");
    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (!getInput().containsKey(POLICY.getName())) throw new IllegalArgumentException("Missing required argument: "+POLICY.getName());
    }

    @Override
    protected Pair<String,String> getStepState(WorkflowStepInstanceExecutionContext context) {
        return (Pair<String,String>) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, Pair<String,String> foundEntityAndPolicyId) {
        context.setStepState(foundEntityAndPolicyId, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Pair<String,String> found = getStepState(context);
        Entity entity = null;
        EntityAdjunct policy = null;

        if (found==null) {
            Object entityToFind = context.getInput(ENTITY);
            entity = entityToFind!=null ? DeleteEntityWorkflowStep.findEntity(context, entityToFind).get() : context.getEntity();
            policy = EntityAdjuncts.tryFindOnEntity(entity, context.getInput(POLICY)).get();
            setStepState(context, Pair.of(entity.getId(), policy.getUniqueTag()));
        } else {
            entity = context.getManagementContext().getEntityManager().getEntity(found.getLeft());
            policy = EntityAdjuncts.tryFindOnEntity(entity, found.getRight()).orNull();
            if (policy==null) {
                // already deleted
                return context.getPreviousStepOutput();
            }
        }

        EntityAdjuncts.removeAdjunct(entity, policy);

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
