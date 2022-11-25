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
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.javalang.Boxing;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.List;

public class DeleteEntityWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(DeleteEntityWorkflowStep.class);

    public static final String SHORTHAND = "[ ${entity} ]";

    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (!getInput().containsKey(ENTITY.getName())) throw new IllegalArgumentException("Missing required argument: "+ENTITY.getName());
    }

    @Override
    protected String getStepState(WorkflowStepInstanceExecutionContext context) {
        return (String) super.getStepState(context);
    }
    void setStepState(WorkflowStepInstanceExecutionContext context, String foundEntityId) {
        context.setStepState(foundEntityId, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        String entityId = getStepState(context);
        Entity entity = null;

        if (entityId==null) {
            Object entityO = context.getInput(ENTITY);
            if (entityO instanceof Entity) {
                entity = (Entity) entityO;
            } else if (entityO instanceof String || Boxing.isPrimitiveOrBoxedObject(entityO)) {
                entityId = entityO.toString();

                List<Entity> firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                        Predicates.and(EntityPredicates.configEqualTo(BrooklynConfigKeys.PLAN_ID, entityId), x->true)::apply);
                if (firstGroupOfMatches.isEmpty()) {
                    firstGroupOfMatches = AppGroupTraverser.findFirstGroupOfMatches(context.getEntity(), true,
                            Predicates.and(EntityPredicates.idEqualTo(entityId), x->true)::apply);
                }
                if (!firstGroupOfMatches.isEmpty()) {
                    entity = firstGroupOfMatches.get(0);
                }

                if (entity==null) throw new IllegalStateException("Cannot find entity with id '"+entityId+"'");

            } else throw new IllegalArgumentException("Invalid value for "+ENTITY.getName()+"; must be a string or entity, not '"+entityO+"'");

            entityId = entity.getId();
            setStepState(context, entityId);
        }

        if (entity==null) {
            entity = context.getManagementContext().getEntityManager().getEntity(entityId);
        }

        if (entity==null || Entities.isUnmanagingOrNoLongerManaged(entity)) {
            LOG.debug("Entity '"+entityId+"' already unmanaged.");

        } else {
            Entities.unmanage(entity);
        }

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
