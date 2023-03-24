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
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class ApplyInitializerWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep {

    private static final Logger LOG = LoggerFactory.getLogger(ApplyInitializerWorkflowStep.class);

    public static final String SHORTHAND = "[ ${type} [ \" at \" ${entity} ] ]";

    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");

    @Override
    public Logger logger() {
        return LOG;
    }

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        validateStepBlueprint(mgmt, workflow);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object entityToFind = context.getInput(ENTITY);
        Entity entity = entityToFind != null ? DeleteEntityWorkflowStep.findEntity(context, entityToFind).get() : context.getEntity();

        Object blueprint = resolveBlueprint(context);

        EntityInitializer initializer;
        try {
            if (blueprint instanceof EntityInitializer) {
                initializer = (EntityInitializer) blueprint;
            } else {
                if (!(blueprint instanceof String))
                    blueprint = BeanWithTypeUtils.newYamlMapper(null, false, null, false).writeValueAsString(blueprint);
                Object yo = Iterables.getOnlyElement(Yamls.parseAll((String) blueprint));
                initializer = TypeCoercions.tryCoerce(yo, EntityInitializer.class).get();
            }
        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Cannot make policy or adjunct from blueprint", e);
        }

        initializer.apply((EntityLocal) entity);

        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
