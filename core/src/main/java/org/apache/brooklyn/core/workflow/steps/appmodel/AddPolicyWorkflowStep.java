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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepResolution;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddPolicyWorkflowStep extends WorkflowStepDefinition implements HasBlueprintWorkflowStep {

    private static final Logger LOG = LoggerFactory.getLogger(AddPolicyWorkflowStep.class);

    public static final String SHORTHAND = "[ ${type} ] [ \" at \" ${entity} ] [ \" unique-tag \" ${uniqueTag} ]";

    public static final ConfigKey<String> UNIQUE_TAG = ConfigKeys.newStringConfigKey("uniqueTag");
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
    public void validateStep(WorkflowStepResolution workflowStepResolution) {
        super.validateStep(workflowStepResolution);
        validateStepBlueprint(workflowStepResolution);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object entityToFind = context.getInput(ENTITY);
        Entity entity = entityToFind != null ? WorkflowStepResolution.findEntity(context, entityToFind).get() : context.getEntity();

        Object blueprint = resolveBlueprint(context);

        AbstractBrooklynObjectSpec spec = null;
        EntityAdjunct inst = null;
        try {
            if (!(blueprint instanceof String)) blueprint = BeanWithTypeUtils.newYamlMapper(null, false, null, false).writeValueAsString(blueprint);

            Object yo = Iterables.getOnlyElement(Yamls.parseAll((String) blueprint));

            // coercion does this _for entities only_ at: org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon.registerSpecCoercionAdapter;
            // other types follow jackson deserialization
            Maybe<AbstractBrooklynObjectSpec> spec0 = TypeCoercions.tryCoerce(yo, AbstractBrooklynObjectSpec.class);
            if (spec0.isAbsent()) {

                inst = TypeCoercions.tryCoerce(yo, EntityAdjunct.class)
//                        .get();  // prefer this exception
                        .orNull();  // or prefer other error
                if (inst == null) {
                    // try camp spec instantiation; and prefer this error. this used to be the only way if loading a registered type of kind spec;
                    // but it is stricter about requiring arguments in `brooklyn.config`
                    spec = context.getManagementContext().getTypeRegistry().createSpecFromPlan(null, blueprint, null, AbstractBrooklynObjectSpec.class);

                    // could prefer first error
                    //spec = spec0.get();
                }
            } else {
                spec = spec0.get();
            }

        } catch (Exception e) {
            throw Exceptions.propagateAnnotated("Cannot make policy or adjunct from blueprint", e);
        }

        BrooklynObject result;
        boolean uniqueTagSet = input.containsKey(UNIQUE_TAG.getName());
        String uniqueTag = uniqueTagSet ? context.getInput(UNIQUE_TAG) :
                Strings.firstNonBlank(context.getWorkflowExectionContext().getName(), context.getWorkflowExectionContext().getRetentionHash())+" - "+(context.getStepIndex()+1);
        // set the unique tag to the workflow step if not already set, to ensure idempotency
        if (spec!=null) {
            if (uniqueTagSet || !spec.getFlags().containsKey("uniqueTag")) {
                spec.configure("uniqueTag", uniqueTag);
            }
            result = EntityAdjuncts.addAdjunct(entity, spec);
        } else {
            if (uniqueTagSet || inst.getUniqueTag()==null) {
                FlagUtils.setFieldFromFlag(inst, "uniqueTag", uniqueTag);
            }
            result = EntityAdjuncts.addAdjunct(entity, inst);
        }

        return MutableMap.of(BrooklynObjectType.of(result).toCamelCase(), result, "result", result);
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
