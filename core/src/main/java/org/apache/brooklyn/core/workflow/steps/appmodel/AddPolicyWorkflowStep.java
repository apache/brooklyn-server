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
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class AddPolicyWorkflowStep extends WorkflowStepDefinition {

    private static final Logger LOG = LoggerFactory.getLogger(AddPolicyWorkflowStep.class);

    public static final String SHORTHAND = "[ ${type} ] [ \" at \" ${entity} ] [ \" unique-tag \" ${uniqueTag} ]";

    public static final ConfigKey<Object> BLUEPRINT = ConfigKeys.newConfigKey(Object.class, "blueprint");
    public static final ConfigKey<String> TYPE = ConfigKeys.newStringConfigKey("type");
    public static final ConfigKey<String> UNIQUE_TAG = ConfigKeys.newStringConfigKey("uniqueTag");
    public static final ConfigKey<Object> ENTITY = ConfigKeys.newConfigKey(Object.class, "entity");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    // don't try to instantiate 'type' here
    @JsonDeserialize(using = JsonPassThroughDeserializer.class)
    void setBlueprint(Object blueprint) {
        setInput(BLUEPRINT, blueprint);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        boolean hasBlueprint = getInput().containsKey(BLUEPRINT.getName());
        boolean hasType = getInput().containsKey(TYPE.getName());
        if (!hasBlueprint && !hasType) throw new IllegalArgumentException("A '"+BLUEPRINT.getName()+"' must be defined or a type supplied in shorthand");
        if (hasBlueprint && hasType) throw new IllegalArgumentException("Cannot provide both a '"+BLUEPRINT.getName()+"' and a type in shorthand");
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        Object entityToFind = context.getInput(ENTITY);
        Entity entity = entityToFind != null ? DeleteEntityWorkflowStep.findEntity(context, entityToFind).get() : context.getEntity();

        Object blueprint = input.get(BLUEPRINT.getName());
        if (blueprint == null) {
            blueprint = "type: " + StringEscapes.JavaStringEscapes.wrapJavaString(context.getInput(TYPE));
        }

        AbstractBrooklynObjectSpec spec = null;
        EntityAdjunct inst = null;
        try {
            if (!(blueprint instanceof String)) blueprint = BeanWithTypeUtils.newYamlMapper(null, false, null, false).writeValueAsString(blueprint);

            Object yo = Iterables.getOnlyElement(Yamls.parseAll((String) blueprint));
            // coercion does this at: org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon.registerSpecCoercionAdapter
            Maybe<AbstractBrooklynObjectSpec> spec0 = TypeCoercions.tryCoerce(yo, AbstractBrooklynObjectSpec.class);
            if (spec0.isAbsent()) {
                inst = TypeCoercions.tryCoerce(yo, EntityAdjunct.class)
//                        .get();  // prefer this exception
                        .orNull();  // or prefer original expression (line below)
                if (inst == null) {
                    // try camp spec instantiation; and prefer this error. this is the only way if loading a registered type of kind spec;
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
        String uniqueTag = uniqueTagSet ? context.getInput(UNIQUE_TAG) : context.getWorkflowStepReference();
        // set the unique tag to the workflow step if not already set, to ensure idempotency
        if (spec!=null) {
            if (uniqueTagSet || !spec.getFlags().containsKey("uniqueTag")) {
                FlagUtils.setFieldFromFlag(spec, "uniqueTag", uniqueTag);
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
