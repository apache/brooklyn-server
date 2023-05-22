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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.resolve.jackson.JsonPassThroughDeserializer;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.function.Supplier;

public interface HasBlueprintWorkflowStep {

    ConfigKey<Object> BLUEPRINT = ConfigKeys.newConfigKey(Object.class, "blueprint");
    ConfigKey<String> TYPE = ConfigKeys.newStringConfigKey("type");

    ConfigKey<SetVariableWorkflowStep.InterpolationMode> INTERPOLATION_MODE = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_MODE, SetVariableWorkflowStep.InterpolationMode.FULL);
    ConfigKey<TemplateProcessor.InterpolationErrorMode> INTERPOLATION_ERRORS = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_ERRORS, TemplateProcessor.InterpolationErrorMode.IGNORE);

    Map<String, Object> getInput();
    Logger logger();

    default void validateStepBlueprint(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        boolean hasBlueprint = getInput().containsKey(BLUEPRINT.getName());
        boolean hasType = getInput().containsKey(TYPE.getName());
        if (!hasBlueprint && !hasType) throw new IllegalArgumentException("A '"+BLUEPRINT.getName()+"' must be defined or a type supplied");
        if (hasBlueprint && hasType) throw new IllegalArgumentException("Cannot provide both a '"+BLUEPRINT.getName()+"' and a '"+TYPE.getName()+"'");
    }

    default Object resolveBlueprint(WorkflowStepInstanceExecutionContext context) {
        return resolveBlueprint(context, () -> {
            String type = context.getInput(TYPE);
            if (Strings.isBlank(type)) throw new IllegalStateException("blueprint or type must be supplied"); // should've been caught earlier but check again for good measure
            return "type: " + StringEscapes.JavaStringEscapes.wrapJavaString(type);
        });
    }

    default Object resolveBlueprint(WorkflowStepInstanceExecutionContext context, Supplier<String> defaultValue) {
        Object blueprint = getInput().get(BLUEPRINT.getName());
        if (blueprint == null) {
            return defaultValue.get();
        }
        logger().debug("Blueprint (pre-resolution) is: "+blueprint);
        Object result = new SetVariableWorkflowStep.ConfigurableInterpolationEvaluation(context, null, blueprint,
                context.getInputOrDefault(INTERPOLATION_MODE), context.getInputOrDefault(INTERPOLATION_ERRORS)).evaluate();
        logger().debug("Blueprint (post-resolution: "+context.getInputOrDefault(INTERPOLATION_MODE)+"/"+context.getInputOrDefault(INTERPOLATION_ERRORS)+") is: "+result);
        if (result instanceof String && ((String)result).matches("[^\\s]+")) {
            // single word value treated as a type
            result = "type: " + StringEscapes.JavaStringEscapes.wrapJavaString((String)result);
        }
        return result;
    }

    <T> void setInput(ConfigKey<T> key, T value);

    // don't try to instantiate 'type' here
    @JsonDeserialize(using = JsonPassThroughDeserializer.class)
    default void setBlueprint(Object blueprint) {
        setInput(BLUEPRINT, blueprint);
    }

}
