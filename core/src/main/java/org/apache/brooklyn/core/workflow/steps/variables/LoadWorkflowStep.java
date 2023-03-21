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
package org.apache.brooklyn.core.workflow.steps.variables;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.nio.charset.Charset;

public class LoadWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(LoadWorkflowStep.class);

    public static final String SHORTHAND =
            "[ \"charset\" ${charset} ] [ ${variable.type} ] ${variable.name} [ \"=\" ${url...} ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> URL = ConfigKeys.newConfigKey(Object.class, "url");
    public static final ConfigKey<String> CHARSET = ConfigKeys.newStringConfigKey("charset");
    public static final ConfigKey<SetVariableWorkflowStep.InterpolationMode> INTERPOLATION_MODE = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_MODE, SetVariableWorkflowStep.InterpolationMode.DISABLED);
    public static final ConfigKey<TemplateProcessor.InterpolationErrorMode> INTERPOLATION_ERRORS = ConfigKeys.newConfigKeyWithDefault(SetVariableWorkflowStep.INTERPOLATION_ERRORS, TemplateProcessor.InterpolationErrorMode.IGNORE);

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (!input.containsKey(VARIABLE.getName())) {
            throw new IllegalArgumentException("Variable name is required");
        }
        if (!input.containsKey(URL.getName())) {
            throw new IllegalArgumentException("url is required");
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
        TypeToken<?> type = context.lookupType(variable.type, () -> TypeToken.of(String.class));

        Object url = context.getInput(URL);

        ResourceUtils r = ResourceUtils.create(context.getEntity());

        String csName = context.getInput(CHARSET);
        String data;
        if (Strings.isNonBlank(csName)) {
            data = r.getResourceAsString("" + url, Charset.forName(csName));
        } else {
            data = r.getResourceAsString("" + url);
        }

        Object resolvedValue = new SetVariableWorkflowStep.ConfigurableInterpolationEvaluation(context, type, data, context.getInputOrDefault(INTERPOLATION_MODE), context.getInputOrDefault(INTERPOLATION_ERRORS)).evaluate();

        context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);

        context.noteOtherMetadata("Loaded", ByteSizeStrings.java().makeSizeString(data.getBytes().length)+" from "+url+" into "+variable);
        return context.getPreviousStepOutput();
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
