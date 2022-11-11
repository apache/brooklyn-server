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
package org.apache.brooklyn.core.workflow.steps;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonType;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.ByteSizeStrings;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class LoadWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(LoadWorkflowStep.class);

    public static final String SHORTHAND =
            "[ \"charset\" ${charset} ] [ ${variable.type} ] ${variable.name} [ \"=\" ${url...} ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> URL = ConfigKeys.newConfigKey(Object.class, "url");
    public static final ConfigKey<String> CHARSET = ConfigKeys.newStringConfigKey("charset");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression);
    }

    @Override
    public void validateStep() {
        super.validateStep();
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

        Object resolvedValue = context.getWorkflowExectionContext().resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_OUTPUT, data, type);

        context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);

        context.noteOtherMetadata("Loaded", ByteSizeStrings.java().makeSizeString(data.getBytes().length)+" from "+url+" into "+variable);
        return context.getPreviousStepOutput();
    }
}
