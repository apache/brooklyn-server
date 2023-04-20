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
import static org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep.ConfigurableInterpolationEvaluation;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TransformSum extends WorkflowTransformDefault {

    private static final Logger log = LoggerFactory.getLogger(TransformSum.class);

    protected static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");

//    @JsonInclude(JsonInclude.Include.NON_EMPTY)
//    protected final Map<String,Object> input;
    public static final ConfigKey<SetVariableWorkflowStep.InterpolationMode> INTERPOLATION_MODE = ConfigKeys.newConfigKey(SetVariableWorkflowStep.InterpolationMode.class, "interpolation_mode",
            "Whether interpolation runs on the full value (not touching quotes; the default in most places), " +
                    "on words (if unquoted, unquoting others; the default for 'let var = value' shorthand), " +
                    "or is disabled (not applied at all)");

    public static final ConfigKey<TemplateProcessor.InterpolationErrorMode> INTERPOLATION_ERRORS = ConfigKeys.newConfigKey(TemplateProcessor.InterpolationErrorMode.class, "interpolation_errors",
            "Whether unresolvable interpolated expressions fail and return an error (the default for 'let'), " +
                    "ignore the expression leaving it in place (the default for 'load'), " +
                    "or replace the expression with a blank string");


    @Override
    public Object apply(Object v) {
        log.info("apply called with object: {" + v.getClass().getSimpleName() + "} "  + v);
        log.info("apply has context: " + this.context);

        if (v==null) return null;
        checkIsIterable(v);
        double result = 0;
        for (Object vi: (Iterable)v) {
            if (!(vi instanceof Number)) throw new IllegalArgumentException("Argument is not a number; cannot compute sum");
            result += ((Number)vi).doubleValue();
        }

//        final ConfigurableInterpolationEvaluation evaluation = this.getEvaluation();
//        final Iterable<List<String>> list = (Iterable)v;
//        final Iterator<List<String>> it = list.iterator();
//        final int numberOfSums = Iterables.size(list) - 1;
//
//        if (numberOfSums < 1) { // add the one item to the sum, if any
//            for (Object vi: list) {
//                checkIsNumberOrString(vi);
//                result += ((Number)vi).doubleValue();
//            }
//        } else { // process items via handleAdd() to coerce numbers from Strings
//            Object left = it.next();
//            Object right;
//            for (int count=1; count<= numberOfSums; count++) {
//                right = it.next();
//                checkIsNumberOrString(left);
//                checkIsNumberOrString(right);
//
//                result += ((Number)evaluation.handleAdd(Arrays.asList(left), Arrays.asList(right))).doubleValue();
//
//                left = right;
//            }
//        }
        return result;
    }

//    protected ConfigurableInterpolationEvaluation getEvaluation() {
//        WorkflowStepInstanceExecutionContext instance = this.context.getCurrentStepInstance();
//        TypeToken<?> type = context.lookupType(this.VARIABLE.getName(), () -> null);
          // need a way to get input/unresolvedValue in order to form the ConfigurableInterpolationEvaluation instance
          // as it is the only way to access the `handleAdd` method.
//        Object unresolvedValue = input.get(VALUE.getName());
//
//        return new ConfigurableInterpolationEvaluation(instance, type, unresolvedValue,
//                instance.getInputOrDefault(INTERPOLATION_MODE),
//                instance.getInputOrDefault(INTERPOLATION_ERRORS)
//        );
//    }

    private static void checkIsIterable(Object o) {
        if (!(o instanceof Iterable)) throw new IllegalArgumentException("Value is not an iterable; cannot take sum");
    }
    private static void checkIsNumberOrString(Object o) {
        if (!(o instanceof Number) && !(o instanceof String)) throw new IllegalArgumentException("Argument is not a number; cannot compute sum");
    }
}
