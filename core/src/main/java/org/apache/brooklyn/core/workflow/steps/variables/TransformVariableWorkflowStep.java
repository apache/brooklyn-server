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

import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.api.typereg.RegisteredTypeLoadingContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.*;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep.setWorkflowScratchVariableDotSeparated;

public class TransformVariableWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(TransformVariableWorkflowStep.class);

    public static final String SHORTHAND =
            "[ [ ${variable.type} ] ${variable.name} " +
            "[ [ \"=\" ${value...} ] \"|\" ${transform...} ] ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<String> TRANSFORM = ConfigKeys.newConfigKey(String.class, "transform");


    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression, true, true);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        if (!input.containsKey(VARIABLE.getName())) {
            throw new IllegalArgumentException("Variable name is required");
        }
        if (!input.containsKey(TRANSFORM.getName())) {
            throw new IllegalArgumentException("Transform is required");
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");

        String transform = context.getInput(TRANSFORM);
        List<String> transforms = MutableList.copyOf(Arrays.stream(transform.split("\\|")).map(String::trim).collect(Collectors.toList()));

        Object v;
        if (input.containsKey(VALUE.getName())) v = input.get(VALUE.getName());
        else v = "${"+variable.name+"}";

        if (Strings.isNonBlank(variable.type)) transforms.add("type "+variable.type);

        boolean isResolved = false;
        for (String t: transforms) {
            WorkflowTransformWithContext tt = getTransform(context, t);

            if (tt.isResolver()) {
                if (isResolved) throw new IllegalArgumentException("Transform '"+t+"' must be first as it requires unresolved input");
                isResolved = true;
            } else {
                if (!isResolved) {
                    v = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, TypeToken.of(Object.class));
                    isResolved = true;
                }
            }

            v = tt.apply(v);
        }

        Object oldValue = setWorkflowScratchVariableDotSeparated(context, name, v);
        context.noteOtherMetadata("Value set", v);
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);
        return context.getPreviousStepOutput();
    }

    static WorkflowTransformWithContext transformOf(final Function f) {
        if (f instanceof WorkflowTransformWithContext) return (WorkflowTransformWithContext) f;
        return new WorkflowTransformDefault() {
            @Override
            public Object apply(Object o) {
                return f.apply(o);
            }
        };
    }

    WorkflowTransformWithContext getTransform(WorkflowStepInstanceExecutionContext context, String transformDef) {
        List<String> transformWords = Arrays.asList(transformDef.split(" "));
        String transformType = transformWords.get(0);
        Function t = Maybe.ofDisallowingNull(TRANSFORMATIONS.get(transformType)).map(Supplier::get).orNull();
        if (t==null) {
            RegisteredTypeLoadingContext lc = RegisteredTypeLoadingContexts.withLoader(
                    RegisteredTypeLoadingContexts.bean(WorkflowTransform.class),
                    RegisteredTypes.getClassLoadingContext(context.getEntity()));
            RegisteredType rt = context.getManagementContext().getTypeRegistry().get(transformType, lc);
            if (rt!=null) t = context.getManagementContext().getTypeRegistry().create(rt, lc, WorkflowTransform.class);
        }
        if (t==null) throw new IllegalStateException("Unknown transform '"+transformType+"'");
        WorkflowTransformWithContext ta = transformOf(t);
        ta.init(context.getWorkflowExectionContext(), transformWords);
        return ta;
    }

    private final static Map<String, Supplier<Function>> TRANSFORMATIONS = MutableMap.of();
    static {
        TRANSFORMATIONS.put("trim", () -> new TransformTrim());
        TRANSFORMATIONS.put("merge", () -> new TransformMerge());
        TRANSFORMATIONS.put("json", () -> new TransformJsonish(true, false, false));
        TRANSFORMATIONS.put("yaml", () -> new TransformJsonish(false, true, false));
        TRANSFORMATIONS.put("bash", () -> new TransformJsonish(true, false, true));
        TRANSFORMATIONS.put("wait", () -> new TransformWait());
        TRANSFORMATIONS.put("type", () -> new TransformType());
        TRANSFORMATIONS.put("first", () -> x -> {
            if (x==null) return null;
            if (!(x instanceof Iterable)) throw new IllegalArgumentException("Cannot take first of non-list type "+x);
            Iterator xi = ((Iterable) x).iterator();
            if (xi.hasNext()) return xi.next();
            return null;
        });
        TRANSFORMATIONS.put("last", () -> x -> {
            if (x==null) return null;
            if (!(x instanceof Iterable)) throw new IllegalArgumentException("Cannot take first of non-list type "+x);
            Iterator xi = ((Iterable) x).iterator();
            Object last = null;
            while (xi.hasNext()) last = xi.next();
            return last;
        });
        TRANSFORMATIONS.put("max", () -> v -> minmax(v, "max", i -> i>0));
        TRANSFORMATIONS.put("min", () -> v -> minmax(v, "min", i -> i<0));
        TRANSFORMATIONS.put("sum", () -> v -> sum(v, "sum"));
        TRANSFORMATIONS.put("average", () -> v -> average(v, "average"));
        TRANSFORMATIONS.put("size", () -> v -> size(v, "size"));
    }

    static final Object minmax(Object v, String word, Predicate<Integer> test) {
        if (v==null) return null;
        if (!(v instanceof Iterable)) throw new IllegalArgumentException("Value is not an iterable; cannot take "+word);
        Object result = null;
        for (Object vi: (Iterable)v) {
            if (result==null) result = vi;
            else if (!(vi instanceof Comparable)) throw new IllegalArgumentException("Argument is not comparable; cannot take "+word);
            else if (test.test( ((Comparable)vi).compareTo(result) )) result = vi;
        }
        return result;
    }

    static final Double sum(Object v, String word) {
        if (v==null) return null;
        if (!(v instanceof Iterable)) throw new IllegalArgumentException("Value is not an iterable; cannot take "+word);
        double result = 0;
        for (Object vi: (Iterable)v) {
            if (!(vi instanceof Number)) throw new IllegalArgumentException("Argument is not a number; cannot compute "+word);
            result += ((Number)vi).doubleValue();
        }
        return result;
    }

    static final Integer size(Object v, String word) {
        if (v==null) return null;
        if (v instanceof Iterable) return Iterables.size((Iterable<?>) v);
        if (v instanceof Map) return ((Map)v).size();
        throw new IllegalArgumentException("Argument is not a set or map; cannot compute "+word);
    }

    static final Object average(Object v, String word) {
        if (v==null) return null;
        Integer count = size(v, "average");
        if (count==null || count==0) throw new IllegalArgumentException("Value is empty; cannot take "+word);
        return sum(v, "average") / count;
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
