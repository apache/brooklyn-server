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
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.brooklyn.core.workflow.WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END;
import static org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep.setWorkflowScratchVariableDotSeparated;

public class TransformVariableWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(TransformVariableWorkflowStep.class);

    public static final String SHORTHAND =
            "[ [ [ ${variable.type} ] [ ?${value_is_initial} \"value\" ] ${variable.name} " +
            "[ [ \"=\" ${value...} ] \"|\" ${transform...} ] ] ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Boolean> VALUE_IS_INITIAL = ConfigKeys.newConfigKey(Boolean.class, "value_is_initial");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<Object> TRANSFORM = ConfigKeys.newConfigKey(Object.class, "transform");


    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression, true, true);
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);
        if (!input.containsKey(TRANSFORM.getName())) {
            throw new IllegalArgumentException("Transform is required");
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        String name;

        Object transformO = context.getInputRaw(TRANSFORM.getName());
        if (!(transformO instanceof Iterable)) transformO = MutableList.of(transformO);
        List<String> transforms = MutableList.of();
        for (Object t: (Iterable)transformO) {
            if (t instanceof String) transforms.addAll(MutableList.copyOf(Arrays.stream( ((String)t).split("\\|") ).map(String::trim).collect(Collectors.toList())));
            else throw new IllegalArgumentException("Argument to transform should be a string or list of strings, not: "+t);
        }

        Object v;
        if (input.containsKey(VALUE.getName())) {
            name = variable==null ? null : context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, variable.name, String.class);

            if (!Strings.isBlank(name) && Boolean.TRUE.equals(context.getInput(VALUE_IS_INITIAL))) {
                // allowed with no var name, we use the value as if "value" was specified, or with a var name, the var will be set
                // but not allowed if "value" keyword was supplied because that means the var name is to be treated as the value
                throw new IllegalArgumentException("Cannot specifiy value_is_initial (keyword \"value\") with an = value");
            }

            v = input.get(VALUE.getName());
            if (variable!=null && Strings.isNonBlank(variable.type)) transforms.add("type "+variable.type);

        } else {
            if (variable==null || Strings.isBlank(variable.name)) throw new IllegalArgumentException("Variable name is required");

            v = "${" + variable.name + "}";
            if (Boolean.TRUE.equals(context.getInput(VALUE_IS_INITIAL)) || "value".equals(variable.type)) {
                // special keyword to treat name as a literal expression
                v = variable.name;
            }
            if (Strings.isNonBlank(variable.type)) {
                if (Boolean.TRUE.equals(context.getInput(VALUE_IS_INITIAL)) || !"value".equals(variable.type)) {
                    transforms.add(0, "type " + variable.type);
                }
            }
            name = null;
        }

        boolean isResolved = false;
        for (String t: transforms) {
            WorkflowTransformWithContext tt = getTransform(context, t);

            Boolean req = tt.resolvedValueRequirement();
            if (req==null) { /* no requirement */ }
            else if (req && !isResolved) {
                v = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, TypeToken.of(Object.class));
                isResolved = true;
            } else if (!req && isResolved) {
                throw new IllegalArgumentException("Transform '" + t + "' must be first as it requires unresolved input");
            } else {
                // requirement matches resolution status, both done, or both not
            }

            v = tt.apply(v);

            Boolean ret = tt.resolvedValueReturned();
            if (ret!=null) isResolved = ret;
        }
        if (!isResolved) {
            // in case "resolve" was supplied at the end
            v = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, v, TypeToken.of(Object.class));
            isResolved = true;
        }

        if (name!=null) {
            Object oldValue = setWorkflowScratchVariableDotSeparated(context, name, v);
            context.noteOtherMetadata("Value set", v);
            if (oldValue != null) context.noteOtherMetadata("Previous value", oldValue);

            if (context.getOutput()!=null) throw new IllegalStateException("Transform that produces output results cannot be used when setting a variable");
            if (STEP_TARGET_NAME_FOR_END.equals(context.next)) throw new IllegalStateException("Return transform cannot be used when setting a variable");

            return context.getPreviousStepOutput();
        } else {
            return v;
        }
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
        ta.init(context.getWorkflowExectionContext(), context, transformWords, transformDef);
        return ta;
    }

    private final static Map<String, Supplier<Function>> TRANSFORMATIONS = MutableMap.of();
    static {
        TRANSFORMATIONS.put("trim", () -> new TransformTrim());
        TRANSFORMATIONS.put("merge", () -> new TransformMerge());
        TRANSFORMATIONS.put("prepend", () -> new TransformPrependAppend(false));
        TRANSFORMATIONS.put("append", () -> new TransformPrependAppend(true));
        TRANSFORMATIONS.put("slice", () -> new TransformSlice());
        TRANSFORMATIONS.put("remove", () -> new TransformRemove());
        TRANSFORMATIONS.put("json", () -> new TransformJsonish(true, false, false));
        TRANSFORMATIONS.put("yaml", () -> new TransformJsonish(false, true, false));
        TRANSFORMATIONS.put("bash", () -> new TransformJsonish(true, false, true));
        TRANSFORMATIONS.put("replace", () -> new TransformReplace());
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
        TRANSFORMATIONS.put("get", () -> v -> {
            if (v instanceof Supplier) return ((Supplier)v).get();
            return v;
        });
        TRANSFORMATIONS.put("to_string", () -> v -> Strings.toString(v));
        TRANSFORMATIONS.put("to_upper_case", () -> v -> ((String)v).toUpperCase());
        TRANSFORMATIONS.put("to_lower_case", () -> v -> ((String)v).toLowerCase());
        TRANSFORMATIONS.put("return", () -> new TransformReturn());
        TRANSFORMATIONS.put("set", () -> new TransformSetWorkflowVariable());
        TRANSFORMATIONS.put("resolve_expression", () -> new TransformResolveExpression());
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

    static final Object sum(Object v, String word) {
        if (v==null) return null;
        if (!(v instanceof Iterable)) throw new IllegalArgumentException("Value is not an iterable; cannot take "+word);
        Iterable<?> vi = (Iterable<?>) v;
        if (vi.iterator().hasNext() && vi.iterator().next() instanceof Duration) {
            Duration result = Duration.ZERO;
            for (Object vii: vi) {
                result = result.add(TypeCoercions.coerce(vii, Duration.class));
            }
            return result;
        }

        double result = 0;
        for (Object vii: vi) {
            result += asDouble(vii).get();
        }
        return simplifiedToIntOrLongIfPossible(result);
    }

    static Maybe<Double> asDouble(Object x) {
        Maybe<Double> v = TypeCoercions.tryCoerce(x, Double.class);
        if (v.isPresent() && !Double.isFinite(v.get())) return Maybe.absent(() -> new IllegalArgumentException("Value cannot be coerced to double: "+v));
        return v;
    }

    static Number simplifiedToIntOrLongIfPossible(Number x) {
        if (x instanceof Integer) return x;
        if (x instanceof Long) {
            if (x.longValue() == x.intValue()) return x.intValue();
            return x;
        }
        if (x instanceof Double || x instanceof Float) {
            double xd = x.doubleValue();
            if (Math.abs(xd - Math.round(xd)) < 0.000000001) {
                return simplifiedToIntOrLongIfPossible(Math.round(xd));
            }
        }
        return x;
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
        Object sum = sum(v, "average");
        if (sum instanceof Duration) {
            return Duration.millis(Math.round(asDouble( ((Duration)sum).toMilliseconds() ).get() / count) );
        }
        return simplifiedToIntOrLongIfPossible(asDouble(sum).get() / count);
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }
}
