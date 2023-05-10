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
import org.apache.brooklyn.util.collections.*;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SetVariableWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(SetVariableWorkflowStep.class);

    public static final String SHORTHAND =
            "[ [ ${variable.type} ] ${variable.name} [ \"=\" ${value...} ] ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    public enum InterpolationMode {
        WORDS,
        DISABLED,
        FULL,
    }
    public static final ConfigKey<InterpolationMode> INTERPOLATION_MODE = ConfigKeys.newConfigKey(InterpolationMode.class, "interpolation_mode",
            "Whether interpolation runs on the full value (not touching quotes; the default in most places), " +
                    "on words (if unquoted, unquoting others; the default for 'let var = value' shorthand), " +
                    "or is disabled (not applied at all)");
    public static final ConfigKey<TemplateProcessor.InterpolationErrorMode> INTERPOLATION_ERRORS = ConfigKeys.newConfigKey(TemplateProcessor.InterpolationErrorMode.class, "interpolation_errors",
            "Whether unresolvable interpolated expressions fail and return an error (the default for 'let'), " +
                    "ignore the expression leaving it in place (the default for 'load'), " +
                    "or replace the expression with a blank string");

    @Override
    public void populateFromShorthand(String expression) {
        Maybe<Map<String, Object>> newInput = populateFromShorthandTemplate(SHORTHAND, expression, true, true);
        if (newInput.isPresentAndNonNull() && newInput.get().get(VALUE.getName())!=null && input.get(INTERPOLATION_MODE.getName())==null) {
            setInput(INTERPOLATION_MODE, InterpolationMode.WORDS);
        }
    }

    @Override
    public void validateStep(@Nullable ManagementContext mgmt, @Nullable WorkflowExecutionContext workflow) {
        super.validateStep(mgmt, workflow);

        if (input.get(VARIABLE.getName())==null) {
            throw new IllegalArgumentException("Variable name is required");
        }
        if (input.get(VALUE.getName())==null) {
            throw new IllegalArgumentException("Value is required");
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_INPUT, variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
        TypeToken<?> type = context.lookupType(variable.type, () -> null);

        Object unresolvedValue = input.get(VALUE.getName());

        Object resolvedValue = new ConfigurableInterpolationEvaluation(context, type, unresolvedValue, context.getInputOrDefault(INTERPOLATION_MODE), context.getInputOrDefault(INTERPOLATION_ERRORS)).evaluate();

        Object oldValue = setWorkflowScratchVariableDotSeparated(context, name, resolvedValue);
        context.noteOtherMetadata("Value set", resolvedValue);
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);
        return context.getPreviousStepOutput();
    }

    static Object setWorkflowScratchVariableDotSeparated(WorkflowStepInstanceExecutionContext context, String name, Object resolvedValue) {
        Object oldValue;
        if (name.contains(".")) {
            String[] names = name.split("\\.");
            String names0 = names[0];
            if ("output".equals(names0)) throw new IllegalArgumentException("Cannot set subfield in output");  // catch common error
            Object h = context.getWorkflowExectionContext().getWorkflowScratchVariables().get(names0);
            if (!(h instanceof Map)) throw new IllegalArgumentException("Cannot set " + name + " because " + names0 + " is " + (h == null ? "unset" : "not a map"));
            for (int i = 1; i < names.length - 1; i++) {
                Object hi = ((Map<?, ?>) h).get(names[i]);
                if (hi == null) {
                    hi = MutableMap.of();
                    ((Map) h).put(names[i], hi);
                } else if (!(hi instanceof Map))
                    throw new IllegalArgumentException("Cannot set " + name + " because " + names[i] + " is not a map");
                h = hi;
            }
            oldValue = ((Map) h).put(names[names.length - 1], resolvedValue);
        } else if (name.contains("[")) {
            String[] names = name.split("((?<=\\[|\\])|(?=\\[|\\]))");
            if (names.length != 4 || !"[".equals(names[1]) || !"]".equals(names[3])) {
                throw new IllegalArgumentException("Invalid list index specifier " + name);
            }
            String listName = names[0];
            int listIndex = Integer.parseInt(names[2]);
            Object o = context.getWorkflowExectionContext().getWorkflowScratchVariables().get(listName);
            if (!(o instanceof List))
                throw new IllegalArgumentException("Cannot set " + name + " because " + listName + " is " + (o == null ? "unset" : "not a list"));

            List l = MutableList.copyOf(((List)o));
            if (listIndex < 0 || listIndex >= l.size()) {
                throw new IllegalArgumentException("Invalid list index " + listIndex);
            }
            oldValue = l.set(listIndex, resolvedValue);
            context.getWorkflowExectionContext().getWorkflowScratchVariables().put(listName, l);
        } else {
            oldValue = context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);
        }
        return oldValue;
    }

    private enum LetMergeMode { NONE, SHALLOW, DEEP }

    public static class ConfigurableInterpolationEvaluation<T> {
        protected final WorkflowStepInstanceExecutionContext context;
        protected final TypeToken<T> type;
        protected final Object unresolvedValue;
        protected final InterpolationMode interpolationMode;
        protected final TemplateProcessor.InterpolationErrorMode errorMode;

        public ConfigurableInterpolationEvaluation(WorkflowStepInstanceExecutionContext context, TypeToken<T> type, Object unresolvedValue) {
            this(context, type, unresolvedValue, null, null);
        }
        public ConfigurableInterpolationEvaluation(WorkflowStepInstanceExecutionContext context, TypeToken<T> type, Object unresolvedValue, InterpolationMode interpolationMode, TemplateProcessor.InterpolationErrorMode errorMode) {
            this.context = context;
            this.unresolvedValue = unresolvedValue;
            this.type = type;
            this.interpolationMode = interpolationMode;
            this.errorMode = errorMode;
        }

        public boolean unquotedStartsWith(String s, char c) {
            if (s==null) return false;
            s = s.trim();
            s = Strings.removeFromStart(s, "\"");
            s = Strings.removeFromStart(s, "\'");
            s = s.trim();
            return (s.startsWith(""+c));
        }

        public boolean trimmedEndsWithQuote(String s) {
            if (s==null) return false;
            s = s.trim();
            return s.endsWith("\"") || s.endsWith("\'");
        }

        public Function<String,TypeToken<?>> ifUnquotedStartsWithThen(char c, Class<?> clazz) {
            return s -> (!unquotedStartsWith(s, c)) ? null : TypeToken.of(clazz);
        }

        public T evaluate() {
            try {
                Object result = unresolvedValue;

                Object resultCoerced;
                TypeToken<? extends Object> typeIntermediate = type == null ? TypeToken.of(Object.class) : type;

                if (interpolationMode == InterpolationMode.DISABLED) {
                    resultCoerced = context.getWorkflowExectionContext().resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, result, typeIntermediate);

                } else if (result instanceof String && interpolationMode == InterpolationMode.WORDS) {
                    result = process((String) result);
                    resultCoerced = context.getWorkflowExectionContext().resolveCoercingOnly(WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, result, typeIntermediate);

                } else {
                    // full, or null the default
                    resultCoerced = resolveSubPart(result, typeIntermediate);
                }

                return (T) resultCoerced;
            } catch (Exception e) {
                throw e;
            }
        }

        <T> T resolveSubPart(Object v, TypeToken<T> type) {
            return new WorkflowExpressionResolution(context.getWorkflowExectionContext(), WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, false, false, errorMode)
                    .resolveWithTemplates(v, type);
        }

        Object process(String input) {
            if (Strings.isBlank(input)) return input;

            // first deal with internal quotes
            QuotedStringTokenizer qst = qst(input);
            List<String> wordsByQuote;
            if (qst.isQuoted(input)) {
                // special treatment if whole line is quoted
                wordsByQuote = MutableList.of(input);
            } else {
                wordsByQuote = qst.remainderAsList();
            }
            // then look for operators etc
            return process(wordsByQuote, null);
        }

        QuotedStringTokenizer qst(String input) {
            return QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(true).expectQuotesDelimited(true).failOnOpenQuote(true).build(input);
        }

        Object process(List<String> w, Function<String,TypeToken<?>> explicitType) {
            // if no tokens, treat as null
            if (w.isEmpty()) return null;

            Maybe<Object> result;
            result = handleTokenIfPresent(w, false, MutableMap.of("??", this::handleNullish));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("+", this::handleAdd, "-", this::handleSubtract));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("*", this::handleMultiply, "/", this::handleDivide));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("%", this::handleModulo));
            if (result.isPresent()) return result.get();

            // tokens include space delimiters and are still quotes, so unwrap then just stitch together. will preserve spaces.
            boolean resolveToString = w.size()>1;
            QuotedStringTokenizer qst = qst("");
            List<Object> objs = w.stream().map(t -> {
                if (qst.isQuoted(t)) return qst.unwrapIfQuoted(t);
                TypeToken<?> target = resolveToString ? TypeToken.of(String.class) : TypeToken.of(Object.class);
                return resolveSubPart(t, target);
            }).collect(Collectors.toList());
            if (!resolveToString) return objs.get(0);
            return ((List<String>)(List)objs).stream().collect(Collectors.joining());
        }

        private Maybe<Object> handleTokenIfPresent(List<String> tokens, boolean startAtRight, Map<String, BiFunction<List<String>,List<String>,Object>> tokenProcessors) {
            for (int i0=0; i0<tokens.size(); i0++) {
                int i = startAtRight ? tokens.size()-1-i0 : i0;
                String t = tokens.get(i);
                BiFunction<List<String>, List<String>, Object> p = tokenProcessors.get(t);
                if (p!=null) {
                    List<String> lhs = trim(tokens.subList(0, i));
                    List<String> rhs = trim(tokens.subList(i + 1, tokens.size()));
                    return Maybe.of(p.apply(lhs, rhs));
                }
            }
            return Maybe.absent();
        }

        private List<String> trim(List<String> l) {
            if (!l.isEmpty() && Strings.isBlank(l.get(0))) l = l.subList(1, l.size());
            if (!l.isEmpty() && Strings.isBlank(l.get(l.size()-1))) l = l.subList(0, l.size()-1);
            return l;
        }

        Object handleNullish(List<String> lhs, List<String> rhs) {
            return processMaybe(lhs, null).or(() -> process(rhs, null));
        }

        Maybe<Object> processMaybe(List<String> lhs, Function<String,TypeToken<?>> explicitType) {
            try {
                Object result = process(lhs, explicitType);
                if (result!=null) return Maybe.of(result);
                return Maybe.absent("null");

            } catch (Exception e) {
                if (Exceptions.isCausedByInterruptInAnyThread(e) && !Thread.currentThread().isInterrupted()) {
                    // value was unavailable, pass through to RHS
                    return Maybe.absent("unavailable");

                } else {
                    Exceptions.propagateIfFatal(e);
                }

                if (log.isTraceEnabled()) {
                    log.trace("Non-fatal exception processing expression "+ lhs +" (in context where there is an alterantive)", e);
                }
                return Maybe.absent(e);
            }
        }

        Maybe<Integer> asInteger(Object x) {
            Maybe<Integer> xi = TypeCoercions.tryCoerce(x, Integer.class);
            Maybe<Double> xd = asDouble(x);
            if (xi.isAbsent() || xd.isAbsent()) return xi;
            if (Math.abs(xd.get() - xi.get())<0.0000000001) return xi;
            return Maybe.absent("Double value does not match integer value");
        }

        Maybe<Double> asDouble(Object x) {
            Maybe<Double> v = TypeCoercions.tryCoerce(x, Double.class);
            if (v.isPresent() && !Double.isFinite(v.get())) return Maybe.absent("Value is undefined");
            return v;
        }

        Object applyMathOperator(String op, List<String> lhs0, List<String> rhs0, BiFunction<Integer,Integer,Number> ifInt, BiFunction<Double,Double,Number> ifDouble) {
            Object lhs = process(lhs0, null);
            Object rhs = process(rhs0, null);

            Maybe<Integer> lhsI = asInteger(lhs);
            Maybe<Integer> rhsI = asInteger(rhs);
            if (lhsI.isPresent() && rhsI.isPresent()) {
                Number x = ifInt.apply(lhsI.get(), rhsI.get());
                return ((Maybe)asInteger(x)).orMaybe(() -> asDouble(x)).get();
            }

            if (ifDouble!=null) {
                Maybe<Double> lhsD = asDouble(lhs);
                Maybe<Double> rhsD = asDouble(rhs);
                if (lhsD.isPresent() && rhsD.isPresent()) return asDouble(ifDouble.apply(lhsD.get(), rhsD.get())).get();

                if (lhsD.isAbsent()) failOnInvalidArgument("left", op, lhs0, lhs);
                if (rhsD.isAbsent()) failOnInvalidArgument("right", op, rhs0, rhs);
            } else {
                if (lhsI.isAbsent()) failOnInvalidArgument("left", op, lhs0, lhs);
                if (rhsI.isAbsent()) failOnInvalidArgument("right", op, rhs0, rhs);
            }

            throw new IllegalArgumentException("Should not come here");
        }

        private IllegalArgumentException failOnInvalidArgument(String side, String op, Object pre, Object post) {
            String msg = "Invalid "+side+" argument to operation '"+op+"'";

            String postS = ""+post;
            if (postS.contains("*") || postS.contains("+") || postS.contains("+") || postS.contains("/")) {
                // we could weaken this contraint but for now at least make it clear
                msg += "; mathematical operations must have spaces around them for disambiguation";
            }

            throw new IllegalArgumentException(msg + ": "+pre+" => "+post);
        }

        Object handleMultiply(List<String> lhs, List<String> rhs) {
            return applyMathOperator("*", lhs, rhs, (a,b)->a*b, (a,b)->a*b);
        }

        Object handleDivide(List<String> lhs, List<String> rhs) {
            return applyMathOperator("/", lhs, rhs, (a,b)->1.0*a/b, (a,b)->a/b);
        }

        Object handleAdd(List<String> lhs, List<String> rhs) {
            return applyMathOperator("+", lhs, rhs, (a,b)->a+b, (a,b)->a+b);
        }

        Object handleSubtract(List<String> lhs, List<String> rhs) {
            return applyMathOperator("-", lhs, rhs, (a,b)->a-b, (a,b)->a-b);
        }

        Object handleModulo(List<String> lhs, List<String> rhs) {
            return applyMathOperator("%", lhs, rhs, (a,b)->a%b, null);
        }
    }

    @Override protected Boolean isDefaultIdempotent() { return true; }

}
