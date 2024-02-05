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
import freemarker.core.ParseException;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.*;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.text.TemplateProcessor;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Timestamp;
import org.apache.commons.lang3.tuple.MutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.time.Instant;
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
        Map<String, Object> newInput = populateFromShorthandTemplate(SHORTHAND, expression, true, true, true);
        if (newInput.get(VALUE.getName())!=null && input.get(INTERPOLATION_MODE.getName())==null) {
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
        // these are easily inferred from workflow vars
//        context.noteOtherMetadata("Value set", resolvedValue);
//        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);
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
            context.getWorkflowExectionContext().updateWorkflowScratchVariable(listName, l);
        } else {
            oldValue = context.getWorkflowExectionContext().updateWorkflowScratchVariable(name, resolvedValue);
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
            return new WorkflowExpressionResolution(context.getWorkflowExectionContext(), WorkflowExpressionResolution.WorkflowExpressionStage.STEP_RUNNING, false, WorkflowExpressionResolution.WrappingMode.NONE, errorMode)
                    .resolveWithTemplates(v, type);
        }

        Object process(String input) {
            if (Strings.isBlank(input)) return input;

            List<String> wordsByQuote = null;
            try {
                // first deal with internal quotes
                QuotedStringTokenizer qst = qst(input);
                if (qst.isQuoted(input)) {
                    // special treatment if whole line is quoted
                    wordsByQuote = MutableList.of(input);
                } else {
                    wordsByQuote = qst.remainderAsList();
                }
                // then look for operators etc
                return process(wordsByQuote);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                if (wordsByQuote==null || wordsByQuote.size()>1) {
                    if (Exceptions.getCausalChain(e).stream().anyMatch(cause -> cause instanceof ParseException || cause instanceof IllegalArgumentException)) {
                        // try again with the whole thing as tokens, if it is an interpolated string with spaces inside it or mismatched quotes
                        try {
                            return process(MutableList.of(input));
                        } catch (Exception e2) {
                            log.debug("Failed to process expression as tokens or as string; preferring error from former, but error from latter was: " + e2);
                        }
                    }
                }
                throw Exceptions.propagate(e);
            }
        }

        QuotedStringTokenizer qst(String input) {
            return QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(true).expectQuotesDelimited(true).failOnOpenQuote(true).build(input);
        }

        Object process(List<String> w) {
            return process(w, false);
        }

        Object process(List<String> w, boolean ternaryColonAllowed) {
            // if no tokens, treat as null
            if (w.isEmpty()) return null;

            Maybe<Object> result;

            // Order of operations:
            // not used: () [] -> . :: (i.e. Function call, scope, array/member access)
            // not used: ! ~ - + & ++ -- (i.e. unary operators)

            // #__: ?: (i.e. ternary)
            result = handleTokenIfPresent(w, false,
                    ternaryColonAllowed ? MutableMap.of(
                        "?", this::handleTernaryCondition,
                        ":", this::handleTernaryArms)
                            : MutableMap.of("?", this::handleTernaryCondition)
            );
            if (result.isPresent()) return result.get();


            // ?: ?? (nullish - treat as an unary operator)
            result = handleTokenIfPresent(w, false, MutableMap.of("??", this::handleNullish));
            if (result.isPresent()) return result.get();
            //NOTE: levels 4 and 3 are out of order (by the C Order or operations)

            // #4: + -
            result = handleTokenIfPresent(w, true, MutableMap.of("+", this::handleAdd, "-", this::handleSubtract));
            if (result.isPresent()) return result.get();

            // #3: - * / % MOD
            result = handleTokenIfPresent(w, true, MutableMap.of("*", this::handleMultiply, "/", this::handleDivide));
            if (result.isPresent()) return result.get();
            result = handleTokenIfPresent(w, true, MutableMap.of("%", this::handleModulo));
            if (result.isPresent()) return result.get();

            // #5: << >> (i.e. bitwise shift left and right

            // #6: < <= > >=
            result = handleTokenIfPresent(w, false, MutableMap.of(
                    "==", this::handleEquals,
                    "<", this::handleOrderedLessThan,
                    "<=", this::handleOrderedLessThanOrEqual,
                    ">", this::handleOrderedGreaterThan,
                    ">=", this::handleOrderedGreaterThanOrEqual
            ));
            if (result.isPresent()) return result.get();

            // #7: == !=
            // #8: & (i.e. bitwise AND)
            // #9: ^ (i.e. bitwise XOR)
            // #10: | (i.e. bitwise OR)

            // #11: && (i.e. logical AND)
            result = handleTokenIfPresent(w, false, MutableMap.of("&&", this::handleBooleanAnd));
            if (result.isPresent()) return result.get();
            // #12: || (i.e. logical OR)
            result = handleTokenIfPresent(w, false, MutableMap.of("||", this::handleBooleanOr));
            if (result.isPresent()) return result.get();


            // #14: = += -= *= /= %= &= |= ^= <<= >>= (i.e. assignment operators)
            // #15: ,

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
            return processMaybe(lhs, null).or(() -> process(rhs));
        }

        Maybe<Object> processMaybe(List<String> lhs, Function<String,TypeToken<?>> explicitType) {
            try {
                Object result = process(lhs);
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
            if (v.isPresent() && !Double.isFinite(v.get())) return Maybe.absent(() -> new IllegalArgumentException("Value cannot be coerced to double: "+v));
            return v;
        }

        Object applyMathOperator(String op, List<String> lhs0, List<String> rhs0, BiFunction<Integer,Integer,Number> ifInt, BiFunction<Double,Double,Number> ifDouble) {
            Object lhs = process(lhs0);
            Object rhs = process(rhs0);

            if ("+".equals(op)) {
                if (lhs instanceof Duration) {
                    if (rhs instanceof Instant || rhs instanceof Date) {
                        Object newRhs = lhs;
                        lhs = rhs;
                        rhs = newRhs;
                        // fall through to below
                    } else {
                        return TypeCoercions.coerce(rhs, Duration.class).add((Duration) lhs);
                    }
                }
                if (lhs instanceof Instant) return TypeCoercions.coerce(rhs, Duration.class).addTo((Instant) lhs);
                if (lhs instanceof Date)
                    return new Timestamp((Instant) TypeCoercions.coerce(rhs, Duration.class).addTo(((Date) lhs).toInstant()));
            } else if ("-".equals(op)) {
                if (lhs instanceof Duration) {
                    return ((Duration)lhs).subtract(TypeCoercions.coerce(rhs, Duration.class));
                }
                if (lhs instanceof Instant) {
                    if (rhs instanceof Instant) return Duration.between((Instant)rhs, (Instant)lhs);
                    return TypeCoercions.coerce(rhs, Duration.class).multiply(-1).addTo((Instant) lhs);
                }
                if (lhs instanceof Date) {
                    if (rhs instanceof Date) return Duration.between(((Date)rhs).toInstant(), ((Date)lhs).toInstant());
                    return new Timestamp((Instant) TypeCoercions.coerce(rhs, Duration.class).multiply(-1).addTo(((Date) lhs).toInstant()));
                }
            }

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

        Object applyBooleanOperator(List<String> lhs0, List<String> rhs0, BiFunction<Boolean, Boolean, Boolean> biFn) {
            Object lhs = process(lhs0);
            Object rhs = process(rhs0);

            Maybe<Boolean> lhsB = asBoolean(lhs);
            Maybe<Boolean> rhsB = asBoolean(rhs);
            if (lhsB.isPresent() && rhsB.isPresent()) {
                return biFn.apply(lhsB.get(), rhsB.get());
            }
            throw new IllegalArgumentException("Should not come here");
        }

        boolean applyComparison(List<String> lhs0, List<String> rhs0, Function<Integer, Boolean> test) {
            Object lhs = process(lhs0);
            Object rhs = process(rhs0);

            return DslPredicates.coercedCompare(lhs, rhs, test);
        }

        Maybe<Boolean> asBoolean(Object x) {
            return TypeCoercions.tryCoerce(x, Boolean.class);
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

        Object handleBooleanAnd(List<String> lhs, List<String> rhs) {
            return applyBooleanOperator(lhs, rhs, (a, b) -> a && b);
        }

        Object handleBooleanOr(List<String> lhs, List<String> rhs) {
            return applyBooleanOperator(lhs, rhs, (a, b) -> a || b);
        }

        Object handleOrderedGreaterThan(List<String> lhs, List<String> rhs) {
            return applyComparison(lhs, rhs, v -> v>0);
        }

        Object handleOrderedGreaterThanOrEqual(List<String> lhs, List<String> rhs) {
            return applyComparison(lhs, rhs, v -> v>=0);
        }

        Object handleOrderedLessThan(List<String> lhs, List<String> rhs) {
            return applyComparison(lhs, rhs, v -> v<0);
        }

        Object handleOrderedLessThanOrEqual(List<String> lhs, List<String> rhs) {
            return applyComparison(lhs, rhs, v -> v<=0);
        }

        boolean handleEquals(List<String> lhs, List<String> rhs) {
            return DslPredicates.coercedEqual(process(lhs), process(rhs));
        }

        Object handleTernaryCondition(List<String> lhs0, List<String> rhs0) {
            //log.info(String.format("Ternary Condition 0: [lhs:%s][rhs:%s]", lhs0, rhs0));
            Object lhs = process(lhs0);
            Object rhs;
            int questionIndex = rhs0.indexOf("?");
            int colonIndex = rhs0.indexOf(":");
            if (questionIndex > -1 && questionIndex < colonIndex) {
                // Nested ternary
                rhs = handleNestedTernaryRhs(rhs0);
            } else if (questionIndex > -1 && colonIndex < questionIndex) {
                // Chained ternary
                rhs = handleChainedTernaryRhs(rhs0);
            } else {
                // non-nested or chained
                rhs = process(rhs0, true);
            }
            //log.info(String.format("Ternary Condition 1: [lhs:%s][rhs:%s]", lhs, rhs));

            if (!(rhs instanceof TernaryArms)) throw new IllegalArgumentException("Mismatched ternary ':' operator");

            Maybe<Boolean> condition = asBoolean(lhs);
            if (condition.isPresent()){
                if (condition.get()) {
                    // ? left : right -- rhs length is 5 [left, ,:, ,right]
                    // ? true && true : false || false -- rhs length is ???
                    return process( ((TernaryArms)rhs).getLeft());
                    //throw new IllegalArgumentException("TERNARY CONDITION IS TRUE");
                } else {
                    return process( ((TernaryArms)rhs).getRight());
                    //throw new IllegalArgumentException("TERNARY CONDITION IS FALSE");
                }
            }

            throw new IllegalArgumentException("Leading term of ternary '"+lhs+"' does not evaluate to a boolean");
        }

        public TernaryArms handleNestedTernaryRhs(List<String> rhs) {
            int lastColonIndex = rhs.lastIndexOf(":");
            if (lastColonIndex == -1) {
                throw new IllegalArgumentException("Mismatched ternary ':' operator");
            }
            int firstColonIndex = rhs.indexOf(":");
            if (firstColonIndex == lastColonIndex) {
                return (TernaryArms) process(rhs);
            }
            return new TernaryArms(trim(rhs.subList(0, lastColonIndex)), trim(rhs.subList(lastColonIndex + 1, rhs.size())));
        }

        public TernaryArms handleChainedTernaryRhs(List<String> rhs) {
            int colonIndex = rhs.indexOf(":");
            if (colonIndex == -1) {
                throw new IllegalArgumentException("Mismatched ternary ':' operator");
            }
            return new TernaryArms(trim(rhs.subList(0, colonIndex)), trim(rhs.subList(colonIndex + 1, rhs.size())));
        }

        static class TernaryArms extends MutablePair<List<String>,List<String>> {
            public TernaryArms(List<String> lhs, List<String> rhs) {
                super(lhs, rhs);
            }
        }

        public Object handleTernaryArms(List<String> lhs0, List<String> rhs0) {
            //log.info(String.format("Ternary 0: [lhs:%s][rhs:%s]", lhs0, rhs0));
            return new TernaryArms(lhs0, rhs0);
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
