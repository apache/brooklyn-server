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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.workflow.ShorthandProcessor;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class SetVariableWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(SetVariableWorkflowStep.class);

    public static final String SHORTHAND = "[ ?${trim} \"trimmed\" ] [ ${variable.type} ] ${variable.name} \"=\" ${value}";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");
    public static final ConfigKey<Boolean> TRIM = ConfigKeys.newConfigKey(Boolean.class, "trim");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression, true);
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
        TypeToken<?> type = context.lookupType(variable.type, () -> null);

        Object unresolvedValue = input.get(VALUE.getName());

        Object resolvedValue = new SetVariableEvaluation(context, type==null ? TypeToken.of(Object.class) : type, unresolvedValue, Boolean.TRUE.equals(context.getInput(TRIM)), type!=null ).evaluate();

        context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);
        return context.getPreviousStepOutput();
    }

    public static class SetVariableEvaluation<T> {
        protected final WorkflowStepInstanceExecutionContext context;
        protected final TypeToken<T> type;
        protected final Object unresolvedValue;
        private final boolean trim;
        private final boolean typeSpecified;
        private QuotedStringTokenizer qst;


        public SetVariableEvaluation(WorkflowStepInstanceExecutionContext context, TypeToken<T> type, Object unresolvedValue, boolean trim, boolean typeSpecified) {
            this.context = context;
            this.type = type;
            this.unresolvedValue = unresolvedValue;
            this.trim = trim;
            this.typeSpecified = typeSpecified;
        }

        public T evaluate() {
            Object result = unresolvedValue;
            if (result instanceof String) {
                result = process((String) result);
                if (trim && result instanceof String) {
                    Class<? super T> rt = type.getRawType();
                    if (typeSpecified) {
                        result = Yamls.lastDocumentFunction().apply((String)result);
                    } else {
                        result = ((String) result).trim();
                    }
                }
                return context.getWorkflowExectionContext().resolveCoercingOnly(result, type);
            } else {
                return context.resolve(result, type);
            }
        }

        Object process(String input) {
            if (Strings.isBlank(input)) return input;

            // first deal with internal quotes
            qst = QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(true).keepInternalQuotes(true).failOnOpenQuote(false).build(input);
            List<String> wordsByQuote = qst.remainderAsList();
            // then look for operators etc
            return process(wordsByQuote);
        }

        Object process(List<String> w) {
            // if no tokens, treat as null
            if (w.isEmpty()) return null;

            Maybe<Object> result;
            result = handleTokenIfPresent(w, false, MutableMap.of("??", this::handleNullish));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("+", this::handleAdd, "-", this::handleSubtract));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("*", this::handleMultiple, "/", this::handleDivide));
            if (result.isPresent()) return result.get();

            // tokens include space delimiters and are still quotes, so unwrap then just stitch together. will preserve spaces.
            boolean resolveToString = w.size()>1;
            List<Object> objs = w.stream().map(t -> {
                if (qst.isQuoted(t)) return qst.unwrapIfQuoted(t);
                return resolveToString ? context.resolve(t, String.class) : context.resolve(t);
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
            try {
                Object result = process(lhs);
                if (result!=null) return result;
            } catch (Exception e) {
                if (Exceptions.isRootCauseIsInterruption(e)) {
                    if (Thread.currentThread().isInterrupted()) {
                        // still interrupted, so we do propagate
                        throw Exceptions.propagate(e);
                    } else {
                        // value was unavailable, pass through to RHS
                    }
                } else {
                    Exceptions.propagateIfFatal(e);
                }
                if (log.isTraceEnabled()) {
                    log.trace("Nullish operator got non-fatal exception processing "+lhs+" (ignoring, returning RHS)", e);
                }
            }

            return process(rhs);
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

        Object applyMathOperator(List<String> lhs0, List<String> rhs0, BiFunction<Integer,Integer,Number> ifInt, BiFunction<Double,Double,Number> ifDouble) {
            Object lhs = process(lhs0);
            Object rhs = process(rhs0);

            Maybe<Integer> lhsI = asInteger(lhs);
            Maybe<Integer> rhsI = asInteger(rhs);
            if (lhsI.isPresent() && rhsI.isPresent()) {
                Number x = ifInt.apply(lhsI.get(), rhsI.get());
                return ((Maybe)asInteger(x)).orMaybe(() -> asDouble(x)).get();
            }

            Maybe<Double> lhsD = asDouble(lhs);
            Maybe<Double> rhsD = asDouble(rhs);
            if (lhsD.isPresent() && rhsD.isPresent()) return asDouble(ifDouble.apply(lhsD.get(), rhsD.get())).get();

            if (lhsD.isAbsent())
                throw new IllegalArgumentException("Invalid value for operation: "+lhs0+" = "+lhs);
            if (rhsD.isAbsent()) throw new IllegalArgumentException("Invalid value for operation: "+rhs0+" = "+rhs);

            throw new IllegalArgumentException("Should not come here");
        }

        Object handleMultiple(List<String> lhs, List<String> rhs) {
            return applyMathOperator(lhs, rhs, (a,b)->a*b, (a,b)->a*b);
        }

        Object handleDivide(List<String> lhs, List<String> rhs) {
            return applyMathOperator(lhs, rhs, (a,b)->1.0*a/b, (a,b)->a/b);
        }

        Object handleAdd(List<String> lhs, List<String> rhs) {
            return applyMathOperator(lhs, rhs, (a,b)->a+b, (a,b)->a+b);
        }

        Object handleSubtract(List<String> lhs, List<String> rhs) {
            return applyMathOperator(lhs, rhs, (a,b)->a-b, (a,b)->a-b);
        }


    }

}
