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
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.util.collections.CollectionMerger;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class SetVariableWorkflowStep extends WorkflowStepDefinition {

    private static final Logger log = LoggerFactory.getLogger(SetVariableWorkflowStep.class);

    public static final String SHORTHAND =
            "[ ?${trim} \"trim \" ] [ ?${trimmed} \"trimmed \" ] " +
            "[ ?${merge} \"merge \" [ ?${merge_deep} \"deep \" ] [ ?${clean} \"clean \" ] ] " +
            "[ ?${wait} \"wait \" ] " +
            "[ [ ${variable.type} ] ${variable.name} [ \"=\" ${value...} ] ]";

    public static final ConfigKey<TypedValueToSet> VARIABLE = ConfigKeys.newConfigKey(TypedValueToSet.class, "variable");
    public static final ConfigKey<Object> VALUE = ConfigKeys.newConfigKey(Object.class, "value");

    public static final ConfigKey<Boolean> TRIM = ConfigKeys.newConfigKey(Boolean.class, "trim");
    public static final ConfigKey<Boolean> TRIMMED = ConfigKeys.newConfigKey(Boolean.class, "trimmed");

    public static final ConfigKey<Boolean> WAIT = ConfigKeys.newConfigKey(Boolean.class, "wait");
    public static final ConfigKey<Boolean> MERGE = ConfigKeys.newConfigKey(Boolean.class, "merge");
    public static final ConfigKey<Boolean> CLEAN = ConfigKeys.newConfigKey(Boolean.class, "clean");
    public static final ConfigKey<Boolean> MERGE_DEEP = ConfigKeys.newConfigKey(Boolean.class, "merge_deep");

    @Override
    public void populateFromShorthand(String expression) {
        populateFromShorthandTemplate(SHORTHAND, expression, true);
    }

    @Override
    public void validateStep() {
        super.validateStep();
        if (!input.containsKey(VARIABLE.getName())) {
            throw new IllegalArgumentException("Variable name is required");
        }
        if (!input.containsKey(VALUE.getName())) {
            throw new IllegalArgumentException("Value is required");
        }
    }

    @Override
    protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
        TypedValueToSet variable = context.getInput(VARIABLE);
        if (variable ==null) throw new IllegalArgumentException("Variable name is required");
        String name = context.resolve(variable.name, String.class);
        if (Strings.isBlank(name)) throw new IllegalArgumentException("Variable name is required");
        TypeToken<?> type = context.lookupType(variable.type, () -> null);

        Object unresolvedValue = input.get(VALUE.getName());

        Object resolvedValue = new SetVariableEvaluation(context, type==null ? TypeToken.of(Object.class) : type, unresolvedValue,
                Boolean.TRUE.equals(context.getInput(TRIM)) || Boolean.TRUE.equals(context.getInput(TRIMMED)),
                Boolean.TRUE.equals(context.getInput(WAIT)),
                Boolean.TRUE.equals(context.getInput(MERGE_DEEP)) ? LetMergeMode.DEEP : Boolean.TRUE.equals(context.getInput(MERGE)) ? LetMergeMode.SHALLOW : LetMergeMode.NONE,
                Boolean.TRUE.equals(context.getInput(CLEAN)),
                type!=null ).evaluate();

        Object oldValue;

        if (name.contains(".")) {
            String[] names = name.split("\\.");
            Object h = context.getWorkflowExectionContext().getWorkflowScratchVariables().get(names[0]);
            if (!(h instanceof Map)) throw new IllegalArgumentException("Cannot set " + name + " because " + name + " is " + (h == null ? "not set" : "not a map"));
            for (int i=1; i<names.length-1; i++) {
                Object hi = ((Map<?, ?>) h).get(names[i]);
                if (hi==null) {
                    hi = MutableMap.of();
                    ((Map)h).put(names[i], hi);
                } else if (!(hi instanceof Map)) throw new IllegalArgumentException("Cannot set " + name + " because " + names[i] + " is not a map");
                h = hi;
            }
            oldValue = ((Map)h).put(names[names.length-1], resolvedValue);
        } else {
            oldValue = context.getWorkflowExectionContext().getWorkflowScratchVariables().put(name, resolvedValue);
        }
        context.noteOtherMetadata("Value set", resolvedValue);
        if (oldValue!=null) context.noteOtherMetadata("Previous value", oldValue);
        return context.getPreviousStepOutput();
    }

    private enum LetMergeMode { NONE, SHALLOW, DEEP }

    public static class SetVariableEvaluation<T> {
        protected final WorkflowStepInstanceExecutionContext context;
        protected final TypeToken<T> type;
        protected final Object unresolvedValue;
        private final boolean trim;
        private final boolean wait;
        private final LetMergeMode merge;
        private final boolean clean;
        private final boolean typeSpecified;
        private QuotedStringTokenizer qst;


        public SetVariableEvaluation(WorkflowStepInstanceExecutionContext context, TypeToken<T> type, Object unresolvedValue, boolean trim, boolean wait, LetMergeMode merge, boolean clean, boolean typeSpecified) {
            this.context = context;
            this.type = type;
            this.unresolvedValue = unresolvedValue;
            this.trim = trim;
            this.wait = wait;
            this.merge = merge;
            this.clean = clean;
            this.typeSpecified = typeSpecified;
        }

        public T evaluate() {
            Object result = unresolvedValue;

            if (merge==LetMergeMode.DEEP || merge==LetMergeMode.SHALLOW) {
                try {
                    if (Map.class.isAssignableFrom(type.getRawType())) {
                        Map holder = type.getRawType().isInterface() ? MutableMap.of() : (Map) type.getRawType().newInstance();
                        mergeInto(result, (term,value) -> {
                            Map xm;
                            if (value instanceof Map) xm = (Map)value;
                            else if (value==null) {
                                if (trim) xm = Collections.emptyMap();
                                else throw new IllegalArgumentException("Cannot merge "+term+" which is null when trim not specified");
                            } else {
                                throw new IllegalArgumentException("Cannot merge "+term+" which is not a map ("+term.getClass().getName()+")");
                            }
                            if (merge==LetMergeMode.DEEP) {
                                xm = CollectionMerger.builder().build().merge(holder, xm);
                            }
                            xm.forEach((k,v) -> {
                                if (clean && (k==null || v==null)) return;
                                holder.put(k, v);
                            });
                        });
                        return (T) holder;
                    } else if (Collection.class.isAssignableFrom(type.getRawType())) {
                        if (merge==LetMergeMode.DEEP) throw new IllegalArgumentException("Merge deep only supported for map");
                        Collection holder = type.getRawType().isInterface() ? Set.class.isAssignableFrom(type.getRawType()) ? MutableSet.of() : MutableList.of() : (Collection) type.getRawType().newInstance();
                        mergeInto(result, (term,value) -> {
                            Collection xm;
                            if (value instanceof Collection) xm = (Collection)value;
                            else if (value==null) {
                                if (trim) xm = Collections.emptyList();
                                else xm = MutableList.of(null);
                            } else {
                                xm = MutableList.of(value);
                            }
                            if (clean) xm = (List) xm.stream().filter(i -> i!=null).collect(Collectors.toList());
                            holder.addAll(xm);
                        });
                        return (T) holder;
                    } else {
                        throw new IllegalArgumentException("Type 'map' or 'list' must be specified for 'let merge'");
                    }
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }

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
                return wait ? context.resolveWaiting(result, type) : context.resolve(result, type);
            }
        }

        public void mergeInto(Object input, BiConsumer<String,Object> holderAdd) {
            if (input instanceof String) {
                qst = QuotedStringTokenizer.builder().includeQuotes(true).includeDelimiters(true).keepInternalQuotes(true).failOnOpenQuote(false).build((String) input);
                List<String> wordsByQuote = qst.remainderAsList();
                wordsByQuote.forEach(word -> {
                    if (Strings.isBlank(word)) return;
                    Maybe<Object> wordResolved = processMaybe(MutableList.of(word));
                    if (trim && wordResolved.isAbsent()) return;
                    holderAdd.accept(word, wordResolved.get());
                });
            } else {
                holderAdd.accept("value", input);
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

            result = handleTokenIfPresent(w, true, MutableMap.of("*", this::handleMultiply, "/", this::handleDivide));
            if (result.isPresent()) return result.get();

            result = handleTokenIfPresent(w, true, MutableMap.of("%", this::handleModulo));
            if (result.isPresent()) return result.get();

            // tokens include space delimiters and are still quotes, so unwrap then just stitch together. will preserve spaces.
            boolean resolveToString = w.size()>1;
            List<Object> objs = w.stream().map(t -> {
                if (qst.isQuoted(t)) return qst.unwrapIfQuoted(t);
                TypeToken<?> target = resolveToString ? TypeToken.of(String.class) : TypeToken.of(Object.class);
                return wait ? context.resolveWaiting(t, target) : context.resolve(t, target);
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
            return processMaybe(lhs).or(() -> process(rhs));
        }

        Maybe<Object> processMaybe(List<String> lhs) {
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
            if (v.isPresent() && !Double.isFinite(v.get())) return Maybe.absent("Value is undefined");
            return v;
        }

        Object applyMathOperator(String op, List<String> lhs0, List<String> rhs0, BiFunction<Integer,Integer,Number> ifInt, BiFunction<Double,Double,Number> ifDouble) {
            Object lhs = process(lhs0);
            Object rhs = process(rhs0);

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

                if (lhsD.isAbsent()) throw new IllegalArgumentException("Invalid left argument to operation '"+op+"': "+lhs0+" => "+lhs);
                if (rhsD.isAbsent()) throw new IllegalArgumentException("Invalid right argument to operation '"+op+"': "+rhs0+" = "+rhs);
            }

            if (lhsI.isAbsent()) throw new IllegalArgumentException("Invalid left argument to operation '"+op+"': "+lhs0+" => "+lhs);
            if (rhsI.isAbsent()) throw new IllegalArgumentException("Invalid right argument to operation '"+op+"': "+rhs0+" = "+rhs);

            throw new IllegalArgumentException("Should not come here");
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

}
