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
package org.apache.brooklyn.core.workflow.utils;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.brooklyn.core.workflow.WorkflowExpressionResolution.WorkflowExpressionStage;
import org.apache.brooklyn.core.workflow.WorkflowStepInstanceExecutionContext;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.CharactersCollectingParseMode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNode;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseValue;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** convenience class for the complexities of setting values, especially nested values */
public class WorkflowSettingItemsUtils {

    /**
     * variable indices are not obvious -- currently you should write x['${index}'] for maps and x[${index}] for lists.
     * but the quotes on the former might suggest it should be a literal value.
     * if you want a literal (unresolved) key of the form ${...}
     * you have to put that into another variable, or use $brooklyn:literal.
     *
     * furthermore x[index] is also ambiguous -- probably that should be the way to imply using index as a variable.
     * but currently (and historially) we have allowed it as a string literal "index".
     * we now warn on that usage, but we might want to switch it, and respect its type as a string or integer (for map or list).
     *
     * this constant can be used to find usages.
     */
    public static final boolean TODO_ALLOW_VARIABLES_IN_INDICES = false;

    public static final String VALUE_SET = "Value set";
    public static final String PREVIOUS_VALUE = "Previous value";
    public static final String PREVIOUS_VALUE_OUTER = "Previous value (outer)";

    private static final Logger log = LoggerFactory.getLogger(WorkflowSettingItemsUtils.class);

    public static Pair<String, List<Object>> resolveNameAndBracketedIndices(WorkflowStepInstanceExecutionContext context, String expression, boolean treatDotAsSubkeySeparator) {
        if (TODO_ALLOW_VARIABLES_IN_INDICES) {
            // this should be a more sophisicated resolution, using ExpressionParser
            // values before a bracket should be taken as literals, interpolated expressions resolved, maybe quotes unquoted without resolving;
            // and more importantly in the brackets unquoted values should be taken as expressions and resolved;
            // not sure what to do for quotes, there is an argument to allow "count_${n}" but also to a quote as a literal.
            throw new UnsupportedOperationException();
        }
        String resolved = context.resolve(WorkflowExpressionStage.STEP_INPUT, expression, String.class);
        return expressionParseNameAndIndices(resolved, treatDotAsSubkeySeparator);
    }

    public static Pair<String, List<Object>> extractNameAndDotOrBracketedIndices(String nameFull) {
        if (TODO_ALLOW_VARIABLES_IN_INDICES) {
            // callers to this need to be updated
            throw new UnsupportedOperationException();
        }
        return expressionParseNameAndIndices(nameFull, true);
    }

    public static Pair<String, List<Object>> expressionParseNameAndIndices(String nameFull, boolean treatDotAsSubkeySeparator) {
        CharactersCollectingParseMode DOT = new CharactersCollectingParseMode("dot", '.');
        ExpressionParserImpl ep = ExpressionParser
                .newDefaultAllowingUnquotedLiteralValues()
                .includeGroupingBracketsAtUsualPlaces(ExpressionParser.SQUARE_BRACKET);

        if (treatDotAsSubkeySeparator) ep.includeAllowedTopLevelTransition(DOT);

        ParseNode p = ep.parse(nameFull).get();
        Iterator<ParseNodeOrValue> contents = p.getContents().iterator();
        if (!contents.hasNext()) throw new IllegalArgumentException("Initial identifier is required");

        ParseNodeOrValue nameBaseC = contents.next();
        if (nameBaseC.isParseNodeMode(ExpressionParser.INTERPOLATED, ExpressionParser.SQUARE_BRACKET))
            throw new IllegalArgumentException("Initial part of identifier cannot be an expression or reference");

        String nameBase = ExpressionParser.getUnquoted(nameBaseC).trim();
        List<Object> indices = MutableList.of();

        while (contents.hasNext()) {
            ParseNodeOrValue t = contents.next();
            if (t.isParseNodeMode(DOT)) {
                if (!contents.hasNext()) throw new IllegalArgumentException("Cannot end with a dot");
                ParseNodeOrValue next = contents.next();
                if (next.isParseNodeMode(ExpressionParser.SQUARE_BRACKET)) {
                    // continue to next block; allow foo.['x'].[adfsads]
                    t = next;
                } else if (next.isParseNodeMode(ParseValue.MODE)) {
                    // only a simple value is allowed
                    indices.add(((ParseValue)next).getContents());
                    t = null;

                } else {
                    throw new IllegalArgumentException("Cannot contain this type of object: "+next);
                }
            }
            if (t!=null && t.isParseNodeMode(ExpressionParser.SQUARE_BRACKET)) {
                List<ParseNodeOrValue> nest = ExpressionParser.trimWhitespace( ((ParseNode) t).getContents() );
                Object index;
                if (nest.size()>1) throw new IllegalArgumentException("Bracketed expression must contain a single string or number");
                if (nest.isEmpty()) index = "";
                else {
                    ParseNodeOrValue n = nest.get(0);

                    if (n instanceof ParseValue) {
                        String v = ((ParseValue) n).getContents().trim();
                        index = asInteger(v).asType(Object.class).or(() -> {
                            if (TODO_ALLOW_VARIABLES_IN_INDICES) {
                                // resolve?
                                throw new UnsupportedOperationException();
                            } else {
                                log.warn("Index to " + nameFull + " should be quoted; allowing unquoted for legacy compatibility");
                            }
                            return v;
                        });
                    } else if (n.isParseNodeMode(ExpressionParser.SINGLE_QUOTE, ExpressionParser.DOUBLE_QUOTE)) {
                        index = ExpressionParser.getUnquoted(n);
                    } else {
                        throw new IllegalArgumentException("Cannot contain this type of object bracketed: " + n);
                    }
                }
                indices.add(index);
            }
        }

        return Pair.of(nameBase, indices);
    }

    public static Maybe<Integer> asInteger(Object x) {
        if (x instanceof Integer) return Maybe.of((Integer)x);
        if (x instanceof String) {
            if (((String)x).matches("-? *[0-9]+")) return Maybe.of(Integer.parseInt((String)x));
        }
        return Maybe.absent("Cannot make an integer out of: "+x);
    }

    public static <T> Maybe<T> ensureMutable(T x) {
        return makeMutable(x, false);
    }
    public static <T> Maybe<T> makeMutableCopy(T x) {
        return makeMutable(x, true);
    }
    private static <T> Maybe<T> makeMutable(T x, boolean alwaysCopyEvenIfMutable) {
        Object result = makeMutable(x, alwaysCopyEvenIfMutable, () -> Maybe.absent("Cannot make a mutable object out of null"), (y) -> Maybe.absent("Cannot make a mutable object out of " + y.getClass()));
        if (result instanceof Maybe) return (Maybe)result;
        return Maybe.of((T)result);
    }
    public static Object makeMutableOrUnchangedDefaultingToMap(Object x) {
        return makeMutable(x, false, () -> MutableMap.of(), v -> v);
    }
    public static Maybe<Object> makeMutableOrUnchangedForIndex(Object x, boolean alwaysCopyEvenIfMutable, Object index) {
        return Maybe.ofDisallowingNull(makeMutable(x, alwaysCopyEvenIfMutable, () -> {
            // number or empty string means list
            if (index instanceof Integer || "".equals(index)) return MutableList.of();
            // string is a map
            if (index instanceof String) return MutableMap.of();
            // other things not supported
            return null;
        }, v -> v));
    }
    public static Object makeMutable(@Nullable Object x, boolean alwaysCopyEvenIfMutable, @Nonnull Supplier<Object> ifNull, @Nonnull Function<Object,Object> ifNotIterableOrMap) {
        if (x==null) {
            return ifNull.get();
        }

        if (x instanceof Set) return (!alwaysCopyEvenIfMutable && x instanceof MutableSet) ? x : MutableSet.copyOf((Set) x);
        if (x instanceof Map) return (!alwaysCopyEvenIfMutable && x instanceof MutableMap) ? x : MutableMap.copyOf((Map) x);
        if (x instanceof Iterable) return (!alwaysCopyEvenIfMutable && x instanceof MutableList) ? x : MutableList.copyOf((Iterable) x);
        return ifNotIterableOrMap.apply(x);
    }

    /** returns pair containing outermost updated object and innermost updated object.
     * will be the same if there are no indices. */
    public static Pair<Object,Object> setAtIndex(Pair<String,List<Object>> nameAndIndices, boolean allowToCreateIntermediate, Function<Object,Object> valueModifierOrSupplier, Function<String, Object> getter0, BiFunction<String, Object, Object> setter0) {

        String name = nameAndIndices.getLeft();
        List<Object> indices = nameAndIndices.getRight();

        Object key = name;
        List<Object> oldValuesReplacedOutermostFirst = MutableList.of();

        if (indices!=null && !indices.isEmpty()) {
            if ("output".equals(name))
                throw new IllegalArgumentException("It is not permitted to set a subfield of the output");  // catch common error
        }

        String path = "";
        Function<Object, Object> getter = (Function) getter0;
        Function<Object,Consumer<Object>> setterCurried = k -> v -> {
            oldValuesReplacedOutermostFirst.add(0, setter0.apply((String)k, v));
        };
        Consumer<Object> setter = null;

        Object last = null;
        for (Object index : MutableList.<Object>of(name).appendAll(indices)) {
            path += (path.length()>0 ? "/" : "") + index;
            setter = setterCurried.apply(index);
            final String pathF = path;
            final Consumer<Object> prevSetter = setter;
            final Object next = getter.apply(index);
            last = next;
            if (next == null) {
                if (!allowToCreateIntermediate) {
                    throw new IllegalArgumentException("Cannot set index '" + index + "' at '" + pathF + "' because that is undefined");
                } else {
                    // create
                    getter = k -> null;
                    setterCurried = k -> v -> {
                        Object target = makeMutableOrUnchangedForIndex(null, true, k).orThrow(
                                () -> new IllegalArgumentException("Cannot set index '" + k + "' at '" + pathF + "' because that is undefined and key type unknown"));
                        if (k instanceof Integer && (((Integer)k)<-1 || ((Integer)k)>0)) {
                            throw new IllegalArgumentException("Cannot set index '" + k + "' at '" + pathF + "' because that is undefined and key type out of range");
                        }
                        Object oldV = target instanceof List ? ((List)target).add(v) : ((Map)target).put(k, v);
                        oldValuesReplacedOutermostFirst.add(0, oldV);
                        prevSetter.accept(target);
                    };
                }
            } else if (next instanceof Map) {
                getter = ((Map)next)::get;
                setterCurried = k -> v -> {
                    Map target = makeMutableCopy((Map)next).get();
                    Object oldV = target.put(k, v);
                    // we could be stricter and block this, in case they thought it was a list?
//                    if (oldV==null) {
//                        if (!(k instanceof String))
//                            throw new IllegalArgumentException("Cannot set non-string index '" + k + "' in map at '" + pathF + "' unless it is replacing (map insertion only supported for string keys)");
//                    }
                    oldValuesReplacedOutermostFirst.add(0, oldV);
                    prevSetter.accept(target);
                };
            } else if (next instanceof List) {
                getter = k -> {
                    k = asInteger(k).asType(Object.class).or(k);
                    if ("".equals(k)) return null; // empty string means to append
                    if (!(k instanceof Integer))
                        throw new IllegalArgumentException("Cannot get index '" + k + "' at '" + pathF + "' because that is a list");
                    Integer kn = (Integer) k;
                    if (kn==-1 || kn==((List)next).size()) return null;  // -1 or N means to append
                    return ((List) next).get((Integer) k);
                };
                setterCurried = k -> v -> {
                    List target = makeMutableCopy((List)next).get();
                    Integer kn = asInteger(k).or(() -> {
                        if ("".equals(k)) return -1;
                        throw new IllegalArgumentException("Cannot set index '" + k + "' at '" + pathF + "' because that is a list");
                    });
                    // -1 or size appends, or empty string

                    // (might be nice for negative numbers to reference from the end also -
                    // but the freemarker getter doesn't support that, so it would cause an odd asymmetry;
                    // however use of the empty string, or size, to add gives easy ways to add that don't need this)
                    oldValuesReplacedOutermostFirst.add(0, kn==-1 || kn==target.size() ? target.add(v) : target.set(kn, v));
                    prevSetter.accept(target);
                };
            } else {
                getter = k -> {
                    throw new IllegalArgumentException("Cannot set sub-index at '" + k + "' at '" + pathF +"' because that is a " + next.getClass());
                };
                setterCurried = null;
            }
        }

        setter.accept(valueModifierOrSupplier.apply(last));
        return Pair.of(oldValuesReplacedOutermostFirst.get(0), oldValuesReplacedOutermostFirst.get(oldValuesReplacedOutermostFirst.size()-1));
    }

    public static void noteValueSetMetadata(WorkflowStepInstanceExecutionContext context, Object newValue, Object oldValue) {
        context.noteOtherMetadata(WorkflowSettingItemsUtils.VALUE_SET, newValue);
        if (oldValue!=null) {
            context.noteOtherMetadata(WorkflowSettingItemsUtils.PREVIOUS_VALUE, oldValue);
        }
    }
    public static void noteValueSetNestedMetadata(WorkflowStepInstanceExecutionContext context, Pair<String, List<Object>> nameAndIndices, Object newNestedValue, Pair<Object, Object> oldOuterAndInnerValues) {
        noteValueSetMetadata(context, newNestedValue, oldOuterAndInnerValues.getRight());
        if (!nameAndIndices.getRight().isEmpty()) {
            Object oldValueOuter = oldOuterAndInnerValues.getLeft();
            if (oldValueOuter != null) {
                context.noteOtherMetadata(WorkflowSettingItemsUtils.PREVIOUS_VALUE_OUTER, oldValueOuter);
            }
        }
    }

}
