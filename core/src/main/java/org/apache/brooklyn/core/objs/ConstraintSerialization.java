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
package org.apache.brooklyn.core.objs;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ResourcePredicates;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ConstraintSerialization {

    private final Map<String, String> predicateToStringToPreferredName = MutableMap.of();
    private final Map<String, Function<List<?>,Predicate<?>>> predicatePreferredNameToConstructor = MutableMap.of();

    public static class PredicateSerializationRuleAdder<T> {
        private String preferredName;
        private Function<List<?>, T> constructorArgsFromList;
        private Function<T, Predicate<?>> constructor;
        private Predicate<?> predicateSample;
        private T constructorSampleInput;
        private Set<String> equivalentNames = MutableSet.of();
        private Set<Predicate<?>> equivalentPredicateSamples = MutableSet.of();

        ConstraintSerialization serialization;
        
        public PredicateSerializationRuleAdder(Function<T, Predicate<?>> constructor, Function<List<?>, T> constructorArgsFromList, T constructorSampleInput) {
            this.constructorArgsFromList = constructorArgsFromList;
            this.constructor = constructor;
            this.constructorSampleInput = constructorSampleInput;
        }
        
        public static PredicateSerializationRuleAdder<List<Predicate<?>>> predicateListConstructor(Function<List<Predicate<?>>,Predicate<?>> constructor) {
            PredicateSerializationRuleAdder<List<Predicate<?>>> result = new PredicateSerializationRuleAdder<List<Predicate<?>>>(constructor,
                null, MutableList.of());
            result.constructorArgsFromList = o -> result.serialization.toPredicateListFromJsonList(o);
            return result;
        }
        
        public static PredicateSerializationRuleAdder<String> stringConstructor(Function<String,Predicate<?>> constructor) {
            return new PredicateSerializationRuleAdder<String>(constructor, 
                o -> Strings.toString(Iterables.getOnlyElement(o)), "");
        }
        
        public static PredicateSerializationRuleAdder<Void> noArgConstructor(Supplier<Predicate<?>> constructor) {
            return new PredicateSerializationRuleAdder<Void>(
                (o) -> constructor.get(), o -> null, null);
        }
        
        /** Preferred name for predicate when serializing. Defaults to the predicate name in the output of the {@link #sample(Predicate)}. */
        public PredicateSerializationRuleAdder<T> preferredName(String preferredName) {
            this.preferredName = preferredName;
            return this;
        }

        /** Other predicates which are different to the type indicated by {@link #sample(Predicate)} but equivalent,
         * and after serialization will be represented by {@link #preferredName} and after deserialization 
         * will result in the {@link Predicate} produced by {@link #constructor}. */
        public PredicateSerializationRuleAdder<T> equivalentNames(String ...equivs) {
            for (String equiv: equivs) equivalentNames.add(equiv);
            return this;
        }

        /** Sample of what the {@link #constructor} will produce, used to recognise this rule when parsing. 
         * Can be omitted if {@link #sampleArg(Object)} supplied or its default is accepted. */
        public PredicateSerializationRuleAdder<T> sample(Predicate<?> samplePreferredPredicate) {
            predicateSample = samplePreferredPredicate;
            return this;
        }

        /** This should supply args accepted by {@link #constructor} to generate a {@link #sample(Predicate)}. 
         * At most one of this or {@link #sample(Predicate)} should be supplied.
         * If the constructor accepts a default empty list/string/null then these can be omitted. */
        public PredicateSerializationRuleAdder<T> sampleArg(T arg) {
            constructorSampleInput = arg;
            return this;
        }

        /** Other predicates which are different to the type indicated by {@link #sample(Predicate)} but equivalent,
         * and after serialization will be represented by {@link #preferredName} and after deserialization 
         * will result in the {@link Predicate} produced by {@link #constructor}. */
        public PredicateSerializationRuleAdder<T> equivalentPredicates(Predicate<?> ...equivs) {
            for (Predicate<?> equiv: equivs) equivalentPredicateSamples.add(equiv);
            return this;
        }

        public void add(ConstraintSerialization constraintSerialization) {
            this.serialization = constraintSerialization;
            if (predicateSample==null) predicateSample = constructor.apply(constructorSampleInput);
            String toStringName = Strings.removeAfter(Preconditions.checkNotNull(predicateSample, "sample or sampleArg must be supplied").toString(), "(", false);
            if (preferredName==null) {
                preferredName = toStringName;
            } else {
                constraintSerialization.predicateToStringToPreferredName.put(preferredName, preferredName);
            }
            constraintSerialization.predicateToStringToPreferredName.put(toStringName, preferredName);
            
            for (String equiv: equivalentNames) {
                constraintSerialization.predicateToStringToPreferredName.put(equiv, preferredName);
            }
            
            constraintSerialization.predicatePreferredNameToConstructor.put(preferredName, constructor.compose(constructorArgsFromList));
            
            for (Predicate<?> equiv: equivalentPredicateSamples) {
                String equivToStringName = Strings.removeAfter(equiv.toString(), "(", false);                
                constraintSerialization.predicateToStringToPreferredName.put(equivToStringName, preferredName);
            }
        }
    }
    
    private static String GROUP(String in) { return "("+in+")"; }
    private static String NOT_CHARSET(String ...in) { return "[^"+Strings.join(in, "")+"]"; }
    private static String OR_GROUP(String ...in) { return GROUP(Strings.join(in, "|")); }
    private static String ZERO_OR_MORE(String in) { return in + "*"; }
    
    private static String DOUBLE_QUOTED_STRING = "\""+GROUP(ZERO_OR_MORE(OR_GROUP(NOT_CHARSET("\\", "\""), "\\.")))+"\"";
    private static String SINGLE_QUOTED_STRING = "\'"+GROUP(ZERO_OR_MORE(OR_GROUP(NOT_CHARSET("\\", "\'"), "\\.")))+"\'";
    
    private static String PREDICATE = "[A-Za-z0-9_\\-\\.]+";
    
    private static Pattern PATTERN_START_WITH_QUOTED_STRING = Pattern.compile("^"+OR_GROUP(DOUBLE_QUOTED_STRING, SINGLE_QUOTED_STRING));
    private static Pattern PATTERN_START_WITH_PREDICATE = Pattern.compile("^"+GROUP(PREDICATE));

    {
        init();
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void init() {
        PredicateSerializationRuleAdder.predicateListConstructor((o) -> ConfigConstraints.required()).
            equivalentPredicates(Predicates.notNull(), StringPredicates.isNonBlank()).add(this);

        PredicateSerializationRuleAdder.predicateListConstructor((o) -> Predicates.or((Iterable)o)).preferredName("any").equivalentNames("or").add(this);
        PredicateSerializationRuleAdder.predicateListConstructor((o) -> /* and predicate is default when given list */ toPredicateFromJson(o)).preferredName("all").sample(Predicates.and(Collections.emptyList())).equivalentNames("and").add(this);
        PredicateSerializationRuleAdder.noArgConstructor(Predicates::alwaysFalse).add(this);
        PredicateSerializationRuleAdder.noArgConstructor(Predicates::alwaysTrue).add(this);
        
        PredicateSerializationRuleAdder.noArgConstructor(ResourcePredicates::urlExists).preferredName("urlExists").add(this);
        PredicateSerializationRuleAdder.noArgConstructor(StringPredicates::isBlank).add(this);
        
        PredicateSerializationRuleAdder.stringConstructor(StringPredicates::matchesRegex).preferredName("regex").add(this);
        PredicateSerializationRuleAdder.stringConstructor(StringPredicates::matchesGlob).preferredName("glob").add(this);
        
        PredicateSerializationRuleAdder.stringConstructor(ConfigConstraints::forbiddenIf).add(this);
        PredicateSerializationRuleAdder.stringConstructor(ConfigConstraints::forbiddenUnless).add(this);
        PredicateSerializationRuleAdder.stringConstructor(ConfigConstraints::requiredIf).add(this);
        PredicateSerializationRuleAdder.stringConstructor(ConfigConstraints::requiredUnless).add(this);
    }
    
    public static ConstraintSerialization INSTANCE = new ConstraintSerialization();
    
    private ConstraintSerialization() {}

    public List<Object> toJsonList(ConfigKey<?> config) {
        return toJsonList(config.getConstraint());
    }
    
    public List<Object> toJsonList(Predicate<?> constraint) {
        // map twice to clean it (flatten "and" lists, etc)
        // but if not possible go with progressively simpler items
        try {
            return toExactJsonList(toPredicateFromJson(toExactJsonList(constraint)));
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            try {
                return toExactJsonList(constraint);
            } catch (Exception e2) {
                Exceptions.propagateIfFatal(e);
                return Collections.singletonList(constraint.toString());
            }
        }
    }
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public List<Object> toExactJsonList(Predicate<?> constraint) {
        StringConstraintParser parser = StringConstraintParser.forConstraint(this, Strings.toString(constraint));
        if (!parser.parse()) throw new IllegalStateException("cannot match: "+constraint);
        if (parser.result instanceof Map && ((Map)parser.result).size()==1 && ((Map)parser.result).containsKey("all")) {
            return (List<Object>) ((Map)parser.result).get("all");
        }
        if ("Predicates.alwaysTrue".equals(parser.result)) {
            return Collections.emptyList(); 
        }
        return ImmutableList.of(parser.result);
    }
    
    private static class StringConstraintParser {
        ConstraintSerialization serialization;
        String remaining;
        Object result;
        List<Object> resultList = MutableList.of();
        boolean list = false;
        
        static StringConstraintParser forConstraint(ConstraintSerialization serialization, String in) {
            StringConstraintParser result = new StringConstraintParser();
            result.serialization = serialization;
            result.remaining = in;
            return result;
        }

        static StringConstraintParser forArgsInternal(ConstraintSerialization serialization, String in) {
            StringConstraintParser result = forConstraint(serialization, in);
            result.list = true;
            return result;
        }

        boolean parse() {
            remaining = remaining.trim();
            Matcher m = PATTERN_START_WITH_PREDICATE.matcher(remaining);
            if (!m.find()) {
                if (!list) return false;
                // when looking at args,
                // allow empty list
                if (remaining.startsWith(")")) {
                    result = resultList;
                    return true;
                }
                // and allow strings
                m = PATTERN_START_WITH_QUOTED_STRING.matcher(remaining);
                if (!m.find()) {
                    return false;
                }
                result = JavaStringEscapes.unwrapJavaString(m.group());
                remaining = remaining.substring(m.end());
            } else {
                String p1 = m.group(1);
                String p2 = serialization.predicateToStringToPreferredName.get(p1);
                if (p2==null) p2 = p1;
                remaining = remaining.substring(m.end()).trim();
                
                if (!remaining.startsWith("(")) {
                    result = p2;
                } else {
                    remaining = remaining.substring(1).trim();
                    StringConstraintParser args = forArgsInternal(serialization, remaining);
                    if (!args.parse()) return false;
                    if (args.resultList.isEmpty()) {
                        result = p2;
                    } else if (args.resultList.size()==1) {
                        result = MutableMap.of(p2, Iterables.getOnlyElement(args.resultList));
                    } else { 
                        result = MutableMap.of(p2, args.result);
                    }
                    remaining = args.remaining;
                    if (!remaining.startsWith(")")) return false;
                    remaining = remaining.substring(1).trim();
                }
                if (!list) return remaining.isEmpty();
            }
            resultList.add(result);
            if (remaining.isEmpty() || remaining.startsWith(")")) {
                result = resultList;
                return true;
            }
            if (!remaining.startsWith(",")) return false;
            remaining = remaining.substring(1);
            return parse();
        }
    }

    private void collectPredicateListFromJson(Object o, Collection<Predicate<?>> result) {
        if (o instanceof Collection) {
            ((Collection<?>)o).stream().forEach(i -> collectPredicateListFromJson(i, result));
            return;
        }
        Predicate<?> p = toPredicateFromJson(o);
        if (Predicates.alwaysTrue().equals(p)) {
            // no point in keeping this one
            return;
        }
        result.add(p);
    }
    public Predicate<?> toPredicateFromJson(Object o) {
        if (o instanceof Collection) {
            @SuppressWarnings({ "rawtypes", "unchecked" })
            Predicate<?> result2 = and((List)toPredicateListFromJsonList((Collection<?>)o));
            return result2;
        }
        
        String key;
        List<Object> args;
        if (o instanceof String) {
            key = (String)o;
            if (key.indexOf("(")>=0) {
                // it wasn't json; delegate to the parser again
                StringConstraintParser parser = StringConstraintParser.forConstraint(this, key);
                if (!parser.parse()) throw new IllegalStateException("cannot match: "+key);
                return toPredicateFromJson(parser.result);
            }
            args = MutableList.of();
        } else if (o instanceof Map) {
            if (((Map<?,?>)o).size()!=1) {
                throw new IllegalArgumentException("Unsupported constraint; map input should have a single key: "+o);
            }
            // we only support single-key maps with string as key and value as list (of args) or other type as single arg, as in predicateName(args)
            key = (String) Iterables.getOnlyElement( ((Map<?,?>)o).keySet() );
            Object v = Iterables.getOnlyElement( ((Map<?,?>)o).values() );
            if (v instanceof Iterable) {
                args = MutableList.copyOf((Iterable<?>)v);
            } else {
                args = Collections.singletonList(v);
            }
        } else if (o instanceof Predicate) {
            return (Predicate<?>)o;
        } else {
            throw new IllegalArgumentException("Unsupported constraint; constraint should be string, list, or single-key map: "+o);
        }
        Function<List<?>, Predicate<?>> constructor = predicatePreferredNameToConstructor.get(key);
        if (constructor==null) {
            String preferredName = predicateToStringToPreferredName.get(key);
            if (preferredName!=null) {
                constructor = predicatePreferredNameToConstructor.get(preferredName);
                if (constructor==null) {
                    throw new IllegalArgumentException("Incomplete constraint: "+key+", maps to "+preferredName+", but no constructor known");
                }
            } else {
                throw new IllegalArgumentException("Unsupported constraint: "+key);
            }
        }
        return constructor.apply(args);
    }
    
    private <T> Predicate<?> and(Iterable<Predicate<? super T>> preds) {
        Iterator<Predicate<? super T>> pi = preds.iterator();
        if (!pi.hasNext()) return Predicates.alwaysTrue();
        Predicate<?> first = pi.next();
        if (!pi.hasNext()) return first;
        return Predicates.and(preds);
    }
    public List<Predicate<?>> toPredicateListFromJsonList(Collection<?> o) {
        Set<Predicate<?>> result = MutableSet.of();
        collectPredicateListFromJson(o, result);
        return MutableList.copyOf(result);
    }
    
}
