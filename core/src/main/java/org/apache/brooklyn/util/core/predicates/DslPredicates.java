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
package org.apache.brooklyn.util.core.predicates;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import com.jayway.jsonpath.JsonPath;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityAdjuncts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonSymbolDependentDeserializer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.core.units.Range;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.UserFacingException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.SerializablePredicate;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.text.WildcardGlobs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.regex.Pattern;

public class DslPredicates {

    private static final Logger LOG = LoggerFactory.getLogger(DslPredicates.class);

    static AtomicBoolean initialized = new AtomicBoolean(false);
    public static void init() {
        if (initialized.getAndSet(true)) return;

        TypeCoercions.registerAdapter(java.util.function.Predicate.class, DslEntityPredicate.class, DslEntityPredicateAdapter::new);
        TypeCoercions.registerAdapter(java.util.function.Predicate.class, DslPredicate.class, DslPredicateAdapter::new);

        // TODO could use json shorthand instead?
        TypeCoercions.registerAdapter(String.class, DslPredicate.class, DslPredicates::implicitlyEqualTo);
    }
    static {
        init();
    }

    public enum WhenPresencePredicate {
        // we might want these, but it is tricky sometimes even in our code to distinguish!
//        /** value is set but unresolvable (unset or not immediately resolvable) */ UNRESOLVABLE,
//        /** value is not set (including set to something not resolvable or not immediately resolvable) */ UNSET,
//        /** value is set to an explicit null */ NULL,

        /** value unavailable (unset or not immediately resolvable) */ ABSENT,
        /** value is null or unavailable */ ABSENT_OR_NULL,
        /** value is available, but might be null */ PRESENT,
        /** value is available and non-null (but might be 0 or empty) */ PRESENT_NON_NULL,
        /** value is available and ready/truthy (eg not false or empty) */ TRUTHY,
        /** value is unavailable or not ready/truthy (eg not false or empty) */ FALSY,
        /** always returns true */ ALWAYS,
        /** always returns false */ NEVER,
    }

    public static final boolean coercedEqual(Object a, Object b) {
        if (a==null || b==null) return a==null && b==null;

        // could handle numbers specially here, if differences between 2.0d and 2 are problematic

        // if either believes they're equal, it's fine
        if (a.equals(b) || b.equals(a)) return true;

        // if classes are equal or one is a subclass of the other, and the above check was false, that is decisive
        if (a.getClass().isAssignableFrom(b.getClass())) return false;
        if (b.getClass().isAssignableFrom(a.getClass())) return false;

        // different type hierarchies, consider coercion if one is json and not json, or one is string and the other a non-string primitive
        BiFunction<Object,Object,Maybe<Boolean>> maybeCoercedEquals = (ma,mb) -> {
            if (ma instanceof String && mb instanceof Class) {
                // if comparing to a class, look at the name
                if (ma.equals(((Class<?>) mb).getName()) || ma.equals(((Class<?>) mb).getSimpleName())) return Maybe.of(true);

            } else if ((isJson(ma) && !isJson(mb)) || (ma instanceof String && Boxing.isPrimitiveOrBoxedClass(mb.getClass()))) {
                Maybe<? extends Object> mma = TypeCoercions.tryCoerce(ma, mb.getClass());
                if (mma.isPresent()) {
                    // repeat equality check
                    if (mma.get().equals(mb) || mb.equals(mma.get())) return Maybe.of(true);
                }
                return Maybe.absent("coercion not supported in equality check, to "+mb.getClass());
            }
            return Maybe.absent("coercion not permitted for equality check with these argument types");
        };
        return maybeCoercedEquals.apply(a, b)
                .orMaybe(()->maybeCoercedEquals.apply(b, a))
                .or(false);
    }

    /** returns negative, zero, positive, or null, depending whether first arg is less than, equal, greater than, or incomparable with the second */
    @Beta
    public static final Integer coercedCompare(Object a, Object b) {
        if (a==null || b==null) return 0;

        // if either believes they're equal, believe it
        if (a.equals(b) || b.equals(a)) return 0;

        if (isStringOrPrimitive(a) && isStringOrPrimitive(b)) {
            return NaturalOrderComparator.INSTANCE.compare(a.toString(), b.toString());
        }

        // if classes are equal or one is a subclass of the other, and the above check was false, that is decisive
        if (a.getClass().isAssignableFrom(b.getClass()) && b instanceof Comparable) return ((Comparable) b).compareTo(a);
        if (b.getClass().isAssignableFrom(a.getClass()) && a instanceof Comparable) return ((Comparable) a).compareTo(b);

        BiFunction<Maybe<?>,Maybe<?>,Integer> maybeCoercedCompare = (ma,mb) -> {
            if (ma.isPresent() && mb.isPresent()) return coercedCompare(ma.get(), mb.get());
            return null;
        };
        // different type hierarchies, consider coercion
        if (isJson(a) && !isJson(b)) return maybeCoercedCompare.apply( TypeCoercions.tryCoerce(a, b.getClass()), Maybe.of(b) );
        if (isJson(b) && !isJson(a)) return maybeCoercedCompare.apply( Maybe.of(a), TypeCoercions.tryCoerce(b, a.getClass()) );

        return null;
    }

    public static final boolean coercedCompare(Object a, Object b, Function<Integer,Boolean> postProcess) {
        Integer result = coercedCompare(a, b);
        if (result==null) return false;
        return postProcess.apply(result);
    }

    private static boolean isStringOrPrimitive(Object a) {
        return a!=null && (a instanceof String || Boxing.isPrimitiveOrBoxedClass(a.getClass()));
    }

    private static boolean isJson(Object a) {
        return isStringOrPrimitive(a) || (a instanceof Map) || (a instanceof Collection);
    }

    static boolean asStringTestOrFalse(Object value, Predicate<String> test) {
        return isStringOrPrimitive(value) || value instanceof Throwable ? test.test(value.toString()) : value instanceof Class ? test.test(((Class)value).getName()) : false;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DslPredicateBase<T> {
        public Object implicitEquals;
        public Object equals;
        public String regex;
        public String glob;

        /** nested check */
        public DslPredicate check;
        public DslPredicate not;
        public List<DslPredicate> any;
        public List<DslPredicate> all;
        @JsonProperty("assert")
        public DslPredicate assertCondition;

        public @JsonProperty("has-element") DslPredicate hasElement;
        public DslPredicate size;
        public DslPredicate filter;
        public Object key;
        public Integer index;
        public String jsonpath;

        public WhenPresencePredicate when;

        public @JsonProperty("in-range") Range inRange;
        public @JsonProperty("less-than") Object lessThan;
        public @JsonProperty("greater-than") Object greaterThan;
        public @JsonProperty("less-than-or-equal-to") Object lessThanOrEqualTo;
        public @JsonProperty("greater-than-or-equal-to") Object greaterThanOrEqualTo;

        public @JsonProperty("java-instance-of") DslPredicate javaInstanceOf;
        public @JsonProperty("error-cause") DslPredicate errorCause;
        public @JsonProperty("error-field") String errorField;

        public static class CheckCounts {
            int checksDefined = 0;
            int checksApplicable = 0;
            int checksPassed = 0;

            public <T> void checkTest(T test, java.util.function.Predicate<T> predicateForTest) {
                if (test!=null) {
                    checksDefined++;
                    checksApplicable++;
                    if (predicateForTest.test(test)) checksPassed++;
                }
            }

            public <T> void check(T test, Maybe<Object> value, java.util.function.BiPredicate<T,Object> check) {
                if (value.isPresent()) {
                    checkTest(test, t -> check.test(t, value.get()));
                } else {
                    if (test!=null) {
                        checksDefined++;
                    }
                }
            }

            @Deprecated /** @deprecated since 1.1 when introduced; pass boolean */
            public boolean allPassed() {
                return checksPassed == checksDefined;
            }

            public boolean allPassed(boolean requireAtLeastOne) {
                if (checksDefined ==0 && requireAtLeastOne) return false;
                return checksPassed == checksDefined;
            }

            public void add(CheckCounts other) {
                checksPassed += other.checksPassed;
                checksDefined += other.checksDefined;
            }
        }

        public boolean apply(T input) {
            Maybe<Object> result = resolveTargetAgainstInput(input);
            if (result.isPresent() && result.get() instanceof RetargettedPredicateEvaluation) {
                return ((RetargettedPredicateEvaluation)result.get()).apply(input);
            }
            return applyToResolved(result);
        }

        protected void collectApplicableSpecialFieldTargetResolvers(Map<String,Function<Object,Maybe<Object>>> resolvers) {
            if (key!=null) resolvers.put("key", (value) -> {
                if (value instanceof Map) {
                    if (((Map) value).containsKey(key)) {
                        return Maybe.ofAllowingNull( ((Map) value).get(key) );
                    } else {
                        return Maybe.absent("Cannot find indicated key '"+key+"' in map");
                    }
                } else {
                    return Maybe.absent("Cannot evaluate key on non-map target "+classOf(value));
                }
            });

            if (index != null) resolvers.put("index", (value) -> {
                Integer i = index;
                Object v0 = value;
                if (value instanceof Map) value = ((Map)value).entrySet();
                if (value instanceof Iterable) {
                    int size = Iterables.size((Iterable) value);
                    if (i<0) {
                        i = size + i;
                    }
                    if (i<0 || i>=size) return Maybe.absent("No element at index "+i+" ("+classOf(v0)+" size "+size+")");
                    return Maybe.of(Iterables.get((Iterable)value, i));
                } else {
                    return Maybe.absent("Cannot evaluate index on non-list target "+classOf(v0));
                }
            });

            if (filter != null) resolvers.put("filter", (value) -> {
                if (value instanceof Map) value = ((Map)value).entrySet();
                if (value instanceof Iterable) {
                    return Maybe.of(Iterables.filter((Iterable) value, filter));
                } else {
                    return Maybe.absent("Cannot evaluate filter on non-list target "+classOf(value));
                }
            });

            if (jsonpath!=null) resolvers.put("jsonpath", (value) -> {
                Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
                String json;
                try {
                    json = BeanWithTypeUtils.newMapper(entity!=null ? ((EntityInternal)entity).getManagementContext() : null, false, null, false).writeValueAsString(value);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    if (LOG.isTraceEnabled()) LOG.trace("Unable to consider jsonpath for non-serializable '"+value+"' due to: "+e, e);
                    return Maybe.absent("Cannot serialize object as JSON: "+e);
                }

                String jsonpathTidied = jsonpath;
                if (jsonpathTidied!=null && !jsonpathTidied.startsWith("$")) {
                    if (jsonpathTidied.startsWith("@") || jsonpathTidied.startsWith(".") || jsonpathTidied.startsWith("[")) {
                        jsonpathTidied = '$' + jsonpathTidied;
                    } else {
                        jsonpathTidied = "$." + jsonpathTidied;
                    }
                }
                Object result;
                try {
                    result = JsonPath.read(json, jsonpathTidied);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    if (LOG.isTraceEnabled()) LOG.trace("Unable to evaluate jsonpath '"+jsonpathTidied+"' for '"+value+"' due to: "+e, e);
                    return Maybe.absent("No jsonpath matches");
                }

                // above will throw if jsonpath doesn't match anything
                // this will return single object possibly null, or a list possibly empty
                return Maybe.ofAllowingNull(result);
            });

            if (errorField!=null) resolvers.put("error-field", (value) -> {
                if (!(value instanceof Throwable)) return Maybe.absent("Unable to apply error-field to non-throwable "+value);
                Throwable t = (Throwable)value;

                Maybe<Method> m = Reflections.getMethodFromArgs(value, "get" + Strings.toInitialCapOnly(errorField), MutableList.of());
                if (m.isPresent()) return m.map(mm -> {
                    try {
                        return mm.invoke(t);
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                });

                Maybe<Object> v = Reflections.getFieldValueMaybe(value, errorField);
                if (v.isPresent()) return v;

                return Maybe.absent("No such field or getter for '"+errorField+"'");
            });

            if (errorCause!=null) resolvers.put("error-cause", (value) -> {
                if (!(value instanceof Throwable)) return Maybe.absent("Unable to apply error-field to non-throwable "+value);
                return Maybe.cast(findErrorCause(errorCause, value));
            });
        }

        private String classOf(Object x) {
            if (x==null) return "null";
            return x.getClass().getName();
        }

        /** returns the resolved, possibly redirected target for this test wrapped in a maybe, or absent if there is no valid value/target.
         * may also return {@link RetargettedPredicateEvaluation} predicate if a different predicate should be run on the same input */
        protected Maybe<Object> resolveTargetAgainstInput(Object input) {
            Map<String,Function<Object,Maybe<Object>>> specialResolvers = MutableMap.of();
            collectApplicableSpecialFieldTargetResolvers(specialResolvers);

            if (!specialResolvers.isEmpty()) {
                if (specialResolvers.size()>1) throw new IllegalStateException("Predicate has multiple incompatible target specifiers: "+specialResolvers.keySet());
                return specialResolvers.values().iterator().next().apply(input);

            } else {
                return Maybe.ofAllowingNull(input);
            }
        }

        public boolean applyToResolved(Maybe<Object> result) {
            CheckCounts counts = new CheckCounts();
            applyToResolved(result, counts);
            if (counts.checksDefined==0) {
                handleNoChecks(result, counts);
            }
            return counts.allPassed(true);
        }

        protected void handleNoChecks(Maybe<Object> result, CheckCounts checker) {
            if (errorCause!=null) {
                // if no test specified, but test or config is, then treat as implicit presence check
                checkWhen(WhenPresencePredicate.PRESENT_NON_NULL, result, checker);
                return;
            }

            // check again in case this, or a subclass, ran some checks
            if (checker.checksDefined==0) {
                throw new IllegalStateException("Predicate does not define any checks; if always true or always false is desired, use 'when'");
            }
        }

        public void applyToResolved(Maybe<Object> result, CheckCounts checker) {
            if (assertCondition!=null) failOnAssertCondition(result, checker);

            checker.check(implicitEquals, result, (test, value) -> {
                if ((!(test instanceof BrooklynObject) && value instanceof BrooklynObject) ||
                        (!(test instanceof Iterable) && value instanceof Iterable)) {
                    throw new IllegalStateException("Implicit string used for equality check comparing "+test+" with "+value+", which is probably not what was meant. Use explicit 'equals: ...' syntax for this case.");
                }
                return DslPredicates.coercedEqual(test, value);
            });
            checker.check(equals, result, DslPredicates::coercedEqual);
            checker.check(regex, result, (test, value) -> asStringTestOrFalse(value, v -> Pattern.compile(test, Pattern.DOTALL).matcher(v).matches()));
            checker.check(glob, result, (test, value) -> asStringTestOrFalse(value, v -> WildcardGlobs.isGlobMatched(test, v)));

            checker.check(inRange, result, (test,value) ->
                // current Range only supports Integer, but this code will support any
                asStringTestOrFalse(value, v -> NaturalOrderComparator.INSTANCE.compare(""+test.min(), v)<=0 && NaturalOrderComparator.INSTANCE.compare(""+test.max(), v)>=0));
            checker.check(lessThan, result, (test,value) -> coercedCompare(value, test, x -> x<0));
            checker.check(lessThanOrEqualTo, result, (test,value) -> coercedCompare(value, test, x -> x<=0));
            checker.check(greaterThan, result, (test,value) -> coercedCompare(value, test, x -> x>0));
            checker.check(greaterThanOrEqualTo, result, (test,value) -> coercedCompare(value, test, x -> x>=0));

            checkWhen(when, result, checker);

            checker.check(hasElement, result, (test,value) -> {
                if (value instanceof Map) value = ((Map)value).entrySet();
                if (value instanceof Iterable) {
                    for (Object v : ((Iterable) value)) {
                        if (test.apply((T) v)) return true;
                    }
                }
                return false;
            });
            checker.check(size, result, (test,value) -> {
                Integer computedSize = null;
                if (value instanceof CharSequence) computedSize = ((CharSequence)value).length();
                else if (value instanceof Map) computedSize = ((Map)value).size();
                else if (value instanceof Iterable) computedSize = Iterables.size((Iterable)value);
                else return nestedPredicateCheck(test, Maybe.absent("size not applicable"));

                return nestedPredicateCheck(test, Maybe.of(computedSize));
            });

            checker.checkTest(not, test -> !nestedPredicateCheck(test, result));
            checker.checkTest(check, test -> nestedPredicateCheck(test, result));
            checker.checkTest(any, test -> test.stream().anyMatch(p -> nestedPredicateCheck(p, result)));
            checker.checkTest(all, test -> test.stream().allMatch(p -> nestedPredicateCheck(p, result)));

            checker.check(javaInstanceOf, result, this::checkJavaInstanceOf);
        }

        protected void failOnAssertCondition(Maybe<Object> result, CheckCounts callerChecker) {
            callerChecker.checksDefined++;
            callerChecker.checksApplicable++;

            boolean assertionPassed;
            if (assertCondition instanceof DslPredicateBase) {

                Object implicitWhen = ((DslPredicateBase) assertCondition).implicitEquals;
                CheckCounts checker = new CheckCounts();
                if (implicitWhen!=null) {
                    WhenPresencePredicate whenT = TypeCoercions.coerce(implicitWhen, WhenPresencePredicate.class);
                    checkWhen(whenT, result, checker);
                    // can assume no other checks, if one is implicit
                } else {
                    ((DslPredicateBase) assertCondition).applyToResolved(result, checker);
                }
                assertionPassed = checker.allPassed(true);

            } else {
                assertionPassed = result.isPresent() && assertCondition.apply(result.get());
            }

            if (!assertionPassed) {
                String msg = "Assertion in DSL predicate failed";
                if (result.isAbsent()) {
                    String msg2 = "value cannot be resolved";
                    RuntimeException e = Maybe.Absent.getException(result);
                    if (e==null) {
                        throw new PredicateAssertionFailedException(msg + ": " + msg2 + " (no further details)");
                    } else {
                        throw new PredicateAssertionFailedException(msg + ": " + msg2 + ": " + Exceptions.collapseText(e), e);
                    }
                } else {
                    throw new PredicateAssertionFailedException(msg+": value '"+result.get()+"'");
                }
            }

            callerChecker.checksPassed++;
        }

        protected void checkWhen(WhenPresencePredicate when, Maybe<Object> result, CheckCounts checker) {
            checker.checkTest(when, test -> {
                switch (test) {
                    case PRESENT: return result.isPresent();
                    case PRESENT_NON_NULL: return result.isPresentAndNonNull();
                    case ABSENT: return result.isAbsent();
                    case ABSENT_OR_NULL: return result.isAbsentOrNull();
                    case ALWAYS: return true;
                    case NEVER: return false;
                    case FALSY: return result.isAbsent() || !JavaGroovyEquivalents.groovyTruth(result.get());
                    case TRUTHY: return result.isPresentAndNonNull() && JavaGroovyEquivalents.groovyTruth(result.get());
                    default: return false;
                }
            });
        }

        protected boolean checkJavaInstanceOf(DslPredicate javaInstanceOf, Object value) {
            if (value==null) return false;

            // first check if implicitly equal to a registered type
            if (javaInstanceOf instanceof DslPredicateBase) {
                Object implicitRegisteredType = ((DslPredicateBase) javaInstanceOf).implicitEquals;
                if (implicitRegisteredType instanceof String) {
                    Entity ent = null;
                    if (value instanceof Entity) ent = (Entity)value;
                    if (ent==null) ent = BrooklynTaskTags.getContextEntity(Tasks.current());
                    if (ent!=null) {
                        Maybe<TypeToken<?>> tm = new BrooklynTypeNameResolution.BrooklynTypeNameResolver("predicate", CatalogUtils.getClassLoadingContext(ent), true, true).findTypeToken((String) implicitRegisteredType);
                        if (tm.isPresent()) {
                            return tm.get().getRawType().isInstance(value);
                        }
                    }
                }
            }

            // now go through type of result and all superclasses
            Set<Class<?>> visited = MutableSet.of();
            Set<Class<?>> toVisit = MutableSet.of(value.getClass());
            while (!toVisit.isEmpty()) {
                MutableList<Class<?>> visitingNow = MutableList.copyOf(toVisit);
                toVisit.clear();
                for (Class<?> v: visitingNow) {
                    if (v==null || !visited.add(v)) continue;
                    if (nestedPredicateCheck(javaInstanceOf, Maybe.of(v))) return true;
                    toVisit.add(v.getSuperclass());
                    toVisit.addAll(Arrays.asList(v.getInterfaces()));
                }
            }
            return false;
        }

        protected Maybe<Throwable> findErrorCause(DslPredicate errorCause, Object value) {
            if (value==null || !(value instanceof Throwable)) return Maybe.absent("Cannot look for causes of non-throwable "+value);

            // now go through type of result and all superclasses
            Set<Throwable> visited = MutableSet.of();
            Set<Throwable> toVisit = MutableSet.of((Throwable)value);
            while (!toVisit.isEmpty()) {
                MutableList<Throwable> visitingNow = MutableList.copyOf(toVisit);
                toVisit.clear();
                for (Throwable v: visitingNow) {
                    if (v==null || !visited.add(v)) continue;
                    if (nestedPredicateCheck(errorCause, Maybe.of(v))) return Maybe.of(v);
                    toVisit.add(v.getCause());
                }
            }
            return Maybe.absent("Nothing in causal chain matches test");
        }

        protected boolean nestedPredicateCheck(DslPredicate p, Maybe<Object> result) {
            return result.isPresent()
                    ? p.apply(result.get())
                    : p instanceof DslPredicateBase
                    // in case it does a when: absent check
                    ? ((DslPredicateBase)p).applyToResolved(result)
                    : false;
        }
    }

    @JsonDeserialize(using=DslPredicateJsonDeserializer.class)
    public interface DslPredicate<T4> extends SerializablePredicate<T4> {
    }

    @JsonDeserialize(using=DslPredicateJsonDeserializer.class)
    public interface DslEntityPredicate extends DslPredicate<Entity> {
        default boolean applyInEntityScope (Entity entity){
            return ((EntityInternal) entity).getExecutionContext().get(Tasks.create("Evaluating predicate " + this, () -> this.apply(entity)));
        }
    }

    private static final ThreadLocal<Map<Object,Object>> PREDICATE_EVALUATION_CONTEXT = new ThreadLocal<>();

    public static Object getFromPredicateEvaluationContext(Object key) {
        Map<Object, Object> map = PREDICATE_EVALUATION_CONTEXT.get();
        if (map==null) return null;
        return map.get(key);
    }

    public static <T> boolean evaluateDslPredicateWithContext(DslPredicate<T> predicate, T target, Map<Object,Object> context) {
        if (PREDICATE_EVALUATION_CONTEXT.get()!=null) throw new IllegalStateException("Nested predicate evaluation with context not supported");
        try {
            PREDICATE_EVALUATION_CONTEXT.set(context);
            return predicate.apply(target);
        } finally {
            PREDICATE_EVALUATION_CONTEXT.remove();
        }
    }

    public static <T> boolean evaluateDslPredicateWithBrooklynObjectContext(DslPredicate<T> predicate, T target, BrooklynObject bo) {
        return evaluateDslPredicateWithContext(predicate, target, MutableMap.of(
                Configurable.class, bo,
                BrooklynObject.class, bo,
                Entity.class, EntityAdjuncts.getEntity(bo, true).orNull()));
    }

    @Beta
    public static class DslPredicateDefault<T2> extends DslPredicateBase<T2> implements DslPredicate<T2>, Cloneable {
        public DslPredicateDefault() {}

        // allow a string or int to be an implicit equality target
        public DslPredicateDefault(String implicitEquals) { this.implicitEquals = implicitEquals; }
        public DslPredicateDefault(Integer implicitEquals) { this.implicitEquals = implicitEquals; }

        public Object target;

        public String config;
        public String sensor;
        public DslPredicate tag;

        @Override
        protected DslPredicateDefault<T2> clone() {
            try {
                return (DslPredicateDefault<T2>) super.clone();
            } catch (CloneNotSupportedException e) {
                throw Exceptions.propagate(e);
            }
        }

        protected <T> T getTypeFromValueOrContext(Class<?> type, Object value) {
            if (type==null || value==null) return (T) value;

            if (Entity.class.isAssignableFrom(type)) {
                // if Entity wanted, try to extract from adjunct
                if (value instanceof BrooklynObject) value = EntityAdjuncts.getEntity((BrooklynObject) value, true).orNull();
            }
            if (type.isInstance(value)) return (T) value;
            Object v2 = getFromPredicateEvaluationContext(Entity.class);
            if (type.isInstance(v2)) return (T) v2;
            if (v2!=null) throw new IllegalStateException("DSL predicate context for "+type+" is incompatible "+v2+" ("+v2.getClass()+")");
            return null;
        }

        protected void collectApplicableSpecialFieldTargetResolvers(Map<String,Function<Object, Maybe<Object>>> resolvers) {
            super.collectApplicableSpecialFieldTargetResolvers(resolvers);

            if (config!=null) resolvers.put("config", (value) -> {
                Configurable cv;
                if (value instanceof Configurable) {
                    cv = (Configurable) value;
                } else {
                    cv = (Configurable) getFromPredicateEvaluationContext(Configurable.class);
                }
                if (cv!=null) {
                    ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> cv.config().get(ConfigKeys.newConfigKey(Object.class, config)))
                            .as(Object.class).allowDeepResolution(true).immediately(true);

                    Entity entity = getTypeFromValueOrContext(Entity.class, value);
                    if (entity!=null) resolver.context( entity );
                    Maybe<Object> result = resolver.getMaybe();
                    if (result.isAbsent()) {
                        if (entity==null && BrooklynTaskTags.getContextEntity(Tasks.current())==null) {
                            throw new IllegalStateException("Unable to resolve config '"+config+"' on "+value+", likely because outside of an entity task unless entity target or context explicitly supplied");
                        }
                    }
                    return result;
                } else {
                    return Maybe.absent("Config not supported on " + value + " and no applicable DslPredicate context (testing config '" + config + "')");
                }
            });

            if (sensor!=null) resolvers.put("sensor", (value) -> {
                Entity entity = getTypeFromValueOrContext(Entity.class, value);
                if (entity!=null) {
                    ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> (entity).sensors().get(Sensors.newSensor(Object.class, sensor)))
                            .as(Object.class).allowDeepResolution(true).immediately(true).context(entity);
                    return resolver.getMaybe();
                } else {
                    return Maybe.absent("Sensors not supported on " + value + " and no applicable DslPredicate context (testing sensor '" + sensor + "')");
                }
            });
        }

        protected Maybe<Object> resolveTargetAgainstInput(Object input) {
            Object target = this.target;
            Maybe<Object> result;
            if (target instanceof String) {
                result = Maybe.of( resolveTargetStringAgainstInput((String) target, input).get() );

                if (result.isPresent() && result.get() instanceof RetargettedPredicateEvaluation) {
                    // do retargetting before doing further resolution (of other keys, e.g. config)
                    return result;
                }
            } else {
                if (target == null) {
                    target = input;
                }
                ValueResolver<Object> resolver = Tasks.resolving(target).as(Object.class).allowDeepResolution(true).immediately(true);
                Entity entity = getTypeFromValueOrContext(Entity.class, input);
                if (entity!=null) resolver = resolver.context(entity);
                result = resolver.getMaybe();
            }

            result = result.isPresent() ? super.resolveTargetAgainstInput(result.get()) : result;
            return result;
        }

        protected Maybe<Object> resolveTargetStringAgainstInput(String target, Object input) {
            Maybe<Object> candidate;
            candidate = resolvePluralNormallyOrSingularAsHasElementRetargettedPredicate(target, input,
                    "locations", Entity.class, x -> Maybe.of(Locations.getLocationsCheckingAncestors(null, (Entity) x)),
                    "location", x -> x instanceof Location);
            if (candidate!=null) return candidate;
            candidate = resolvePluralNormallyOrSingularAsHasElementRetargettedPredicate(target, input,
                    "tags", BrooklynObject.class, x -> Maybe.of( ((BrooklynObject)x).tags().getTags() ),
                    "tag", x -> false);
            if (candidate!=null) return candidate;
            candidate = resolvePluralNormallyOrSingularAsHasElementRetargettedPredicate(target, input,
                    "children", Entity.class, x -> Maybe.of(((Entity) x).getChildren()),
                    "child", x -> false);
            if (candidate!=null) return candidate;

            return Maybe.absent("Unsupported target '"+target+"' on input "+input);
        }

        protected <T> Maybe<Object> resolveAsHasElementRetargettedPredicate(String target, Object inputValue, Predicate<Object> checkPredicateRetargettingNotNeeded, Class<T> suitableTargetType, Function<Object,Maybe<Object>> retargetValue) {
            if (inputValue == null || checkPredicateRetargettingNotNeeded.test(inputValue)) {
                // already processsed
                return Maybe.of(inputValue);
            }
            if (hasElement!=null) {
                // caller is already asking for a member of this list, don't rewrite
                return retargetValue.apply(inputValue);
            }

            T retargettableTarget = getTypeFromValueOrContext(suitableTargetType, inputValue);
            if (retargettableTarget==null) {
                return Maybe.absent("Target " + target + " not applicable to " + inputValue);
            }

            // keyword 'location' means to re-run checking any element
            RetargettedPredicateEvaluation retargetPredicate = new RetargettedPredicateEvaluation();
            retargetPredicate.target = target;
            retargetPredicate.hasElement = this.clone();
            ((DslPredicateDefault)retargetPredicate.hasElement).target = null;
            return Maybe.of(retargetPredicate);
        }

        protected <T> Maybe<Object> resolvePluralNormallyOrSingularAsHasElementRetargettedPredicate(String target, Object inputValue, String plural, Class<T> suitableTargetType, Function<Object,Maybe<Object>> retargetValue,
                                                                                                String singular, Predicate<Object> checkSingularTargetPredicateRetargettingNotNeeded) {
            if (plural.equals(target)) {
                T retargettableTarget = getTypeFromValueOrContext(suitableTargetType, inputValue);

                if (retargettableTarget==null) {
                    return Maybe.absent("Target " + target + " not applicable to " + inputValue);
                }
                return retargetValue.apply(retargettableTarget);
            }

            if (singular.equals(target)) {
                return resolveAsHasElementRetargettedPredicate(target, inputValue, checkSingularTargetPredicateRetargettingNotNeeded, suitableTargetType, retargetValue);
            }

            // checks didn't apply
            return null;
        }

        @Override
        public void applyToResolved(Maybe<Object> result, CheckCounts checker) {
            super.applyToResolved(result, checker);

            checker.check(tag, result, this::checkTag);
        }

        @Override
        protected void handleNoChecks(Maybe<Object> result, CheckCounts checker) {
            if (target!=null || config!=null || sensor!=null) {
                // if no test specified, but test or config is, then treat as implicit presence check
                checkWhen(WhenPresencePredicate.PRESENT_NON_NULL, result, checker);
                return;
            }

            super.handleNoChecks(result, checker);
        }

        public boolean checkTag(DslPredicate tagCheck, Object value) {
            if (value instanceof BrooklynObject) return ((BrooklynObject) value).tags().getTags().stream().anyMatch(tag);

            return false;
        }

    }

    @Beta
    public static class DslEntityPredicateDefault extends DslPredicateDefault<Entity> implements DslEntityPredicate {
        public DslEntityPredicateDefault() { super(); }
        public DslEntityPredicateDefault(String implicitEquals) { super(implicitEquals); }
    }

    @Beta
    public static class DslPredicateJsonDeserializer extends JsonSymbolDependentDeserializer {
        public static final Set<Class> DSL_SUPPORTED_CLASSES = ImmutableSet.<Class>of(
            java.util.function.Predicate.class, Predicate.class,
            DslPredicate.class, DslEntityPredicate.class);

        public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
            return super.createContextual(ctxt, property);
        }

        protected boolean isTypeReplaceableByDefault() {
            if (super.isTypeReplaceableByDefault()) return true;
            if (type.getRawClass().isInterface()) return true;
            return false;
        }

        @Override
        public JavaType getDefaultType() {
            return ctxt.constructType(DslPredicateDefault.class);
        }

        @Override
        protected Object deserializeObject(JsonParser p0) throws IOException {
            MutableList<Throwable> errors = MutableList.of();
            TokenBuffer pb = BrooklynJacksonSerializationUtils.createBufferForParserCurrentObject(p0, ctxt);

            try {
                Object raw = null;
                JsonDeserializer<?> deser;

                if (DSL_SUPPORTED_CLASSES.contains(type.getRawClass())) {
                    // just load a map/string, then handle it
                    deser = ctxt.findRootValueDeserializer(ctxt.constructType(Object.class));
                    raw = deser.deserialize(pb.asParserOnFirstToken(), ctxt);

                } else {
                    // if type is more specific, don't use above routine
                    // in fact, just fall through to trying super
//                    deser = CommonTypesSerialization.createBeanDeserializer(ctxt, type);
//                    raw = deser.deserialize(p, ctxt);
                }

                if (raw instanceof Map) {
                    if (((Map<?, ?>) raw).containsKey("type")) {
                        // fall through to trying super
                        raw = null;
                    } else {
                        // read as predicate instead
                        deser = ctxt.findRootValueDeserializer(ctxt.constructType(DslEntityPredicateDefault.class));
                        raw = deser.deserialize(pb.asParserOnFirstToken(), ctxt);
                    }
                }
                if (type.getRawClass().isInstance(raw)) {
                    return raw;
                }
                if (raw instanceof Predicate || raw instanceof java.util.function.Predicate) {
                    return TypeCoercions.coerce(raw, type.getRawClass());
                }

                if (raw!=null) errors.add(new IllegalArgumentException("Cannot parse '"+raw+"' as a "+type));

            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                errors.add(e);
            }

            try {
                // try super
                return super.deserializeObject(BrooklynJacksonSerializationUtils.createParserFromTokenBufferAndParser(pb, p0));
            } catch (Exception e) {
                errors.add(e);
            }

            throw Exceptions.propagate("Unable to read "+DslPredicate.class, errors);
        }
    }

    public static class DslPredicateAdapter implements DslPredicate {
        java.util.function.Predicate predicate;
        public DslPredicateAdapter(java.util.function.Predicate predicate) {
            this.predicate = predicate;
        }
        @Override
        public boolean apply(@Nullable Object t) { return predicate.test(t); }

        public static DslPredicate of(Predicate<?> p) {
            if (p instanceof DslPredicate) return (DslPredicate) p;
            return new DslPredicateAdapter(p);
        }
    }

    public static class DslEntityPredicateAdapter implements DslEntityPredicate {
        java.util.function.Predicate predicate;
        public DslEntityPredicateAdapter(java.util.function.Predicate predicate) {
            this.predicate = predicate;
        }
        @Override
        public boolean apply(@Nullable Entity t) { return predicate.test(t); }

        public static DslEntityPredicate of(Predicate<? super Entity> p) {
            if (p instanceof DslEntityPredicate) return (DslEntityPredicate) p;
            return new DslEntityPredicateAdapter(p);
        }
    }

    public static DslPredicate alwaysFalse() {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.when = WhenPresencePredicate.NEVER;
        return result;
    }

    public static DslPredicate alwaysTrue() {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.when = WhenPresencePredicate.ALWAYS;
        return result;
    }

    public static DslPredicate equalTo(Object x) {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.equals = x;
        return result;
    }

    public static DslPredicate implicitlyEqualTo(Object x) {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.implicitEquals = x;
        return result;
    }

    public static DslPredicate instanceOf(Object x) {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.javaInstanceOf = x instanceof DslPredicate ? ((DslPredicate) x) : implicitlyEqualTo(x);
        return result;
    }

    static class RetargettedPredicateEvaluation<T> extends DslPredicateDefault<T> {
    }

    public static class PredicateAssertionFailedException extends UserFacingException {
        public PredicateAssertionFailedException(String msg) { super(msg); }
        public PredicateAssertionFailedException(String msg, Throwable cause) { super(msg, cause); }
    }

}
