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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.annotations.Beta;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.resolve.jackson.JsonSymbolDependentDeserializer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.core.units.Range;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.SerializablePredicate;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.NaturalOrderComparator;
import org.apache.brooklyn.util.text.WildcardGlobs;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;
import java.util.function.Function;

public class DslPredicates {

    static AtomicBoolean initialized = new AtomicBoolean(false);
    public static void init() {
        if (initialized.getAndSet(true)) return;

        TypeCoercions.registerAdapter(java.util.function.Predicate.class, DslEntityPredicate.class, DslEntityPredicateAdapter::new);
        TypeCoercions.registerAdapter(java.util.function.Predicate.class, DslPredicate.class, DslPredicateAdapter::new);

        // TODO could use json shorthand instead?
        TypeCoercions.registerAdapter(String.class, DslPredicate.class, s -> {
            DslPredicateDefault result = new DslPredicateDefault();
            result.implicitEquals = s;
            return result;
        });
    }
    static {
        init();
    }

    public static enum WhenPresencePredicate {
        /** value cannot be resolved (not even as null) */ ABSENT,
        /** value is null or cannot be resolved */ ABSENT_OR_NULL,
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

        // different type hierarchies, consider coercion
        if (a instanceof String) a = ((Maybe<Object>)TypeCoercions.tryCoerce(a, b.getClass())).or(a);
        if (b instanceof String) b = ((Maybe<Object>)TypeCoercions.tryCoerce(b, a.getClass())).or(b);

        // repeat equality check
        if (a.equals(b) || b.equals(a)) return true;

        return false;
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
        return a!=null && a instanceof String || Boxing.isPrimitiveOrBoxedClass(a.getClass());
    }

    private static boolean isJson(Object a) {
        return isStringOrPrimitive(a) || (a instanceof Map) || (a instanceof Collection);
    }

    static boolean asStringTestOrFalse(Object value, Predicate<String> test) {
        return isStringOrPrimitive(value) ? test.test(value.toString()) : false;
    }

    public static class DslPredicateBase<T> {
        Object implicitEquals;
        Object equals;
        String regex;
        String glob;

        List<DslPredicate> any;
        List<DslPredicate> all;

        WhenPresencePredicate when;

        @JsonProperty("in-range") Range inRange;
        @JsonProperty("less-than") Object lessThan;
        @JsonProperty("greater-than") Object greaterThan;
        @JsonProperty("less-than-or-equal-to") Object lessThanOrEqualTo;
        @JsonProperty("greater-than-or-equal-to") Object greaterThanOrEqualTo;

        @JsonProperty("java-type-name") DslPredicate javaTypeName;
        @JsonProperty("java-instance-of") String javaInstanceOf;

        public static class CheckCounts {
            int checksTried = 0;
            int checksPassed = 0;
            public <T> void check(T test, java.util.function.Predicate<T> check) {
                if (test!=null) {
                    checksTried++;
                    if (check.test(test)) checksPassed++;
                }
            }
            public <T> void check(T test, Maybe<Object> value, java.util.function.BiPredicate<T,Object> check) {
                if (value.isPresent()) check(test, t -> check.test(t, value.get()));
            }

            public boolean allPassed() {
                return checksPassed == checksTried;
            }

            public void add(CheckCounts other) {
                checksPassed += other.checksPassed;
                checksTried += other.checksTried;
            }
        }

        public boolean apply(T input) {
            Maybe<Object> result = resolveTargetAgainstInput(input);
            return applyToResolved(result);
        }

        protected Maybe<Object> resolveTargetAgainstInput(Object input) {
            return Maybe.ofAllowingNull(input);
        }

        public boolean applyToResolved(Maybe<Object> result) {
            CheckCounts counts = new CheckCounts();
            applyToResolved(result, counts);
            return counts.allPassed();
        }

        public void applyToResolved(Maybe<Object> result, CheckCounts checker) {
            checker.check(implicitEquals, result, (test,value) -> {
                if ((!(test instanceof BrooklynObject) && value instanceof BrooklynObject) ||
                        (!(test instanceof Iterable) && value instanceof Iterable)) {
                    throw new IllegalStateException("Implicit string used for equality check comparing "+test+" with "+value+", which is probably not what was meant. Use explicit 'equals: ...' syntax for this case.");
                }
                return DslPredicates.coercedEqual(test, value);
            });
            checker.check(equals, result, DslPredicates::coercedEqual);
            checker.check(regex, result, (test,value) -> asStringTestOrFalse(value, v -> v.matches(test)));
            checker.check(glob, result, (test,value) -> asStringTestOrFalse(value, v -> WildcardGlobs.isGlobMatched(test, v)));

            checker.check(inRange, result, (test,value) ->
                // current Range only supports Integer, but this code will support any
                asStringTestOrFalse(value, v -> NaturalOrderComparator.INSTANCE.compare(""+test.min(), v)<=0 && NaturalOrderComparator.INSTANCE.compare(""+test.max(), v)>=0));
            checker.check(lessThan, result, (test,value) -> coercedCompare(value, test, x -> x<0));
            checker.check(lessThanOrEqualTo, result, (test,value) -> coercedCompare(value, test, x -> x<=0));
            checker.check(greaterThan, result, (test,value) -> coercedCompare(value, test, x -> x>0));
            checker.check(greaterThanOrEqualTo, result, (test,value) -> coercedCompare(value, test, x -> x>=0));

            checkWhen(when, result, checker);

            checker.check(any, test -> test.stream().anyMatch(p -> nestedPredicateCheck(p, result)));
            checker.check(all, test -> test.stream().allMatch(p -> nestedPredicateCheck(p, result)));

            checker.check(javaInstanceOf, result, (test, value) -> {
                Entity ent = null;
                if (value instanceof Entity) ent = (Entity)value;
                if (ent==null) BrooklynTaskTags.getContextEntity(Tasks.current());
                if (ent==null) return false;

                Maybe<TypeToken<?>> tt = new BrooklynTypeNameResolution.BrooklynTypeNameResolver("predicate", CatalogUtils.getClassLoadingContext(ent), true, true).findTypeToken(test);
                if (tt.isAbsentOrNull()) return false;

                return tt.get().isSupertypeOf(value.getClass());
            });
            checker.check(javaTypeName, (test) -> nestedPredicateCheck(test, result.map(v -> v.getClass().getName())));
        }

        protected void checkWhen(WhenPresencePredicate when, Maybe<Object> result, CheckCounts checker) {
            checker.check(when, test -> {
                switch (test) {
                    case PRESENT: return result.isPresent();
                    case PRESENT_NON_NULL: return result.isPresentAndNonNull();
                    case ABSENT: return result.isAbsent();
                    case ABSENT_OR_NULL: return result.isAbsentOrNull();
                    case ALWAYS: return true;
                    case NEVER: return false;
                    case FALSY: return result.isAbsentOrNull() && !JavaGroovyEquivalents.groovyTruth(result.get());
                    case TRUTHY: return result.isPresentAndNonNull() && JavaGroovyEquivalents.groovyTruth(result.get());
                    default: return false;
                }
            });
        }

        protected boolean nestedPredicateCheck(DslPredicate p, Maybe<Object> result) {
            return result.isPresent() ? p.apply(result.get()) : p instanceof DslEntityPredicateDefault ? ((DslEntityPredicateDefault)p).applyToResolved(result) : false;
        }
    }

    @JsonDeserialize(using= DslPredicateJsonDeserializer.class)
    public interface DslPredicate<T4> extends SerializablePredicate<T4> {
    }

    @JsonDeserialize(using= DslPredicateJsonDeserializer.class)
    public interface DslEntityPredicate extends DslPredicate<Entity> {
        default boolean applyInEntityScope (Entity entity){
            return ((EntityInternal) entity).getExecutionContext().get(Tasks.create("Evaluating predicate " + this, () -> this.apply(entity)));
        }
    }

    @Beta
    public static class DslPredicateDefault<T2> extends DslPredicateBase<T2> implements DslPredicate<T2> {
        Object target;

        /** test to be applied prior to any flattening of lists (eg if targetting children */
        DslPredicate unflattened;

        String config;
        String sensor;
        DslPredicate tag;

        protected Maybe<Object> resolveTargetAgainstInput(Object input) {
            Object target = this.target;
            Maybe<Object> result;
            if (target instanceof String) {
                result = Maybe.of( resolveTargetStringAgainstInput((String) target, input).get() );
            } else {
                if (target == null) {
                    target = input;
                }
                ValueResolver<Object> resolver = Tasks.resolving(target).as(Object.class).allowDeepResolution(true).immediately(true);
                if (input instanceof Entity) resolver = resolver.context((Entity) input);
                result = resolver.getMaybe();
            }

            return result;
        }

        protected Maybe<Object> resolveTargetStringAgainstInput(String target, Object input) {
            if ("location".equals(target) && input instanceof Entity) return Maybe.of( Locations.getLocationsCheckingAncestors(null, (Entity)input) );
            if ("children".equals(target) && input instanceof Entity) return Maybe.of( ((Entity)input).getChildren() );
            return Maybe.absent("Unsupported target '"+target+"' on input "+input);
        }

        @Override
        public void applyToResolved(Maybe<Object> result, CheckCounts checker) {
            checker.check(unflattened, test -> nestedPredicateCheck(test, result));

            if (result.isPresent() && result.get() instanceof Iterable) {
                // iterate through lists
                for (Object v : ((Iterable) result.get())) {
                    CheckCounts checker2 = new CheckCounts();
                    applyToResolvedFlattened(v instanceof Maybe ? (Maybe) v : Maybe.of(v), checker2);
                    if (checker2.allPassed()) {
                        checker.add(checker2);
                        return;
                    }
                }
                // did not pass when flattened; try with unflattened
            }

            applyToResolvedFlattened(result, checker);
        }

        public void applyToResolvedFlattened(Maybe<Object> result, CheckCounts checker) {
            if (config!=null) {
                if (sensor!=null) {
                    throw new IllegalStateException("One predicate cannot specify to test both config key '"+config+"' and sensor '"+sensor+"'; use 'all' with children");
                }

                if (result.isPresent()) {
                    Object ro = result.get();
                    if (ro instanceof Configurable) {
                        ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> ((Configurable)ro).config().get(ConfigKeys.newConfigKey(Object.class, config)))
                                .as(Object.class).allowDeepResolution(true).immediately(true);
                        if (ro instanceof Entity) resolver.context( (Entity)ro );
                        result = resolver.getMaybe();
                    } else {
                        result = Maybe.absent("Config not supported on " + ro + " (testing config '" + config + "')");
                    }
                }
            }

            if (sensor!=null) {
                if (result.isPresent()) {
                    Object ro = result.get();
                    if (ro instanceof Entity) {
                        ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> ((Entity)ro).sensors().get(Sensors.newSensor(Object.class, sensor)))
                                .as(Object.class).allowDeepResolution(true).immediately(true);
                        result = resolver.getMaybe();
                    } else {
                        result = Maybe.absent("Sensors not supported on " + ro + " (testing sensor '" + config + "')");
                    }
                }
            }

            super.applyToResolved(result, checker);
            checker.check(tag, result, this::checkTag);

            if (checker.checksTried==0) {
                if (target!=null || config!=null) {
                    // if no test specified, but test or config is, then treat as implicit presence check
                    checkWhen(WhenPresencePredicate.PRESENT_NON_NULL, result, checker);
                }
            }
        }

        public boolean checkTag(DslPredicate tagCheck, Object value) {
            if (value instanceof BrooklynObject) return ((BrooklynObject) value).tags().getTags().stream().anyMatch(tag);

            // not needed, caller iterates
            //if (value instanceof Iterable) return MutableList.of((Iterable) value).stream().anyMatch(vv -> checkTag(tagCheck, vv));

            return false;
        }

    }

    @Beta
    public static class DslEntityPredicateDefault extends DslPredicateDefault<Entity> implements DslEntityPredicate {
    }

    @Beta
    public static class DslPredicateJsonDeserializer extends JsonSymbolDependentDeserializer {
        public static final Set<Class> DSL_SUPPORTED_CLASSES = ImmutableSet.<Class>of(
            DslPredicate.class, DslEntityPredicate.class);

        public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
            return super.createContextual(ctxt, property);
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
    }

    public static class DslEntityPredicateAdapter implements DslEntityPredicate {
        java.util.function.Predicate predicate;
        public DslEntityPredicateAdapter(java.util.function.Predicate predicate) {
            this.predicate = predicate;
        }
        @Override
        public boolean apply(@Nullable Entity t) { return predicate.test(t); }
    }

}
