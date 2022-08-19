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
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
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
import java.util.function.Supplier;

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
            if ((isJson(ma) && !isJson(mb)) || (ma instanceof String && Boxing.isPrimitiveOrBoxedClass(mb.getClass()))) {
                Maybe<? extends Object> mma = TypeCoercions.tryCoerce(ma, mb.getClass());
                if (mma.isPresent()) {
                    // repeat equality check
                    if (mma.get().equals(mb) || mb.equals(mma.get())) return Maybe.of(true);
                }
                return Maybe.absent("coercion not supported in equality check, to "+mb.getClass());
            }
            return Maybe.absent("coercion not permitted for equality check with these arhument types");
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
        return a!=null && a instanceof String || Boxing.isPrimitiveOrBoxedClass(a.getClass());
    }

    private static boolean isJson(Object a) {
        return isStringOrPrimitive(a) || (a instanceof Map) || (a instanceof Collection);
    }

    static boolean asStringTestOrFalse(Object value, Predicate<String> test) {
        return isStringOrPrimitive(value) ? test.test(value.toString()) : false;
    }

    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class DslPredicateBase<T> {
        public Object implicitEquals;
        public Object equals;
        public String regex;
        public String glob;

        /** nested check */
        public DslPredicate check;
        public List<DslPredicate> any;
        public List<DslPredicate> all;

        public @JsonProperty("has-element") DslPredicate hasElement;
        public Object key;
        public Integer index;
        public String jsonpath;

        public WhenPresencePredicate when;

        public @JsonProperty("in-range") Range inRange;
        public @JsonProperty("less-than") Object lessThan;
        public @JsonProperty("greater-than") Object greaterThan;
        public @JsonProperty("less-than-or-equal-to") Object lessThanOrEqualTo;
        public @JsonProperty("greater-than-or-equal-to") Object greaterThanOrEqualTo;

        public @JsonProperty("java-type-name") DslPredicate javaTypeName;
        public @JsonProperty("java-instance-of") Object javaInstanceOf;

        public static class CheckCounts {
            int checksDefined = 0;
            int checksApplicable = 0;
            int checksPassed = 0;
            public <T> void check(T test, java.util.function.Predicate<T> check) {
                if (test!=null) {
                    checksDefined++;
                    checksApplicable++;
                    if (check.test(test)) checksPassed++;
                }
            }
            public <T> void check(T test, Maybe<Object> value, java.util.function.BiPredicate<T,Object> check) {
                if (value.isPresent()) {
                    check(test, t -> check.test(t, value.get()));
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
            return applyToResolved(result);
        }

        protected void collectApplicableSpecialFieldTargetResolvers(Map<String,Function<Object,Maybe<Object>>> resolvers) {
            if (key!=null) resolvers.put("key", (value) -> {
                if (value instanceof Map) {
                    if (((Map) value).containsKey(key)) {
                        return Maybe.ofAllowingNull( ((Map) value).get(key) );
                    } else {
                        return Maybe.absent("Cannot find indicated key in map");
                    }
                } else {
                    return Maybe.absent("Cannot evaluate key on non-map target");
                }
            });

            if (index != null) resolvers.put("index", (value) -> {
                Integer i = index;
                if (value instanceof Iterable) {
                    int size = Iterables.size((Iterable) value);
                    if (i<0) {
                        i = size + i;
                    }
                    if (i<0 || i>=size) return Maybe.absent("No element at index "+i+"; size "+size);
                    return Maybe.of(Iterables.get((Iterable)value, i));
                } else {
                    return Maybe.absent("Cannot evaluate index on non-list target");
                }
            });
        }

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
            if (counts.checksDefined==0) throw new IllegalStateException("Predicate does not define any checks; if always true or always false is desired, use 'when'");
            return counts.allPassed(true);
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

            checker.check(hasElement, result, (test,value) -> {
                if (value instanceof Map) value = ((Map)value).entrySet();
                if (value instanceof Iterable) {
                    for (Object v : ((Iterable) value)) {
                        if (test.apply((T) v)) return true;
                    }
                }
                return false;
            });

            checker.check(check, test -> nestedPredicateCheck(check, result));
            checker.check(any, test -> test.stream().anyMatch(p -> nestedPredicateCheck(p, result)));
            checker.check(all, test -> test.stream().allMatch(p -> nestedPredicateCheck(p, result)));

            checker.check(javaInstanceOf, result, (test, value) -> {
                Entity ent = null;
                if (value instanceof Entity) ent = (Entity)value;
                if (ent==null) BrooklynTaskTags.getContextEntity(Tasks.current());
                if (ent==null) return false;

                Maybe<TypeToken<?>> tt = Maybe.absent("Unsupported type "+test);
                if (test instanceof String) tt = new BrooklynTypeNameResolution.BrooklynTypeNameResolver("predicate", CatalogUtils.getClassLoadingContext(ent), true, true).findTypeToken((String)test);
                else if (test instanceof Class) tt = Maybe.of(TypeToken.of((Class)test));
                else if (test instanceof TypeToken) tt = Maybe.of((TypeToken)test);
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

    @Beta
    public static class DslPredicateDefault<T2> extends DslPredicateBase<T2> implements DslPredicate<T2> {
        public DslPredicateDefault() {}
        public DslPredicateDefault(String implicitEquals) { this.implicitEquals = implicitEquals; }

        public Object target;

        public String config;
        public String sensor;
        public DslPredicate tag;

        protected void collectApplicableSpecialFieldTargetResolvers(Map<String,Function<Object, Maybe<Object>>> resolvers) {
            super.collectApplicableSpecialFieldTargetResolvers(resolvers);

            if (config!=null) resolvers.put("config", (value) -> {
                if (value instanceof Configurable) {
                    ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> ((Configurable)value).config().get(ConfigKeys.newConfigKey(Object.class, config)))
                            .as(Object.class).allowDeepResolution(true).immediately(true);
                    if (value instanceof Entity) resolver.context( (Entity)value );
                    return resolver.getMaybe();
                } else {
                    return Maybe.absent("Config not supported on " + value + " (testing config '" + config + "')");
                }
            });

            if (sensor!=null) resolvers.put("sensor", (value) -> {
                if (value instanceof Entity) {
                    ValueResolver<Object> resolver = Tasks.resolving((DeferredSupplier) () -> ((Entity)value).sensors().get(Sensors.newSensor(Object.class, sensor)))
                            .as(Object.class).allowDeepResolution(true).immediately(true);
                    return resolver.getMaybe();
                } else {
                    return Maybe.absent("Sensors not supported on " + value + " (testing sensor '" + config + "')");
                }
            });
        }

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

            result = result.isPresent() ? super.resolveTargetAgainstInput(result.get()) : result;
            return result;
        }

        protected Maybe<Object> resolveTargetStringAgainstInput(String target, Object input) {
            if ("location".equals(target) && input instanceof Entity) return Maybe.of( Locations.getLocationsCheckingAncestors(null, (Entity)input) );
            if ("children".equals(target) && input instanceof Entity) return Maybe.of( ((Entity)input).getChildren() );
            return Maybe.absent("Unsupported target '"+target+"' on input "+input);
        }

        @Override
        public void applyToResolved(Maybe<Object> result, CheckCounts checker) {
            super.applyToResolved(result, checker);

            checker.check(tag, result, this::checkTag);

            if (checker.checksDefined==0) {
                if (target!=null || config!=null || sensor!=null) {
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

    public static DslPredicate instanceOf(Object x) {
        DslEntityPredicateDefault result = new DslEntityPredicateDefault();
        result.javaInstanceOf = x;
        return result;
    }

}
