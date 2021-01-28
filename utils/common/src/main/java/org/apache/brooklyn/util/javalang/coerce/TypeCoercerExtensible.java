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
package org.apache.brooklyn.util.javalang.coerce;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.core.validation.BrooklynValidation;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.AnyExceptionSupplier;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.collect.Table;
import com.google.common.reflect.TypeToken;

/**
 * Attempts to coerce {@code value} to {@code targetType}.
 * <p>
 * Maintains a registry of adapter functions for type pairs in a {@link Table} which
 * is searched after checking various strategies, including the following:
 * <ul>
 * <li>{@code value.asTargetType()}
 * <li>{@code TargetType.fromType(value)} (if {@code value instanceof Type})
 * <li>{@code value.targetTypeValue()} (handy for primitives)
 * <li>{@code TargetType.valueOf(value)} (for enums)
 * </ul>
 * <p>
 * A default set of adapters will handle most common Java-type coercions
 * as well as <code>String</code> coercion to:
 * <ul>
 * <li> {@link Set}, {@link List}, {@link Map} and similar -- parses as YAML
 * <li> {@link Date} -- parses using {@link Time#parseDate(String)}
 * <li> {@link Duration} -- parses using {@link Duration#parse(String)}
 * </ul>
 */
public class TypeCoercerExtensible implements TypeCoercer {

    private static final Logger log = LoggerFactory.getLogger(TypeCoercerExtensible.class);
    
    protected TypeCoercerExtensible() {}

    /** has all the strategies (primitives, collections, etc) 
     * and all the adapters from {@link CommonAdaptorTypeCoercions} */
    public static TypeCoercerExtensible newDefault() {
        TypeCoercerExtensible result = newEmpty();
        new CommonAdaptorTypeCoercions(result).registerAllAdapters();
        new CommonAdaptorTryCoercions(result).registerAllAdapters();
        return result;
    }

    /** has all the strategies (primitives, collections, etc) but no adapters, 
     * so caller can pick and choose e.g. from {@link CommonAdaptorTypeCoercions} */
    public static TypeCoercerExtensible newEmpty() {
        return new TypeCoercerExtensible();
    }

    /** Store the coercion {@link Function functions} in a {@link Table table}. */
    private final Table<Class<?>, Class<?>, Function<?,?>> registry = HashBasedTable.create();

    /** Store the generic coercers, ordered by the name. */
    private final Map<String,TryCoercer> genericCoercersByName = Maps.newTreeMap();
    /** Put the list in a cache, reset each time the map is updated. */
    private List<TryCoercer> genericCoercers = new ArrayList<>();

    @Override
    public <T> T coerce(Object value, Class<T> targetType) {
        return coerce(value, TypeToken.of(targetType));
    }
    
    public <T> T coerce(Object value, TypeToken<T> targetTypeToken) {
        return tryCoerce(value, targetTypeToken).get();
    }
    
    @Override
    public <T> Maybe<T> tryCoerce(Object input, Class<T> type) {
        return changeExceptionSupplier( tryCoerceInternal(input, null, type) );
    }

    @Override
    public <T> Maybe<T> tryCoerce(Object value, TypeToken<T> targetTypeToken) {
        return changeExceptionSupplier( tryCoerceInternal(value, targetTypeToken, null) );
    }

    protected <T> Maybe<T> changeExceptionSupplier(Maybe<T> result) {
        return Maybe.Absent.changeExceptionSupplier(result, ClassCoercionException.class);
    }
    
    @SuppressWarnings("unchecked")
    protected <T> Maybe<T> tryCoerceInternal(Object value, TypeToken<T> targetTypeToken, Class<T> targetType) {
        return tryCoerceInternal2(value, targetTypeToken, targetType).map(BrooklynValidation.getInstance()::ensureValid);
    }

    protected <T> Maybe<T> tryCoerceInternal2(Object value, TypeToken<T> targetTypeToken, Class<T> targetType) {
        if (value==null) return Maybe.of((T)null);
        Maybe<T> result = null;
        Maybe<T> firstError = null;

        //recursive coercion of parameterized collections and map entries
        targetType = TypeTokens.getRawType(targetTypeToken, targetType);
        if (targetTypeToken!=null && targetTypeToken.getType() instanceof ParameterizedType) {
            if (value instanceof Iterable && Iterable.class.isAssignableFrom(targetType)) {
                result = tryCoerceIterable(value, targetTypeToken, targetType);
            } else if (value.getClass().isArray() && Iterable.class.isAssignableFrom(targetType)) {
                result = tryCoerceArray(value, targetTypeToken, targetType);
            } else if (value instanceof Map && Map.class.isAssignableFrom(targetType)) {
                result = tryCoerceMap(value, targetTypeToken);
            }
            if (result!=null) {
                if (result.isPresent()) return result;
                // Previous to v1.0.0 we'd overlook errors in generics with warnings; now we bail
                TypeToken<T> targetTypeTokenF = targetTypeToken;
                RuntimeException e = Maybe.getException(result);
                return Maybe.absent(new AnyExceptionSupplier<>(ClassCoercionException.class,
                    () -> "Generic type mismatch coercing "+value.getClass().getName()+" to "+targetTypeTokenF+": "+Exceptions.collapseText(e), e));
            }
        }
        
        if (targetType.isInstance(value)) return Maybe.of( (T) value );

        targetTypeToken = TypeTokens.getTypeToken(targetTypeToken, targetType);
        for (TryCoercer coercer : genericCoercers) {
            result = coercer.tryCoerce(value, targetTypeToken);
            
            if (result!=null && result.isPresentAndNonNull()) {
                // Check if need to unwrap again (e.g. if want List<Integer> and are given a String "1,2,3"
                // then we'll have so far converted to List.of("1", "2", "3"). Call recursively.
                // First check that value has changed, to avoid stack overflow!
                if (!Objects.equal(value, result.get()) && !Objects.equal(value.getClass(), result.get().getClass())
                        // previously did this just for generics but it's more useful than that, e.g. if was a WrappedValue
                        //&& targetTypeToken.getType() instanceof ParameterizedType
                        ) {
                    Maybe<T> resultM = tryCoerce(result.get(), targetTypeToken);
                    if (resultM!=null) {
                        if (resultM.isPresent()) return resultM;
                        // if couldn't coerce parameterized types then back out of this coercer
                        result = resultM;
                    }
                } else {
                    return result;
                }
            }
            
            // remember any error if we were first
            if (result!=null && firstError==null) firstError = result;
        }
        
        //ENHANCEMENT could look in type hierarchy of both types for a conversion method...
        
        //at this point, if either is primitive then run instead over boxed types
        Class<?> boxedT = Boxing.PRIMITIVE_TO_BOXED.get(targetType);
        Class<?> boxedVT = Boxing.PRIMITIVE_TO_BOXED.get(value.getClass());
        if (boxedT!=null || boxedVT!=null) {
            try {
                if (boxedT==null) boxedT=targetType;
                Object boxedV = boxedVT==null ? value : boxedVT.getConstructor(value.getClass()).newInstance(value);
                return tryCoerce(boxedV, (Class<T>)boxedT);
            } catch (Exception e) {
                return Maybe.absent(new ClassCoercionException("Cannot coerce type "+value.getClass()+" to "+targetType.getCanonicalName()+" ("+value+"): unboxing failed", e));
            }
        }
        
        //now look in registry
        synchronized (registry) {
            Map<Class<?>, Function<?,?>> adapters = registry.row(targetType);
            for (Map.Entry<Class<?>, Function<?,?>> entry : adapters.entrySet()) {
                if (entry.getKey().isInstance(value)) {
                    try {
                        T resultT = ((Function<Object,T>)entry.getValue()).apply(value);
                        
                        // Check if need to unwrap again (e.g. if want List<Integer> and are given a String "1,2,3"
                        // then we'll have so far converted to List.of("1", "2", "3"). Call recursively.
                        // First check that value has changed, to avoid stack overflow!
                        if (!Objects.equal(value, resultT) && targetTypeToken.getType() instanceof ParameterizedType) {
                            // Could duplicate check for `result instanceof Collection` etc; but recursive call
                            // will be fine as if that doesn't match we'll safely reach `targetType.isInstance(value)`
                            // and just return the result.
                            Maybe<T> resultM = tryCoerce(resultT, targetTypeToken);
                            if (resultM!=null) {
                                if (resultM.isPresent()) return resultM;
                                // if couldn't coerce parameterized types then back out of this coercer
                                // but remember the error if we were first
                                if (firstError==null) firstError = resultM;
                            }
                        } else {
                            return Maybe.of(resultT);
                        }
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        if (log.isDebugEnabled()) {
                            log.debug("When coercing, registry adapter "+entry+" gave error on "+value+" -> "+targetType+" "
                                + (firstError==null ? "(rethrowing)" : "(suppressing as there is already an error)")
                                + ": "+e, e);
                        }
                        if (firstError==null) {
                            if (e instanceof ClassCoercionException) {
                                firstError = Maybe.absent(e);
                            } else {
                                firstError = Maybe.absent(new ClassCoercionException("Cannot coerce type "+value.getClass().getCanonicalName()+" to "+targetTypeToken+" ("+value+"): registered coercer failed", e));
                            }
                        }
                        continue;
                    }
                }
            }
        }

        // not found
        if (firstError!=null) {
            // it might be nice to have more than just the first error but for now that's all we remember
            return firstError;
        }
        if (value instanceof Map) {
            if (((Map)value).containsKey("type")) {
                return Maybe.absent(new ClassCoercionException("Cannot coerce map containing {type: \""+((Map)value).get("type")+"\"} to "+targetTypeToken+": type not known or not supported here"));
            }
            return Maybe.absent(new ClassCoercionException("Cannot coerce map to "+targetTypeToken+" ("+value+"): no adapter known"));
        }
        return Maybe.absent(new ClassCoercionException("Cannot coerce type "+value.getClass().getCanonicalName()+" to "+targetTypeToken+" ("+value+"): no adapter known"));
    }

    @SuppressWarnings("unchecked")
    protected <T> Maybe<T> tryCoerceMap(Object value, TypeToken<T> targetTypeToken) {
        if (!(value instanceof Map) || !(TypeTokens.isAssignableFromRaw(Map.class, targetTypeToken))) return null;
        Type[] arguments = ((ParameterizedType) targetTypeToken.getType()).getActualTypeArguments();
        if (arguments.length != 2) {
            throw new IllegalStateException("Unexpected number of parameters in map type: " + arguments);
        }
        Map<Object,Object> coerced = Maps.newLinkedHashMap();
        TypeToken<?> mapKeyType = TypeToken.of(arguments[0]);
        TypeToken<?> mapValueType = TypeToken.of(arguments[1]);
        int i=0;
        for (Map.Entry<?,?> entry : ((Map<?,?>) value).entrySet()) {
            Maybe<?> k = tryCoerce(entry.getKey(), mapKeyType);
            if (k.isAbsent()) return Maybe.absent(new ClassCoercionException(
                "Could not coerce key of entry "+i+" ("+entry.getKey()+") to "+mapKeyType+" in "+targetTypeToken,
                ((Maybe.Absent<T>)k).getException()));

            Maybe<?> v = tryCoerce(entry.getValue(), mapValueType);
            if (v.isAbsent()) return Maybe.absent(new ClassCoercionException(
                "Could not coerce value of entry "+i+" ("+entry.getValue()+") to "+mapValueType+" in "+targetTypeToken,
                ((Maybe.Absent<T>)v).getException()));
            
            coerced.put(k.get(), v.get());
            
            i++;
        }
        return Maybe.of((T) Maps.newLinkedHashMap(coerced));
    }

    protected <T> Maybe<T> tryCoerceArray(Object value, TypeToken<T> targetTypeToken, Class<? super T> targetType) {
       List<?> listValue = Reflections.arrayToList(value);
       return tryCoerceIterable(listValue, targetTypeToken, targetType);
    }
    
    /** tries to coerce a list;
     * returns null if it just doesn't apply, a {@link Maybe.Present} if it succeeded,
     * or {@link Maybe.Absent} with a good exception if it should have applied but couldn't */
    @SuppressWarnings("unchecked")
    protected <T> Maybe<T> tryCoerceIterable(Object value, TypeToken<T> targetTypeToken, Class<? super T> targetType) {
        if (!(value instanceof Iterable) || !(TypeTokens.isAssignableFromRaw(Iterable.class, targetTypeToken))) return null;
        Type[] arguments = ((ParameterizedType) targetTypeToken.getType()).getActualTypeArguments();
        if (arguments.length != 1) {
            return Maybe.absent(new IllegalStateException("Unexpected number of parameters in iterable type: " + arguments));
        }
        Collection<Object> coerced = Lists.newLinkedList();
        TypeToken<?> listEntryType = TypeToken.of(arguments[0]);
        int i = 0;
        for (Object entry : (Iterable<?>) value) {
            Maybe<?> entryCoerced = tryCoerce(entry, listEntryType);
            if (entryCoerced.isPresent()) {
                coerced.add(entryCoerced.get());
            } else {
                return Maybe.absent(new ClassCoercionException(
                    "Could not coerce entry "+i+" ("+entry+") to "+listEntryType,
                    ((Maybe.Absent<T>)entryCoerced).getException()));
            }
            i++;
        }
        if (Set.class.isAssignableFrom(targetType)) {
            return Maybe.of((T) Sets.newLinkedHashSet(coerced));
        } else {
            return Maybe.of((T) Lists.newArrayList(coerced));
        }
    }

    /**
     * Returns a function that does a type coercion to the given type. For example,
     * {@code TypeCoercions.function(Double.class)} will return a function that will
     * coerce its input value to a {@link Double} (or throw a {@link ClassCoercionException}
     * if that is not possible).
     */
    public <T> Function<Object, T> function(final Class<T> type) {
        return new CoerceFunctionals.CoerceFunction<T>(this, type);
    }
    
    /** Registers an adapter for use with type coercion. Returns any old adapter registered for this pair. */
    @SuppressWarnings("unchecked")
    public synchronized <A,B> Function<? super A,B> registerAdapter(Class<A> sourceType, Class<B> targetType, Function<? super A,B> fn) {
        return (Function<? super A,B>) registry.put(targetType, sourceType, fn);
    }
    
    /** Registers a generic adapter for use with type coercion. */
    @Beta
    public synchronized void registerAdapter(String nameAndOrder, TryCoercer fn) {
        synchronized (genericCoercersByName) {
            genericCoercersByName.put(nameAndOrder, fn);
            genericCoercers = ImmutableList.copyOf(genericCoercersByName.values());
        }
    }
    
    /** @deprecated since introduction, use {@link #registerAdapter(String, TryCoercer)} */
    @Beta @Deprecated
    public synchronized void registerAdapter(TryCoercer fn) {
        synchronized (genericCoercersByName) {
            genericCoercersByName.put(Time.makeDateStampString()+"-"+Strings.makePaddedString(""+(genericCoercersByName.size()), 3, "0", ""), fn);
            genericCoercers = ImmutableList.copyOf(genericCoercersByName.values());
        }
    }

}
