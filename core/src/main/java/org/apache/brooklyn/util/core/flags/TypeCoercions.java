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
package org.apache.brooklyn.util.core.flags;

import java.lang.reflect.Constructor;
import java.util.Map;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.internal.BrooklynInitialization;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.usage.UsageListener;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.javalang.coerce.CommonAdaptorTryCoercions;
import org.apache.brooklyn.util.javalang.coerce.CommonAdaptorTypeCoercions;
import org.apache.brooklyn.util.javalang.coerce.EnumTypeCoercions;
import org.apache.brooklyn.util.javalang.coerce.PrimitiveStringTypeCoercions;
import org.apache.brooklyn.util.javalang.coerce.TryCoercer;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

import groovy.lang.Closure;
import groovy.time.TimeDuration;

/** Static class providing a shared {@link TypeCoercer} for all of Brooklyn */
public class TypeCoercions {

    private static final Logger log = LoggerFactory.getLogger(TypeCoercions.class);
    
    private TypeCoercions() {}

    private static final TypeCoercerExtensible coercer;
    static {
        coercer = TypeCoercerExtensible.newEmpty();
        BrooklynInitialization.initTypeCoercionStandardAdapters(); 
    }
    
    public static void initStandardAdapters() {
        new BrooklynCommonAdaptorTypeCoercions(coercer).registerAllAdapters();
        new CommonAdaptorTryCoercions(coercer).registerAllAdapters();
        registerDeprecatedBrooklynAdapters();
        registerBrooklynAdapters();
        registerGroovyAdapters();
    }
    
    public static <T> T coerce(Object input, Class<T> type) { return coercer.coerce(input, type); }
    public static <T> T coerce(Object input, TypeToken<T> type) { return coercer.coerce(input, type); }
    public static <T> Maybe<T> tryCoerce(Object input, Class<T> type) { return coercer.tryCoerce(input, type); }
    public static <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) { return coercer.tryCoerce(input, type); }

    public static <A,B> Function<? super A,B> registerAdapter(Class<A> sourceType, Class<B> targetType, Function<? super A,B> fn) {
        return coercer.registerAdapter(sourceType, targetType, fn);
    }
    
    @Beta
    public static void registerAdapter(TryCoercer fn) {
        coercer.registerAdapter(fn);
    }
    
    public static <T> Function<Object, T> function(final Class<T> type) {
        return coercer.function(type);
    }

    public static void registerDeprecatedBrooklynAdapters() {
    }
    
    @SuppressWarnings("rawtypes")
    public static void registerBrooklynAdapters() {
        registerAdapter(String.class, AttributeSensor.class, new Function<String,AttributeSensor>() {
            @Override
            public AttributeSensor apply(final String input) {
                Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
                if (entity!=null) {
                    Sensor<?> result = entity.getEntityType().getSensor(input);
                    if (result instanceof AttributeSensor) 
                        return (AttributeSensor) result;
                }
                return Sensors.newSensor(Object.class, input);
            }
        });
        registerAdapter(String.class, Sensor.class, new Function<String,Sensor>() {
            @Override
            public AttributeSensor apply(final String input) {
                Entity entity = BrooklynTaskTags.getContextEntity(Tasks.current());
                if (entity!=null) {
                    Sensor<?> result = entity.getEntityType().getSensor(input);
                    if (result != null) 
                        return (AttributeSensor) result;
                }
                return Sensors.newSensor(Object.class, input);
            }
        });
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    @SuppressWarnings("rawtypes")
    public static void registerGroovyAdapters() {
        registerAdapter(Closure.class, Predicate.class, new Function<Closure,Predicate>() {
            @Override
            public Predicate<?> apply(final Closure closure) {
                log.warn("Use of groovy.lang.Closure is deprecated, in TypeCoercions Closure->Predicate");
                return new Predicate<Object>() {
                    @Override public boolean apply(Object input) {
                        return (Boolean) closure.call(input);
                    }
                };
            }
        });
        registerAdapter(Closure.class, Function.class, new Function<Closure,Function>() {
            @Override
            public Function apply(final Closure closure) {
                log.warn("Use of groovy.lang.Closure is deprecated, in TypeCoercions Closure->Function");
                return new Function() {
                    @Override public Object apply(Object input) {
                        return closure.call(input);
                    }
                };
            }
        });
        registerAdapter(Object.class, TimeDuration.class, new Function<Object,TimeDuration>() {
            @Override
            public TimeDuration apply(final Object input) {
                log.warn("deprecated automatic coercion of Object to TimeDuration (set breakpoint in TypeCoercions to inspect, convert to Duration)");
                return JavaGroovyEquivalents.toTimeDuration(input);
            }
        });
        registerAdapter(TimeDuration.class, Long.class, new Function<TimeDuration,Long>() {
            @Override
            public Long apply(final TimeDuration input) {
                log.warn("deprecated automatic coercion of TimeDuration to Long (set breakpoint in TypeCoercions to inspect, use Duration instead of Long!)");
                return input.toMilliseconds();
            }
        });
    }

    // ---- legacy compatibility

    /** @deprecated since 0.10.0 see method in {@link EnumTypeCoercions} */ @Deprecated
    public static <E extends Enum<E>> Function<String, E> stringToEnum(final Class<E> type, @Nullable final E defaultValue) {
        return EnumTypeCoercions.stringToEnum(type, defaultValue);
    }
        
    /** @deprecated since 0.10.0 see method in {@link PrimitiveStringTypeCoercions} */ @Deprecated
    public static <T> T castPrimitive(Object value, Class<T> targetType) {
        return PrimitiveStringTypeCoercions.castPrimitive(value, targetType);
    }
    
    /** @deprecated since 0.10.0 see method in {@link PrimitiveStringTypeCoercions} */ @Deprecated
    public static boolean isPrimitiveOrBoxer(Class<?> type) {
        return PrimitiveStringTypeCoercions.isPrimitiveOrBoxer(type);
    }

    /** @deprecated since 0.10.0 see method in {@link PrimitiveStringTypeCoercions} */ @Deprecated
    public static <T> T stringToPrimitive(String value, Class<T> targetType) {
        return PrimitiveStringTypeCoercions.stringToPrimitive(value, targetType);
    }
    
    /** @deprecated since 0.10.0 see {@link JavaClassNames#verySimpleClassName(Class)} */ @Deprecated
    @SuppressWarnings("rawtypes")
    public static String getVerySimpleName(Class c) {
        return JavaClassNames.verySimpleClassName(c);
    }
    
    /** @deprecated since 0.10.0 see {@link Boxing#PRIMITIVE_TO_BOXED} and its <code>inverse()</code> method */
    @Deprecated
    @SuppressWarnings("rawtypes")
    public static final Map<Class,Class> BOXED_TO_UNBOXED_TYPES = ImmutableMap.<Class,Class>builder().
            put(Integer.class, Integer.TYPE).
            put(Long.class, Long.TYPE).
            put(Boolean.class, Boolean.TYPE).
            put(Byte.class, Byte.TYPE).
            put(Double.class, Double.TYPE).
            put(Float.class, Float.TYPE).
            put(Character.class, Character.TYPE).
            put(Short.class, Short.TYPE).
            build();
    /** @deprecated since 0.10.0 see {@link Boxing#PRIMITIVE_TO_BOXED} */ @Deprecated
    @SuppressWarnings("rawtypes")
    public static final Map<Class,Class> UNBOXED_TO_BOXED_TYPES = ImmutableMap.<Class,Class>builder().
            put(Integer.TYPE, Integer.class).
            put(Long.TYPE, Long.class).
            put(Boolean.TYPE, Boolean.class).
            put(Byte.TYPE, Byte.class).
            put(Double.TYPE, Double.class).
            put(Float.TYPE, Float.class).
            put(Character.TYPE, Character.class).
            put(Short.TYPE, Short.class).
            build();
    
    /** for automatic conversion;
     * @deprecated since 0.10.0 not used; there may be something similar in {@link Reflections} */ 
    @Deprecated
    @SuppressWarnings("rawtypes")
    public static Object getMatchingConstructor(Class target, Object ...arguments) {
        Constructor[] cc = target.getConstructors();
        for (Constructor c: cc) {
            if (c.getParameterTypes().length != arguments.length)
                continue;
            boolean matches = true;
            Class[] tt = c.getParameterTypes();
            for (int i=0; i<tt.length; i++) {
                if (arguments[i]!=null && !tt[i].isInstance(arguments[i])) {
                    matches=false;
                    break;
                }
            }
            if (matches) 
                return c;
        }
        return null;
    }
    
    public static class BrooklynCommonAdaptorTypeCoercions extends CommonAdaptorTypeCoercions {
        
        public BrooklynCommonAdaptorTypeCoercions(TypeCoercerExtensible coercer) { super(coercer); }

        @SuppressWarnings("rawtypes")
        @Override
        public void registerClassForNameAdapters() {
            registerAdapter(String.class, Class.class, new Function<String,Class>() {
                @Override
                public Class apply(final String input) {
                    try {
                        //return Class.forName(input);
                        return new ClassLoaderUtils(this.getClass()).loadClass(input);
                    } catch (ClassNotFoundException e) {
                        throw Exceptions.propagate(e);
                    }
                }
            });        
        }
        
        public static <T> void registerInstanceForClassnameAdapter(ClassLoaderUtils loader, Class<T> supertype) {
            TypeCoercions.registerAdapter(String.class, supertype, new Function<String, T>() {
                @Override public T apply(String input) {
                    Class<?> clazz;
                    try {
                        clazz = loader.loadClass(input);
                    } catch (ClassNotFoundException e) {
                        throw new IllegalStateException("Failed to load " + supertype.getSimpleName() + " class " + input, e);
                    }
                    Maybe<Object> result = Reflections.invokeConstructorFromArgs(clazz);
                    if (result.isPresentAndNonNull() && supertype.isInstance(result.get())) {
                        return (T) result.get();
                    } else if (result.isPresent()) {
                        throw new IllegalStateException("Object is not a " + supertype.getSimpleName()+": " + result.get());
                    } else {
                        throw new IllegalStateException("Failed to create "+supertype.getSimpleName()+" from class name '"+input+"' using no-arg constructor");
                    }
                }
            });
        }
    }

    public static TypeCoercer asTypeCoercer() {
        return new TypeCoercer() {
            @Override public <T> T coerce(Object input, Class<T> type) {
                return TypeCoercions.coerce(input, type);
            }
            @Override public <T> Maybe<T> tryCoerce(Object input, Class<T> type) {
                return TypeCoercions.tryCoerce(input, type);
            }
            @Override public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
                return TypeCoercions.tryCoerce(input, type);
            }
        };
    }
}
