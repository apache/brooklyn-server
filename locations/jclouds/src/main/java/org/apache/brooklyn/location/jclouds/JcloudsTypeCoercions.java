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
package org.apache.brooklyn.location.jclouds;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.util.core.flags.MethodCoercions;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.coerce.TryCoercer;
import org.jclouds.json.SerializedNames;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.collect.Sets.SetView;
import com.google.common.reflect.TypeToken;

public class JcloudsTypeCoercions {

    private JcloudsTypeCoercions() {}
    
    private static final AtomicBoolean initialized = new AtomicBoolean(false);

    public static void init() {
        synchronized (initialized) {
            if (initialized.compareAndSet(false, true)) {
                TypeCoercions.registerAdapter(new CoercionFromAutoValueBuilder());
                TypeCoercions.registerAdapter(new CoercionFromAutoValueCreate());
            }
        }
    }
    
    static class CoercionFromAutoValueBuilder implements TryCoercer {
        @Override
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> targetTypeToken) {
            Class<? super T> targetType = targetTypeToken.getRawType();

            // If we don't have a map of named params, we can't map it to builder methods
            if (!(input instanceof Map)) {
                return null;
            }

            Optional<Method> builderMethod = findBuilderMethod(targetType);
            if (!builderMethod.isPresent()) return null;
            
            Maybe<?> builder = MethodCoercions.tryFindAndInvokeMultiParameterMethod(targetType, ImmutableList.of(builderMethod.get()), ImmutableList.of());
            if (builder.isAbsent()) {
                return (Maybe<T>) (Maybe) builder;
            }

            Optional<Method> buildMethod = findBuildMethod(builder.get());
            if (!buildMethod.isPresent()) {
                return Maybe.absent("Builder for "+targetType.getCanonicalName()+" has no build() method");
            }
            
            for (Map.Entry<?, ?> entry : ((Map<?,?>)input).entrySet()) {
                String key = (String) entry.getKey();
                Object val = entry.getValue();
                
                Maybe<?> invoked = MethodCoercions.tryFindAndInvokeBestMatchingMethod(builder.get(), key, val);
                if (invoked.isAbsent()) {
                    RuntimeException cause = Maybe.Absent.getException(invoked);
                    return Maybe.absent("Builder for "+targetType.getCanonicalName()+" failed to call method for "+key, cause);
                }
            }
            
            Maybe<?> result = MethodCoercions.tryFindAndInvokeMultiParameterMethod(builder.get(), ImmutableList.of(buildMethod.get()), ImmutableList.of());
            if (result.isAbsent()) {
                RuntimeException cause = Maybe.Absent.getException(result);
                return Maybe.absent("Builder for "+targetType.getCanonicalName()+" failed to call build method", cause);
            } else {
                return (Maybe<T>) (Maybe) result;
            }
        }
        
        private Optional<Method> findBuilderMethod(Class<?> clazz) {
            for (Method m: clazz.getMethods()) {
                if (((m.getModifiers()&Modifier.STATIC) == Modifier.STATIC) && m.getName().equals("builder")
                        && m.getParameterTypes().length == 0) {
                    return Optional.of(m);
                }
            }
            return Optional.empty();
        }
        
        private Optional<Method> findBuildMethod(Object instance) {
            for (Method m: instance.getClass().getMethods()) {
                if (((m.getModifiers()&Modifier.STATIC) != Modifier.STATIC) && m.getName().equals("build")
                        && m.getParameterTypes().length == 0) {
                    return Optional.of(m);
                }
            }
            return Optional.empty();
        }
    }
    
    static class CoercionFromAutoValueCreate implements TryCoercer {
        @Override
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> targetTypeToken) {
            Class<? super T> targetType = targetTypeToken.getRawType();
            Maybe<?> result = null;
            Maybe<?> firstError = null;

            // If we don't have a map of named params, we can't map it to "@SerializedNames"?
            if (!(input instanceof Map)) {
                return null;
            }

            Optional<Method> method = findCreateMethod(targetType);
            if (!method.isPresent()) return null;
            
            // Do we have a map of args, and need to look at the "@SerializedNames" annotation?
            if (method.get().isAnnotationPresent(SerializedNames.class)) {
                String[] argNames = method.get().getAnnotation(SerializedNames.class).value();
                
                SetView<?> extraArgs = Sets.difference(((Map<?,?>)input).keySet(), ImmutableSet.copyOf(argNames));
                if (!extraArgs.isEmpty()) {
                    return Maybe.absent("create() for "+targetType.getCanonicalName()+" does not accept extra args "+extraArgs);
                }
                
                List<Object> orderedArgs = Lists.newArrayList();
                for (String argName : argNames) {
                    orderedArgs.add(((Map<?,?>)input).get(argName));
                }
                result = MethodCoercions.tryFindAndInvokeMultiParameterMethod(targetType, ImmutableList.of(method.get()), orderedArgs);
                if (result != null && result.isPresent()) return Maybe.of((T)result.get());
                if (result != null && firstError == null) firstError = result;
            }
            
            //not found
            if (firstError != null) return (Maybe<T>) (Maybe) firstError;
            return null;
        }
        
        private Optional<Method> findCreateMethod(Class<?> clazz) {
            for (Method m: clazz.getMethods()) {
                if (((m.getModifiers()&Modifier.STATIC) == Modifier.STATIC) && m.getName().equals("create")
                        && clazz.isAssignableFrom(m.getReturnType())) {
                    return Optional.of(m);
                }
            }
            return Optional.empty();
        }
    }
}
