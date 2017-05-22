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

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.List;

import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;

/**
 * Defines and registers common generic coercers (e.g. to call a "fromXyz" method, to 
 * convert from one type to another).
 */
public class CommonAdaptorTryCoercions {

    private final TypeCoercerExtensible coercer;

    public CommonAdaptorTryCoercions(TypeCoercerExtensible coercer) {
        this.coercer = coercer;
    }

    public CommonAdaptorTryCoercions registerAllAdapters() {
        registerAdapter(new TryCoercerWithFromMethod());
        registerAdapter(new TryCoercerToEnum());
        registerAdapter(new TryCoercerForPrimitivesAndStrings());
        return this;
    }
    
    /** Registers an adapter for use with type coercion. */
    public synchronized void registerAdapter(TryCoercer fn) {
        coercer.registerAdapter(fn);
    }
    
    protected static class TryCoercerWithFromMethod implements TryCoercer {
        @Override
        @SuppressWarnings("unchecked")
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> targetType) {
            Class<? super T> rawTargetType = targetType.getRawType();
            
            List<ClassCoercionException> exceptions = Lists.newArrayList();
            //now look for static TargetType.fromType(Type t) where value instanceof Type  
            for (Method m: rawTargetType.getMethods()) {
                if (((m.getModifiers()&Modifier.STATIC)==Modifier.STATIC) && 
                        m.getName().startsWith("from") && m.getParameterTypes().length==1 &&
                        m.getParameterTypes()[0].isInstance(input)) {
                    if (m.getName().equals("from"+JavaClassNames.verySimpleClassName(m.getParameterTypes()[0]))) {
                        try {
                            return Maybe.of((T) m.invoke(null, input));
                        } catch (Exception e) {
                            exceptions.add(new ClassCoercionException("Cannot coerce type "+input.getClass()+" to "+rawTargetType.getCanonicalName()+" ("+input+"): "+m.getName()+" adapting failed", e));
                        }
                    }
                }
            }
            if (exceptions.isEmpty()) {
                return null;
            } else if (exceptions.size() == 1) {
                return Maybe.absent(exceptions.get(0));
            } else {
                String errMsg = "Failed coercing type "+input.getClass()+" to "+rawTargetType.getCanonicalName();
                return Maybe.absent(new CompoundRuntimeException(errMsg, exceptions));
            }
        }
    }
    
    protected static class TryCoercerToEnum implements TryCoercer {
        @Override
        @SuppressWarnings("unchecked")
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> targetType) {
            Class<? super T> rawTargetType = targetType.getRawType();
            
            //for enums call valueOf with the string representation of the value
            if (rawTargetType.isEnum()) {
                return EnumTypeCoercions.tryCoerceUntyped(Strings.toString(input), (Class<T>)rawTargetType);
            } else {
                return null;
            }
        }
    }

    protected static class TryCoercerForPrimitivesAndStrings implements TryCoercer {
        @Override
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> targetType) {
            return PrimitiveStringTypeCoercions.tryCoerce(input, targetType.getRawType());
        }
    }
}
