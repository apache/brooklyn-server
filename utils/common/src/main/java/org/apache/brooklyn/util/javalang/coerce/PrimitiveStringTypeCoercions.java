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

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;

import com.google.common.primitives.Primitives;

public class PrimitiveStringTypeCoercions {

    public PrimitiveStringTypeCoercions() {}
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <T> Maybe<T> tryCoerce(Object value, Class<? super T> targetType) {
        //deal with primitive->primitive casting
        if (isPrimitiveOrBoxer(targetType) && isPrimitiveOrBoxer(value.getClass())) {
            // Don't just rely on Java to do its normal casting later; if caller writes
            // long `l = coerce(new Integer(1), Long.class)` then letting java do its casting will fail,
            // because an Integer will not automatically be unboxed and cast to a long
            return Maybe.of(castPrimitive(value, (Class<T>)targetType));
        }

        //deal with string->primitive
        if (value instanceof String && isPrimitiveOrBoxer(targetType)) {
            return Maybe.of(stringToPrimitive((String)value, (Class<T>)targetType));
        }

        //deal with primitive->string
        if (isPrimitiveOrBoxer(value.getClass()) && targetType.equals(String.class)) {
            return Maybe.of((T) value.toString());
        }

        //look for value.asType where Type is castable to targetType
        String targetTypeSimpleName = JavaClassNames.verySimpleClassName(targetType);
        if (targetTypeSimpleName!=null && targetTypeSimpleName.length()>0) {
            for (Method m: value.getClass().getMethods()) {
                if (m.getName().startsWith("as") && m.getParameterTypes().length==0 &&
                        targetType.isAssignableFrom(m.getReturnType()) ) {
                    if (m.getName().equals("as"+JavaClassNames.verySimpleClassName(m.getReturnType()))) {
                        try {
                            return Maybe.of((T) m.invoke(value));
                        } catch (Exception e) {
                            Exceptions.propagateIfFatal(e);
                            return Maybe.absent(new ClassCoercionException("Cannot coerce type "+value.getClass()+" to "+targetType.getCanonicalName()+" ("+value+"): "+m.getName()+" adapting failed, "+e));
                        }
                    }
                }
            }
        }
        
        return null;
    }
    
    /**
     * Sometimes need to explicitly cast primitives, rather than relying on Java casting.
     * For example, when using generics then type-erasure means it doesn't actually cast,
     * which causes tests to fail with 0 != 0.0
     */
    @SuppressWarnings("unchecked")
    public static <T> T castPrimitive(Object value, Class<T> targetType) {
        if (value==null) return null;
        assert isPrimitiveOrBoxer(targetType) : "targetType="+targetType;
        assert isPrimitiveOrBoxer(value.getClass()) : "value="+targetType+"; valueType="+value.getClass();

        Class<?> sourceWrapType = Primitives.wrap(value.getClass());
        Class<?> targetWrapType = Primitives.wrap(targetType);
        
        // optimization, for when already correct type
        if (sourceWrapType == targetWrapType) {
            return (T) value;
        }
        
        if (targetWrapType == Boolean.class) {
            // only char can be mapped to boolean
            // (we could say 0=false, nonzero=true, but there is no compelling use case so better
            // to encourage users to write as boolean)
            if (sourceWrapType == Character.class)
                return (T) stringToPrimitive(value.toString(), targetType);
            
            throw new ClassCoercionException("Cannot cast "+sourceWrapType+" ("+value+") to "+targetType);
        } else if (sourceWrapType == Boolean.class) {
            // boolean can't cast to anything else
            
            throw new ClassCoercionException("Cannot cast "+sourceWrapType+" ("+value+") to "+targetType);
        }
        
        // for whole-numbers (where casting to long won't lose anything)...
        long v = 0;
        boolean islong = true;
        if (sourceWrapType == Character.class) {
            v = (long) ((Character)value).charValue();
        } else if (sourceWrapType == Byte.class) {
            v = (long) ((Byte)value).byteValue();
        } else if (sourceWrapType == Short.class) {
            v = (long) ((Short)value).shortValue();
        } else if (sourceWrapType == Integer.class) {
            v = (long) ((Integer)value).intValue();
        } else if (sourceWrapType == Long.class) {
            v = ((Long)value).longValue();
        } else {
            islong = false;
        }
        if (islong) {
            if (targetWrapType == Character.class) return (T) Character.valueOf((char)v); 
            if (targetWrapType == Byte.class) return (T) Byte.valueOf((byte)v); 
            if (targetWrapType == Short.class) return (T) Short.valueOf((short)v); 
            if (targetWrapType == Integer.class) return (T) Integer.valueOf((int)v); 
            if (targetWrapType == Long.class) return (T) Long.valueOf((long)v); 
            if (targetWrapType == Float.class) return (T) Float.valueOf((float)v); 
            if (targetWrapType == Double.class) return (T) Double.valueOf((double)v);
            throw new IllegalStateException("Unexpected: sourceType="+sourceWrapType+"; targetType="+targetWrapType);
        }
        
        // for real-numbers (cast to double)...
        double d = 0;
        boolean isdouble = true;
        if (sourceWrapType == Float.class) {
            d = (double) ((Float)value).floatValue();
        } else if (sourceWrapType == Double.class) {
            d = (double) ((Double)value).doubleValue();
        } else {
            isdouble = false;
        }
        if (isdouble) {
            if (targetWrapType == Character.class) return (T) Character.valueOf((char)d); 
            if (targetWrapType == Byte.class) return (T) Byte.valueOf((byte)d); 
            if (targetWrapType == Short.class) return (T) Short.valueOf((short)d); 
            if (targetWrapType == Integer.class) return (T) Integer.valueOf((int)d); 
            if (targetWrapType == Long.class) return (T) Long.valueOf((long)d); 
            if (targetWrapType == Float.class) return (T) Float.valueOf((float)d); 
            if (targetWrapType == Double.class) return (T) Double.valueOf((double)d);
            throw new IllegalStateException("Unexpected: sourceType="+sourceWrapType+"; targetType="+targetWrapType);
        } else {
            throw new IllegalStateException("Unexpected: sourceType="+sourceWrapType+"; targetType="+targetWrapType);
        }
    }
    
    public static boolean isPrimitiveOrBoxer(Class<?> type) {
        // cf Boxing.isPrimitiveOrBoxerClass
        return Primitives.allPrimitiveTypes().contains(type) || Primitives.allWrapperTypes().contains(type);
    }
    
    @SuppressWarnings("unchecked")
    public static <T> T stringToPrimitive(String value, Class<T> targetType) {
        assert Primitives.allPrimitiveTypes().contains(targetType) || Primitives.allWrapperTypes().contains(targetType) : "targetType="+targetType;
        // If char, then need to do explicit conversion
        if (targetType == Character.class || targetType == char.class) {
            if (value.length() == 1) {
                return (T) (Character) value.charAt(0);
            } else if (value.length() != 1) {
                throw new ClassCoercionException("Cannot coerce type String to "+targetType.getCanonicalName()+" ("+value+"): adapting failed");
            }
        }
        value = value.trim();
        // For boolean we could use valueOf, but that returns false whereas we'd rather throw errors on bad values
        if (targetType == Boolean.class || targetType == boolean.class) {
            if ("true".equalsIgnoreCase(value)) return (T) Boolean.TRUE;
            if ("false".equalsIgnoreCase(value)) return (T) Boolean.FALSE;
            if ("yes".equalsIgnoreCase(value)) return (T) Boolean.TRUE;
            if ("no".equalsIgnoreCase(value)) return (T) Boolean.FALSE;
            if ("t".equalsIgnoreCase(value)) return (T) Boolean.TRUE;
            if ("f".equalsIgnoreCase(value)) return (T) Boolean.FALSE;
            if ("y".equalsIgnoreCase(value)) return (T) Boolean.TRUE;
            if ("n".equalsIgnoreCase(value)) return (T) Boolean.FALSE;
            
            throw new ClassCoercionException("Cannot coerce type String to "+targetType.getCanonicalName()+" ("+value+"): adapting failed"); 
        }
        
        // Otherwise can use valueOf reflectively
        Class<?> wrappedType;
        if (Primitives.allPrimitiveTypes().contains(targetType)) {
            wrappedType = Primitives.wrap(targetType);
        } else {
            wrappedType = targetType;
        }
        
        try {
            return (T) wrappedType.getMethod("valueOf", String.class).invoke(null, value);
        } catch (Exception e) {
            ClassCoercionException tothrow = new ClassCoercionException("Cannot coerce "+JavaStringEscapes.wrapJavaString(value)+" to "+targetType.getCanonicalName()+" ("+value+"): adapting failed");
            tothrow.initCause(e);
            throw tothrow;
        }
    }
    
}
