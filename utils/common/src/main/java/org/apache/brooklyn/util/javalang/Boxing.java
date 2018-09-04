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
package org.apache.brooklyn.util.javalang;

import java.lang.reflect.Array;
import java.lang.reflect.Type;

import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.collect.ImmutableBiMap;
import com.google.common.primitives.Primitives;
import com.google.common.reflect.TypeToken;

/** Conveniences for working with primitives and their boxed (wrapper) types.
 * NB: there is redundancy with {@link Primitives}
 */
public class Boxing {

    public static boolean unboxSafely(Boolean ref, boolean valueIfNull) {
        if (ref==null) return valueIfNull;
        return ref.booleanValue();
    }
    
    public static final ImmutableBiMap<Class<?>, Class<?>> PRIMITIVE_TO_BOXED =
        ImmutableBiMap.<Class<?>, Class<?>>builder()
            .put(Integer.TYPE, Integer.class)
            .put(Long.TYPE, Long.class)
            .put(Double.TYPE, Double.class)
            .put(Float.TYPE, Float.class)
            .put(Boolean.TYPE, Boolean.class)
            .put(Character.TYPE, Character.class)
            .put(Byte.TYPE, Byte.class)
            .put(Short.TYPE, Short.class)
            .put(Void.TYPE, Void.class)
            .build();
    
    /** Returns the unboxed type for the given primitive type name, if available;
     * e.g. {@link Integer#TYPE} for <code>int</code> (distinct from <code>Integer.class</code>),
     * or null if not a primitive.
     * */
    public static Maybe<Class<?>> getPrimitiveType(String typeName) {
        if (typeName!=null) {
            for (Class<?> t: PRIMITIVE_TO_BOXED.keySet()) {
                if (typeName.equals(t.getName())) return Maybe.<Class<?>>of(t);
            }
        }
        return Maybe.absent("Not a primitive: "+typeName);
    }

    /** returns name of primitive corresponding to the given (boxed or unboxed) type */
    public static Maybe<String> getPrimitiveName(Class<?> type) {
        if (type!=null) {
            if (PRIMITIVE_TO_BOXED.containsKey(type)) return Maybe.of(type.getName());
            if (PRIMITIVE_TO_BOXED.containsValue(type)) {
                return Maybe.of(PRIMITIVE_TO_BOXED.inverse().get(type).getName());
            }
        }
        return Maybe.absent("Not a primitive or boxed primitive: "+type);
    }
    
    public static Class<?> boxedType(Class<?> type) {
        if (PRIMITIVE_TO_BOXED.containsKey(type))
            return PRIMITIVE_TO_BOXED.get(type);
        return type;
    }
    
    public static TypeToken<?> boxedTypeToken(Type type) {
        TypeToken<?> tt = TypeToken.of(type);
        Class<?> clazz = tt.getRawType();
        if (PRIMITIVE_TO_BOXED.containsKey(clazz))
            return TypeToken.of(PRIMITIVE_TO_BOXED.get(clazz));
        return tt;
    }

    public static boolean isPrimitiveOrBoxedObject(Object o) {
        if (o==null) return false;
        return isPrimitiveOrBoxedClass(o.getClass());
    }
    public static boolean isPrimitiveOrBoxedClass(Class<?> t) {
        // TODO maybe better to switch to using Primitives, eg:
        // return Primitives.allPrimitiveTypes().contains(type) || Primitives.allWrapperTypes().contains(type);
        
        if (t==null) return false;
        if (t.isPrimitive()) return true;
        if (PRIMITIVE_TO_BOXED.containsValue(t)) return true;
        return false;
    }
    
    /** sets the given element in an array to the indicated value;
     * if the type is a primitive type, the appropriate primitive method is used
     * <p>
     * this is needed because arrays do not deal with autoboxing */
    public static void setInArray(Object target, int index, Object value, Class<?> type) {
        if (PRIMITIVE_TO_BOXED.containsKey(type)) {
            if (type.equals(Integer.TYPE))
                Array.setInt(target, index, (Integer)value);
            else if (type.equals(Long.TYPE))
                Array.setLong(target, index, (Long)value);
            else if (type.equals(Double.TYPE))
                Array.setDouble(target, index, (Double)value);
            else if (type.equals(Float.TYPE))
                Array.setFloat(target, index, (Float)value);
            else if (type.equals(Boolean.TYPE))
                Array.setBoolean(target, index, (Boolean)value);
            else if (type.equals(Character.TYPE))
                Array.setChar(target, index, (Character)value);
            else if (type.equals(Byte.TYPE))
                Array.setByte(target, index, (Byte)value);
            else if (type.equals(Short.TYPE))
                Array.setShort(target, index, (Short)value);
            
            else if (type.equals(Void.TYPE))
                Array.set(target, index, value);
            
            else 
                // should not happen!
                throw new IllegalStateException("Unsupported primitive: "+type);
            
            return;
        }
        
        Array.set(target, index, value);
    }
    
}
