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
package org.apache.brooklyn.util.guava;

import javax.annotation.Nullable;

import com.google.common.reflect.TypeToken;

public class TypeTokens {

    // creating TypeToken is surprisingly expensive so cache these common ones
    public static TypeToken<String> STRING = TypeToken.of(String.class);
    public static TypeToken<Object> OBJECT = TypeToken.of(Object.class);
    public static TypeToken<Integer> INTEGER = TypeToken.of(Integer.class);
    public static TypeToken<Boolean> BOOLEAN = TypeToken.of(Boolean.class);
    public static TypeToken<Double> DOUBLE = TypeToken.of(Double.class);
    public static TypeToken<Long> LONG = TypeToken.of(Long.class);
    
    /** returns raw type, if it's raw, else null;
     * used e.g. to set only one of the raw type or the type token,
     * for instance to make serialized output nicer */
    @Nullable
    public static <T> Class<? super T> getRawTypeIfRaw(@Nullable TypeToken<T> type) {
        if (type==null || !type.equals(TypeToken.of(type.getRawType()))) {
            return null;
        } else {
            return type.getRawType();
        }
    }
    
    /** returns null if it's raw, else the type token */
    @Nullable
    public static <T> TypeToken<T> getTypeTokenIfNotRaw(@Nullable TypeToken<T> type) {
        if (type==null || type.equals(TypeToken.of(type.getRawType()))) {
            return null;
        } else {
            return type;
        }
    }
    
    /** given either a token or a raw type, returns the raw type */
    @SuppressWarnings("unchecked")
    public static <T,U extends T> Class<T> getRawType(TypeToken<U> token, Class<T> raw) {
        if (raw!=null) return raw;
        if (token!=null) return (Class<T>) token.getRawType();
        throw new IllegalStateException("Both indicators of type are null");
    }
    
    
    /** given either a token or a raw type, returns the token */
    @SuppressWarnings("unchecked")
    public static <T> TypeToken<T> getTypeToken(TypeToken<T> token, Class<? super T> raw) {
        if (token!=null) return token;
        if (raw!=null) {
            if (String.class.equals(raw)) return (TypeToken<T>) STRING;
            if (Object.class.equals(raw)) return (TypeToken<T>) OBJECT;
            if (Integer.class.equals(raw)) return (TypeToken<T>) INTEGER;
            if (Boolean.class.equals(raw)) return (TypeToken<T>) BOOLEAN;
            if (Double.class.equals(raw)) return (TypeToken<T>) DOUBLE;
            if (Long.class.equals(raw)) return (TypeToken<T>) LONG;
            return TypeToken.of((Class<T>)raw);
        }
        throw new IllegalStateException("Both indicators of type are null");
    }

    /** gets the Class<T> object from a token; normal methods return Class<? super T> which may technically be correct 
     * with generics but this sloppily but handily gives you Class<T> which is usually what you have anyway */
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public static <T> Class<T> getRawRawType(TypeToken<T> token) {
        return (Class)token.getRawType();
    }

    /** Checks that if both type and token are supplied, either exactly one is null, or 
     * they both refer to the same non-null type */
    public static <T> void checkCompatibleOneNonNull(Class<? super T> type, TypeToken<T> typeToken) {
        if ((type==null && typeToken!=null) || (type!=null && typeToken==null)) {
            return;
        }
        if (type==null && typeToken==null) {
            throw new NullPointerException("Type not set (neither class or type token)");
        }
        if (!type.equals(typeToken.getRawType())) {
            throw new NullPointerException("Invalid types, token is "+typeToken+" (raw "+typeToken.getRawType()+") but class is "+type);
        }
    }
    
}
