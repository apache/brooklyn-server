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

import java.util.Map;

import javax.annotation.Nullable;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public class TypeTokens {

    // creating TypeToken is surprisingly expensive so cache these common ones
    public static final Map<Class<?>, TypeToken<?>> COMMON_TYPE_TOKENS = ImmutableMap.<Class<?>, TypeToken<?>>builder()
        .put(String.class, TypeToken.of(String.class))
        .put(Object.class, TypeToken.of(Object.class))
        .put(Integer.class, TypeToken.of(Integer.class))
        .put(Boolean.class, TypeToken.of(Boolean.class))
        .put(Double.class, TypeToken.of(Double.class))
        .put(Long.class, TypeToken.of(Long.class))
        .build();
    
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
            TypeToken<?> result = COMMON_TYPE_TOKENS.get(raw);
            if (result==null) result = TypeToken.of((Class<T>)raw);
            return (TypeToken<T>) result;
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
            throw new IllegalStateException("Invalid types, token is "+typeToken+" (raw "+typeToken.getRawType()+") but class is "+type);
        }
    }
    
}
