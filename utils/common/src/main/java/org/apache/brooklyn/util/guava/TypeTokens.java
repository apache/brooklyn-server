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

import com.fasterxml.jackson.databind.type.ArrayType;
import com.google.common.annotations.Beta;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.fasterxml.jackson.databind.JavaType;
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
        if (type==null || !TypeTokens.isRaw(type)) {
            return null;
        } else {
            return type.getRawType();
        }
    }
    
    /** returns null if it's raw, else the type token */
    @Nullable
    public static <T> TypeToken<T> getTypeTokenIfNotRaw(@Nullable TypeToken<T> type) {
        if (type==null || isRaw(type)) {
            return null;
        } else {
            return type;
        }
    }
    
    /** given either a token or a raw type, returns the raw type */
    @SuppressWarnings("unchecked")
    public static <T,U extends T> Class<T> getRawType(TypeToken<U> token, Class<T> raw) {
        if (raw!=null) return raw;
        if (token!=null) {
            if (token.getType() instanceof JavaType) {
                return (Class<T>) ((JavaType)token.getType()).getRawClass();
            }
            return (Class<T>) token.getRawType();
        }
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
        return (Class) getRawType(token, null);
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
        if (!equalsRaw(type, typeToken)) {
            throw new IllegalStateException("Invalid types, token is "+typeToken+" (raw "+getRawRawType(typeToken)+") but class is "+type);
        }
    }

    public static List<TypeToken<?>> getGenericArguments(TypeToken<?> token) {
        Type t = token.getType();
        if (t instanceof ParameterizedType) {
            return Arrays.stream(((ParameterizedType) t).getActualTypeArguments()).map(TypeToken::of).collect(Collectors.toList());
        }
        return null;
    }

    public static <T> TypeToken<?> getComponentType(TypeToken<T> tt) {
        if (tt.getType() instanceof JavaType) {
            return TypeToken.of( ((JavaType)tt.getType()).getContentType() );
        }
        return tt.getComponentType();
    }

    public static <T> boolean isArray(TypeToken<T> tt) {
        if (tt.getType() instanceof JavaType) {
            return (tt.getType() instanceof ArrayType);
        }
        return tt.isArray();
    }

    public static <T> boolean isRaw(TypeToken<T> type) {
        return type.equals(TypeToken.of(getRawRawType(type)));
    }

    public static boolean equalsRaw(Class<?> clazz, TypeToken<?> tt) {
        return clazz.equals(getRawRawType(tt));
    }

    public static boolean isAssignableFromRaw(Class<?> clazz, TypeToken<?> tt) {
        return clazz.isAssignableFrom(getRawRawType(tt));
    }

    public static boolean isAssignableFromRaw(TypeToken<?> tt, Class<?> clazz) {
        return getRawRawType(tt).isAssignableFrom(clazz);
    }

    public static <T> boolean isInstanceRaw(TypeToken<T> typeT, Object v) {
        return getRawRawType(typeT).isInstance(v);
    }

    public static TypeToken<?> resolveType(TypeToken<?> tt, Type t) {
        if (tt.getType() instanceof JavaType) {
            return TypeToken.of( getRawRawType(tt) ).resolveType(t);
        }

        return tt.resolveType(t);
    }

    /** given Map<String,Integer> returns [ String, Integer ] */
    public static TypeToken<?>[] getGenericParameterTypeTokens(TypeToken<?> t) {
        Class<?> rawType = TypeTokens.getRawRawType(t);
        TypeVariable<?>[] pT = rawType.getTypeParameters();
        TypeToken<?> pTT[] = new TypeToken<?>[pT.length];
        for (int i=0; i<pT.length; i++) {
            pTT[i] = TypeTokens.resolveType(t, pT[i]);
        }
        return pTT;
    }

    public static <T> TypeToken<?>[] getGenericParameterTypeTokensWhenUpcastToClass(TypeToken<T> t, Class<? super T> clazz) {
        if (!clazz.isAssignableFrom(getRawRawType(t))) return new TypeToken<?>[0];
        return getGenericParameterTypeTokens(t.getSupertype(clazz));
    }

    public static <T> TypeToken<?>[] getGenericParameterTypeTokensWhenUpcastToClassRaw(TypeToken<?> t, Class<?> clazz) {
        return getGenericParameterTypeTokensWhenUpcastToClass(t, (Class)clazz);
    }

    /** find the lowest common ancestor of the two types, filling in generics info, and ignoring Object; but that's hard so this does some heuristics for common cases */
    @Beta
    public static TypeToken<?> union(TypeToken<?> t1, TypeToken<?> t2, boolean ignoreObject) {
        if (t1.equals(t2)) return t1;

        if (ignoreObject) {
            if (t1.getRawType().equals(Object.class)) return t2;
            if (t2.getRawType().equals(Object.class)) return t1;
        }

        if (t1.getRawType().equals(t2.getRawType())) {
            // if equal, prefer one whose generics are more specific
            TypeToken<?>[] tokens1 = getGenericParameterTypeTokens(t1);
            TypeToken<?>[] tokens2 = getGenericParameterTypeTokens(t1);
            if (tokens1.length>0 && tokens2.length>0) {
                // just look at first one to see who wins
                TypeToken<?> union0 = union(tokens1[0], tokens2[0], true);
                if (union0==tokens2[0]) return t2;
                return t1;
            } else if (tokens2.length>0) {
                return t2;
            }
            return t1;
        }

        // prefer an ancestor (ideally we'd infer generics if needed at the parent, but skip for now)
        if (t1.isSupertypeOf(t2)) return t1;
        if (t2.isSupertypeOf(t1)) return t2;
        if (t1.getRawType().isAssignableFrom(t2.getRawType())) return t1;
        if (t2.getRawType().isAssignableFrom(t1.getRawType())) return t2;

        // can't figure anything out, just pick one
        return t1;
    }

}
