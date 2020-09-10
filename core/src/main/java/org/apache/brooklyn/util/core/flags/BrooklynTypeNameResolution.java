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

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.Type;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.reflect.TypeUtils;

public class BrooklynTypeNameResolution {

    private static final Map<String, Class<?>> BUILT_IN_TYPES = ImmutableMap.<String, Class<?>>builder()
            .put("string", String.class)
            .put("bool", Boolean.class)
            .put("boolean", Boolean.class)
            .put("byte", Byte.class)
            .put("char", Character.class)
            .put("character", Character.class)
            .put("short", Short.class)
            .put("integer", Integer.class)
            .put("int", Integer.class)
            .put("long", Long.class)
            .put("float", Float.class)
            .put("double", Double.class)

            .put("map", Map.class)
            .put("list", List.class)

            .put("duration", Duration.class)
            .put("timestamp", Date.class)
            .put("port", PortRange.class)
            .build();

    public static Maybe<TypeToken<?>> getTypeTokenForBuiltInTypeName(String className) {
        return getClassForBuiltInTypeName(className).transform(TypeToken::of);
    }

    public static Maybe<Class<?>> getClassForBuiltInTypeName(String className) {
        if (className==null) return Maybe.absent(new NullPointerException("className is null"));
        return Maybe.ofDisallowingNull(BUILT_IN_TYPES.get(className.trim().toLowerCase()));
    }

    public static Map<String, Class<?>> standardTypesMap() {
        return BUILT_IN_TYPES;
    }

    private static final Set<String> BUILT_IN_TYPE_RESERVED_WORDS = MutableSet.copyOf(BUILT_IN_TYPES.keySet())
            .putAll(BUILT_IN_TYPES.values().stream().map(c -> c.getName().toLowerCase()).collect(Collectors.toList()))
            .asUnmodifiable();

    public static boolean isBuiltInType(String s) {
        return BUILT_IN_TYPE_RESERVED_WORDS.contains(s.trim().toLowerCase());
    }

    public static TypeToken<?> getTypeTokenForBuiltInAndJava(String typeName, String context, BrooklynClassLoadingContext loader) {
        return parseTypeToken(typeName, newTypeResolverComposite(context, loader));
    }

    static Function<String,Type> newTypeResolverComposite(String context, BrooklynClassLoadingContext loaderContext) {
        Map<String,Function<String,Maybe<Class<?>>>> rules = MutableMap.of(
                "simple types ("+Strings.join(BrooklynTypeNameResolution.standardTypesMap().keySet(), ", ")+")",
                    BrooklynTypeNameResolution::getClassForBuiltInTypeName,
                "Java types", loaderContext::tryLoadClass);

        Function<String,Maybe<Class<?>>> composite = s -> {
            for (Function<String, Maybe<Class<?>>> r : rules.values()) {
                Maybe<Class<?>> candidate = r.apply(s);
                if (candidate.isPresent()) return candidate;
            }
            return Maybe.absent(() -> new IllegalArgumentException("Invalid type for "+context+": '"+s+"' not found in "+rules.keySet()));
        };

        return s -> composite.apply(s).get();
    }

    static GenericsRecord parseTypeGenerics(String s) { return parseTypeGenerics(s, (String t, List<GenericsRecord> tt) -> new GenericsRecord(t, tt)); }
    static TypeToken<?> parseTypeToken(String s, Function<String,Type> typeLookup) {
        return TypeToken.of(parseTypeGenerics(s, (String t, List<Type> tt) -> {
            Type c = typeLookup.apply(t);
            if (tt.isEmpty()) {
                return c;
            }
            if (c instanceof Class) {
                return TypeUtils.parameterize((Class<?>)c, tt.toArray(new Type[0]));
            }
            throw new IllegalStateException("Cannot make generic type with base '"+t+"' and generic parameters "+tt);
        }));
    }

    static <T> T parseTypeGenerics(String s, BiFunction<String,List<T>,T> baseTypeConverter) {
        GenericsRecord.Parser<T> p = new GenericsRecord.Parser<T>();
        p.s = s;
        p.baseTypeConverter = baseTypeConverter;
        return p.parse(0);
    }

    static class GenericsRecord {
        String baseName;
        List<GenericsRecord> params;

        GenericsRecord(String baseName, List<GenericsRecord> params) {
            this.baseName = baseName;
            this.params = params;
        }

        @Override
        public String toString() {
            return baseName + (!params.isEmpty() ? "<" + Strings.join(params, ",") + ">" : "");
        }

        static class Parser<T> {
            String s;
            BiFunction<String,List<T>,T> baseTypeConverter;

            private void skipWhitespace() {
                while (index<s.length() && Character.isWhitespace(s.charAt(index))) index++;
            }

            int index = 0;

            T parse(int depth) {
                int baseNameStart = index;
                MutableList<T> params = MutableList.of();
                int baseNameEnd = -1;
                while (index<s.length()) {
                    char c = s.charAt(index);
                    if (c=='<') {
                        baseNameEnd = index;
                        index++;
                        do {
                            params.add(parse(depth+1));
                            c = s.charAt(index);
                            index++;
                            skipWhitespace();
                        } while (c==',');
                        if (c!='>') {
                            // shouldn't happen
                            throw new IllegalArgumentException("Invalid type '"+s+"': unexpected character preceeding position "+index);
                        }
                        // skip spaces
                        break;
                    }
                    if (depth>0) {
                        if (c==',' || c=='>') {
                            baseNameEnd = index;
                            break;
                        }
                    }
                    index++;
                }
                if (depth==0) {
                    if (index < s.length()) {
                        throw new IllegalArgumentException("Invalid type '"+s+"': characters not permitted after generics at position "+index);
                    }
                    if (baseNameEnd<0) {
                        baseNameEnd = s.length();
                    }
                } else {
                    if (index >= s.length()) {
                        throw new IllegalArgumentException("Invalid type '"+s+"': unterminated generics for argument starting at position "+baseNameStart);
                    }
                }
                String baseName = s.substring(baseNameStart, baseNameEnd).trim();
                if (baseName.isEmpty()) throw new IllegalArgumentException("Invalid type '"+s+"': missing base type name at position "+baseNameStart);
                return baseTypeConverter.apply(baseName, params.asUnmodifiable());
            }
        }

    }
}
