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
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.reflect.TypeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrooklynTypeNameResolution {

    private static Logger LOG = LoggerFactory.getLogger(BrooklynTypeNameResolution.class);

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

            .put("wrapped", WrappedValue.class)
            .put("wrapped-value", WrappedValue.class)
            .put("wrappedvalue", WrappedValue.class)

            .put("duration", Duration.class)
            .put("timestamp", Date.class)
            .put("port", PortRange.class)
            .build();

    private static final Map<String,Class<?>> BUILT_IN_TYPE_CLASSES;
    static {
        MutableMap<String,Class<?>> collector = MutableMap.of();
        BUILT_IN_TYPES.values().forEach(t -> collector.put(t.getName(), t));
        BUILT_IN_TYPE_CLASSES = collector.asUnmodifiable();
    }
    private static final Set<String> BUILT_IN_TYPE_RESERVED_WORDS = MutableSet.copyOf(
            BUILT_IN_TYPE_CLASSES.keySet().stream().map(String::toLowerCase).collect(Collectors.toList()))
            .asUnmodifiable();



    public static Maybe<TypeToken<?>> getTypeTokenForBuiltInTypeName(String className) {
        return getClassForBuiltInTypeName(className).transform(TypeToken::of);
    }

    public static Maybe<Class<?>> getClassForBuiltInTypeName(String className) {
        if (className==null) return Maybe.absent(new NullPointerException("className is null"));
        Class<?> candidate = BUILT_IN_TYPES.get(className.trim().toLowerCase());
        if (candidate!=null) return Maybe.of(candidate);
        candidate = BUILT_IN_TYPE_CLASSES.get(className);
        if (candidate!=null) return Maybe.of(candidate);
        return Maybe.absent();
    }

    public static Map<String, Class<?>> standardTypesMap() {
        return BUILT_IN_TYPES;
    }

    public static boolean isBuiltInType(String s) {
        return BUILT_IN_TYPE_RESERVED_WORDS.contains(s.trim().toLowerCase());
    }

    public static class BrooklynTypeNameResolver {
        final String context;
        final ManagementContext mgmt;
        final BrooklynClassLoadingContext loader;
        final boolean allowJavaType;
        final boolean allowRegisteredTypes;
        final Map<String,Function<String,Maybe<Class<?>>>> rules = MutableMap.of();

        /** resolver supporting only built-ins */
        public BrooklynTypeNameResolver(String context) {
            this( context, null, null, false, false );
        }
        /** resolver supporting only built-ins and registered types, ie not java (which requires a loader);
         * or just built-ins if mgmt is null */
        public BrooklynTypeNameResolver(String context, ManagementContext mgmt) {
            this(context, mgmt, null, false, mgmt != null);
        }
        /** resolver supporting configurable sources of types */
        public BrooklynTypeNameResolver(String context, BrooklynClassLoadingContext loader, boolean allowJavaType, boolean allowRegisteredTypes) {
            this(context, loader.getManagementContext(), loader, allowJavaType, allowRegisteredTypes);
        }
        private BrooklynTypeNameResolver(String context, ManagementContext mgmt, BrooklynClassLoadingContext loader, boolean allowJavaType, boolean allowRegisteredTypes) {
            this.context = context;
            this.mgmt = mgmt;
            this.loader = loader;
            this.allowJavaType = allowJavaType;
            this.allowRegisteredTypes = allowRegisteredTypes;

            rules.put("simple types ("+Strings.join(BrooklynTypeNameResolution.standardTypesMap().keySet(), ", ")+")",
                    BrooklynTypeNameResolution::getClassForBuiltInTypeName);

            if (allowJavaType) {
                rules.put("Java types visible to bundles", loader::tryLoadClass);
                rules.put("Java types", JavaBrooklynClassLoadingContext.create(mgmt)::tryLoadClass);
            }

            if (allowRegisteredTypes) {
                rules.put("Brooklyn registered types", s -> {
                    Maybe<RegisteredType> t = mgmt.getTypeRegistry().getMaybe(s, RegisteredTypeLoadingContexts.loader(loader));
                    if (t.isPresent()) {
                        Optional<Object> st1 = t.get().getSuperTypes().stream().filter(st -> st instanceof Class).findFirst();
                        if (st1.isPresent()) {
                            return Maybe.of( (Class<?>) st1.get() );
                        }
                        // tests may not set supertypes which could cause odd behaviour; real OSGi addition should set supertypes so this shouldn't normally happen outside of test/pojo
                        LOG.debug("Attempt to use registered type '"+s+"' as a type but no associated Java type is yet recorded (normal on install); returning as Object");
                        return Maybe.of(Object.class);
                    }
                    return Maybe.absent();
                });
            }
        }

        // more efficient method if s has already been sliced
        Maybe<Class<?>> findBaseClassInternal(String s) {
            for (Function<String, Maybe<Class<?>>> r : rules.values()) {
                Maybe<Class<?>> candidate = r.apply(s);
                if (candidate.isPresent()) return candidate;
            }
            return Maybe.absent(() -> new IllegalArgumentException("Invalid type for "+context+": '"+s+"' not found in "+rules.keySet()));
        }

        public Maybe<Class<?>> findBaseClass(String typeName) {
            typeName = Strings.removeAfter(typeName, "<", true).trim();
            return findBaseClassInternal(typeName);
        }
        public <T> TypeToken<T> getTypeToken(String typeName) {
            return (TypeToken<T>) parseTypeToken(typeName, bs -> findBaseClassInternal(bs).get());
        }
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
