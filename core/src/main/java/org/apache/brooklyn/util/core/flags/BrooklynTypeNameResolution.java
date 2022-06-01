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

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.core.mgmt.classloading.JavaBrooklynClassLoadingContext;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonType;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Timestamp;
import org.apache.commons.lang3.ObjectUtils;
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
            .put("timestamp", Timestamp.class)
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
        final Map<String,Function<String,Maybe<TypeToken<?>>>> rules = MutableMap.of();

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
                    BrooklynTypeNameResolution::getTypeTokenForBuiltInTypeName);

            if (allowJavaType) {
                rules.put("Java types visible to bundles", s -> loader.tryLoadClass(s).map(TypeToken::of));
                rules.put("Java types", s -> JavaBrooklynClassLoadingContext.create(mgmt).tryLoadClass(s).map(TypeToken::of));
            }

            if (allowRegisteredTypes) {
                rules.put("Brooklyn registered types",
                        s -> mgmt.getTypeRegistry().getMaybe(s, RegisteredTypeLoadingContexts.loader(loader)).map(BrooklynJacksonType::asTypeToken));
            }
        }

        // more efficient method if s has already been sliced
        Maybe<TypeToken<?>> findTypeTokenOfBaseNameInternal(String s) {
            for (Function<String, Maybe<TypeToken<?>>> r : rules.values()) {
                Maybe<TypeToken<?>> candidate = r.apply(s);
                if (candidate.isPresent()) return candidate;
            }
            return Maybe.absent(() -> new IllegalArgumentException("Invalid type for "+context+": '"+s+"' not found in "+rules.keySet()));
        }

        public Maybe<TypeToken<?>> findBaseTypeToken(String typeName) {
            typeName = Strings.removeAfter(typeName, "<", true).trim();
            return findTypeTokenOfBaseNameInternal(typeName);
        }
        public TypeToken<?> getTypeToken(String typeName) {
            return parseTypeToken(typeName, bs -> findTypeTokenOfBaseNameInternal(bs)).get();
        }
        public Maybe<TypeToken<?>> findTypeToken(String typeName) {
            try {
                return parseTypeToken(typeName, bs -> findTypeTokenOfBaseNameInternal(bs));
            } catch (Exception e) {
                return Maybe.absent(e);
            }
        }
    }

    static Maybe<GenericsRecord> parseTypeGenerics(String s) { return parseTypeGenerics(s, (String t, List<GenericsRecord> tt) -> Maybe.of(new GenericsRecord(t, tt))); }
    @VisibleForTesting
    public static Maybe<TypeToken<?>> parseTypeToken(String s, Function<String,Maybe<TypeToken<?>>> typeLookup) {
        return parseTypeGenerics(s, (String t, List<TypeToken<?>> tt) -> {
            Maybe<TypeToken<?>> c = typeLookup.apply(t);
            if (c.isAbsent()) return c;
            if (tt.isEmpty()) return c;
            return Maybe.of(TypeToken.of(parameterizedType(TypeTokens.getRawRawType(c.get()), tt.stream()
                    .map(TypeToken::getType).collect(Collectors.toList()) )));
        });
    }

    static ParameterizedType parameterizedType(Class<?> raw, List<Type> types) {
        return new BetterToStringParameterizedTypeImpl(raw, null, types.toArray(new Type[0]));
    }

    @Beta
    public static ParameterizedType parameterizedType(ParameterizedType t) {
        return new BetterToStringParameterizedTypeImpl(t);
    }

    @Beta
    public static final class BetterToStringParameterizedTypeImpl implements ParameterizedType {
        // because Apache Commons ParameterizedTypeImpl toString is too rigid

        private Type raw;
        private Type useOwner;
        private Type[] typeArguments;

        /**
         * Constructor
         * @param raw type
         * @param useOwner owner type to use, if any
         * @param typeArguments formal type arguments
         */
        private BetterToStringParameterizedTypeImpl(Class<?> raw, Type useOwner, Type[] typeArguments) {
            this.raw = raw;
            this.useOwner = useOwner;
            this.typeArguments = typeArguments;
        }

        // JSON deserializer constructor
        private BetterToStringParameterizedTypeImpl() {
            this(null, null, null);
        }

        private BetterToStringParameterizedTypeImpl(ParameterizedType t) {
            this.raw = t.getRawType();
            this.useOwner = t.getOwnerType();
            this.typeArguments = t.getActualTypeArguments();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Type getRawType() {
            return raw;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Type getOwnerType() {
            return useOwner;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Type[] getActualTypeArguments() {
            return typeArguments.clone();
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append(getRawType().getTypeName());
            sb.append("<");
            Type[] args = getActualTypeArguments();
            if (args.length > 0) {
                sb.append(toString(args[0]));
                for (int i=1; i<args.length; i++) {
                    sb.append(",");
                    sb.append(toString(args[i]));
                }
            }
            sb.append(">");
            return sb.toString();
        }

        private static String toString(Type t) {
            if (t instanceof BetterToStringParameterizedTypeImpl) return t.toString();
            if (t instanceof BrooklynJacksonType) return t.getTypeName();
            return TypeUtils.toString(t);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public boolean equals(Object t) {
            // from TypeEquals.equals(ParameterizedType, Object) -- because TypeEquals.equals(Object,Object) calls back to this method
            if (t instanceof ParameterizedType) {
                final ParameterizedType other = (ParameterizedType) t;
                if (Objects.equals(getRawType(), other.getRawType()) && Objects.equals(getOwnerType(), other.getOwnerType())) {
                    return Objects.deepEquals(getActualTypeArguments(), other.getActualTypeArguments());
                }
            }
            return false;
        }

        /**
         * {@inheritDoc}
         */
        @SuppressWarnings( "deprecation" )  // ObjectUtils.hashCode(Object) has been deprecated in 3.2
        @Override
        public int hashCode() {
            int result = 71 << 4;
            result |= raw.hashCode();
            result <<= 4;
            result |= ObjectUtils.hashCode(useOwner);
            result <<= 8;
            result |= Arrays.hashCode(typeArguments);
            return result;
        }

        // compatibility setters to match sun ParameterizedTypeImpl
        private void setActualTypeArguments(Type[] typeArguments) {
            this.typeArguments = typeArguments;
        }
        private void setOwnerType(Type useOwner) {
            this.useOwner = useOwner;
        }
        private void setRawType(Type raw) {
            this.raw = raw;
        }
        private void setTypeName(Object ignored) {
        }
    }

    static <T> Maybe<T> parseTypeGenerics(String s, BiFunction<String,List<T>,Maybe<T>> baseTypeConverter) {
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
            BiFunction<String,List<T>,Maybe<T>> baseTypeConverter;

            private void skipWhitespace() {
                while (index<s.length() && Character.isWhitespace(s.charAt(index))) index++;
            }

            int index = 0;

            Maybe<T> parse(int depth) {
                int baseNameStart = index;
                MutableList<T> params = MutableList.of();
                int baseNameEnd = -1;
                while (index<s.length()) {
                    char c = s.charAt(index);
                    if (c=='<') {
                        baseNameEnd = index;
                        index++;
                        do {
                            Maybe<T> pd = parse(depth + 1);
                            if (pd.isAbsent()) return pd;
                            params.add(pd.get());
                            c = s.charAt(index);
                            index++;
                            skipWhitespace();
                        } while (c==',');
                        if (c!='>') {
                            // shouldn't happen
                            return Maybe.absent(() -> new IllegalArgumentException("Invalid type '"+s+"': unexpected character preceeding position "+index));
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
                        return Maybe.absent(() -> new IllegalArgumentException("Invalid type '"+s+"': characters not permitted after generics at position "+index));
                    }
                    if (baseNameEnd<0) {
                        baseNameEnd = s.length();
                    }
                } else {
                    if (index >= s.length()) {
                        return Maybe.absent(() -> new IllegalArgumentException("Invalid type '"+s+"': unterminated generics for argument starting at position "+baseNameStart));
                    }
                }
                String baseName = s.substring(baseNameStart, baseNameEnd).trim();
                if (baseName.isEmpty()) return Maybe.absent(() -> new IllegalArgumentException("Invalid type '"+s+"': missing base type name at position "+baseNameStart));
                return baseTypeConverter.apply(baseName, params.asUnmodifiable());
            }
        }

    }
}
