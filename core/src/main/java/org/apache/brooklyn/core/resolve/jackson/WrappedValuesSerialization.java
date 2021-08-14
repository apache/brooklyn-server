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
package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig;
import com.fasterxml.jackson.databind.introspect.AnnotatedMember;
import com.fasterxml.jackson.databind.introspect.BeanPropertyDefinition;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.jsontype.impl.TypeDeserializerBase;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.PropertyBuilder;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.collect.Iterables;
import java.util.Map;
import java.util.Optional;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerialization.BrooklynRegisteredTypeAndClassNameIdResolver;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.function.Supplier;

public class WrappedValuesSerialization {

    private static final Logger log = LoggerFactory.getLogger(WrappedValuesSerialization.class);

    public static class WrappedValueDeserializer extends JsonDeserializer {
        ManagementContext mgmt;
        public WrappedValueDeserializer(ManagementContext mgmt) {
            this.mgmt = mgmt;
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return deserializeWithType(p, ctxt, null);
        }
        @Override
        public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            Object v = deserializeWithTypeUnwrapped(p, ctxt, typeDeserializer);
            if (JsonDeserializerForCommonBrooklynThings.BROOKLYN_PARSE_DSL_FUNCTION!=null && mgmt!= null) {
                if (looksLikeDsl(v)) {
                    v = JsonDeserializerForCommonBrooklynThings.BROOKLYN_PARSE_DSL_FUNCTION.apply(mgmt, v);
                }
            }
            return WrappedValue.of(v);
        }

        private boolean looksLikeDsl(Object v) {
            if (v instanceof String) {
                return ((String)v).startsWith("$brooklyn:");
            }
            if (v instanceof Map) {
                if (((Map)v).size()==1) {
                    return looksLikeDsl(Iterables.getOnlyElement( ((Map)v).keySet() ));
                }
            }
            return false;
        }

        Object deserializeWithTypeUnwrapped(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
            List<Exception> exceptions = MutableList.of();
            try {
                TokenBuffer b = new TokenBuffer(p, ctxt);
                b.copyCurrentStructure(p);
                try {
                    // this should work for primitives, objects, and suppliers (which will declare type)
                    // only time it won't is where generics are used to drop the type declaration during serialization
                    JavaType genericType = getGenericType(typeDeserializer);
                    if (genericType != null) {
                        return ctxt.findNonContextualValueDeserializer(genericType).deserialize(b.asParserOnFirstToken(), ctxt);
                    }
                } catch (Exception e) {
                    exceptions.add(e);
                }

                // fall back to just using object
                try {
                    return ctxt.findRootValueDeserializer(ctxt.constructType(Object.class)).deserialize(b.asParserOnFirstToken(), ctxt);
                } catch (Exception e) {
                    exceptions.add(e);
                }

                // prefer exceptions that aren't from jackson, that usually indicates a deeper problem
                Optional<Exception> preferred = exceptions.stream().filter(err -> !(err instanceof JsonProcessingException)).findFirst();
                if (!preferred.isPresent()) preferred = exceptions.stream().findFirst();
                throw new IllegalStateException("Cannot parse wrapper data and contextual type info not available: "+exceptions,
                        preferred.orElse(null));
            } finally {
                if (!exceptions.isEmpty() && log.isTraceEnabled()) {
                    log.trace("Exceptions encountered while deserializing: "+exceptions);
                    exceptions.forEach(e -> log.trace("- ", e));
                }
            }
        }
    }

    public static class WrappedValueSerializer<T> extends JsonSerializer<WrappedValue<T>> {
        @Override
        public void serialize(WrappedValue<T> value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            serializeWithType(value, gen, serializers, null);
        }

        @Override
        public void serializeWithType(WrappedValue<T> value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            JavaType baseType = getGenericType(typeSer);
            Object valueToWrite;

            if (value.getSupplier() != null) {
                // could use the generic type here and get a serializer for Supplier<t> --
                // but normally suppliers will have their own serialization and deserialization
                baseType = serializers.constructType(Supplier.class);
                valueToWrite = value.getSupplier();
            } else {
                if (baseType==null) {
                    baseType = serializers.constructType(Object.class);
                }
                valueToWrite = value.get();
            }
            if (valueToWrite==null) {
                // should be omitted
                gen.writeNull();
            } else {
                serializers.findValueSerializer(serializers.constructType(valueToWrite.getClass())).serializeWithType(valueToWrite, gen, serializers, serializers.findTypeSerializer(baseType));
            }
        }
    }

    private static JavaType getGenericType(TypeDeserializer typeDeserializer) {
        if (!(typeDeserializer instanceof TypeDeserializerBase)) return null;
        return getSingleGenericArgumentJavaType(((TypeDeserializerBase) typeDeserializer).baseType());
    }
    private static JavaType getGenericType(TypeSerializer typeSerializer) {
        if (typeSerializer==null) return null;
        if (!(typeSerializer.getTypeIdResolver() instanceof BrooklynRegisteredTypeAndClassNameIdResolver)) return null;
        return getSingleGenericArgumentJavaType( ((BrooklynRegisteredTypeAndClassNameIdResolver)typeSerializer.getTypeIdResolver()).getBaseType() );
    }
    private static JavaType getSingleGenericArgumentJavaType(JavaType x) {
        return x.getBindings().getTypeParameters().stream().findFirst().orElse(null);
    }

    static class NullWrappedValueSuppressingPropertyBuilder extends PropertyBuilder {
        public NullWrappedValueSuppressingPropertyBuilder(SerializationConfig config, BeanDescription beanDesc) {
            super(config, beanDesc);
        }

        @Override
        protected BeanPropertyWriter buildWriter(SerializerProvider prov, BeanPropertyDefinition propDef, JavaType declaredType, JsonSerializer<?> ser, TypeSerializer typeSer, TypeSerializer contentTypeSer, AnnotatedMember am, boolean defaultUseStaticTyping) throws JsonMappingException {
            BeanPropertyWriter bpw = super.buildWriter(prov, propDef, declaredType, ser, typeSer, contentTypeSer, am, defaultUseStaticTyping);
            if (WrappedValue.class.isAssignableFrom(bpw.getMember().getRawType())) {
                bpw = new BeanPropertyWriter(propDef,
                    am, _beanDesc.getClassAnnotations(), declaredType,
                    ser, typeSer, bpw.getSerializationType(), bpw.willSuppressNulls(),
                    WrappedValue.ofNull(), bpw.getViews());
            }
            return bpw;
        }
    }

    static class NullWrappedValueSuppressingBeanSerializerFactory extends BeanSerializerFactory {
        protected NullWrappedValueSuppressingBeanSerializerFactory(SerializerFactoryConfig config) {
            super(config);
        }

        public static NullWrappedValueSuppressingBeanSerializerFactory extending(SerializerFactory factory) {
            if (factory == null) return new NullWrappedValueSuppressingBeanSerializerFactory(null);
            if (factory instanceof NullWrappedValueSuppressingBeanSerializerFactory) return (NullWrappedValueSuppressingBeanSerializerFactory) factory;
            if (factory.getClass() == BeanSerializerFactory.class) return new NullWrappedValueSuppressingBeanSerializerFactory( ((BeanSerializerFactory) factory).getFactoryConfig() );
            throw new IllegalStateException("Cannot extend "+factory);
        }
        @Override
        public SerializerFactory withConfig(SerializerFactoryConfig config) {
            if (_factoryConfig == config) return this;
            return new NullWrappedValueSuppressingBeanSerializerFactory(config);
        }

        // --- our special behaviour

        @Override
        protected PropertyBuilder constructPropertyBuilder(SerializationConfig config, BeanDescription beanDesc) {
            return new NullWrappedValueSuppressingPropertyBuilder(config, beanDesc);
        }

    }

    public static <T> T ensureWrappedValuesInitialized(T x) {
        if (x == null) return x;
        Reflections.findFields(x.getClass(), f -> WrappedValue.class.isAssignableFrom(f.getType()), null)
                .forEach(f -> {
                    try {
                        if (Reflections.getFieldValueMaybe(x, f).isNull()) {
                            f.set(x, WrappedValue.of(null));
                        }
                    } catch (IllegalAccessException e) {
                        Exceptions.propagate(e);
                    }
                });
        return x;
    }

    /** Applies de/serializers which will automatically wrap/unwrap objects and suppliers in a WrappedValue where a WrappedValue is expected.
     * If {@link ManagementContext} is supplied and a DSL deserialization hook is registered this will additionally resolve DSL expressions in the wrapped value. */
    public static ObjectMapper apply(ObjectMapper mapper, ManagementContext mgmt) {
        if (mapper.getSerializationConfig().getDefaultTyper(null) == null) {
            throw new IllegalStateException("Mapper must be set up to use a TypeResolverBuilder including type info for wrapped value serialization to work.");
        }
        return mapper
            .setSerializerFactory(NullWrappedValueSuppressingBeanSerializerFactory.extending(mapper.getSerializerFactory()))
            // we need to see private fields for this to work
            .setVisibility(new VisibilityChecker.Std(Visibility.ANY, Visibility.ANY, Visibility.ANY, Visibility.ANY, Visibility.ANY))
            .registerModule(new SimpleModule()
                .addSerializer(WrappedValue.class, new WrappedValueSerializer())
                .addDeserializer(WrappedValue.class, new WrappedValueDeserializer(mgmt))
            );
    }

}
