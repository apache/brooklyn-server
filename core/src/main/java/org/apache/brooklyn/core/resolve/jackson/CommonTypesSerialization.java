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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.DeserializerFactory;
import com.fasterxml.jackson.databind.deser.ResolvableDeserializer;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.createBeanDeserializer;

public class CommonTypesSerialization {

    private static final Logger LOG = LoggerFactory.getLogger(CommonTypesSerialization.class);

    public static void apply(ObjectMapper mapper) {
        apply(mapper, null);
    }

    public static void apply(ObjectMapper mapper, ManagementContext mgmt) {

        SimpleModule m = new SimpleModule();
        InterceptibleDeserializers interceptible = new InterceptibleDeserializers();
        m.setDeserializers(interceptible);
        new DurationSerialization().apply(m);
        new DateSerialization().apply(m);
        new InstantSerialization().apply(m);
        new ManagementContextSerialization(mgmt).apply(m);
        new BrooklynObjectSerialization(mgmt).apply(m, interceptible);
        new ConfigKeySerialization(mgmt).apply(m);
        new GuavaTypeTokenSerialization().apply(mapper, m, interceptible);

        // see also JsonDeserializerForCommonBrooklynThings, and BrooklynDslCommon coercion of Spec

        mapper.registerModule(m);
    }

    public static class InterceptibleDeserializers extends SimpleDeserializers {
        final List<Function<JavaType,JsonDeserializer<?>>> interceptors = MutableList.of();
        @Override
        public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
            if (type!=null) {
                for (Function<JavaType,JsonDeserializer<?>> ic: interceptors) {
                    JsonDeserializer<?> interception = ic.apply(type);
                    if (interception != null) return interception;
                }
            }
            return super.findBeanDeserializer(type, config, beanDesc);
        }

        public void addInterceptor(Function<JavaType,JsonDeserializer<?>> typeRewriter) {
            interceptors.add(typeRewriter);
        }
        public void addSubtypeInterceptor(Class<?> type, JsonDeserializer<?> deserializer) {
            interceptors.add(jt -> jt.findSuperType(type)!=null ? deserializer : null);
        }
    }

    public static abstract class ObjectAsStringSerializerAndDeserializer<T> {

        public abstract Class<T> getType();
        public abstract String convertObjectToString(T value, JsonGenerator gen, SerializerProvider provider) throws IOException;
        public abstract T convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException;

        public T newEmptyInstance() {
            try {
                Class<T> t = getType();
                Constructor<T> tc = t.getDeclaredConstructor();
                tc.setAccessible(true);
                return tc.newInstance();
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new IllegalArgumentException("Empty instances of " + getType() + " are not supported; provide a 'value:' indicating the value", e);
            }
        }

        public T convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
            throw new IllegalStateException(getType()+" should be supplied as map with 'value'; instead had " + value);
        }

        protected T doConvertSpecialMapViaNewSimpleMapper(Map value) throws IOException {
            // a hack to support default bean deserialization as a fallback; we could deprecate, but some tests support eg nanos: xx for duration
            // TODO - would be better to use createBeanDeserializer ?
            ObjectMapper m = BeanWithTypeUtils.newSimpleYamlMapper();
            m.setVisibility(new VisibilityChecker.Std(JsonAutoDetect.Visibility.ANY, JsonAutoDetect.Visibility.ANY, JsonAutoDetect.Visibility.ANY, JsonAutoDetect.Visibility.ANY, JsonAutoDetect.Visibility.ANY));
            return m.readerFor(getType()).readValue(m.writeValueAsString(value));
        }

        public <T extends SimpleModule> T apply(T module) {
            module.addSerializer(getType(), new Serializer());
            module.addDeserializer(getType(), new Deserializer());
            return module;
        }

        protected class Serializer extends JsonSerializer<T> {
            @Override
            public void serialize(T value, JsonGenerator gen, SerializerProvider provider) throws IOException {
                gen.writeString( convertObjectToString(value, gen, provider) );
            }
            @Override
            public void serializeWithType(T value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
                if (value==null) {
                    gen.writeNull();
                    return;
                }

                if (typeSer.getTypeIdResolver() instanceof AsPropertyIfAmbiguous.HasBaseType) {
                    if (((AsPropertyIfAmbiguous.HasBaseType)typeSer.getTypeIdResolver()).getBaseType().findSuperType(getType())!=null) {
                        gen.writeString(convertObjectToString(value, gen, serializers));
                        return;
                    }
                }

                // write as object with type and value if type is ambiguous
                gen.writeStartObject();
                gen.writeStringField(BrooklynJacksonSerializationUtils.TYPE, getType().getName());
                gen.writeStringField(BrooklynJacksonSerializationUtils.VALUE, convertObjectToString(value, gen, serializers));
                gen.writeEndObject();
            }
        }

        protected class Deserializer extends JsonDeserializer<T> {
            @Override
            public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                try {
                    Object valueO = BrooklynJacksonSerializationUtils.readObject(ctxt, p);
                    Object value = valueO;
                    if (value instanceof Map) {
                        if (((Map)value).size()==1 && ((Map)value).containsKey(BrooklynJacksonSerializationUtils.VALUE)) {
                            value = ((Map) value).get(BrooklynJacksonSerializationUtils.VALUE);
                        } else {
                            return convertSpecialMapToObject((Map)value, p, ctxt);
                        }
                    }

                    if (value==null) {
                        if (valueO==null) return newEmptyInstance();
                        return null;
                    } else if (value instanceof String || Boxing.isPrimitiveOrBoxedClass(value.getClass())) {
                        return convertStringToObject(value.toString(), p, ctxt);
                    } else if (value instanceof Map) {
                        return convertSpecialMapToObject((Map)value, p, ctxt);
                    } else if (value.getClass().equals(Object.class)) {
                        return newEmptyInstance();
                    } else {
                        throw new IllegalStateException(getType()+" should be supplied as string or map with 'type' and 'value'; instead had " + value);
                    }
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }
        }
    }

    public static class DurationSerialization extends ObjectAsStringSerializerAndDeserializer<Duration> {
        @Override public Class<Duration> getType() { return Duration.class; }
        @Override public String convertObjectToString(Duration value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return value.toString();
        }
        @Override public Duration convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return Time.parseDuration(value);
        }
        @Override public Duration convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return doConvertSpecialMapViaNewSimpleMapper(value);
        }
    }

    public static class DateSerialization extends ObjectAsStringSerializerAndDeserializer<Date> {
        @Override public Class<Date> getType() { return Date.class; }
        @Override public String convertObjectToString(Date value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return Time.makeIso8601DateString(value);
        }
        @Override public Date convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return Time.parseDate(value);
        }
        @Override public Date convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return doConvertSpecialMapViaNewSimpleMapper(value);
        }
    }

    public static class InstantSerialization extends ObjectAsStringSerializerAndDeserializer<Instant> {
        @Override public Class<Instant> getType() { return Instant.class; }
        @Override public String convertObjectToString(Instant value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return Time.makeIso8601DateStringZ(value);
        }
        @Override public Instant convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return Time.parseInstant(value);
        }
        @Override public Instant convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
            return doConvertSpecialMapViaNewSimpleMapper(value);
        }
    }

    public static class ManagementContextSerialization extends ObjectAsStringSerializerAndDeserializer<ManagementContext> {
        private final ManagementContext mgmt;

        public ManagementContextSerialization(ManagementContext mgmt) { this.mgmt = mgmt; }
        @Override public Class<ManagementContext> getType() { return ManagementContext.class; }

        @Override public String convertObjectToString(ManagementContext value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return BrooklynJacksonSerializationUtils.DEFAULT;
        }
        @Override public ManagementContext convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            if (BrooklynJacksonSerializationUtils.DEFAULT.equals(value)) {
                if (mgmt!=null) return mgmt;
                throw new IllegalArgumentException("ManagementContext cannot be deserialized here");
            }
            throw new IllegalStateException("ManagementContext should be recorded as 'default' to be deserialized correctly");
        }
    }

    public static class BrooklynObjectSerialization extends ObjectAsStringSerializerAndDeserializer<BrooklynObject> {
        private final ManagementContext mgmt;

        public BrooklynObjectSerialization(ManagementContext mgmt) { this.mgmt = mgmt; }

        public <T extends SimpleModule> T apply(T module, InterceptibleDeserializers interceptable) {
            // apply to all subtypes of BO
            interceptable.addSubtypeInterceptor(getType(), new Deserializer());

            return apply(module);
        }

        @Override public Class<BrooklynObject> getType() { return BrooklynObject.class; }

        @Override public String convertObjectToString(BrooklynObject value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return value.getId();
        }
        @Override public BrooklynObject convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            if (mgmt==null) {
                throw new IllegalArgumentException("BrooklynObject cannot be deserialized here");
            }
            // we could support 'current' to use tasks to resolve, which might be handy
            BrooklynObject result = mgmt.lookup(value);
            if (result!=null) return result;
            throw new IllegalStateException("Entity or other BrooklynObject '"+value+"' is not known here");
        }
    }

    public static class ConfigKeySerialization {
        private final ManagementContext mgmt;
        public ConfigKeySerialization(ManagementContext mgmt) { this.mgmt = mgmt; }
        public void apply(SimpleModule m) {
            m.addKeyDeserializer(ConfigKey.class, new CKKeyDeserializer());
        }
        static class CKKeyDeserializer extends KeyDeserializer {
            @Override
            public Object deserializeKey(String key, DeserializationContext ctxt) throws IOException {
                // ignores type, but allows us to serialize entity specs etc
                return ConfigKeys.newConfigKey(Object.class, key);
            }
        }
    }

    /** Serializing TypeTokens is annoying; basically we wrap the Type, and intercept 3 things specially */
    public static class GuavaTypeTokenSerialization extends BeanSerializerModifier {

        public static final String RUNTIME_TYPE = "runtimeType";

        public void apply(ObjectMapper m, SimpleModule module, InterceptibleDeserializers interceptible) {
            m.setSerializerFactory(m.getSerializerFactory().withSerializerModifier(this));

            TypeTokenDeserializer ttDeserializer = new TypeTokenDeserializer();
            interceptible.addSubtypeInterceptor(TypeToken.class, ttDeserializer);
            module.addDeserializer(TypeToken.class, (JsonDeserializer) ttDeserializer);

            ParameterizedTypeDeserializer ptDeserializer = new ParameterizedTypeDeserializer();
            interceptible.addSubtypeInterceptor(ParameterizedType.class, ptDeserializer);
            module.addDeserializer(ParameterizedType.class, (JsonDeserializer) ptDeserializer);

            module.addDeserializer(Type.class, (JsonDeserializer) new JavaLangTypeDeserializer());
        }

        @Override
        public List<BeanPropertyWriter> changeProperties(SerializationConfig config, BeanDescription beanDesc, List<BeanPropertyWriter> beanProperties) {
            if (TypeToken.class.isAssignableFrom(beanDesc.getBeanClass())) {
                // not needed, but kept in case useful in future
//                Set<String> fields = MutableList.copyOf(Reflections.findFields(beanDesc.getBeanClass(), null, null))
//                        .stream().map(f -> f.getName()).collect(Collectors.toSet());
                beanProperties = beanProperties.stream().filter(p -> RUNTIME_TYPE.equals(p.getName())).collect(Collectors.toList());
            }

            return beanProperties;
        }

        static class TypeTokenDeserializer extends JsonSymbolDependentDeserializer {
            @Override
            public JavaType getDefaultType() {
                return ctxt.constructType(RuntimeTypeHolder.class);
            }

            @Override
            protected Object deserializeObject(JsonParser p) throws IOException {
                Object holder = createBeanDeserializer(ctxt, getDefaultType()).deserialize(p, ctxt);
                return TypeToken.of( ((RuntimeTypeHolder)holder).runtimeType );
            }
        }

        static class ParameterizedTypeDeserializer extends JsonSymbolDependentDeserializer {
            @Override
            public JavaType getDefaultType() {
                return ctxt.constructType(BrooklynTypeNameResolution.BetterToStringParameterizedTypeImpl.class);
            }

            @Override
            public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
                super.createContextual(ctxt, property);
                // always use this one
                type = getDefaultType();
                return this;
            }
        }

        static class RuntimeTypeHolder {
            private Type runtimeType;
        }

        static class JavaLangTypeDeserializer extends JsonSymbolDependentDeserializer {
            @Override
            public JavaType getDefaultType() {
                return ctxt.constructType(Class.class);
            }

            @Override
            protected JsonDeserializer<?> getTokenDeserializer() throws IOException {
                return createBeanDeserializer(ctxt, getDefaultType());
            }
        }
    }

}
