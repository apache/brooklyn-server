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
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.introspect.VisibilityChecker;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.BeanPropertyWriter;
import com.fasterxml.jackson.databind.ser.BeanSerializerModifier;
import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.core.typereg.TypePlanTransformers;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.time.Instant;
import java.util.*;
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
        new ByteArrayObjectStreamSerialization().apply(m);
        new InstantSerialization().apply(m);
        new ManagementContextSerialization(mgmt).apply(m);
        new BrooklynObjectSerialization(mgmt).apply(m, interceptible);
        new ConfigKeySerialization(mgmt).apply(m);
        new PredicateSerialization(mgmt).apply(m);
        new GuavaTypeTokenSerialization().apply(mapper, m, interceptible);
        //mapper.setAnnotationIntrospector(new CustomAnnotationInspector());

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
        public Class<? extends T> getType(Object instance) {
            return getType();
        }
        public abstract String convertObjectToString(T value, JsonGenerator gen, SerializerProvider provider) throws IOException;
        public abstract T convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException;

        public T newEmptyInstance() {
            return newEmptyInstance(getType());
        }

        public T newEmptyInstance(Class<T> t) {
            try {
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

        protected T copyInto(T src, T target) {
            throw new IllegalStateException("Not supported to read into "+getType()+", from "+src+" into "+target);
        }

        public <T extends SimpleModule> T apply(T module) {
            module.addSerializer(getType(), new Serializer());
            module.addDeserializer(getType(), newDeserializer());
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
                gen.writeStringField(BrooklynJacksonSerializationUtils.TYPE, getType(value).getName());
                gen.writeStringField(BrooklynJacksonSerializationUtils.VALUE, convertObjectToString(value, gen, serializers));
                gen.writeEndObject();
            }
        }

        protected JsonDeserializer<T> newDeserializer() {
            return new Deserializer();
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
                        return deserializeOther(value);
                    }
                } catch (Exception e) {
                    throw Exceptions.propagate(e);
                }
            }

            public T deserializeOther(Object value) {
                throw new IllegalStateException(getType()+" should be supplied as string or map with 'type' and 'value'; instead had " + value);
            }

            @Override
            public T deserialize(JsonParser p, DeserializationContext ctxt, T intoValue) throws IOException, JsonProcessingException {
                return copyInto(deserialize(p, ctxt), intoValue);
            }

            protected T newEmptyInstance() {
                return ObjectAsStringSerializerAndDeserializer.this.newEmptyInstance();
            }

            public T convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
                return ObjectAsStringSerializerAndDeserializer.this.convertSpecialMapToObject(value, p, ctxt);
            }

            public T convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
                return ObjectAsStringSerializerAndDeserializer.this.convertStringToObject(value, p, ctxt);
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

    public static class ByteArrayObjectStreamSerialization extends ObjectAsStringSerializerAndDeserializer<ByteArrayOutputStream> {
        @Override public Class<ByteArrayOutputStream> getType() { return ByteArrayOutputStream.class; }
        @Override public String convertObjectToString(ByteArrayOutputStream value, JsonGenerator gen, SerializerProvider provider) throws IOException {
            return provider.getConfig().getBase64Variant().encode(value.toByteArray());
//            byte[] array = value.toByteArray();
//            gen.writeBinary(provider.getConfig().getBase64Variant(), array, 0, array.length);
        }
        @Override public ByteArrayOutputStream convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            out.write(ctxt.getConfig().getBase64Variant().decode(value));
//            out.write(p.getBinaryValue());
            return out;
        }

        @Override
        protected ByteArrayOutputStream copyInto(ByteArrayOutputStream src, ByteArrayOutputStream target) {
            try {
                target.write(src.toByteArray());
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
            return target;
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
            interceptable.addSubtypeInterceptor(getType(), newDeserializer());

            return apply(module);
        }

        @Override public Class<BrooklynObject> getType() {
            return BrooklynObject.class;
        }
        @Override public Class<? extends BrooklynObject> getType(Object instance) {
            return BrooklynObjectType.of((BrooklynObject) instance).getInterfaceType();
        }

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

        @Override
        protected JsonDeserializer<BrooklynObject> newDeserializer() {
            return new BODeserializer();
        }

        class BODeserializer extends Deserializer implements ContextualDeserializer, BrooklynJacksonSerializationUtils.RecontextualDeserializer {
            private DeserializationContext context;
            private JavaType knownConcreteType;

            BODeserializer() {}
            BODeserializer(DeserializationContext context, JavaType knownConcreteType) {
                this.context = context;
                this.knownConcreteType = knownConcreteType;
            }
            @Override
            public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
                return new BODeserializer(ctxt, null);
            }
            @Override
            public JsonDeserializer<?> recreateContextual() {
                if (context!=null && context.getContextualType()!=null && context.getContextualType().isConcrete()) {
                    return new BODeserializer(context, context.getContextualType());
                }
                return this;
            }

            @Override
            protected BrooklynObject newEmptyInstance() {
                /* context buries the contextual types when called via createContextual, because we are a delegate; and has cleared it by the time it gets here */
                if (knownConcreteType != null) {
                    return BrooklynObjectSerialization.this.newEmptyInstance((Class<BrooklynObject>) knownConcreteType.getRawClass());
                }
                return super.newEmptyInstance();
            }

            @Override public BrooklynObject convertSpecialMapToObject(Map value, JsonParser p, DeserializationContext ctxt) throws IOException {
                BrooklynObject result = null;
                if (value.size()<=2) {
                    Object id = value.get("id");
                    Object type = value.get("type");
                    boolean isExistingInstance = false;
                    if (id instanceof String) {
                        // looks like a type+id map, so we should be able to look it up
                        if (type instanceof String) {
                            Optional<BrooklynObjectType> typeO = Arrays.stream(BrooklynObjectType.values()).filter(t -> type.equals(t.getInterfaceType().getName())).findAny();
                            if (typeO.isPresent()) {
                                isExistingInstance = true;
                                result = mgmt.lookup((String) id, typeO.get().getInterfaceType());
                            } else {
                                // fall through to below
                            }
                        } else {
                            isExistingInstance = true;
                            result = mgmt.lookup((String) id, null);
                        }
                    }

                    if (result==null) {
                        if (isExistingInstance) {
                            throw new IllegalStateException("Cannot find serialized shorthand reference to entity " + value);
                        }
                        // attempt to instantiate it - to support add-policy workflow step
                        // TODO we should prefer a spec, and coerce to a spec (separate coercer),
                        // moving some of the code from BrooklynYamlTypeInstantiator into here / near.
                        // if using this, we should take care to keep it JsonPassThroughDeserializer until we want it

                        if (type==null && knownConcreteType!=null) {
                            // type should have come from outer deserialier
                            result = newEmptyInstance();

                            FlagUtils.setFieldsFromFlags(value, result);
                        }
                    }
                }
                if (result!=null) return result;

                throw new IllegalStateException("Entity instances and other Brooklyn objects should be supplied as unique IDs; they cannot be instantiated from YAML. If a spec is desired, the type should be known or use $brooklyn:entitySpec.");
            }

            @Override public BrooklynObject convertStringToObject(String value, JsonParser p, DeserializationContext ctxt) throws IOException {
                try {
                    return super.convertStringToObject(value, p, ctxt);
                } catch (Exception e) {
                    Exceptions.propagateIfFatal(e);
                    LOG.warn("Reference to BrooklynObject "+value+" which is no longer available; replacing with 'null'");
                    return null;
                }
            }
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

    public static class PredicateSerialization {
        private final ManagementContext mgmt;
        public PredicateSerialization(ManagementContext mgmt) { this.mgmt = mgmt; }
        public void apply(SimpleModule m) {
            m.addDeserializer(Predicate.class, (JsonDeserializer) new DslPredicates.DslPredicateJsonDeserializer());
            m.addDeserializer(java.util.function.Predicate.class, (JsonDeserializer) new DslPredicates.DslPredicateJsonDeserializer());
            m.addDeserializer(DslPredicates.DslPredicate.class, (JsonDeserializer) new DslPredicates.DslPredicateJsonDeserializer());
            m.addDeserializer(DslPredicates.DslEntityPredicate.class, (JsonDeserializer) new DslPredicates.DslPredicateJsonDeserializer());
        }
    }

    /** Serializing TypeTokens is annoying; basically we wrap the Type, and intercept 3 things specially */
    // we've tried jackson-datatype-guava's mapper.registerModule(new GuavaModule())
    // but it doesn't support TypeToken (or guava Predicates)
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
                Object holder = contextualize(createBeanDeserializer(ctxt, getDefaultType())).deserialize(p, ctxt);
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

            // jackson compatibility
            private void setType(Object type) {
                // ignore; always deserialize as SimpleTypeToken
            }
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

    // kept for reference, if we did want to customize the mode (but you cannot from here inject parameter names)
//    public static class CustomAnnotationInspector extends JacksonAnnotationIntrospector {
//        @Override
//        public JsonCreator.Mode findCreatorAnnotation(MapperConfig<?> config, Annotated a) {
//            // does not work
////            if (a.getAnnotated() instanceof Constructor && ((Constructor)a.getAnnotated()).getDeclaringClass().getPackage().equals(Predicate.class.getPackage())) {
////                return JsonCreator.Mode.DELEGATING;
////            }
//            return super.findCreatorAnnotation(config, a);
//        }
//    }

}
