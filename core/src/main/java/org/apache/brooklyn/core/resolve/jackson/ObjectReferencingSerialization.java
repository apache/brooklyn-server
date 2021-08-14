package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanDescription;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.cfg.SerializerFactoryConfig;
import com.fasterxml.jackson.databind.jsontype.TypeSerializer;
import com.fasterxml.jackson.databind.ser.BeanSerializerFactory;
import com.fasterxml.jackson.databind.ser.SerializerFactory;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import java.io.IOException;
import java.util.Map;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectReferencingSerialization {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectReferencingSerialization.class);

    public ObjectMapper useMapper(ObjectMapper mapper) {
        BiMap<String,Object> backingMap = HashBiMap.create();
        mapper.setSerializerFactory(ObjectReferencingSerializerFactory.extending(mapper.getSerializerFactory(), new ObjectReferenceSerializer(backingMap)));
        mapper = new ConfigurableBeanDeserializerModifier()
                .addDeserializerWrapper(
                        d -> new ObjectReferencingJsonDeserializer(d, backingMap)
                ).apply(mapper);

//        mapper.registerModule(new SimpleModule()
//                .addSerializer(Object.class, new ObjectReferenceSerializer(backingMap))
//                .addDeserializer(Object.class, new ObjectReferenceDeserializer(backingMap))
//        );
        return mapper;
    }


    static class ObjectReference {
        String id;
        public ObjectReference() {}
        public ObjectReference(String id) { this.id = id; }
    }


    static class ObjectReferenceSerializer extends StdSerializer<Object> {
        private final BiMap<String, Object> backingMap;

        public ObjectReferenceSerializer(BiMap<String, Object> backingMap) {
            super(Object.class);
            this.backingMap = backingMap;
        }

        @Override
        public void serializeWithType(Object value, JsonGenerator gen, SerializerProvider serializers, TypeSerializer typeSer) throws IOException {
            serialize(value, gen, serializers);
        }

        @Override
        public void serialize(Object value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            String id = backingMap.inverse().get(value);
            if (id==null) {
                id = Identifiers.makeRandomId(12);
                backingMap.put(id, value);
            }

            gen.writeObjectRef(id);

//            serializers.findValueSerializer(Map.class, null).serializeWithType(MutableMap.of("@ref", id), gen, serializers,
//                    serializers.findTypeSerializer(serializers.constructType(Object.class)));
        }
    }

    static class ObjectReferenceDeserializer extends JsonDeserializer<Object> {
        public ObjectReferenceDeserializer(Map<String, Object> backingMap) {
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            return null;
        }
    }

    public static class ObjectReferencingSerializerFactory extends BeanSerializerFactory {
        private final ObjectReferenceSerializer serializer;

        protected ObjectReferencingSerializerFactory(ObjectReferenceSerializer serializer, SerializerFactoryConfig config) {
            super(config);
            this.serializer = serializer;
        }

        public static ObjectReferencingSerializerFactory extending(SerializerFactory factory, ObjectReferenceSerializer serializer) {
            if (factory == null) return new ObjectReferencingSerializerFactory(serializer, null);
            if (factory instanceof BeanSerializerFactory) return new ObjectReferencingSerializerFactory(serializer, ((BeanSerializerFactory) factory).getFactoryConfig() );
            throw new IllegalStateException("Cannot extend "+factory);
        }
        @Override
        public ObjectReferencingSerializerFactory withConfig(SerializerFactoryConfig config) {
            if (_factoryConfig == config) return this;
            return new ObjectReferencingSerializerFactory(serializer, config);
        }

        // --- our special behaviour

        @Override
        protected JsonSerializer<Object> constructBeanOrAddOnSerializer(SerializerProvider prov, JavaType type, BeanDescription beanDesc, boolean staticTyping) throws JsonMappingException {
            return serializer;
        }
    }

    static class ObjectReferencingJsonDeserializer extends JacksonBetterDelegatingDeserializer {
        private final BiMap<String, Object> backingMap;

        public ObjectReferencingJsonDeserializer(JsonDeserializer<?> d, BiMap<String, Object> backingMap) {
            super(d, (d2) -> new ObjectReferencingJsonDeserializer(d2, backingMap));
            this.backingMap = backingMap;
        }

        @Override
        protected Object deserializeWrapper(JsonParser jp, DeserializationContext ctxt, BiFunctionThrowsIoException<JsonParser, DeserializationContext, Object> nestedDeserialize) throws IOException {
            String v = jp.getCurrentToken()== JsonToken.VALUE_STRING ? jp.getValueAsString() : null;
            if (v!=null) {
                Object result = backingMap.get(v);
                if (result!=null) return result;
            }
            return nestedDeserialize.apply(jp, ctxt);
        }
    }
//
//    public static class ObjectReferencingDeserializerFactory extends BeanDeserializerFactory {
//        protected ObjectReferencingDeserializerFactory(DeserializerFactoryConfig config) {
//            super(config);
//        }
//
//        public static ObjectReferencingDeserializerFactory extending(DeserializerFactory factory) {
//            if (factory == null) return new ObjectReferencingDeserializerFactory(null);
//            if (factory instanceof ObjectReferencingDeserializerFactory) return (ObjectReferencingDeserializerFactory) factory;
//            if (factory instanceof BeanDeserializerFactory) return new ObjectReferencingDeserializerFactory( ((BeanDeserializerFactory) factory).getFactoryConfig() );
//            throw new IllegalStateException("Cannot extend "+factory);
//        }
//        @Override
//        public ObjectReferencingDeserializerFactory withConfig(DeserializerFactoryConfig config) {
//            if (_factoryConfig == config) return this;
//            return new ObjectReferencingDeserializerFactory(config);
//        }
//
//        // --- our special behaviour
//
//
//        @Override
//        protected BeanDeserializerBuilder constructBeanDeserializerBuilder(DeserializationContext ctxt, BeanDescription beanDesc) {
//            return new BeanDeserializerBuilder(beanDesc, ctxt) {
//                {
//                    _objectIdReader = new ObjectIdReader() {
//
//                    };
//                }
//            };
//        }
//    }

}
