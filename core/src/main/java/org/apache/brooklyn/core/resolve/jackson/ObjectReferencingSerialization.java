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

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
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
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.io.StringReader;
import java.lang.reflect.Type;
import java.util.function.Supplier;

import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ObjectReferencingSerialization {

    // some other explorations of approaches, including object id, are in the git history;
    // but this seemed the best as object id was harder to use for all bean types

    private static final Logger LOG = LoggerFactory.getLogger(ObjectReferencingSerialization.class);

    public static final String PREFIX = "_"+ObjectReferencingSerialization.class.getSimpleName()+"_";

    final BiMap<String,Object> backingMap = HashBiMap.create();
    ObjectMapper mapper = null;

    public Object serializeAndDeserialize(Object input) throws IOException {
        return getMapper().readValue(new StringReader(getMapper().writer().writeValueAsString(input)), Object.class);
    }

    public BiMap<String, Object> getBackingMap() {
        return backingMap;
    }

    public ObjectMapper getMapper() {
        if (mapper==null) {
            useAndApplytoMapper(YAMLMapper.builder().build());
        }
        return mapper;
    }

    public ObjectMapper useAndApplytoMapper(ObjectMapper mapper) {
        mapper.setSerializerFactory(ObjectReferencingSerializerFactory.extending(mapper.getSerializerFactory(), new ObjectReferenceSerializer(backingMap)));
        mapper = new ConfigurableBeanDeserializerModifier()
                .addDeserializerWrapper(
                        d -> new ObjectReferencingJsonDeserializer(d, backingMap)
                ).apply(mapper);
        this.mapper = mapper;
        return mapper;
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
                id = PREFIX+Identifiers.makeRandomId(12);
                backingMap.put(id, value);
            }

            gen.writeObjectRef(id);
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
                // we need to exclude the case where a literal 'v' was expected;
                // TokenBuffer does not preserve YAMLParser.isCurrentAlias() so we can't use that;
                // instead we use a prefix to ensure our references are almost certainly unique
                if (v.startsWith(PREFIX)) {
                    Type expected = _valueType != null ? _valueType : _valueClass;

                    // not sure if we ever need to look at contextual type
                    Type expected2 = ctxt.getContextualType() == null ? null : ctxt.getContextualType();
                    if (expected2 != null) {
                        if (expected == null) {
                            expected = expected2;
                        } else {
                            // we have two expectations
                            LOG.debug("Object reference deserialization ambiguity, expected " + expected + " and " + expected2);
                        }
                    }
                    if (expected == null) {
                        expected = Object.class;
                    }

                    Object result = backingMap.get(v);
                    if (result != null) {
                        // previously we excluded a string from being expected, since we wouldn't have converted that to a reference;
                        // however there are contexts where a string is expected but other types are _accepted_ so just trust it if we come here,
                        // and if the expected type is overly strict, return the original object.
                        // if the token buffer is used too much we might have lost the alias reference and still end up with a string,
                        // but so long as this deserializer is preferred which it normally is, losing the alias reference is okay.
                        Maybe resultCoerced = TypeCoercions.tryCoerce(result, TypeToken.of(expected));
                        if (resultCoerced.isAbsent()) {
                            // not uncommon when we are trying to deserialize in a few different ways, or if we are using a string deserializer because the json input is a string;
                            // however it can be useful, to explain why some things aren't coercing via BeanWithTypeUtils.convert.
                            // caller can do 'tryCoerce' to explain.

//                            if (LOG.isDebugEnabled()) LOG.debug("Reference to "+result+" when deserialization could not be coerced to expected type "+expected+"; proceeding but might cause errors");
                        }
                        return resultCoerced.or(result);
                    } else {
                        LOG.debug("Odd - what looks like a reference was received but not found: "+v);
                    }
                }
            }
            return nestedDeserialize.apply(jp, ctxt);
        }
    }

}
