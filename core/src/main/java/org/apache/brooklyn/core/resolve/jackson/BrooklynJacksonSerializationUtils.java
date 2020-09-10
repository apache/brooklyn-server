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

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerModifier;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.function.Function;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrooklynJacksonSerializationUtils {

    private static final Logger log = LoggerFactory.getLogger(BrooklynJacksonSerializationUtils.class);

    public static <T> TypeReference<T> asTypeReference(TypeToken<T> typeToken) {
        return new TypeReference<T>() {
            @Override
            public Type getType() {
                return typeToken.getType();
            }
        };
    }

    public static class ConfigurableBeanDeserializerModifier extends BeanDeserializerModifier {
        List<Function<JsonDeserializer<?>,JsonDeserializer<?>>> deserializerWrappers = MutableList.of();
        List<Function<Object,Object>> postConstructFunctions = MutableList.of();

        public ConfigurableBeanDeserializerModifier addDeserializerWrapper(Function<JsonDeserializer<?>,JsonDeserializer<?>> ...f) {
            for (Function<JsonDeserializer<?>,JsonDeserializer<?>> fi: f) deserializerWrappers.add(fi);
            return this;
        }

        public ConfigurableBeanDeserializerModifier addPostConstructFunction(Function<Object,Object> f) {
            postConstructFunctions.add(f);
            return this;
        }

        public JsonDeserializer<?> modifyDeserializer(DeserializationConfig config,
                                                      BeanDescription beanDesc,
                                                      JsonDeserializer<?> deserializer) {
            for (Function<JsonDeserializer<?>,JsonDeserializer<?>> d: deserializerWrappers) {
                deserializer = d.apply(deserializer);
            }
            if (!postConstructFunctions.isEmpty()) {
                deserializer = new JsonDeserializerInvokingPostConstruct(postConstructFunctions, deserializer);
            }
            return deserializer;
        }

        public <T extends ObjectMapper> T apply(T mapper) {
            SimpleModule module = new SimpleModule();
            module.setDeserializerModifier(this);
            return (T)mapper.registerModule(module);
        }
    }

    static class JsonDeserializerInvokingPostConstruct extends DelegatingDeserializer {
        final List<Function<Object,Object>> postConstructFunctions;
        public JsonDeserializerInvokingPostConstruct(List<Function<Object,Object>> postConstructFunctions, JsonDeserializer<?> deserializer) {
            super(deserializer);
            this.postConstructFunctions = postConstructFunctions;
        }

        @Override
        protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
            return new JsonDeserializerInvokingPostConstruct(postConstructFunctions, newDelegatee);
        }

        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return postConstructFunctions.stream().reduce(Function::andThen).orElse(x -> x).apply( _delegatee.deserialize(jp, ctxt) );
        }
    }

    public static class NestedLoggingDeserializer extends DelegatingDeserializer {
        private final StringBuilder prefix;

        public NestedLoggingDeserializer(StringBuilder prefix, JsonDeserializer<?> deserializer) {
            super(deserializer);
            this.prefix = prefix;
        }

        @Override
        protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
            prefix.append(".");
            try {
                return new NestedLoggingDeserializer(prefix, newDelegatee);
            } finally {
                prefix.setLength(prefix.length()-1);
            }
        }

        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            String v = jp.getCurrentToken()==JsonToken.VALUE_STRING ? jp.getValueAsString() : null;
            try {
                prefix.append("  ");
                log.info(prefix+"> "+jp.getCurrentToken());
                Object result = _delegatee.deserialize(jp, ctxt);
                log.info(prefix+"< "+result);
                return result;
            } catch (Exception e) {
                log.info(prefix+"< "+e);
                throw e;
            } finally {
                prefix.setLength(prefix.length()-2);
            }
        }
    }

    public static class JsonDeserializerForCommonBrooklynThings extends DelegatingDeserializer {
        public JsonDeserializerForCommonBrooklynThings(JsonDeserializer<?> deserializer) {
            super(deserializer);
        }

        @Override
        protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
            return new JsonDeserializerForCommonBrooklynThings(newDelegatee);
        }

        @Override
        public Object deserialize(JsonParser jp, DeserializationContext ctxt) throws IOException {
            String v = jp.getCurrentToken()==JsonToken.VALUE_STRING ? jp.getValueAsString() : null;
            try {
                return _delegatee.deserialize(jp, ctxt);
            } catch (Exception e) {
                // if it fails, get the raw json and attempt a coercion?; currently just for strings
                if (v!=null && handledType()!=null) {
                    // attempt type coercion
                    Maybe<?> coercion = TypeCoercions.tryCoerce(v, handledType());
                    if (coercion.isPresent()) return coercion.get();
                }
                throw e;
            }
        }

    }


}
