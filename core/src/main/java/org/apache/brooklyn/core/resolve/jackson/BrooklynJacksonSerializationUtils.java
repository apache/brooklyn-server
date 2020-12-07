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
import com.fasterxml.jackson.databind.deser.std.UntypedObjectDeserializer;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.function.BiFunction;
import java.util.function.Function;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
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

    static class JsonDeserializerInvokingPostConstruct extends JacksonBetterDelegatingDeserializer {
        final List<Function<Object,Object>> postConstructFunctions;
        public JsonDeserializerInvokingPostConstruct(List<Function<Object,Object>> postConstructFunctions, JsonDeserializer<?> deserializer) {
            super(deserializer, d -> new JsonDeserializerInvokingPostConstruct(postConstructFunctions, d));
            this.postConstructFunctions = postConstructFunctions;
        }

        @Override
        protected Object deserializeWrapper(JsonParser jp, DeserializationContext ctxt, BiFunctionThrowsIoException<JsonParser, DeserializationContext, Object> nestedDeserialize) throws IOException {
            return postConstructFunctions.stream().reduce(Function::andThen).orElse(x -> x).apply(
                    nestedDeserialize.apply(jp, ctxt) );
        }
    }

    public static class NestedLoggingDeserializer extends JacksonBetterDelegatingDeserializer {
        private final StringBuilder prefix;

        public NestedLoggingDeserializer(StringBuilder prefix, JsonDeserializer<?> deserializer) {
            super(deserializer, d -> new NestedLoggingDeserializer(prefix, d));
            this.prefix = prefix;
        }

        @Override
        protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
            prefix.append(".");
            try {
                return constructor.apply(newDelegatee);
            } finally {
                prefix.setLength(prefix.length()-1);
            }
        }

        @Override
        protected Object deserializeWrapper(JsonParser jp, DeserializationContext ctxt, BiFunctionThrowsIoException<JsonParser, DeserializationContext, Object> nestedDeserialize) throws IOException {
            String v = jp.getCurrentToken()==JsonToken.VALUE_STRING ? jp.getValueAsString() : null;
            try {
                prefix.append("  ");
                log.info(prefix+"> "+jp.getCurrentToken());
                Object result = nestedDeserialize.apply(jp, ctxt);
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

    public static class JsonDeserializerForCommonBrooklynThings extends JacksonBetterDelegatingDeserializer {
        // injected from CAMP platform; inelegant, but effective
        public static BiFunction<ManagementContext,Object,Object> BROOKLYN_PARSE_DSL_FUNCTION = null;

        private final ManagementContext mgmt;
        public JsonDeserializerForCommonBrooklynThings(ManagementContext mgmt, JsonDeserializer<?> delagatee) {
            super(delagatee, d -> new JsonDeserializerForCommonBrooklynThings(mgmt, d));
            this.mgmt = mgmt;
        }

        @Override
        protected Object deserializeWrapper(JsonParser jp, DeserializationContext ctxt, BiFunctionThrowsIoException<JsonParser, DeserializationContext, Object> nestedDeserialize) throws IOException {
            String v = jp.getCurrentToken()==JsonToken.VALUE_STRING ? jp.getValueAsString() : null;
            try {
                Object result = nestedDeserialize.apply(jp, ctxt);

                if (BROOKLYN_PARSE_DSL_FUNCTION!=null && mgmt!=null && result instanceof Map) {
                    Map<?, ?> rm = (Map<?, ?>) result;
                    if (Object.class.equals(_valueClass) || _valueClass==null) {
                        // this marker indicates that a DSL object was serialized and we need to re-parse it to deserialize it
                        Object brooklynLiteral = rm.get("$brooklyn:literal");
                        if (brooklynLiteral != null) {
                            return BROOKLYN_PARSE_DSL_FUNCTION.apply(mgmt, brooklynLiteral);
                        }
                    }
                }

                return result;
            } catch (Exception e) {
                // if it fails, get the raw object and attempt a coercion?; currently just for strings
                if (v!=null && handledType()!=null) {
                    // attempt type coercion
                    Maybe<?> coercion = TypeCoercions.tryCoerce(v, handledType());
                    if (coercion.isPresent()) return coercion.get();
                }
                if (e instanceof IOException) throw (IOException)e;
                throw Exceptions.propagate(e);
            }
        }
    }


}
