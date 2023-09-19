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

import java.io.IOException;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.brooklyn.core.resolve.jackson.PrimitiveTokenOrExpectedObject.PrimitiveTokenOrExpectedObjectDeserializer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;

import static org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.createBeanDeserializer;

/**
 * This is an object intended for use in Jackson setters. When supplied with a primitive token, the value is stored as the primitive.
 * For anything else, it is deserialized according to the type T.
 * Thus setters can inspect the object and call the right method, and a custom string can be handled specially without need for lower-level Jackson interrogation,
 * but if the object, or a map of the object is supplied, it is used.
 */
@JsonDeserialize(using = PrimitiveTokenOrExpectedObjectDeserializer.class)
public class PrimitiveTokenOrExpectedObject<T> {

    // exactly one of these will be set
    public T object;
    public Object primitive;

    public boolean hasObject() { return object!=null; }
    public boolean hasPrimitive() { return primitive!=null; }
    public boolean hasStringPrimitive() { return primitive instanceof String; }

    public T asObject() { return object; }
    public Object asPrimitive() { return primitive; }
    public String asString() { if (hasStringPrimitive()) return (String)primitive; return null; }

    public static class PrimitiveTokenOrExpectedObjectDeserializer extends JsonSymbolDependentDeserializer {
        public PrimitiveTokenOrExpectedObjectDeserializer() {
            super();
        }
        @Override
        public JavaType getDefaultType() {
            return ctxt.constructType(Object.class);
        }

        protected Maybe<Object> getTokenValue(JsonToken token, JsonParser p) {
            try {
                if (SIMPLE_TOKENS.contains(token)) {
                    if (JsonToken.VALUE_STRING.equals(token)) return Maybe.of(p.getValueAsString());
                    if (JsonToken.VALUE_NUMBER_INT.equals(token)) return Maybe.of(p.getValueAsInt());
                    if (JsonToken.VALUE_NUMBER_FLOAT.equals(token)) return Maybe.of(p.getValueAsDouble());
                    if (token.isBoolean()) return Maybe.of(p.getValueAsBoolean());
                    if (JsonToken.VALUE_NULL.equals(token)) return Maybe.ofAllowingNull(null);
                }
                return Maybe.absent();
            } catch (IOException e) {
                throw Exceptions.propagate(e);
            }
        }
        @Override
        protected Object deserializeToken(JsonParser p) throws IOException {
            PrimitiveTokenOrExpectedObject result = new PrimitiveTokenOrExpectedObject();
            result.primitive = getTokenValue(p.getCurrentToken(), p).get();
            return result;
        }

        @Override
        protected Object deserializeObject(JsonParser p) throws IOException {
            PrimitiveTokenOrExpectedObject result = new PrimitiveTokenOrExpectedObject();
            result.object = super.deserializeObject(p);
            return result;
        }

        protected JsonDeserializer<?> getObjectDeserializer() throws IOException, JsonProcessingException {
            if (type!=null && PrimitiveTokenOrExpectedObject.class.equals(type.getRawClass())) {
                // this should always happen
                return ctxt.findRootValueDeserializer(type.containedType(0));
            } else {
                return super.getObjectDeserializer();
            }
        }

        public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
            return super.createContextual(ctxt, property);
        }
    }
}
