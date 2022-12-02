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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.google.common.annotations.Beta;

import java.io.IOException;

/** deserializer intended for use via contentUsing (not content), to prevent type expansion */
@Beta
public class JsonPassThroughDeserializer extends JsonDeserializer {

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JacksonException {
        try {
            AsPropertyIfAmbiguous.startSuppressingTypeFieldDeserialization();
            return ctxt.readValue(p, Object.class);
        } finally {
            AsPropertyIfAmbiguous.stopSuppressingTypeFieldDeserialization();
        }
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt, Object intoValue) throws IOException {
        throw new IllegalStateException("Unsupported to deserialize into an object");
    }

    @Override
    public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
        return deserialize(p, ctxt);
    }

    @Override
    public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer, Object intoValue) throws IOException {
        return deserialize(p, ctxt, intoValue);
    }

    public static class JsonObjectHolder {
        @JsonCreator
        public JsonObjectHolder(@JsonDeserialize(using=JsonPassThroughDeserializer.class) Object value) {
            this.value = value;
        }
//        public JsonObjectHolder(String value) {
//            this.value = value;
//        }

        @JsonValue
        public Object value;
    }

}
