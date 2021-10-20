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
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.ser.std.NonTypedScalarSerializerBase;
import java.io.IOException;
import java.time.Instant;
import java.util.Date;
import org.apache.brooklyn.util.core.json.DurationSerializer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;

public class CommonTypesSerialization {

    public static void apply(ObjectMapper mapper) {
        mapper.registerModule(new SimpleModule()
                .addSerializer(Duration.class, new DurationSerializer())

                .addSerializer(Date.class, new DateSerializer())
                .addDeserializer(Date.class, (JsonDeserializer) new DateDeserializer())
                .addSerializer(Instant.class, new InstantSerializer())
                .addDeserializer(Instant.class, (JsonDeserializer) new InstantDeserializer())
        );
    }

    public static class DateSerializer extends NonTypedScalarSerializerBase<Date> {
        protected DateSerializer() { super(Date.class); }
        @Override
        public void serialize(Date value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(Time.makeIso8601DateString(value));
        }
    }
    public static class DateDeserializer extends JsonSymbolDependentDeserializer {
        @Override
        protected Object deserializeToken(JsonParser p) throws IOException {
            Object v = p.readValueAs(Object.class);
            if (v instanceof String) return Time.parseDate((String)v);
            throw new IllegalArgumentException("Cannot deserialize '"+v+"' as Date");
        }
    }

    public static class InstantSerializer extends NonTypedScalarSerializerBase<Instant> {
        protected InstantSerializer() { super(Instant.class); }
        @Override
        public void serialize(Instant value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(Time.makeIso8601DateStringZ(value));
        }
    }
    public static class InstantDeserializer extends JsonSymbolDependentDeserializer {
        @Override
        protected Object deserializeToken(JsonParser p) throws IOException {
            Object v = p.readValueAs(Object.class);
            if (v instanceof String) return Time.parseInstant((String)v);
            throw new IllegalArgumentException("Cannot deserialize '"+v+"' as Instant");
        }
    }

}
