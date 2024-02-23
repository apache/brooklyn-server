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
package org.apache.brooklyn.util.core.json;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.Collections;
import java.util.Set;
import javax.annotation.Nullable;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonStreamContext;
import com.fasterxml.jackson.core.json.JsonWriteContext;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.databind.ser.impl.UnknownSerializer;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * for non-json-serializable classes (quite a lot of them!) simply provide a sensible error message and a toString.
 * TODO maybe we want to attempt to serialize fields instead?  (but being careful not to be self-referential!)
 */
public class ErrorAndToStringUnknownTypeSerializer extends UnknownSerializer {

    private static final Logger log = LoggerFactory.getLogger(ErrorAndToStringUnknownTypeSerializer.class);
    private static Set<String> WARNED_CLASSES = Collections.synchronizedSet(MutableSet.<String>of());

    @Override
    public void serialize(Object value, JsonGenerator jgen, SerializerProvider provider) throws IOException {
        if (BidiSerialization.isStrictSerialization())
            throw new JsonMappingException("Cannot serialize object containing "+value.getClass().getName()+" when strict serialization requested");

        serializeFromError(jgen.getOutputContext(), null, value, jgen, provider);
    }

    public void serializeFromError(JsonStreamContext ctxt, @Nullable Exception error, Object value, JsonGenerator jgen, SerializerProvider configurableSerializerProvider) throws IOException {
        if (log.isDebugEnabled())
            log.debug("Recovering from json serialization error, serializing "+value+": "+error);

        if (BidiSerialization.isStrictSerialization())
            throw new JsonMappingException("Cannot serialize "
                + (ctxt!=null && !ctxt.inRoot() ? "object containing " : "")
                + value.getClass().getName()+" when strict serialization requested");

        if (WARNED_CLASSES.add(value.getClass().getCanonicalName())) {
            log.warn("Standard serialization not possible for "+value.getClass()+" ("+value+")", error);
        }

        // flush seems necessary when working with large objects; presumably a buffer which is allowed to clear itself?
        // without this, when serializing the large (1.5M) Server json object from BrooklynJacksonSerializerTest creates invalid json,
        // containing:  "foo":false,"{"error":true,...
        jgen.flush();

        // if nested very deeply, come out, because there will be errors writing deeper things
        int closed = 0;
        while (jgen.getOutputContext().getNestingDepth() > 30 && writeEndCurrentThing(jgen, closed++)) {}
        if (jgen.getOutputContext().getNestingDepth() > 30) {
            throw new IllegalStateException("Cannot recover from serialization object; nesting is too deep");
        }

        boolean createObject = !jgen.getOutputContext().inObject();
        if (createObject) {
            // create if we're not in an object (ie in an array)
            // or if we're in an object, but we've just written the field name
            jgen.writeStartObject();
        } else {
            // we might need to write a value, and then write the error fields next
            writeErrorValueIfNeeded(jgen);
        }

        if (allowEmpty(value.getClass())) {
            // write nothing
        } else {

            jgen.writeFieldName("error");
            jgen.writeBoolean(true);

            jgen.writeFieldName("errorType");
            jgen.writeString(NotSerializableException.class.getCanonicalName());

            jgen.writeFieldName("type");
            jgen.writeString(value.getClass().getCanonicalName());

            jgen.writeFieldName("viaErrorSerializer");
            jgen.writeString(ErrorAndToStringUnknownTypeSerializer.class.getName());

            jgen.writeFieldName("toString");
            jgen.writeString(value.toString());

            if (error!=null) {
                jgen.writeFieldName("causedByError");
                jgen.writeString(error.toString());
            }

        }

        if (createObject) {
            jgen.writeEndObject();
        }

        while (jgen.getOutputContext()!=null && !jgen.getOutputContext().equals(ctxt) && writeEndCurrentThing(jgen, closed+1)) {}
    }

    private static boolean writeEndCurrentThing(JsonGenerator jgen, int count) throws IOException {
        if (jgen.getOutputContext().inArray()) {
            jgen.writeEndArray();
            return true;
        }
        if (jgen.getOutputContext().inObject()) {
            if (count==0) writeErrorValueIfNeeded(jgen);
            jgen.writeEndObject();
            return true;
        }
        return false;
    }

    private static void writeErrorValueIfNeeded(JsonGenerator jgen) {
        try {
            // at count 0, we usually need to write a value
            // (but there is no way to tell for sure; the internal status on JsonWriteContext is protected,
            // and the jgen methods are closely coupled to their state; and any attempt to mutate will insert an extra colon etc)
            if (jgen.getOutputContext().hasCurrentName()) {
                // assume it wrote the name, but the output context might not have the correct state;
                // have tried updated output context but it is mostly protected; instead just write this, usually good enough.
                jgen.writeRaw("\"ERROR\"");
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // if we couldn't write, we're probably at a place where a field would be written, so just end
        }
    }

    protected boolean allowEmpty(Class<? extends Object> clazz) {
        if (clazz.getAnnotation(JsonSerialize.class)!=null && Reflections.hasNoNonObjectFields(clazz)) {
            return true;
        } else {
            return false;
        }
    }
}
