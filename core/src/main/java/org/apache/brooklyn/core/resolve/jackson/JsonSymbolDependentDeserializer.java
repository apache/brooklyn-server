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
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.BeanDeserializerFactory;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.deser.DeserializerFactory;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.Set;
import java.util.function.Function;

import org.apache.brooklyn.util.core.units.Range;
import org.apache.brooklyn.util.core.xstream.ImmutableSetConverter;

import static org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.createBeanDeserializer;

public abstract class JsonSymbolDependentDeserializer extends JsonDeserializer<Object> implements ContextualDeserializer {

    public static final Set<JsonToken> SIMPLE_TOKENS = ImmutableSet.of(
            JsonToken.VALUE_STRING,
            JsonToken.VALUE_NUMBER_FLOAT,
            JsonToken.VALUE_NUMBER_INT,
            JsonToken.VALUE_TRUE,
            JsonToken.VALUE_FALSE,
            JsonToken.VALUE_NULL
    );
    protected DeserializationContext ctxt;
    protected BeanProperty beanProp;
    private BeanDescription beanDesc;
    protected JavaType type;

    public BeanDescription getBeanDescription() {
        if (beanDesc!=null) return beanDesc;
        if (type!=null) return beanDesc = ctxt.getConfig().introspect(type);
        return null;
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
        this.ctxt = ctxt;
        beanProp = property;
        if (property!=null) {
            type = property.getType();
        }
        if (type==null) {
            // ctxt.getContextualType() is normally set during primary contextualization and first round of secondary (if known)
            // but not usually available during deserialization, so do it now;
            // however it can be suppressed if in a nested secondary contextualization (eg via DelegatingDeserializer),
            // but our JacksonBetterDelegatingDeserializer attempts to avoid this
            type = ctxt.getContextualType();
        }
        if (isTypeReplaceableByDefault()) {
            type = getDefaultType();
        }

        return this;
    }

    protected boolean isTypeReplaceableByDefault() {
        if (type==null) return true;
        if (type.getRawClass().isInterface()) return true;
        return false;
    }

    public abstract JavaType getDefaultType();

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonDeserializer<?> deser;
        Function<Object,Object> post = x -> x;

        if (p.getCurrentToken() == JsonToken.START_ARRAY) {
            return deserializeArray(p);
        } else if (SIMPLE_TOKENS.contains(p.getCurrentToken())) {
            // string
            return deserializeToken(p);
        } else {
            // other primitives not yet supported
            // assume object
            return deserializeObject(p);
        }
    }

    protected JsonDeserializer<?> contextualize(JsonDeserializer<?> deser) throws IOException, JsonProcessingException {
        if (deser instanceof ContextualDeserializer) {
            // collection requires this initialization (beanProp seems usually to be null / irrelevant, but capture and pass it for good measure)
            return ((ContextualDeserializer)deser).createContextual(ctxt, beanProp);
        }
        return deser;
    }

    protected Object deserializeArray(JsonParser p) throws IOException, JsonProcessingException {
        return contextualize(getArrayDeserializer()).deserialize(p, ctxt);
    }
    protected JsonDeserializer<?> getArrayDeserializer() throws IOException {
        if (type!=null) {
            Object handler = type.getTypeHandler();
            if (handler==null) {
                // drop type info, in case the default type was overly restrictive
                type = ctxt.constructType(Object.class);
                handler = ctxt.getFactory().findTypeDeserializer(ctxt.getConfig(), type);
            }
            if (handler instanceof AsPropertyIfAmbiguous.AsPropertyButNotIfFieldConflictTypeDeserializer) {
                /** Object.class can be encoded as array ["Class", "Object"] if type is unknown;
                 *  it doesn't want to use { type: Class, value: Object } because it is trying to write a value string.
                 *  this is a cheap-and-cheerful way to support that.
                 */
                AsPropertyIfAmbiguous.AsPropertyButNotIfFieldConflictTypeDeserializer hf = (AsPropertyIfAmbiguous.AsPropertyButNotIfFieldConflictTypeDeserializer) handler;
                return new JsonDeserializer<Object>() {
                    @Override
                    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                        return hf.deserializeArrayContainingType(p, ctxt);
                    }
                };
            }
        }
        throw new IllegalStateException("List input not supported for "+type);
    }

    protected Object deserializeToken(JsonParser p) throws IOException, JsonProcessingException {
        return contextualize(getTokenDeserializer()).deserialize(p, ctxt);
    }
    protected JsonDeserializer<?> getTokenDeserializer() throws IOException, JsonProcessingException {
        return getObjectDeserializer();
    }

    /** deserializes if we know we have an object; if we have a string, it will typically go into deserializeToken */
    protected Object deserializeObject(JsonParser p) throws IOException, JsonProcessingException {
        return contextualize(getObjectDeserializer()).deserialize(p, ctxt);
    }
    protected JsonDeserializer<?> getObjectDeserializer() throws IOException, JsonProcessingException {
        return createBeanDeserializer(ctxt, type, getBeanDescription(),
                /** try to do low level build so we don't recreate ourselves and loop endlessly */ true,
                true);
    }
}
