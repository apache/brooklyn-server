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
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import java.io.IOException;
import java.util.function.Function;

public class JsonSymbolDependentDeserializer extends JsonDeserializer<Object> implements ContextualDeserializer {

    protected DeserializationContext ctxt;
    protected BeanProperty beanProp;
    private BeanDescription beanDesc;
    protected JavaType type;

    public BeanDescription getBeanDescription() {
        if (beanDesc!=null) return beanDesc;
        return beanDesc = ctxt.getConfig().introspect(type);
    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) throws JsonMappingException {
        this.ctxt = ctxt;
        beanProp = property;
        if (property!=null) {
            type = property.getType();
        }
        if (type==null) {
            // this is normally set during contextualization but not during deserialization (although not if we're the ones contextualizing it)
            type = ctxt.getContextualType();
        }

        return this;
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
        JsonDeserializer<?> deser;
        Function<Object,Object> post = x -> x;

        if (p.getCurrentToken() == JsonToken.START_ARRAY) {
            return deserializeArray(p);
        } else {
            // (primitives, string, etc not yet supported)

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
    protected JsonDeserializer<?> getArrayDeserializer() throws IOException, JsonProcessingException {
        throw new IllegalStateException("List input not supported for "+type);
    }

    protected Object deserializeObject(JsonParser p) throws IOException, JsonProcessingException {
        return contextualize(getObjectDeserializer()).deserialize(p, ctxt);
    }
    protected JsonDeserializer<?> getObjectDeserializer() throws IOException, JsonProcessingException {
        return ctxt.getFactory().createBeanDeserializer(ctxt, type, getBeanDescription());
    }

}
