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
import com.fasterxml.jackson.core.JsonTokenId;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.AbstractDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializer;
import com.fasterxml.jackson.databind.deser.BeanDeserializerBase;
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
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Jackson's {@link DelegatingDeserializer} does not get invoked when maps/lists deserialize other maps/lists.
 * This corrects that, ensuring subclasses of this get invoked on each returned object (as far as we have encountered).
 */
public abstract class JacksonBetterDelegatingDeserializer extends DelegatingDeserializer {

    private static final Logger log = LoggerFactory.getLogger(JacksonBetterDelegatingDeserializer.class);

    // longwinded way to detect if it's non-merging
    public static class UntypedObjectDeserializerInfoAccess extends UntypedObjectDeserializer {
        public UntypedObjectDeserializerInfoAccess(UntypedObjectDeserializer base) {
            super(base, null, null, null, null);
        }
        public boolean isNonMerging() {
            return _nonMerging;
        }
    }

    public static class CollectionDelegatingUntypedObjectDeserializer extends UntypedObjectDeserializer {
        DelegatingDeserializer outer;
        public CollectionDelegatingUntypedObjectDeserializer(UntypedObjectDeserializer base) {
            super(base, new UntypedObjectDeserializerInfoAccess(base).isNonMerging());
            if (_mapDeserializer==null) _mapDeserializer = this;
            if (_listDeserializer==null) _listDeserializer = this;
        }
        public void init(DelegatingDeserializer delegator) {
            outer = delegator;
        }

        /* This awkward pattern ensures that if the delegatee tries sneakily to deserialize more things (which mapObject and mapArray do)
         * then it gets redirected to the delegating deserializer
         */
        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return outer.deserialize(p, ctxt);
        }

        @Override
        public Object deserialize(JsonParser p, DeserializationContext ctxt, Object intoValue) throws IOException {
            return outer.deserialize(p, ctxt, intoValue);
        }

        public Object deserializeReal(JsonParser p, DeserializationContext ctxt) throws IOException {
            return super.deserialize(p, ctxt);
        }

        public Object deserializeReal(JsonParser p, DeserializationContext ctxt, Object intoValue) throws IOException {
            return super.deserialize(p, ctxt, intoValue);
        }
    }

    protected final Function<JsonDeserializer<?>, JacksonBetterDelegatingDeserializer> constructor;

    public JacksonBetterDelegatingDeserializer(JsonDeserializer<?> delagatee, Function<JsonDeserializer<?>,JacksonBetterDelegatingDeserializer> constructor) {
        super(newDelagatee(delagatee));
        this.constructor = constructor;
        if (_delegatee instanceof CollectionDelegatingUntypedObjectDeserializer) {
            ((CollectionDelegatingUntypedObjectDeserializer)_delegatee).init(this);
        }

    }

    protected static JsonDeserializer<?> newDelagatee(JsonDeserializer<?> delegatee) {
        if (delegatee instanceof UntypedObjectDeserializer) {
            return new CollectionDelegatingUntypedObjectDeserializer((UntypedObjectDeserializer)delegatee);
        }
        return delegatee;
    }

    @Override
    protected JsonDeserializer<?> newDelegatingInstance(JsonDeserializer<?> newDelegatee) {
        return constructor.apply(newDelegatee);
    }

    @Override
    public Object deserialize(JsonParser jp1, DeserializationContext ctxt1) throws IOException {
        return deserializeWrapper(jp1, ctxt1, (jp2, ctxt2) -> {
            if (_delegatee instanceof CollectionDelegatingUntypedObjectDeserializer)
                return ((CollectionDelegatingUntypedObjectDeserializer) _delegatee).deserializeReal(jp2, ctxt2);

                    // might be necessary to do this if we've started to analyse the type; but impls seems to be flexible enough to adapt as needed
//                    : jp2.currentTokenId() == JsonTokenId.ID_FIELD_NAME && (_delegatee instanceof BeanDeserializerBase)
//                        ? ((BeanDeserializerBase)_delegatee).deserializeFromObject(jp2, ctxt2)

            // type names in arrays are handled by the PropertyIfAmbiguous; but we could catch abstract and treat better if we wanted
//            if (_delegatee instanceof AbstractDeserializer) {
//                if (jp2.getCurrentToken()==JsonToken.START_ARRAY) {
//                    throw new IllegalStateException("TODO catch abstract array attempts and treat as typed");
//                }
//            }

            return _delegatee.deserialize(jp2, ctxt2);
        });
    }

    @Override
    public Object deserialize(JsonParser jp1, DeserializationContext ctxt1, Object intoValue) throws IOException {
        return deserializeWrapper(jp1, ctxt1, (jp2, ctxt2) -> {
            if (_delegatee instanceof CollectionDelegatingUntypedObjectDeserializer)
                    return ((CollectionDelegatingUntypedObjectDeserializer) _delegatee).deserializeReal(jp2, ctxt2, intoValue);

            if (jp2.currentToken()==JsonToken.VALUE_STRING && _delegatee instanceof BeanDeserializer) {
                // parser was on a string value, and still is. probably the parser is a bean parser and has done nothing.
                // we should use the coercion routines instead (the wrapper will catch the exception)
                // (ie if deserializing from a string into a bean, constructor couldn't be used because registered type was declared; coercion is required)

                // advancing seems unnecessary
//                jp2.nextToken();

                throw new IllegalStateException("String deserialization using BeanDeserializer is not supported for '"+intoValue+"'; coercion may fix, otherwise this will propagate and input cannot handle strings for "+getValueType());
            }
            return((JsonDeserializer<Object>) _delegatee).deserialize(jp2, ctxt2, intoValue);
        });
    }

    interface BiFunctionThrowsIoException<I1,I2,O> {
        O apply(I1 i1, I2 i2) throws IOException;
    }

    protected abstract Object deserializeWrapper(JsonParser jp, DeserializationContext ctxt, BiFunctionThrowsIoException<JsonParser, DeserializationContext, Object> nestedDeserialize) throws IOException;

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt,
                                                BeanProperty property)
            throws JsonMappingException
    {
        JavaType vt = ctxt.constructType(_delegatee.handledType());

        //override parent to make this available
        if (vt==null) vt = ctxt.getContextualType();

        JsonDeserializer<?> del = ctxt.handleSecondaryContextualization(_delegatee,
                property, vt);
        if (del == _delegatee) {
            return this;
        }
        return newDelegatingInstance(del);
    }
}
