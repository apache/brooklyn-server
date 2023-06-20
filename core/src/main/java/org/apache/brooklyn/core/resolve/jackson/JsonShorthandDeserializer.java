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
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.deser.ContextualDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.type.MapLikeType;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.javalang.coerce.TryCoercer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.*;

/** Deserializer which can be set on a field in a bean to indicate that a special
 * {@link JsonShorthandInstantiator}-annotated static 1-arg method should be used to instantiate the object
 * from another type if primary instantiation does not succeed.
 */
public class JsonShorthandDeserializer extends JsonDeserializer<Object> implements ContextualDeserializer {

    @Target({ElementType.METHOD})
    @Retention(RetentionPolicy.RUNTIME)
    public @interface JsonShorthandInstantiator {}

    private static final Logger LOG = LoggerFactory.getLogger(JsonShorthandDeserializer.class);
    private final JavaType type;

    public JsonShorthandDeserializer() {
        type = null;
    }

    public JsonShorthandDeserializer(JavaType type) {
        this.type = type;
    }

    // inefficient, and unnecessary normally as the shorthand is only from json, and it is quite flexible
//    static {
//        TypeCoercions.registerAdapter("80-json-shorthand", new TryCoercer() {
//            @Override
//            public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
//                Optional<Method> shorthand = Arrays.stream(type.getRawType().getMethods()).filter(m -> m.getAnnotation(JsonShorthandInstantiator.class) != null).findFirst();
//                if (!shorthand.isPresent()) return null;
//                try {
//                    return Maybe.of((T) shorthand.get().invoke(null, input));
//                } catch (Exception e) {
//                    return Maybe.absent("Unable to coerce "+input.getClass()+" to "+type+" using json shorthand");
//                }
//            }
//        });
//    }

    @Override
    public JsonDeserializer<?> createContextual(DeserializationContext ctxt, BeanProperty property) {
        JavaType t;
        if (property!=null) {
            t = property.getType();
        } else {
            t = ctxt.getContextualType();
        }
        if (t==null) {
            // shorthand not supported - could warn
            return this;
        }
        if (t instanceof MapLikeType) {
            return new JsonShorthandDeserializer(t.getContentType() );
        }
        if (Collection.class.isAssignableFrom(t.getRawClass())) {
            return new JsonShorthandDeserializer(t.getContentType() );
        }
        return new JsonShorthandDeserializer(t);
    }

    // TODO do these methods need the BJSU routines?

    @Override
    public Object deserializeWithType(JsonParser p, DeserializationContext ctxt, TypeDeserializer typeDeserializer) throws IOException {
        TokenBuffer b = new TokenBuffer(p, ctxt);
        b.copyCurrentStructure(p);
        Object r1 = null;
        try {
            r1 = super.deserializeWithType(b.asParserOnFirstToken(), ctxt, typeDeserializer);
        } catch (Exception e) {
            // ignore if "with type" deserialization didn't work here; it was probably incompatible for assigning _to_ X
            // the type will be re-read and instantiated, and assigned to the _value_ of X
        }
        if (r1!=null && type.getRawClass().isInstance(r1)) return r1;
        return deserialize(b.asParserOnFirstToken(), ctxt);
    }

    @Override
    public Object deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
        if (type==null) {
            // shorthand only supported where specified on a property and type is determinable from context
            return ctxt.findNonContextualValueDeserializer(ctxt.constructType(Object.class)).deserialize(p, ctxt);
        }

        List<Exception> exceptions = MutableList.of();
        try {
//            TokenBuffer b = BrooklynJacksonSerializationUtils.createBufferForParserCurrentObject(p, ctxt);
//            // above might be better, then access with createParserFromTokenBufferAndParser
            TokenBuffer b = new TokenBuffer(p, ctxt);
            b.copyCurrentStructure(p);

            try {
                Object r1 = ctxt.findNonContextualValueDeserializer(type).deserialize(b.asParserOnFirstToken(), ctxt);
                if (r1!=null && type.getRawClass().isInstance(r1)) return r1;
            } catch (Exception e) {
                exceptions.add(e);
            }

            try {
                Method inst = Arrays.stream(type.getRawClass().getMethods())
                        .filter(m -> m.getAnnotation(JsonShorthandInstantiator.class) != null).findAny()
//                        .orElseThrow(() -> new IllegalStateException("No public method annotated @JsonShorthandInstantiator"));
                        .orElseThrow(() -> {
                            return new IllegalStateException("No public method annotated @JsonShorthandInstantiator");
                        });
                if ((inst.getModifiers() & Modifier.STATIC)==0) throw new IllegalStateException("@JsonShorthandInstantiator method must be static: "+inst);
                if (inst.getParameterCount()!=1) throw new IllegalStateException("@JsonShorthandInstantiator method should take a single argument: "+inst);
                Object v = ctxt.findRootValueDeserializer(ctxt.constructType(inst.getParameters()[0].getParameterizedType())).deserialize(b.asParserOnFirstToken(), ctxt);
                if (v instanceof Map || (v instanceof WrappedValue && ((WrappedValue)v).getSupplier()==null && ((WrappedValue)v).get() instanceof Map)) {
                    // possibly we are unable to instantiate the type using the above; most of the time it is preferred, but in some cases we need the below,
                    // eg to handle DSL expressions in wrapped values (but many times the below fails). see JacksonJsonShorthandDeserializerTest failures when either is disallowed.
                    Object v2 = ctxt.findNonContextualValueDeserializer(ctxt.constructType(inst.getParameters()[0].getParameterizedType())).deserialize(b.asParserOnFirstToken(), ctxt);
                    if (!(v2 instanceof Map)) {
                        v = v2;
                    }
                }
                return inst.invoke(null, v);
            } catch (Exception e) {
                exceptions.add(e);
            }

            throw new IllegalStateException("Cannot instantiate as longhand or shorthand: " + exceptions, exceptions.stream().findFirst().orElse(null));
        } finally {
            if (!exceptions.isEmpty() && LOG.isTraceEnabled()) {
                LOG.trace("Exceptions encountered while deserializing: " + exceptions);
                exceptions.forEach(e -> LOG.trace("- ", e));
            }
        }

    }

}
