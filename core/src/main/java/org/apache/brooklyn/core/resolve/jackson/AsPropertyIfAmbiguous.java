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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.core.util.JsonParserSequence;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;

import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AsPropertyIfAmbiguous {

    public static final String CONFLICTING_TYPE_NAME_PROPERTY_PREFIX = "@";

    public interface HasBaseType {
        JavaType getBaseType();
    }

    /**
     * Type serializer which omits the type if it's unambiguous.
     *
     * Lists, maps, and arrays are always written without type information.
     *
     * Deserialization of a map with a property that indicates a type normally returns the type;
     * however in some places, e.g. when expecting an object (no type info) and we get a list or a map,
     * if there is a map containing a type property within it, that is _not_ interpreted as the type.
     * See PerverseSerializationTest.testDeserializeListMapWithType
     */
    public static class AsPropertyIfAmbiguousTypeSerializer extends AsPropertyTypeSerializer {

        public AsPropertyIfAmbiguousTypeSerializer(TypeIdResolver idRes, BeanProperty property, String propName) {
            super(idRes, property, propName);
        }

        @Override
        public WritableTypeId writeTypePrefix(JsonGenerator g, WritableTypeId idMetadata) throws IOException {
            boolean skip = false;
            Object currentObject = idMetadata.forValue;
            Class<?> currentClass = currentObject==null ? null : currentObject.getClass();
            if (_idResolver instanceof HasBaseType) {
                JavaType impliedType = ((HasBaseType) _idResolver).getBaseType();
                Class<?> impliedClass = impliedType==null ? null : impliedType.getRawClass();
                if (Objects.equals(currentClass, impliedClass)) {
//                if (g.getCurrentValue()!=null) {
//                    if (impliedType.getRawClass().equals(g.getCurrentValue().getClass())) {
                        // skip type id if the expected type matches the actual type
                    skip = true;
                }
                if (!skip && impliedClass!=null) {
                    skip = impliedType.isCollectionLikeType() || Map.class.isAssignableFrom(impliedClass);
                }
                if (!skip && currentClass!=null) {
                    skip = List.class.isAssignableFrom(currentClass) || Map.class.isAssignableFrom(currentClass) || currentClass.isArray();
                }
            }
            if (skip) {
                _generateTypeId(idMetadata);
                if (idMetadata.valueShape == JsonToken.START_OBJECT) {
                    g.writeStartObject(idMetadata.forValue);
                } else if (idMetadata.valueShape == JsonToken.START_ARRAY) {
                    g.writeStartArray();
                }
                return idMetadata;
            }
            String tpn = idMetadata.asProperty;
            if (tpn==null) tpn = _typePropertyName;
            if (currentClass!=null && Reflections.findFieldMaybe(currentClass, tpn).isPresent()) {
                // the class has a field called 'type'; prefix with an '@'
                tpn = CONFLICTING_TYPE_NAME_PROPERTY_PREFIX+tpn;
                idMetadata.asProperty = tpn;
            }
            return super.writeTypePrefix(g, idMetadata);
        }

        @Override
        public AsPropertyTypeSerializer forProperty(BeanProperty prop) {
            return (_property == prop) ? this :
                    new AsPropertyIfAmbiguousTypeSerializer(this._idResolver, prop, this._typePropertyName);
        }
    }

    /** Type deserializer which undersrtands a '@type' property if 'type' conflicts with a field on the class and which uses the base type if no type is specified */
    public static class AsPropertyButNotIfFieldConflictTypeDeserializer extends AsPropertyTypeDeserializer {
        public AsPropertyButNotIfFieldConflictTypeDeserializer(JavaType bt, TypeIdResolver idRes, String typePropertyName, boolean typeIdVisible, JavaType defaultImpl, As inclusion) {
            super(bt, idRes, typePropertyName, typeIdVisible, defaultImpl, inclusion);
        }

        public AsPropertyButNotIfFieldConflictTypeDeserializer(AsPropertyButNotIfFieldConflictTypeDeserializer src, BeanProperty prop) {
            super(src, prop);
        }

        @Override
        public Object deserializeTypedFromArray(JsonParser jp, DeserializationContext ctxt) throws IOException {
            return super.deserializeTypedFromArray(jp, ctxt);
        }

        AsPropertyButNotIfFieldConflictTypeDeserializer cloneWithNewTypePropertyName(String newTypePropertyName) {
            return new AsPropertyButNotIfFieldConflictTypeDeserializer(_baseType, _idResolver, newTypePropertyName, _typeIdVisible, _defaultImpl, _inclusion);
        }

        @Override
        public Object deserializeTypedFromObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (_idResolver instanceof HasBaseType) {
                if (// object has field with same name as the type property - don't treat the type property supplied here as the type
                        presentAndNotJsonIgnored(Reflections.findFieldMaybe(((HasBaseType)_idResolver).getBaseType().getRawClass(), _typePropertyName))
                        || // or object has getter with same name as the type property
                        presentAndNotJsonIgnored(Reflections.findMethodMaybe(((HasBaseType)_idResolver).getBaseType().getRawClass(), "get"+ Strings.toInitialCapOnly(_typePropertyName)))
                ) {
                    // look for an '@' type
                    return cloneWithNewTypePropertyName(CONFLICTING_TYPE_NAME_PROPERTY_PREFIX+_typePropertyName).deserializeTypedFromObject(p, ctxt);

                    // previous behaviour:
//                    // don't read type id, just deserialize
//                    JsonDeserializer<Object> deser = ctxt.findContextualValueDeserializer(((HasBaseType)_idResolver).getBaseType(), _property);
//                    return deser.deserialize(p, ctxt);
                }
                // ? - MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL should do this
                if (!Objects.equals(_defaultImpl, ((HasBaseType) _idResolver).getBaseType())) {
                    AsPropertyButNotIfFieldConflictTypeDeserializer delegate = new AsPropertyButNotIfFieldConflictTypeDeserializer(_baseType, _idResolver, _typePropertyName, _typeIdVisible, ((HasBaseType) _idResolver).getBaseType(), _inclusion);
                    return delegate.deserializeTypedFromObject(p, ctxt);
                }
            }
            return super.deserializeTypedFromObject(p, ctxt);
        }

        private boolean presentAndNotJsonIgnored(Maybe<? extends AccessibleObject> fm) {
            if (!fm.isPresent()) return false;
            AccessibleObject f = fm.get();
            JsonIgnore ignored = f.getAnnotation(JsonIgnore.class);
            if (ignored!=null) return false;
            return true;
        }

        @Override
        public TypeDeserializer forProperty(BeanProperty prop) {
            return (prop == _property) ? this : new AsPropertyButNotIfFieldConflictTypeDeserializer(this, prop);
        }

        // deserialize list-like things
        @Override
        protected Object _deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.isExpectedStartArrayToken()) {
                // when we suppress types for collections, the deserializer
                // doesn't differentiate and so expects another array start.
                // we assume the default impl

                // previously for RTs this downcasted to java class; now it remains as RT name
                String typeId = _idResolver.idFromBaseType();
                JsonDeserializer<Object> deser = _findDeserializer(ctxt, typeId);

                if (p.currentToken() == JsonToken.END_ARRAY) {
                    return deser.getEmptyValue(ctxt);
                }
                return deser.deserialize(p, ctxt);
            } else {
                return super._deserialize(p, ctxt);
            }
        }

        @Override
        protected Object _deserializeTypedForId(JsonParser p, DeserializationContext ctxt,
                                                TokenBuffer tb) throws IOException
        {
            // first part copied from parent

            String typeId = p.getText();
            JsonDeserializer<Object> deser = _findDeserializer(ctxt, typeId);
            if (_typeIdVisible) { // need to merge id back in JSON input?
                if (tb == null) {
                    tb = new TokenBuffer(p, ctxt);
                }
                tb.writeFieldName(p.getCurrentName());
                tb.writeString(typeId);
            }
            if (tb != null) { // need to put back skipped properties?
                // 02-Jul-2016, tatu: Depending on for JsonParserSequence is initialized it may
                //   try to access current token; ensure there isn't one
                p.clearCurrentToken();
                p = JsonParserSequence.createFlattened(false, tb.asParser(p), p);
            }
            // Must point to the next value; tb had no current, jp pointed to VALUE_STRING:
            p.nextToken(); // to skip past String value
            // deserializer should take care of closing END_OBJECT as well

            boolean wasEndToken = (p.currentToken() == JsonToken.END_OBJECT);
            try {
                return deser.deserialize(p, ctxt);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                if (wasEndToken) {
                    // new -- we allow e.g. `{ type: list-extended }` to mean it is a list;
                    // however the only way to set values for it in the same object would be to define a custom deserializer for it;
                    // normal use case is that context implies e.g. list-extended, then collection deserializer does the right thing when it is used.
                    // but if we got an empty map somehow, e.g. user supplied, don't use it.
                    Object candidate = deser.getEmptyValue(ctxt);
                    if (candidate!=null) {
                        return candidate;
                    }
                }
                throw e;
            }
        }
    }
}
