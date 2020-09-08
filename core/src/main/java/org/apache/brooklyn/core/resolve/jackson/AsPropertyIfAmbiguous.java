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

import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.HasBaseType;
import org.apache.brooklyn.util.javalang.Reflections;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class AsPropertyIfAmbiguous {
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
            return super.writeTypePrefix(g, idMetadata);
        }

        @Override
        public AsPropertyTypeSerializer forProperty(BeanProperty prop) {
            return (_property == prop) ? this :
                    new AsPropertyIfAmbiguousTypeSerializer(this._idResolver, prop, this._typePropertyName);
        }
    }

    /** Type deserializer which ignores the 'type' property if it conflicts with a field on the class and which uses the base type if no type is specified */
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

        @Override
        public Object deserializeTypedFromObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (_idResolver instanceof HasBaseType) {
                if (Reflections.findFieldMaybe(((HasBaseType)_idResolver).getBaseType().getRawClass(), _typePropertyName).isPresent()) {
                    // don't read type id, just deserialize
                    JsonDeserializer<Object> deser = ctxt.findContextualValueDeserializer(((HasBaseType)_idResolver).getBaseType(), _property);
                    return deser.deserialize(p, ctxt);
                }

                // TODO MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL should do this
                if (!Objects.equals(_defaultImpl, ((HasBaseType) _idResolver).getBaseType())) {
                    AsPropertyButNotIfFieldConflictTypeDeserializer delegate = new AsPropertyButNotIfFieldConflictTypeDeserializer(_baseType, _idResolver, _typePropertyName, _typeIdVisible, ((HasBaseType) _idResolver).getBaseType(), _inclusion);
                    return delegate.deserializeTypedFromObject(p, ctxt);
                }
            }

            return super.deserializeTypedFromObject(p, ctxt);
        }

        @Override
        public TypeDeserializer forProperty(BeanProperty prop) {
            return (prop == _property) ? this : new AsPropertyButNotIfFieldConflictTypeDeserializer(this, prop);
        }

        // deserialize list-like things
        protected Object _deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (p.isExpectedStartArrayToken()) {
                // when we suppress types for collections, the deserializer
                // doesn't differentiate and so expects another array start.
                // we assume the default impl
                String typeId = _idResolver.idFromBaseType();
                JsonDeserializer<Object> deser = _findDeserializer(ctxt, typeId);

                if (p.currentToken() == JsonToken.END_ARRAY) {
                    return deser.getNullValue(ctxt);
                }
                return deser.deserialize(p, ctxt);
            } else {
                return super._deserialize(p, ctxt);
            }
        }

    }
}