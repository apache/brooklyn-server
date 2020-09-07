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
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.databind.BeanProperty;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import org.apache.brooklyn.util.javalang.Reflections;

import java.io.IOException;
import java.util.Objects;

public class AsPropertyIfAmbiguous {
    public interface HasBaseType {
        JavaType getBaseType();
    }

    /**
     * Type serializer which omits the type if it's unambiguous
     */
    public static class AsPropertyIfAmbiguousTypeSerializer extends AsPropertyTypeSerializer {

        public AsPropertyIfAmbiguousTypeSerializer(TypeIdResolver idRes, BeanProperty property, String propName) {
            super(idRes, property, propName);
        }

        @Override
        public WritableTypeId writeTypePrefix(JsonGenerator g, WritableTypeId idMetadata) throws IOException {
            //if (g.getCurrentValue())
            if (_idResolver instanceof HasBaseType) {
                if (((HasBaseType) _idResolver).getBaseType().getRawClass().equals(g.getCurrentValue().getClass())) {
                    // skip type id if the expected type matches the actual type
                    g.writeStartObject(idMetadata.forValue);
                    return idMetadata;
                }
            }
            return super.writeTypePrefix(g, idMetadata);
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
        public Object deserializeTypedFromObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            if (_idResolver instanceof HasBaseType) {
                if (Reflections.findFieldMaybe(((HasBaseType)_idResolver).getBaseType().getRawClass(), _typePropertyName).isPresent()) {
                    // don't read type id, just deserialize
                    JsonDeserializer<Object> deser = ctxt.findContextualValueDeserializer(((HasBaseType)_idResolver).getBaseType(), _property);
                    return deser.deserialize(p, ctxt);
                }

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
    }
}