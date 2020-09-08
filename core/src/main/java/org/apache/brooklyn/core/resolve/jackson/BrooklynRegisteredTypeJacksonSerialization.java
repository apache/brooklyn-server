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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.*;
import com.fasterxml.jackson.databind.jsontype.impl.ClassNameIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.databind.type.SimpleType;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.AsPropertyButNotIfFieldConflictTypeDeserializer;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.AsPropertyIfAmbiguousTypeSerializer;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.HasBaseType;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerialization.BrooklynRegisteredTypeAndClassNameIdResolver;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerialization.RegisteredTypeDeserializers;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;

import java.io.IOException;
import java.util.*;

public class BrooklynRegisteredTypeJacksonSerialization {

    static class BrooklynJacksonType extends SimpleType {
        private final ManagementContext mgmt;
        private final RegisteredType type;
        public BrooklynJacksonType(ManagementContext mgmt, RegisteredType type) {
            super(pickSuperType(type));
            this.mgmt = mgmt;
            this.type = type;
        }
        private static Class<?> pickSuperType(RegisteredType t) {
            for (Object x : t.getSuperTypes()) {
                if (x instanceof Class) return (Class<?>) x;
            }
            return Object.class;
        }

        public RegisteredType getRegisteredType() {
            return type;
        }

        @Override
        public String toString() {
            return "BrooklynJacksonType{" + type.getId() + '/' + _class + "}";
        }
    }

    static class RegisteredTypeDeserializer<T> extends JsonDeserializer<T> {
        private final BrooklynJacksonType type;

        public RegisteredTypeDeserializer(BrooklynJacksonType type) {
            this.type = type;
        }

        @Override
        public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            try {
                Object target = type.mgmt.getTypeRegistry().createBean(type.type, null, null);
                JsonDeserializer<Object> delegate = ctxt.findContextualValueDeserializer(ctxt.constructType(target.getClass()), null);
                delegate.deserialize(p, ctxt, target);
                return (T)target;
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }
    }

    static class RegisteredTypeDeserializers extends SimpleDeserializers {
        @Override
        public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
            if (type instanceof BrooklynJacksonType) {
                return new RegisteredTypeDeserializer<>((BrooklynJacksonType)type);
            }
            return super.findBeanDeserializer(type, config, beanDesc);
        }
    }

    static class BrooklynRegisteredTypeAndClassNameIdResolver extends ClassNameIdResolver implements HasBaseType {
        private final ManagementContext mgmt;
        private final boolean allowRegisteredTypes;
        private final boolean allowJavaTypes;

        public BrooklynRegisteredTypeAndClassNameIdResolver(JavaType baseType, MapperConfig<?> config, PolymorphicTypeValidator subtypeValidator, ManagementContext mgmt,
                boolean allowRegisteredTypes, boolean allowJavaTypes) {
            super(baseType, config.getTypeFactory(), subtypeValidator);
            this.mgmt = mgmt;
            this.allowRegisteredTypes = allowRegisteredTypes;
            this.allowJavaTypes = allowJavaTypes;
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id) throws IOException {
            if (allowRegisteredTypes && mgmt!=null) {
                RegisteredType rt = mgmt.getTypeRegistry().get(id);
                if (rt != null) {
                    return new BrooklynJacksonType(mgmt, rt);
                }
            }
            if (allowJavaTypes) {
                return super.typeFromId(context, id);
            }

            // copied from super if it fails to find the type
            if (context instanceof DeserializationContext) {
                return ((DeserializationContext) context).handleUnknownTypeId(_baseType, id, this, "no such class found");
            }
            return null;
        }

        @Override
        public JavaType getBaseType() {
            return _baseType;
        }
    }

    static class BrtTypeResolverBuilder extends DefaultTypeResolverBuilder {
        private final ManagementContext mgmt;
        private final boolean allowRegisteredTypes;
        private final boolean allowJavaTypes;

        public BrtTypeResolverBuilder(ManagementContext mgmt, boolean allowRegisteredTypes, boolean allowJavaTypes) {
            super(DefaultTyping.NON_FINAL, LaissezFaireSubTypeValidator.instance);
            this.mgmt = mgmt;

            // normally custom resolvers are passed to the init method below, one instance used through the entire parse --
            // unlike Jackson's internal resolvers which are aware of the current baseType;
            // to allow our resolver to have the baseType we instead generate our resolver in the overridden idResolver method above
            init(JsonTypeInfo.Id.CLASS, null);
            inclusion(As.PROPERTY);
            typeProperty("type");
            this.allowRegisteredTypes = allowRegisteredTypes;
            this.allowJavaTypes = allowJavaTypes;
        }

        @Override
        protected TypeIdResolver idResolver(MapperConfig<?> config, JavaType baseType, PolymorphicTypeValidator subtypeValidator, Collection<NamedType> subtypes, boolean forSer, boolean forDeser) {
            return new BrooklynRegisteredTypeAndClassNameIdResolver(baseType, config, subtypeValidator, mgmt, allowRegisteredTypes, allowJavaTypes);
        }

        @Override
        public TypeSerializer buildTypeSerializer(SerializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
            if (!useForType(baseType)) return null;
            if (baseType.isPrimitive()) {
                return null;
            }
            if (baseType.isCollectionLikeType() || baseType.isMapLikeType()) {
//                List.class.isAssignableFrom(baseType.getRawClass()) ||
//                        Map.class.isAssignableFrom(baseType.getRawClass())
                // never serialize these types
                return null;
            }
            TypeIdResolver idRes = idResolver(config, baseType, subTypeValidator(config),
                    subtypes, true, false);

            // above is copied from parents; below is to provide the serializer we want
            return new AsPropertyIfAmbiguousTypeSerializer(idRes, null, _typeProperty);
        }

        @Override
        public TypeDeserializer buildTypeDeserializer(DeserializationConfig config, JavaType baseType, Collection<NamedType> subtypes) {
            if (!useForType(baseType)) return null;
            if (baseType.isPrimitive()) {
                return null;
            }
            final PolymorphicTypeValidator subTypeValidator = verifyBaseTypeValidity(config, baseType);
            TypeIdResolver idRes = idResolver(config, baseType, subTypeValidator, subtypes, false, true);
            JavaType defaultImpl = defineDefaultImpl(config, baseType);

            // above is copied from parents; below is to provide the serializer we want
            return new AsPropertyButNotIfFieldConflictTypeDeserializer(baseType, idRes, _typeProperty, _typeIdVisible, defaultImpl, _includeAs);
        }

        @Override
        protected JavaType defineDefaultImpl(DeserializationConfig config, JavaType baseType) {
            JavaType result = super.defineDefaultImpl(config, baseType);
            if (result!=null) return result;
            if (baseType.isMapLikeType()) return config.constructType(LinkedHashMap.class);
            if (baseType.isCollectionLikeType()) return config.constructType(LinkedList.class);
            return null;
        }
    }

    public static ObjectMapper apply(ObjectMapper mapper, ManagementContext mgmt, boolean allowRegisteredTypes, boolean allowJavaTypes) {
        // the type resolver is extended to recognise brooklyn registered type names
        // and return a subtype of jackson JavaType
        mapper.setDefaultTyping(new BrtTypeResolverBuilder(mgmt, allowRegisteredTypes, allowJavaTypes));

        SimpleModule module = new SimpleModule();
        if (allowRegisteredTypes) {
            module.setDeserializers(new RegisteredTypeDeserializers());
        }

        // the module defines how to deserialize the registered type id
        mapper.registerModule(module);

        // preferred defaults
        mapper.setSerializationInclusion(Include.NON_NULL);

        return mapper;
    }

}
