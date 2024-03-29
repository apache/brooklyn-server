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

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTypeResolverBuilder;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.jsontype.*;
import com.fasterxml.jackson.databind.jsontype.impl.ClassNameIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import com.fasterxml.jackson.databind.module.SimpleDeserializers;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.AsPropertyButNotIfFieldConflictTypeDeserializer;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.AsPropertyIfAmbiguousTypeSerializer;
import org.apache.brooklyn.core.resolve.jackson.AsPropertyIfAmbiguous.HasBaseType;
import org.apache.brooklyn.core.typereg.RegisteredTypeLoadingContexts;
import org.apache.brooklyn.util.core.flags.BrooklynTypeNameResolution;
import org.apache.brooklyn.util.exceptions.Exceptions;

import java.io.IOException;
import java.util.*;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;

public class BrooklynRegisteredTypeJacksonSerialization {

    static class RegisteredTypeDeserializer<T> extends JsonDeserializer<T> {
        private final BrooklynJacksonType type;
        private final ManagementContext mgmt;

        public RegisteredTypeDeserializer(ManagementContext mgmt, BrooklynJacksonType type) {
            this.mgmt = mgmt;
            this.type = type;
        }

        @Override
        public T deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
            try {
                Object target = getEmptyValue(ctxt);
                JsonDeserializer<Object> delegate = ctxt.findContextualValueDeserializer(ctxt.constructType(target.getClass()), null);
                return (T) delegate.deserialize(p, ctxt, target);
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        public Object getEmptyValue(DeserializationContext ctxt) throws JsonMappingException {
            // empty for us is the underlying definition, not null
            if (mgmt==null) {
                // could do this to use the type
                //return Reflections.invokeConstructorFromArgs(type.getRawClass(), new Object[] {}, true).orThrow("Cannot create "+type+" because no management context and no accessible constructor on "+type.getRawClass());
                // but instead treat as error
                throw new NullPointerException("Requested to deserialize Brooklyn type "+type+" without a management context");
            }
            if (type.getRegisteredType().getKind().equals(BrooklynTypeRegistry.RegisteredTypeKind.SPEC)) {
                return mgmt.getTypeRegistry().createSpec(type.getRegisteredType(), null, null);
            }

            return mgmt.getTypeRegistry().createBean(type.getRegisteredType(), null, null);
        }
    }

    static class RegisteredTypeDeserializers extends SimpleDeserializers {
        private final ManagementContext mgmt;
        public RegisteredTypeDeserializers(ManagementContext mgmt) {
            this.mgmt = mgmt;
        }
        @Override
        public JsonDeserializer<?> findBeanDeserializer(JavaType type, DeserializationConfig config, BeanDescription beanDesc) throws JsonMappingException {
            if (type instanceof BrooklynJacksonType) {
                return new RegisteredTypeDeserializer<>(mgmt, (BrooklynJacksonType)type);
            }
            return super.findBeanDeserializer(type, config, beanDesc);
        }
    }

    static class BrooklynRegisteredTypeAndClassNameIdResolver extends ClassNameIdResolver implements HasBaseType {
        private final ManagementContext mgmt;
        private final boolean allowRegisteredTypes;
        private final BrooklynClassLoadingContext loader;
        private final boolean allowPojoJavaTypes;

        public BrooklynRegisteredTypeAndClassNameIdResolver(JavaType baseType, MapperConfig<?> config, PolymorphicTypeValidator subtypeValidator, ManagementContext mgmt,
                boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowPojoJavaTypes) {
            super(baseType, config.getTypeFactory(), subtypeValidator);
            this.mgmt = mgmt;
            this.allowRegisteredTypes = allowRegisteredTypes;
            this.loader = loader;
            this.allowPojoJavaTypes = allowPojoJavaTypes;
        }

        @Override
        public JavaType typeFromId(DatabindContext context, String id) throws IOException {
            Maybe<Class<?>> builtin = BrooklynTypeNameResolution.getClassForBuiltInTypeName(id);
            if (builtin.isPresent()) {
                return context.constructType(builtin.get());
            }

            if (allowRegisteredTypes && mgmt!=null) {
                RegisteredType rt = mgmt.getTypeRegistry().get(id, RegisteredTypeLoadingContexts.loader(loader));
                if (rt != null) {
                    return new BrooklynJacksonType(rt);
                }
            }
            // TODO - this would be nice to support complex types
//            if (type is present in a registered type) {
//                get the bundle of registered type
//                use that classloader to instantiate the type
//            }
            if (allowPojoJavaTypes) {
                if (loader!=null) {
                    Maybe<Class<?>> fromLoader = loader.tryLoadClass(id);
                    if (fromLoader.isPresent()) {
                        // contextual base type ignored; more sophisticated handling is done by jackson but this seems good enough
                        return context.constructType(fromLoader.get());
                    }
                }

                // this will validate id is a subtype of expected type, and take most specific
                try {
                    return super.typeFromId(context, id);
                } catch (IOException e) {
                    Exceptions.propagateIfFatal(e);
                    // loader-based registered types logic does not validate, so for consistency we proceed even if unvalidated;
                    // note if the type is not a subtype, we will typically get an error when validating the deserializer or using it
                    // (coercion could be attempted for both by extending in AsPropertyIfAmbiguous)
                    try {
                        JavaType result = context.resolveAndValidateSubType(context.constructType(Object.class), id, _subTypeValidator);
                        if (result!=null) return result;
                    } catch (Exception e2) {
                        Exceptions.propagateIfFatal(e2);
                    }

                    // finally, if we have a base type which is a RT, for backwards compatibility reasons we switch to using its class
                    // this is only needed for tests which don't support RTs but supply registered types
                    if (id.equals(idFromBaseType()) && !id.equals(super.idFromBaseType())) {
                        return typeFromId(context, super.idFromBaseType());
                    }

                    throw e;
                }
            }

            // even if we aren't allowed to load java types, if the expected type matches, then we will allow it
            if (_baseType!=null && _baseType.toCanonical().equals(id)) {
                return _baseType;
            }

            if (id!=null && id.startsWith("org.apache.brooklyn.camp.brooklyn.spi.dsl.")) {
                // allow DSL objects to be treated as objects even if we can't instantiate the types
                return context.constructType(Object.class);
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

        @Override
        public String idFromBaseType() {
            if (_baseType instanceof BrooklynJacksonType) {
                return ((BrooklynJacksonType)_baseType).getTypeName();
            }
            // fall back to the raw class name
            return super.idFromBaseType();
        }
    }

    static class BrtTypeResolverBuilder extends DefaultTypeResolverBuilder {
        private final ManagementContext mgmt;
        private final boolean allowRegisteredTypes;
        private final BrooklynClassLoadingContext loader;
        private final boolean allowPojoJavaTypes;

        public BrtTypeResolverBuilder(ManagementContext mgmt, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowPojoJavaTypes) {
            super(DefaultTyping.NON_FINAL, LaissezFaireSubTypeValidator.instance);
            this.mgmt = mgmt;

            // normally custom resolvers are passed to the init method below, one instance used through the entire parse --
            // unlike Jackson's internal resolvers which are aware of the current baseType;
            // to allow our resolver to have the baseType we instead generate our resolver in the overridden idResolver method above
            init(JsonTypeInfo.Id.CLASS, null);
            inclusion(As.PROPERTY);
            typeProperty(BrooklynJacksonSerializationUtils.TYPE);
            this.allowRegisteredTypes = allowRegisteredTypes;
            this.loader = loader;
            this.allowPojoJavaTypes = allowPojoJavaTypes;
        }

        @Override
        protected TypeIdResolver idResolver(MapperConfig<?> config, JavaType baseType, PolymorphicTypeValidator subtypeValidator, Collection<NamedType> subtypes, boolean forSer, boolean forDeser) {
            return new BrooklynRegisteredTypeAndClassNameIdResolver(baseType, config, subtypeValidator, mgmt, allowRegisteredTypes, loader, allowPojoJavaTypes);
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

    public static ObjectMapper apply(ObjectMapper mapper, ManagementContext mgmt, boolean allowRegisteredTypes, BrooklynClassLoadingContext loader, boolean allowPojoJavaTypes) {
        // the type resolver is extended to recognise brooklyn registered type names
        // and return a subtype of jackson JavaType
        mapper.setDefaultTyping(new BrtTypeResolverBuilder(mgmt, allowRegisteredTypes, loader, allowPojoJavaTypes));

//        // this is tempting but it breaks places where we rely on type: ... to tell us the type
//        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        SimpleModule module = new SimpleModule();
        if (allowRegisteredTypes) {
            module.setDeserializers(new RegisteredTypeDeserializers(mgmt));
        }

        // the module defines how to deserialize the registered type id
        mapper.registerModule(module);

        // preferred defaults
        mapper.setSerializationInclusion(Include.NON_NULL);

        return mapper;
    }

}
