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
import com.fasterxml.jackson.core.JsonLocation;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.WritableTypeId;
import com.fasterxml.jackson.core.util.JsonParserSequence;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.deser.AbstractDeserializer;
import com.fasterxml.jackson.databind.deser.std.DelegatingDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeDeserializer;
import com.fasterxml.jackson.databind.jsontype.impl.AsPropertyTypeSerializer;
import com.fasterxml.jackson.databind.util.TokenBuffer;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.AccessibleObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class AsPropertyIfAmbiguous {

    private static final Logger LOG = LoggerFactory.getLogger(AsPropertyIfAmbiguous.class);
    private static Set<String> warnedAmbiguousTypeProperty = MutableSet.of();

    public static final Function<String,String> CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM = t -> "("+t+")";  // prefer this as YAML allows it unquoted
    public static final Function<String,String> CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM_ALT = t -> "@" + t;  // allow this old form too
    /** @deprecated since 1.1 now use transform fn, and prefer wrapped in parens */
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
                // the class has a field called 'type'; prefix with a '!'
                tpn = CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM.apply(tpn);
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

    static ThreadLocal<AtomicInteger> suppressingTypeFieldDeserialization = new ThreadLocal<>();
    static boolean isSuppressingTypeFieldDeserialization() {
        AtomicInteger count = suppressingTypeFieldDeserialization.get();
        if (count==null) return false;
        return count.get() > 0;
    }
    static void startSuppressingTypeFieldDeserialization() {
        AtomicInteger count = suppressingTypeFieldDeserialization.get();
        if (count==null) {
            count = new AtomicInteger();
            suppressingTypeFieldDeserialization.set(count);
        }
        count.incrementAndGet();
    }
    static void stopSuppressingTypeFieldDeserialization() {
        AtomicInteger count = suppressingTypeFieldDeserialization.get();
        if (count==null) {
            throw new IllegalStateException("Count mismatch starting/stopping type field deserialization");
        }
        if (count.decrementAndGet()==0) {
            suppressingTypeFieldDeserialization.remove();
        }
    }

    /** Type deserializer which understands a '@type' property if 'type' conflicts with a field on the class and which uses the base type if no type is specified */
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

        protected boolean hasTypePropertyNameAsField(JavaType type) {
            // object has field with same name as the type property - don't treat the type property supplied here as the type
            return presentAndNotJsonIgnored(Reflections.findFieldMaybe(type.getRawClass(), _typePropertyName))
                    || // or object has getter with same name as the type property
                    presentAndNotJsonIgnored(Reflections.findMethodMaybe(type.getRawClass(), "get" + Strings.toInitialCapOnly(_typePropertyName)));
        }
        @Override
        public Object deserializeTypedFromObject(JsonParser p, DeserializationContext ctxt) throws IOException {
            AsPropertyButNotIfFieldConflictTypeDeserializer target = this;
            boolean mustUseConflictingTypePrefix = false;

            if (_idResolver instanceof HasBaseType) {
                JavaType baseType = ((HasBaseType) _idResolver).getBaseType();
                if (baseType != null ) {
                    if (hasTypePropertyNameAsField(baseType) && !AbstractBrooklynObjectSpec.class.isAssignableFrom(baseType.getRawClass())) {
                        // look for an '@' type
//                    return cloneWithNewTypePropertyName(CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM.apply(_typePropertyName)).deserializeTypedFromObject(p, ctxt);
                        // now we always look for @ first, in case the type is not known but that field is present; but if we know 'type' is a bean field, don't allow it to be used
                        mustUseConflictingTypePrefix = true;

                        // previous behaviour:
//                    // don't read type id, just deserialize
//                    JsonDeserializer<Object> deser = ctxt.findContextualValueDeserializer(((HasBaseType)_idResolver).getBaseType(), _property);
//                    return deser.deserialize(p, ctxt);
                    }

                    // ? - MapperFeature.USE_BASE_TYPE_AS_DEFAULT_IMPL should do this
                    if (!Objects.equals(_defaultImpl, baseType)) {
                        // note: needed even if baseType is object
                        target = new AsPropertyButNotIfFieldConflictTypeDeserializer(_baseType, _idResolver, _typePropertyName, _typeIdVisible, ((HasBaseType) _idResolver).getBaseType(), _inclusion);
                    }
                }
            }
            return target.deserializeTypedFromObjectSuper(p, ctxt, mustUseConflictingTypePrefix);
        }

        // copied from super class
        private Object deserializeTypedFromObjectSuper(JsonParser p, DeserializationContext ctxt, boolean mustUseConflictingTypePrefix) throws IOException {
            if (isSuppressingTypeFieldDeserialization()) {
                return _deserializeTypedUsingDefaultImpl(p, ctxt, null, "typed deserialization is suppressed");
            }

//            return super.deserializeTypedFromObject(p, ctxt);

            // 02-Aug-2013, tatu: May need to use native type ids
            Object typeId;
            if (p.canReadTypeId()) {
                typeId = p.getTypeId();
                if (typeId != null) {
                    return _deserializeWithNativeTypeId(p, ctxt, typeId);
                }
            }

            // but first, sanity check to ensure we have START_OBJECT or FIELD_NAME
            JsonToken t = p.currentToken();
            if (t == JsonToken.START_OBJECT) {
                t = p.nextToken();
            } else if (/*t == JsonToken.START_ARRAY ||*/ t != JsonToken.FIELD_NAME) {
                /* This is most likely due to the fact that not all Java types are
                 * serialized as JSON Objects; so if "as-property" inclusion is requested,
                 * serialization of things like Lists must be instead handled as if
                 * "as-wrapper-array" was requested.
                 * But this can also be due to some custom handling: so, if "defaultImpl"
                 * is defined, it will be asked to handle this case.
                 */
                return _deserializeTypedUsingDefaultImpl(p, ctxt, null, _msgForMissingId);
            }
            // Ok, let's try to find the property. But first, need token buffer...
            TokenBuffer tb = null;
            boolean ignoreCase = ctxt.isEnabled(MapperFeature.ACCEPT_CASE_INSENSITIVE_PROPERTIES);

            // will look for conflicting property first

            // cache it in case we need to rollback the type (this is a bit expensive; but can optimize later if problematic)
            TokenBuffer tb0 = BrooklynJacksonSerializationUtils.createBufferForParserCurrentObject(p, ctxt);
            p = tb0.asParserOnFirstToken(); //BrooklynJacksonSerializationUtils.createParserFromTokenBufferAndParser(tb0, p);

            DiscoveredTypeAndCachedTokenBuffer typeIdFindResult = findTypeIdOrUnambiguous(p, ctxt, t, tb, ignoreCase, mustUseConflictingTypePrefix);
            tb = typeIdFindResult.tb;

            IOException preferredError = null;
            if (typeIdFindResult.type!=null) {
                boolean canTryWithoutType = !typeIdFindResult.isUnambiguous;
                try {
                    Object result = _deserializeTypedForId(p, ctxt, tb, typeIdFindResult.type);
                    if (_idResolver instanceof HasBaseType) {
                        JavaType baseType = ((HasBaseType) _idResolver).getBaseType();
                        if (baseType != null) {
                            Class<?> rawClass = baseType.getRawClass();
                            if (rawClass != null && !rawClass.isAssignableFrom(result.getClass())) {
                                canTryWithoutType = true;  // will be allow to try without a type, but prefer our error
                                preferredError = new IOException("Invalid result: deserialized type "+result.getClass()+" when expected "+baseType);
                                throw preferredError;
                            }
                        }
                    }
                    return result;
                } catch (Exception e) {
                    if (!canTryWithoutType) throw e;
                    // if ambiguous then deserialize using default, below; but reset the parser first
                    p = tb0.asParserOnFirstToken();
                    tb = tb0;
                }
            }

            try {
                return _deserializeTypedUsingDefaultImpl(p, ctxt, tb, _msgForMissingId);
            } catch (Exception e2) {
                if (preferredError!=null) throw preferredError;
                throw e2;
            }

        }

        static class DiscoveredTypeAndCachedTokenBuffer {
            String type;
            TokenBuffer tb;
            boolean isUnambiguous;

            DiscoveredTypeAndCachedTokenBuffer(String type, TokenBuffer tb, boolean isUnambiguous) {
                this.type = type;
                this.tb = tb;
                this.isUnambiguous = isUnambiguous;
            }
        }

        private DiscoveredTypeAndCachedTokenBuffer findTypeIdOrUnambiguous(JsonParser p, DeserializationContext ctxt, JsonToken t, TokenBuffer tb, boolean ignoreCase, boolean mustUseConflictingTypePrefix) throws IOException {
            String typeUnambiguous1 = CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM.apply(_typePropertyName);
            String typeUnambiguous2 = CONFLICTING_TYPE_NAME_PROPERTY_TRANSFORM_ALT.apply(_typePropertyName);

            int fieldsRead = 0;
            for (; t == JsonToken.FIELD_NAME; t = p.nextToken()) {
                final String name = p.currentName();
                p.nextToken(); // to point to the value

                // unambiguous property should precede ambiguous property name in cases where property name is required
                // maintaining the parser and token buffer in the desired states to allow either anywhere is too hard
                boolean unambiguousName = name.equals(typeUnambiguous1) || name.equals(typeUnambiguous2);
                boolean ambiguousName = !unambiguousName && (!mustUseConflictingTypePrefix && (name.equals(_typePropertyName)
                        || (ignoreCase && name.equalsIgnoreCase(_typePropertyName))));
                if (ambiguousName || unambiguousName) { // gotcha!
                    // 09-Sep-2021, tatu: [databind#3271]: Avoid converting null to "null"
                    String typeId = p.getValueAsString();
                    if (typeId != null) {
                        boolean disallowed = false;
                        if (ambiguousName) {
                            JavaType tt = _idResolver.typeFromId(ctxt, typeId);
                            if (BrooklynObject.class.isAssignableFrom(tt.getRawClass()) && !Feed.class.isAssignableFrom(tt.getRawClass())) {
                                Boolean wantsSpec = null;
                                Boolean wantsBO = null;

                                JavaType baseType = null;
                                if (_idResolver instanceof HasBaseType) {
                                    baseType = ((HasBaseType) _idResolver).getBaseType();
                                    if (baseType != null) {
                                        wantsSpec = AbstractBrooklynObjectSpec.class.isAssignableFrom(baseType.getRawClass());
                                        wantsBO = BrooklynObject.class.isAssignableFrom(baseType.getRawClass());
                                    }
                                }

                                if (Boolean.TRUE.equals(wantsSpec)) {
                                    if (tt instanceof BrooklynJacksonType && BrooklynTypeRegistry.RegisteredTypeKind.SPEC.equals(((BrooklynJacksonType)tt).getRegisteredType().getKind())) {
                                        // if it's a spec registered type, we should load it, like normal
                                        // (no-op)
                                    } else {
                                        // if it's a class then we need to (1) infer the BOSpec type, then (2) re-read the type and set that as the field
                                        typeId = BrooklynObjectType.of(tt.getRawClass()).getSpecType().getName();
                                        tt = null;
                                        if (tb == null) {
                                            tb = ctxt.bufferForInputBuffering(p);
                                        }
                                        tb.writeFieldName(name);
                                        tb.copyCurrentStructure(p);
                                    }
                                } else if (Boolean.TRUE.equals(wantsBO)) {
                                    // if caller wants a BO we just read it normally, whether loading from an ID or created a (non-entity) instance such as a feed
                                    // no-op

                                } else if (!(tt instanceof BrooklynJacksonType) && BrooklynObjectType.of(tt.getRawClass()).getInterfaceType().equals(tt.getRawClass())) {
                                    // if caller hasn't explicitly asked for a BO, and a base BO type (eg Entity) is specified, probably we are loading from an ID
                                    // by specifying Entity class exactly (not a sub-type interface and not registered type) we allow re-instantiation using ID
                                    // no-op

                                } else {
                                    // caller hasn't explicitly asked for a BO, and it isn't a recognized pattern, so in this case we do not load the type;
                                    // will probably remain as a map, unless (type) is specified

                                    if (LOG.isTraceEnabled()) LOG.trace("Ambiguous request for "+baseType+" / "+tt+"; allowing");
                                    tt = null;
                                    disallowed = true;
                                }
                            }

                            if (tt!=null && hasTypePropertyNameAsField(tt)) {
                                // if there is a property called 'type' then caller should use @type.
                                disallowed = true;
                                // unless we need a type to conform to coercion.
                                if (_idResolver instanceof HasBaseType) {
                                    JavaType baseType = ((HasBaseType) _idResolver).getBaseType();
                                    if (baseType==null || baseType.getRawClass().equals(Object.class)) {
                                        if (fieldsRead==0) {
                                            // 'type' should be treated as a normal key when an object is expected, if type it references has a field 'type',
                                            // except if it is the first key in the definition, to facilitate messy places where we say 'type: xxx' as the definition
                                            if (warnedAmbiguousTypeProperty.add(typeId)) {
                                                LOG.warn("Ambiguous type property '" + _typePropertyName + "' used for '" + typeId + "' as first entry in definition; this looks like a type specification but this could also refer to the property; " +
                                                        "using for the former, but specification should have used '" + typeUnambiguous1 + "' as key earlier in the map, " +
                                                        "or if setting the field is intended put an explicit '" + typeUnambiguous1 + "' before it");
                                            }
                                            disallowed = false;
                                        } else {
                                            // leave disallowed
                                        }
                                    } else if (baseType.isMapLikeType()) {
                                        // leave disalloed
                                    } else {
                                        if (warnedAmbiguousTypeProperty.add(typeId)) {
                                            LOG.warn("Ambiguous type property '" + _typePropertyName + "' used for '" + typeId + "'; a type specification is needed to comply with expectations, but this could also refer to the property; " +
                                                    "using for the former, but specification should have used " + typeUnambiguous1 + " as key earlier in the map");
                                        }
                                        disallowed = false;
                                    }
                                }
                            }
                        }
                        if (!disallowed) {
                            return new DiscoveredTypeAndCachedTokenBuffer(typeId, tb, unambiguousName);
                        }
                    }
                }
                if (tb == null) {
                    tb = ctxt.bufferForInputBuffering(p);
                }
                tb.writeFieldName(name);
                tb.copyCurrentStructure(p);

                // advance so we no longer think we are at the beginning
                fieldsRead++;
            }
            return new DiscoveredTypeAndCachedTokenBuffer(null, tb, true);
        }

        private boolean presentAndNotJsonIgnored(Maybe<? extends AccessibleObject> fm) {
            if (!fm.isPresent()) return false;
            AccessibleObject f = fm.get();
            JsonIgnore ignored = f.getAnnotation(JsonIgnore.class);
            if (ignored != null) return false;
            return true;
        }

        @Override
        public TypeDeserializer forProperty(BeanProperty prop) {
            return (prop == _property) ? this : new AsPropertyButNotIfFieldConflictTypeDeserializer(this, prop);
        }

        /** if the type is detected as probably serialized inside an array, we need to skip the default _deserialize which assumes we know the type */
        public Object deserializeArrayContainingType(JsonParser p, DeserializationContext ctxt) throws IOException {
            return super._deserialize(p, ctxt);
            // better than deserializeTypedFromAny directly because does not come in to our _deserialize
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
        protected Object _deserializeTypedForId(JsonParser p, DeserializationContext ctxt, TokenBuffer tb
                // jackson 2.13 below
                , String typeId
                ) throws IOException {
            // first part copied from parent

            // jackson 2.11 only
//            String typeId = p.getText();

            JsonDeserializer<Object> deser = _findDeserializer(ctxt, typeId);
            if (_typeIdVisible) { // need to merge id back in JSON input?
                if (tb == null) {
                    tb = new TokenBuffer(p, ctxt);
                }
                tb.writeFieldName(p.getCurrentName());
                tb.writeString(typeId);
            }
            if (tb != null) {
                p.clearCurrentToken();
                p = JsonParserSequence.createFlattened(false, tb.asParser(p), p);
            }

            if (p.currentToken() != JsonToken.END_OBJECT) {
                p.nextToken();
            }

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
                    if (candidate != null) {
                        return candidate;
                    }
                }
                throw e;
            }
        }

        @Override
        protected Object _deserializeTypedUsingDefaultImpl(JsonParser p, DeserializationContext ctxt, TokenBuffer tb
                // jackson 2.13
                , String priorFailureMsg
        ) throws IOException {
            JsonDeserializer<Object> deserPeek = _findDefaultImplDeserializer(ctxt);
            if (isAbstract(deserPeek)) {
                // if it's abstract, don't use untyped
                if (p.getCurrentToken()==JsonToken.START_ARRAY) {
                    return deserializeArrayContainingType(p, ctxt);
                }
            }
            return super._deserializeTypedUsingDefaultImpl(p, ctxt, tb
                    // jackson 2.13
                    , priorFailureMsg
                    );
        }

        protected boolean isAbstract(JsonDeserializer d) {
            if (d instanceof AbstractDeserializer) return true;
            if (d instanceof DelegatingDeserializer) return isAbstract( ((DelegatingDeserializer)d).getDelegatee() );
            return false;
        }
    }
}
