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
package org.apache.brooklyn.util.yoml.serializers;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yoml.annotations.YomlAsPrimitive;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

public class InstantiateTypePrimitive extends YomlSerializerComposition {

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    public class Worker extends InstantiateTypeWorkerAbstract {
        
        @Override
        public Class<?> getExpectedTypeJava() {
            Class<?> result = super.getExpectedTypeJava();
            if (result!=null) return result;
            return getSpecialKnownTypeName(context.getExpectedType());
        }
        
        public void read() {
            if (!canDoRead()) return;
            
            Class<?> expectedJavaType;
            Maybe<?> value = Maybe.absent();
            
            if (isJsonPrimitiveObject(getYamlObject())) {
                // pure primitive - we must know the type and then we should simply be able to coerce
                
                expectedJavaType = getExpectedTypeJava();
                if (expectedJavaType==null && !isJsonMarkerTypeExpected()) return;
                
                // type should be coercible
                value = tryCoerceAndNoteError(getYamlObject(), expectedJavaType);
                if (value.isAbsent()) return;
                
            } else {
                // not primitive; either should be coercible or should be of {type: ..., value: ...} format with type being the primitive
                
                expectedJavaType = getExpectedTypeJava();
                if (!isJsonComplexObject(getYamlObject()) && (expectedJavaType!=null || isJsonMarkerTypeExpected())) {
                    // if it's not a json map/list (and not a primitive) than try a coercion;
                    // maybe a bit odd to call that "primitive" but it is primitive in the sense it is pass-through unparsed
                    value = tryCoerceAndNoteError(getYamlObject(), expectedJavaType);
                }
                
                if (value.isAbsent()) {
                    String typeName = readingTypeFromFieldOrExpected();
                    if (typeName==null) return;
                    expectedJavaType = config.getTypeRegistry().getJavaTypeMaybe(typeName).orNull();
                    if (expectedJavaType==null) expectedJavaType = getSpecialKnownTypeName(typeName);
                    // could restrict read coercion to basic types as follows, but no harm in trying to coerce if it's
                    // a value map, unless the target is a special json which will be handled by another serializer
                    if (isJsonComplexType(expectedJavaType) || isGeneric(typeName)) return;

                    value = readingValueFromTypeValueMap();
                    if (value.isAbsent()) return;
                    value = tryCoerceAndNoteError(value.get(), expectedJavaType);
                    if (value.isAbsent()) return;
                    removeTypeAndValueKeys();
                }
            }
            
            storeReadObjectAndAdvance(value.get(), false);
        }

        public void write() {
            if (!canDoWrite()) return;
            
            Object jIn = getJavaObject();
            Object jOut = null;
            
            if (jIn==null) return;
            
            if (!YomlUtils.JsonMarker.isPureJson(jIn)) {
                // not json, but can we coerce to json?
                if (jIn.getClass().getAnnotation(YomlAsPrimitive.class)!=null) {
                    Object jo;
                    if (jOut==null) {
                        jo = config.getCoercer().tryCoerce(jIn, String.class).orNull();
                        if (isReverseCoercible(jo, jIn)) jOut = jo;
                    }
                    if (jOut==null) {
                        jo = jIn.toString();
                        if (isReverseCoercible(jo, jIn)) jOut = jo;
                    }
                    // could convert to other primitives eg int
                    // but no good use case so far
                }
                if (jOut!=null) {
                    // check whether we'll be able to read it back without additional type information
                    Maybe<?> typeNeededCheck = config.getCoercer().tryCoerce(jOut, getExpectedTypeJava());
                    if (typeNeededCheck.isPresent() && jIn.equals(typeNeededCheck.get())) {
                        // expected type is good enough to coerce, so write without type info
                        storeWriteObjectAndAdvance(jOut);
                        return;                    
                        
                    } else {
                        // fall through to below and write as type/value map
                    }
                }
            }
            
            if (jOut==null) jOut = jIn;
            if (!YomlUtils.JsonMarker.isPureJson(jOut)) {
                // it input is not pure json at this point, we don't apply
                return;
            }
            
            if (isJsonPrimitiveType(getExpectedTypeJava()) || isJsonMarkerTypeExpected()) {
                // store it as pure primitive
                storeWriteObjectAndAdvance(jOut);
                return;                    
            }

            // not expecting a primitive/json; bail out if it's not a primitive (map/list might decide to write `json` as the type)
            if (!isJsonPrimitiveObject(jOut)) return;
            
            String typeName = config.getTypeRegistry().getTypeName(jIn);
            if (addSerializersForDiscoveredRealType(typeName)) {
                // if new serializers, bail out and we'll re-run
                context.phaseRestart();
                return;
            }

            MutableMap<Object, Object> map = writingMapWithTypeAndLiteralValue(typeName, jOut);
            context.phaseInsert(YomlContext.StandardPhases.MANIPULATING);
            storeWriteObjectAndAdvance(map);
        }

        private boolean isReverseCoercible(Object input, Object target) {
            Maybe<? extends Object> coerced = config.getCoercer().tryCoerce(input, target.getClass());
            if (coerced.isAbsent()) return false;
            return (target.equals(coerced.get()));
        }
    }

}
