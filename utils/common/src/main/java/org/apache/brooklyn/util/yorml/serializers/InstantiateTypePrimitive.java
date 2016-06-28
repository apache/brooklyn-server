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
package org.apache.brooklyn.util.yorml.serializers;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;

public class InstantiateTypePrimitive extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
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
            Maybe<?> value;
            
            if (isJsonPrimitiveObject(getYamlObject())) {
                // pure primitive - we must know the type and then we should simply be able to coerce
                
                expectedJavaType = getExpectedTypeJava();
                if (expectedJavaType==null && !isJsonMarkerTypeExpected()) return;
                
                // type should be coercible
                value = tryCoerceAndNoteError(getYamlObject(), expectedJavaType);
                if (value.isAbsent()) return;
                
            } else {
                // not primitive; it should be of {type: ..., value: ...} format with type being the primitive
                
                String typeName = readingTypeFromFieldOrExpected();
                if (typeName==null) return;
                expectedJavaType = config.getTypeRegistry().getJavaType(typeName);
                if (expectedJavaType==null) expectedJavaType = getSpecialKnownTypeName(typeName);
                if (!isJsonPrimitiveType(expectedJavaType) && !isJsonMarkerType(typeName)) return;
                
                value = readingValueFromTypeValueMap(expectedJavaType);
                if (value.isAbsent()) return;
                if (tryCoerceAndNoteError(value.get(), expectedJavaType).isAbsent()) return;
                
                removeTypeAndValueKeys();
            }
            
            storeReadObjectAndAdvance(value.get(), false);
        }

        protected Maybe<?> tryCoerceAndNoteError(Object value, Class<?> expectedJavaType) {
            if (expectedJavaType==null) return Maybe.of(value);
            Maybe<?> coerced = config.getCoercer().tryCoerce(value, expectedJavaType);
            if (coerced.isAbsent()) {
                // type present but not coercible - error
                ReadingTypeOnBlackboard.get(blackboard).addNote("Cannot interpret '"+value+"' as primitive "+expectedJavaType);
            }
            return coerced;
        }

        public void write() {
            if (!canDoWrite()) return;
            
            if (!YormlUtils.JsonMarker.isPureJson(getJavaObject())) return;
            
            if (isJsonPrimitiveType(getExpectedTypeJava()) || isJsonMarkerTypeExpected()) {
                storeWriteObjectAndAdvance(getJavaObject());
                return;                    
            }

            // not expecting a primitive/json; bail out if it's not a primitive (map/list might decide to write `json` as the type)
            if (!isJsonPrimitiveObject(getJavaObject())) return;
            
            MutableMap<Object, Object> map = writingMapWithTypeAndLiteralValue(
                config.getTypeRegistry().getTypeName(getJavaObject()),
                getJavaObject());
                
            context.phaseInsert(YormlContext.StandardPhases.MANIPULATING);
            storeWriteObjectAndAdvance(map);
        }
    }

}
