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

import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.yorml.YormlContext;

public class InstantiateTypeEnum extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }

    public class Worker extends InstantiateTypeWorkerAbstract {
        
        public void read() {
            if (!canDoRead()) return;
            
            Class<?> type = null;
            boolean fromMap = false;
            Maybe<?> value = Maybe.absent();
            
            if (getExpectedTypeJava()!=null) {
                if (!getExpectedTypeJava().isEnum()) return;
                
                value = Maybe.of(getYamlObject());
                if (!isJsonPrimitiveObject(value.get())) {
                    // warn, but try { type: .., value: ... } syntax
                    warn("Enum "+getExpectedTypeJava()+" is not a string");
                    
                } else {
                    type = getExpectedTypeJava();
                }
            }
                
            if (type==null) {
                String typeName = readingTypeFromFieldOrExpected();
                if (typeName==null) return;
                type = config.getTypeRegistry().getJavaType(typeName);
                if (type==null || !type.isEnum()) return;
                value = readingValueFromTypeValueMap();
                if (value.isAbsent()) {
                    warn("No value declared for enum "+type);
                    return;
                }
                if (!isJsonPrimitiveObject(value.get())) {
                    warn("Enum "+getExpectedTypeJava()+" is not a string");
                    return;
                }
                
                fromMap = true;
            }
            
            Maybe<?> enumValue = tryCoerceAndNoteError(value.get(), type);
            if (enumValue.isAbsent()) return;
//            Maybe<? extends Enum> v = Enums.valueOfIgnoreCase((Class<? extends Enum>)type, Strings.toString(getYamlObject()));
            
            storeReadObjectAndAdvance(enumValue.get(), false);
            if (fromMap) removeTypeAndValueKeys();
        }

        public void write() {
            if (!canDoWrite()) return;
            if (!getJavaObject().getClass().isEnum()) return;

            boolean wrap = true;
            if (getExpectedTypeJava()!=null) {
                if (!getExpectedTypeJava().isEnum()) return;
                wrap = false;
            }
            
            Object result = ((Enum<?>)getJavaObject()).name();

            if (wrap) {
                result = writingMapWithTypeAndLiteralValue(
                    config.getTypeRegistry().getTypeName(getJavaObject()),
                    result);
            }
                
            context.phaseInsert(YormlContext.StandardPhases.MANIPULATING);
            storeWriteObjectAndAdvance(result);
        }
    }

}
