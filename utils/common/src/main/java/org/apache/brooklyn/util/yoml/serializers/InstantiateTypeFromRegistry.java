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
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

public class InstantiateTypeFromRegistry extends YomlSerializerComposition {

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public class Worker extends InstantiateTypeWorkerAbstract {
        
        public void read() {
            if (!canDoRead()) return;
            
            String type = null;
            if (getYamlObject() instanceof CharSequence) {
                // string on its own interpreted as a type
                type = Strings.toString(getYamlObject());
            } else {
                // otherwise should be map
                if (!isYamlMap()) return;
                
                type = readingTypeFromFieldOrExpected();
            }
            
            if (type==null) return;
                        
            if (!readType(type)) return;
            
            if (isYamlMap()) {
                removeFromYamlKeysOnBlackboardRemaining("type");
            }
        }

        protected boolean readType(String type) {
            if (addSerializersForDiscoveredRealType(type)) {
                // added new serializers so restart phase
                // in case another serializer wants to create it
                context.phaseRestart();
                return false;
            }

            Maybe<Object> resultM = config.getTypeRegistry().newInstanceMaybe(type, Yoml.newInstance(config));
            if (resultM.isAbsent()) {
                String message = "Unable to create type '"+type+"'";
                RuntimeException exc = null;
                
                Maybe<Class<?>> jt = config.getTypeRegistry().getJavaTypeMaybe(type);
                if (jt.isAbsent()) {
                    exc = ((Maybe.Absent<?>)jt).getException();
                } else {
                    exc = ((Maybe.Absent<?>)resultM).getException();
                }
                warn(new IllegalStateException(message, exc));
                return false;
            }
            
            storeReadObjectAndAdvance(resultM.get(), true);
            return true;
        }
        
        public void write() {
            if (!canDoWrite()) return;
            
            if (Reflections.hasSpecialSerializationMethods(getJavaObject().getClass())) {
                warn("Cannot write "+getJavaObject().getClass()+" using default strategy as it has custom serializaton methods");
                return;
            }
            
            // common primitives and maps/lists will have been handled
            
            // (osgi syntax isn't supported, because we expect items to be in the registry)
            
            String typeName = getJavaObject().getClass().equals(getExpectedTypeJava()) ? null : config.getTypeRegistry().getTypeName(getJavaObject());
            if (addSerializersForDiscoveredRealType(typeName)) {
                // if new serializers, bail out and we'll re-run
                context.phaseRestart();
                return;
            }
            
            MutableMap<Object, Object> map = writingMapWithType(
                // explicitly write the type (unless it is the expected one)
                typeName);
                
            writingPopulateBlackboard();
            writingInsertPhases();
            
            storeWriteObjectAndAdvance(map);
            return;
        }

        protected void writingInsertPhases() {
            context.phaseInsert(YomlContext.StandardPhases.HANDLING_FIELDS, YomlContext.StandardPhases.MANIPULATING);
        }

        protected void writingPopulateBlackboard() {
            // collect fields
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            fib.fieldsToWriteFromJava.addAll(YomlUtils.getAllNonTransientNonStaticFieldNamesUntyped(getJavaObject().getClass(), getJavaObject()));
        }

    }
}
