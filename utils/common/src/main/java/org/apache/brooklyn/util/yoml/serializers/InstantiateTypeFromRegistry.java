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
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlContext;
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

            Maybe<Object> resultM = config.getTypeRegistry().newInstanceMaybe((String)type, Yoml.newInstance(config));
            if (resultM.isAbsent()) {
                Class<?> jt = config.getTypeRegistry().getJavaType((String)type);
                String message = jt==null ? "Unknown type '"+type+"'" : "Unable to instantiate type '"+type+"' ("+jt+")";
                RuntimeException exc = ((Maybe.Absent<?>)resultM).getException();
                if (exc!=null) message+=": "+Exceptions.collapseText(exc);
                warn(message);
                return;
            }
            
            addSerializers(type);
            storeReadObjectAndAdvance(resultM.get(), true);
            
            if (isYamlMap()) {
                removeFromYamlKeysOnBlackboard("type");
            }
        }

        public void write() {
            if (!context.isPhase(YomlContext.StandardPhases.HANDLING_TYPE)) return;
            if (hasYamlObject()) return;
            if (!hasJavaObject()) return;
            if (JavaFieldsOnBlackboard.isPresent(blackboard)) return;
            
            if (Reflections.hasSpecialSerializationMethods(getJavaObject().getClass())) {
                warn("Cannot write "+getJavaObject().getClass()+" using default strategy as it has custom serializaton methods");
                return;
            }
            
            // common primitives and maps/lists will have been handled
            // TODO support osgi
            
            MutableMap<Object, Object> map = writingMapWithType(
                // explicitly write the type unless it is the expected one
                getJavaObject().getClass().equals(getExpectedTypeJava()) ? null : config.getTypeRegistry().getTypeName(getJavaObject()));
                
            // collect fields
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            fib.fieldsToWriteFromJava.addAll(YomlUtils.getAllNonTransientNonStaticFieldNamesUntyped(getJavaObject().getClass(), getJavaObject()));
                
            context.phaseInsert(YomlContext.StandardPhases.HANDLING_FIELDS, YomlContext.StandardPhases.MANIPULATING);
            storeWriteObjectAndAdvance(map);
            return;
        }

    }
}
