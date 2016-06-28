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

import java.lang.reflect.Field;
import java.util.List;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.FieldOrderings;
import org.apache.brooklyn.util.javalang.ReflectionPredicates;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.Yorml;
import org.apache.brooklyn.util.yorml.YormlContext;

public class InstantiateTypeFromRegistry extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
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

            Object result = config.getTypeRegistry().newInstanceMaybe((String)type, Yorml.newInstance(config)).orNull();
            if (result==null) {
                warn("Unknown type '"+type+"'");
                return;
            }
            
            addSerializers(type);
            storeReadObjectAndAdvance(result, true);
            
            if (isYamlMap()) {
                removeFromYamlKeysOnBlackboard("type");
            }
        }

        public void write() {
            if (!context.isPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return;
            if (hasYamlObject()) return;
            if (!hasJavaObject()) return;
            if (JavaFieldsOnBlackboard.isPresent(blackboard)) return;
            
            // common primitives and maps/lists will have been handled
            // TODO support osgi
            
            MutableMap<Object, Object> map = writingMapWithType(
                // explicitly write the type unless it is the expected one
                getJavaObject().getClass().equals(getExpectedTypeJava()) ? null : config.getTypeRegistry().getTypeName(getJavaObject()));
                
            // collect fields
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            List<Field> fields = Reflections.findFields(getJavaObject().getClass(), 
                null,
                FieldOrderings.ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST);
            Field lastF = null;
            for (Field f: fields) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (ReflectionPredicates.IS_FIELD_NON_TRANSIENT.apply(f) && ReflectionPredicates.IS_FIELD_NON_STATIC.apply(f) && v.isPresentAndNonNull()) {
                    String name = f.getName();
                    if (lastF!=null && lastF.getName().equals(f.getName())) {
                        // if field is shadowed use FQN
                        name = f.getDeclaringClass().getCanonicalName()+"."+name;
                    }
                    fib.fieldsToWriteFromJava.add(name);
                }
                lastF = f;
            }
                
            context.phaseInsert(YormlContext.StandardPhases.HANDLING_FIELDS, YormlContext.StandardPhases.MANIPULATING);
            storeWriteObjectAndAdvance(map);
            return;
        }

    }
}
