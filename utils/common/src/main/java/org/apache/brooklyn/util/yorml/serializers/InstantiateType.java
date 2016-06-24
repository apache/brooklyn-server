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

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.FieldOrderings;
import org.apache.brooklyn.util.javalang.ReflectionPredicates;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlInternals.YormlContinuation;

public class InstantiateType extends YormlSerializerComposition {

    public InstantiateType() { super(Worker.class); }
    
    public static class Worker extends YormlSerializerWorker {
        public YormlContinuation read() {
            if (hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Class<?> expectedJavaType = getExpectedTypeJava();
            
            String type = null;
            if ((getYamlObject() instanceof String) || Boxing.isPrimitiveOrBoxedObject(getYamlObject())) {
                // default handling of primitives:
                // if type is expected, try to coerce
                if (expectedJavaType!=null) {
                    Maybe<?> result = config.getCoercer().tryCoerce(getYamlObject(), expectedJavaType);
                    if (result.isPresent()) {
                        context.setJavaObject(result.get());
                        return YormlContinuation.RESTART;
                    }
                    ReadingTypeOnBlackboard.get(blackboard).addNote("Cannot interpret '"+getYamlObject()+"' as "+expectedJavaType.getCanonicalName());
                    return YormlContinuation.CONTINUE_UNCHANGED;
                }
                // if type not expected, treat as a type
                type = Strings.toString(getYamlObject());
            }
            
            // TODO if map and list?
            
            YamlKeysOnBlackboard.create(blackboard).yamlKeysToReadToJava = MutableMap.copyOf(getYamlMap());
            
            if (type==null) type = peekFromYamlKeysOnBlackboard("type", String.class).orNull();
            Object result = null;
            if (type==null && expectedJavaType!=null) {
                Maybe<Object> resultM = Reflections.invokeConstructorFromArgsIncludingPrivate(expectedJavaType);
                if (resultM.isPresent()) result = resultM.get();
                else ReadingTypeOnBlackboard.get(blackboard).addNote("Expected type is not no-arg instantiable: '"+expectedJavaType+"' ("+
                    ((Maybe.Absent<?>)resultM).getException()+")");
            }
            if (type==null && result==null) return YormlContinuation.CONTINUE_UNCHANGED;
            
            if (result==null) {
                result = config.getTypeRegistry().newInstance((String)type);
            }
            if (result==null) {
                ReadingTypeOnBlackboard.get(blackboard).addNote("Unknown type '"+type+"'");
                return YormlContinuation.CONTINUE_UNCHANGED;
            }
            removeFromYamlKeysOnBlackboard("type");
            
            context.setJavaObject(result);
            return YormlContinuation.RESTART;
        }

        public YormlContinuation write() {
            if (hasYamlObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (JavaFieldsOnBlackboard.isPresent(blackboard)) return YormlContinuation.CONTINUE_UNCHANGED;
            
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.create(blackboard);
            fib.fieldsToWriteFromJava = MutableList.of();
            
            Object jo = getJavaObject();
            if (Boxing.isPrimitiveOrBoxedObject(jo) || jo instanceof CharSequence) {
                context.setYamlObject(jo);
                return YormlContinuation.FINISHED;
            }

            // TODO map+list -- here, or in separate serializers?
            
            MutableMap<Object, Object> map = MutableMap.of();
            context.setYamlObject(map);
            
            // TODO look up registry type
            // TODO support osgi
            
            if (getJavaObject().getClass().equals(getExpectedTypeJava())) {
                // skip explicitly writing the type
            } else {
                map.put("type", config.getTypeRegistry().getTypeName(getJavaObject()) );
            }
            
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
            
            return YormlContinuation.RESTART;
        }
    }
}
