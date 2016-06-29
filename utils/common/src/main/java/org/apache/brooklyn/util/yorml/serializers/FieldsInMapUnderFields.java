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
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;

public class FieldsInMapUnderFields extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public static class Worker extends YormlSerializerWorker {
        public void read() {
            if (!context.isPhase(YormlContext.StandardPhases.HANDLING_FIELDS)) return;
            if (!hasJavaObject()) return;
            
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            if (fields==null) return;
            
            boolean changed = false;
            for (Object f: MutableList.copyOf( ((Map<?,?>)fields).keySet() )) {
                Object v = ((Map<?,?>)fields).get(f);
                try {
                    Maybe<Field> ffm = Reflections.findFieldMaybe(getJavaObject().getClass(), Strings.toString(f));
                    if (ffm.isAbsentOrNull()) {
                        // just skip (could throw, but leave it in case something else recognises it?)
                    } else {
                        Field ff = ffm.get();
                        if (Modifier.isStatic(ff.getModifiers())) {
                            // as above
                        } else {
                            String fieldType = YormlUtils.getFieldTypeName(ff, config);
                            Object v2 = converter.read( new YormlContextForRead(v, context.getJsonPath()+"/"+f, fieldType) );
                            
                            ff.setAccessible(true);
                            ff.set(getJavaObject(), v2);
                            ((Map<?,?>)fields).remove(Strings.toString(f));
                            changed = true;
                        }
                    }
                } catch (Exception e) { throw Exceptions.propagate(e); }
            }
            
            if (changed) {
                if (((Map<?,?>)fields).isEmpty()) {
                    removeFromYamlKeysOnBlackboard("fields");
                }
                // restart (there is normally nothing after this so could equally continue with rerun)
                context.phaseRestart();
            }
        }

        public void write() {
            if (!context.isPhase(YormlContext.StandardPhases.HANDLING_FIELDS)) return;
            if (!isYamlMap()) return;
            if (getFromYamlMap("fields", Map.class).isPresent()) return;
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null || fib.fieldsToWriteFromJava.isEmpty()) return;
            
            Map<String,Object> fields = MutableMap.of();
            
            for (String f: MutableList.copyOf(fib.fieldsToWriteFromJava)) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (v.isPresent()) {
                    fib.fieldsToWriteFromJava.remove(f);
                    if (v.get()==null) {
                        // silently drop null fields
                    } else {
                        Field ff = Reflections.findFieldMaybe(getJavaObject().getClass(), f).get();
                        String fieldType = YormlUtils.getFieldTypeName(ff, config);
                        Object v2 = converter.write(new YormlContextForWrite(v.get(), context.getJsonPath()+"/"+f, fieldType) );
                        fields.put(f, v2);
                    }
                }
            }
            
            if (!fields.isEmpty()) {
                setInYamlMap("fields", fields);
                // restart in case a serializer moves the `fields` map somewhere else
                context.phaseRestart();
            }
        }
    }
    
}
