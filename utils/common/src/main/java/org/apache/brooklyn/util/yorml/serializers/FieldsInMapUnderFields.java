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
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.YormlContinuation;

public class FieldsInMapUnderFields extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public static class Worker extends YormlSerializerWorker {
        public YormlContinuation read() {
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard("fields", Map.class).orNull();
            if (fields==null) return YormlContinuation.CONTINUE_UNCHANGED;
            
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
                            String fieldType = config.getTypeRegistry().getTypeNameOfClass(ff.getType());
                            YormlContextForRead subcontext = new YormlContextForRead(context.getJsonPath()+"/"+f, fieldType);
                            subcontext.setYamlObject(v);
                            Object v2 = converter.read(subcontext);
                            
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
                return YormlContinuation.RESTART;
            }
            
            return YormlContinuation.CONTINUE_UNCHANGED;
        }

        public YormlContinuation write() {
            if (!isYamlMap()) return YormlContinuation.CONTINUE_UNCHANGED;
            if (getFromYamlMap("fields", Map.class)!=null) return YormlContinuation.CONTINUE_UNCHANGED;
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null || fib.fieldsToWriteFromJava.isEmpty()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Map<String,Object> fields = MutableMap.of();
            
            for (String f: MutableList.copyOf(fib.fieldsToWriteFromJava)) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (v.isPresent()) {
                    fib.fieldsToWriteFromJava.remove(f);
                    if (v.get()==null) {
                        // silently drop null fields
                    } else {
                        Field ff = Reflections.findFieldMaybe(getJavaObject().getClass(), f).get();
                        String fieldType = config.getTypeRegistry().getTypeNameOfClass(ff.getType());
                        YormlContextForWrite subcontext = new YormlContextForWrite(context.getJsonPath()+"/"+f, fieldType);
                        subcontext.setJavaObject(v.get());
                        
                        Object v2 = converter.write(subcontext);
                        fields.put(f, v2);
                    }
                }
            }
            
            if (fields.isEmpty()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            setInYamlMap("fields", fields);
            // restart in case a serializer moves the `fields` map somewhere else
            return YormlContinuation.RESTART;
        }
    }
    
}
