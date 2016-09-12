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

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.YomlContext.StandardPhases;
import org.apache.brooklyn.util.yoml.YomlContextForRead;
import org.apache.brooklyn.util.yoml.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FieldsInMapUnderFields extends YomlSerializerComposition {

    private static final Logger log = LoggerFactory.getLogger(FieldsInMapUnderFields.class);
    
    public static String KEY_NAME_FOR_MAP_OF_FIELD_VALUES = "fields";
    
    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    protected String getKeyNameForMapOfGeneralValues() {
        return KEY_NAME_FOR_MAP_OF_FIELD_VALUES;
    }
    
    protected String getExpectedPhaseRead() {
        return YomlContext.StandardPhases.HANDLING_FIELDS;
    }

    public class Worker extends YomlSerializerWorker {
        
        protected boolean setKeyValueForJavaObjectOnRead(String key, Object value)
                throws IllegalAccessException {
            Maybe<Field> ffm = Reflections.findFieldMaybe(getJavaObject().getClass(), key);
            if (ffm.isAbsentOrNull()) {
                // just skip (could throw, but leave it in case something else recognises it)
                return false;
            } else {
                Field ff = ffm.get();
                if (Modifier.isStatic(ff.getModifiers())) {
                    // as above
                    return false;
                } else {
                    String fieldType = YomlUtils.getFieldTypeName(ff, config);
                    Object v2 = converter.read( new YomlContextForRead(value, context.getJsonPath()+"/"+key, fieldType) );
                    
                    ff.setAccessible(true);
                    ff.set(getJavaObject(), v2);
                    return true;
                }
            }
        }

        protected boolean shouldHaveJavaObject() { return true; }
        
        public void read() {
            if (!context.isPhase(getExpectedPhaseRead())) return;
            if (hasJavaObject() != shouldHaveJavaObject()) return;
            
            @SuppressWarnings("unchecked")
            Map<String,Object> fields = peekFromYamlKeysOnBlackboard(getKeyNameForMapOfGeneralValues(), Map.class).orNull();
            if (fields==null) return;
            
            boolean changed = false;
            for (Object fo: MutableList.copyOf( ((Map<?,?>)fields).keySet() )) {
                String f = (String)fo;
                Object v = ((Map<?,?>)fields).get(f);
                try {
                    if (setKeyValueForJavaObjectOnRead(f, v)) {
                        ((Map<?,?>)fields).remove(f);
                        changed = true;
                    }
                } catch (Exception e) { throw Exceptions.propagate(e); }
            }
            
            if (changed) {
                if (((Map<?,?>)fields).isEmpty()) {
                    removeFromYamlKeysOnBlackboard(getKeyNameForMapOfGeneralValues());
                }
                // restart (there is normally nothing after this so could equally continue with rerun)
                context.phaseRestart();
            }
        }

        public void write() {
            if (!context.isPhase(StandardPhases.HANDLING_FIELDS)) return;
            if (!isYamlMap()) return;
            if (getFromYamlMap(getKeyNameForMapOfGeneralValues(), Map.class).isPresent()) return;
            
            Map<String, Object> fields = writePrepareGeneralMap();
            if (fields!=null && !fields.isEmpty()) {
                setInYamlMap(getKeyNameForMapOfGeneralValues(), fields);
                // restart in case a serializer moves the `fields` map somewhere else
                context.phaseRestart();
            }
        }

        protected Map<String, Object> writePrepareGeneralMap() {
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null || fib.fieldsToWriteFromJava==null || fib.fieldsToWriteFromJava.isEmpty()) return null;
            Map<String,Object> fields = MutableMap.of();

            for (String f: MutableList.copyOf(fib.fieldsToWriteFromJava)) {
                Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), f);
                if (v.isPresent()) {
                    fib.fieldsToWriteFromJava.remove(f);
                    if (v.get()==null) {
                        // silently drop null fields
                    } else {
                        Field ff = Reflections.findFieldMaybe(getJavaObject().getClass(), f).get();
                        String fieldType = YomlUtils.getFieldTypeName(ff, config);
                        Object v2 = converter.write(new YomlContextForWrite(v.get(), context.getJsonPath()+"/"+f, fieldType) );
                        fields.put(f, v2);
                    }
                }
            }
            if (log.isTraceEnabled()) {
                log.trace(this+": built fields map "+fields);
            }
            return fields;
        }
    }
    
}
