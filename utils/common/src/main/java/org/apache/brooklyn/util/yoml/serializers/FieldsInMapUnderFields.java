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
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlContext.StandardPhases;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlContextForWrite;
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
        
        protected boolean setKeyValueForJavaObjectOnRead(String key, Object value, String optionalTypeConstraint)
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
                    String fieldType = getFieldTypeName(ff, optionalTypeConstraint);
                    Object v2 = converter.read( ((YomlContextForRead)context).subpath("/"+key, value, fieldType) );
                    
                    ff.setAccessible(true);
                    ff.set(getJavaObject(), v2);
                    return true;
                }
            }
        }

        protected String getFieldTypeName(Field ff, String optionalTypeConstraint) {
            return merge(ff, ff.getType(), YomlUtils.getFieldTypeName(ff, config), optionalTypeConstraint);
        }
        
        protected String merge(Object elementForError, Class<?> localTypeJ, String localTypeName, String optionalTypeConstraint) {
            String fieldType;
            if (optionalTypeConstraint!=null) {
                if (localTypeJ!=null && !Object.class.equals(localTypeJ)) {
                    throw new YomlException("Cannot apply inferred type "+optionalTypeConstraint+" for non-Object field "+elementForError, context);
                    // is there a "combineTypes(fieldType, optionalTypeConstraint)" method?
                    // that would let us weaken the above
                }
                fieldType = optionalTypeConstraint;
            } else {
                fieldType = localTypeName;
            }
            return fieldType;
        }

        protected boolean shouldHaveJavaObject() { return true; }
        
        public void read() {
            if (!context.isPhase(getExpectedPhaseRead())) return;
            if (hasJavaObject() != shouldHaveJavaObject()) return;
            
            @SuppressWarnings("unchecked")
            // written by the individual TopLevelFieldSerializers
            Map<String,Object> fields = peekFromYamlKeysOnBlackboardRemaining(getKeyNameForMapOfGeneralValues(), Map.class).orNull();
            if (fields==null) return;
            
            MutableMap<String, Object> initialFields = MutableMap.copyOf(fields);
            
            List<String> deferred = MutableList.of();
            boolean changed = false;
            for (Object fo: initialFields.keySet()) {
                String f = (String)fo;
                if (TypeFromOtherFieldBlackboard.get(blackboard).getTypeConstraintField(f)!=null) {
                    deferred.add(f);
                    continue;
                }
                Object v = ((Map<?,?>)fields).get(f);
                try {
                    if (setKeyValueForJavaObjectOnRead(f, v, null)) {
                        ((Map<?,?>)fields).remove(f);
                        changed = true;
                    }
                } catch (Exception e) { throw Exceptions.propagate(e); }
            }

            // defer for objects whose types come from another field
            for (String f: deferred) {
                Object typeO;
                String tf = TypeFromOtherFieldBlackboard.get(blackboard).getTypeConstraintField(f);
                boolean isTypeFieldReal = TypeFromOtherFieldBlackboard.get(blackboard).isTypeConstraintFieldReal(f);
                
                if (isTypeFieldReal) {
                    typeO = initialFields.get(tf);
                } else {
                    typeO = peekFromYamlKeysOnBlackboardRemaining(tf, Object.class).orNull();
                }
                if (typeO!=null && !(typeO instanceof String)) {
                    throw new YomlException("Wrong type of value '"+typeO+"' inferred as type of '"+f+"' on "+getJavaObject(), context);
                }
                String type = (String)typeO;
                
                Object v = ((Map<?,?>)fields).get(f);
                try {
                    if (setKeyValueForJavaObjectOnRead(f, v, type)) {
                        ((Map<?,?>)fields).remove(f);
                        if (type!=null && !isTypeFieldReal) {
                            removeFromYamlKeysOnBlackboardRemaining(tf);
                        }
                        changed = true;
                    }
                } catch (Exception e) { throw Exceptions.propagate(e); }                
            }
            
            if (((Map<?,?>)fields).isEmpty()) {
                removeFromYamlKeysOnBlackboardRemaining(getKeyNameForMapOfGeneralValues());
            }
            if (changed) {
                // restart (there is normally nothing after this so could equally continue with rerun)
                context.phaseRestart();
            }
        }

        public void write() {
            if (!context.isPhase(StandardPhases.HANDLING_FIELDS)) return;
            if (!isYamlMap()) return;
            
            // should have been set by FieldsInMapUnderFields if we are to run
            if (getFromOutputYamlMap(getKeyNameForMapOfGeneralValues(), Map.class).isPresent()) return;
            
            Map<String, Object> fields = writePrepareGeneralMap();
            if (fields!=null && !fields.isEmpty()) {
                setInOutputYamlMap(getKeyNameForMapOfGeneralValues(), fields);
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
                        
                        String fieldType = getFieldTypeName(ff, null);
                        
                        // can we record additional information about the type in the yaml?
                        String tf = TypeFromOtherFieldBlackboard.get(blackboard).getTypeConstraintField(f);
                        if (tf!=null) {
                            if (!Object.class.equals(ff.getType())) {
                                // currently we only support smart types if the base type is object;
                                // see getFieldTypeName
                                
                            } else {
                                if (!TypeFromOtherFieldBlackboard.get(blackboard).isTypeConstraintFieldReal(f)) {
                                    String realType = config.getTypeRegistry().getTypeName(v.get());
                                    fieldType = realType;
                                    // for non-real, just write the pseudo-type-field at root
                                    getOutputYamlMap().put(tf, realType);
                                    
                                } else {
                                    Maybe<Object> rt = Reflections.getFieldValueMaybe(getJavaObject(), tf);
                                    if (rt.isPresentAndNonNull()) {
                                        if (rt.get() instanceof String) {
                                            fieldType = (String) rt.get();
                                        } else {
                                            throw new YomlException("Cannot use type information from "+tf+" for "+f+" as it is "+rt.get(), context);
                                        }
                                    }
                                }
                            }
                        }
                        
                        Object v2 = converter.write( ((YomlContextForWrite)context).subpath("/"+f, v.get(), fieldType) );
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
