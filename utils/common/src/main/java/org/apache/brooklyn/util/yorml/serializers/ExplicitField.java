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

import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.YormlContinuation;

public class ExplicitField extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }
    
    protected String fieldName;
    protected String fieldType;
    
    protected String keyName;
    public String getKeyName() { if (keyName!=null) return keyName; return fieldName; }
    
    public class Worker extends YormlSerializerWorker {
        public YormlContinuation read() {
            if (!hasJavaObject()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            String keyName = getKeyName();
            Maybe<Object> value = peekFromYamlKeysOnBlackboard(keyName, Object.class);
            if (value.isAbsent()) return YormlContinuation.CONTINUE_UNCHANGED;
            
            Maybe<Field> ffm = Reflections.findFieldMaybe(getJavaObject().getClass(), fieldName);
            if (ffm.isAbsentOrNull()) {
                throw new IllegalStateException("Expected field `"+fieldName+"` in "+getJavaObject());
            }
            
            Field ff = ffm.get();
            if (Modifier.isStatic(ff.getModifiers())) {
                throw new IllegalStateException("Cannot set static `"+fieldName+"` in "+getJavaObject());
            }

            String fieldType = ExplicitField.this.fieldType;
            if (fieldType==null) fieldType = config.getTypeRegistry().getTypeNameOfClass(ff.getType());

            YormlContextForRead subcontext = new YormlContextForRead(context.getJsonPath()+"/"+keyName, fieldType);
            subcontext.setYamlObject(value.get());
            Object v2 = converter.read(subcontext);

            ff.setAccessible(true);
            try {
                ff.set(getJavaObject(), v2);
            } catch (Exception e) {
                // TODO do we need to say where?
                Exceptions.propagate(e);
            }
            removeFromYamlKeysOnBlackboard(keyName);
                        
            return YormlContinuation.CONTINUE_CHANGED;
        }

        public YormlContinuation write() {
            if (!isYamlMap()) return YormlContinuation.CONTINUE_UNCHANGED;
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null || fib.fieldsToWriteFromJava.isEmpty()) return YormlContinuation.CONTINUE_UNCHANGED;

            Maybe<Object> v = Reflections.getFieldValueMaybe(getJavaObject(), fieldName);
            if (v.isAbsent()) {
                return YormlContinuation.CONTINUE_UNCHANGED;
            }
            
            fib.fieldsToWriteFromJava.remove(fieldName);
            if (v.get()==null) {
                // silently drop 
                return YormlContinuation.CONTINUE_CHANGED;
            } else {
                Field ff = Reflections.findFieldMaybe(getJavaObject().getClass(), fieldName).get();
                
                String fieldType = ExplicitField.this.fieldType;
                if (fieldType==null) fieldType = config.getTypeRegistry().getTypeNameOfClass(ff.getType());
                
                YormlContextForWrite subcontext = new YormlContextForWrite(context.getJsonPath()+"/"+getKeyName(), fieldType);
                subcontext.setJavaObject(v.get());

                Object v2 = converter.write(subcontext);
                setInYamlMap(getKeyName(), v2);
                // no reason to restart?
                return YormlContinuation.CONTINUE_CHANGED;
            }
        }
    }
    
}
