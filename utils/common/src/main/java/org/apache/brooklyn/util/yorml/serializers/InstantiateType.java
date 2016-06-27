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
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.FieldOrderings;
import org.apache.brooklyn.util.javalang.ReflectionPredicates;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.Yorml;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.apache.brooklyn.util.yorml.internal.SerializersOnBlackboard;

public class InstantiateType extends YormlSerializerComposition {

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public static class Worker extends YormlSerializerWorker {
        public void read() {
            if (!context.isPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return;
            if (hasJavaObject()) return;
            
            String type = null;
            if ((getYamlObject() instanceof String) || Boxing.isPrimitiveOrBoxedObject(getYamlObject())) {
                // default handling of primitives:
                // if type is expected, try to coerce
                Class<?> expectedJavaType = getExpectedTypeJava();
                if (expectedJavaType!=null) {
                    Maybe<?> result = config.getCoercer().tryCoerce(getYamlObject(), expectedJavaType);
                    if (result.isPresent()) {
                        context.setJavaObject(result.get());
                        context.phaseAdvance();
                        return;
                    }
                    ReadingTypeOnBlackboard.get(blackboard).addNote("Cannot interpret '"+getYamlObject()+"' as "+expectedJavaType.getCanonicalName());
                    return;
                }
                // if type not expected, treat as a type
                type = Strings.toString(getYamlObject());
            }
            
            // TODO if map and list?
            
            if (!isYamlMap()) return;
            
            YamlKeysOnBlackboard.create(blackboard).yamlKeysToReadToJava = MutableMap.copyOf(getYamlMap());
            
            if (type==null) type = peekFromYamlKeysOnBlackboard("type", String.class).orNull();
            if (type==null) type = context.getExpectedType();
            if (type==null) return;

            Class<?> javaType = config.getTypeRegistry().getJavaType(type);
            boolean primitive = javaType!=null && (Boxing.isPrimitiveOrBoxedClass(javaType) || CharSequence.class.isAssignableFrom(javaType));
            
            Object result;
            if (primitive) {
                if (!getYamlMap().containsKey("value")) {
                    ReadingTypeOnBlackboard.get(blackboard).addNote("Primitive '"+type+"' does not declare a 'value'");
                    return;
                    
                }
                result = config.getCoercer().coerce(getYamlMap().get("value"), javaType);
                removeFromYamlKeysOnBlackboard("value");
                
            } else {
                result = config.getTypeRegistry().newInstanceMaybe((String)type, Yorml.newInstance(config)).orNull();
                if (result==null) {
                    ReadingTypeOnBlackboard.get(blackboard).addNote("Unknown type '"+type+"'");
                    return;
                }
                if (!type.equals(context.getExpectedType())) {
                    Set<YormlSerializer> serializers = MutableSet.of();
                    config.typeRegistry.collectSerializers(type, serializers, MutableSet.<String>of());
                    SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(serializers);
                }
                context.phaseInsert(YormlContext.StandardPhases.MANIPULATING, YormlContext.StandardPhases.HANDLING_FIELDS);
            }
            
            removeFromYamlKeysOnBlackboard("type");
            
            context.setJavaObject(result);
            context.phaseAdvance();
        }

        public void write() {
            if (!context.isPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return;
            if (hasYamlObject()) return;
            if (!hasJavaObject()) return;
            if (JavaFieldsOnBlackboard.isPresent(blackboard)) return;
            
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.create(blackboard);
            fib.fieldsToWriteFromJava = MutableList.of();
            
            Object jo = getJavaObject();
            boolean primitive = Boxing.isPrimitiveOrBoxedObject(jo) || jo instanceof CharSequence; 
            if (primitive && getJavaObject().getClass().equals(Boxing.boxedType(getExpectedTypeJava()))) {
                context.setYamlObject(jo);
                context.phaseAdvance();
                return;
            }

            // TODO map+list -- here, or in separate serializers?
            
            MutableMap<Object, Object> map = MutableMap.of();
            context.setYamlObject(map);
            
            // TODO look up registry type
            // TODO support osgi
            
            if (getJavaObject().getClass().equals(getExpectedTypeJava())) {
                // skip explicitly writing the type
            } else {
                String typeName = config.getTypeRegistry().getTypeName(getJavaObject());
                map.put("type", typeName);
                if (!primitive) {
                    Set<YormlSerializer> serializers = MutableSet.of();
                    config.typeRegistry.collectSerializers(typeName, serializers, MutableSet.<String>of());
                    SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(serializers);
                }
            }

            if (primitive) {
                map.put("value", jo);
                context.phaseInsert(YormlContext.StandardPhases.MANIPULATING);
                
            } else {
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
            }

            context.phaseAdvance();
            return;
        }
    }
}
