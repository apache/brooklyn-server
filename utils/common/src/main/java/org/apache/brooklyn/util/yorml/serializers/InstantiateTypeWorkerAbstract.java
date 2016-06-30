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

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.apache.brooklyn.util.yorml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;
import org.apache.brooklyn.util.yorml.serializers.YormlSerializerComposition.YormlSerializerWorker;

public abstract class InstantiateTypeWorkerAbstract extends YormlSerializerWorker {
    
    protected boolean isJsonPrimitiveType(Class<?> type) {
        if (type==null) return false;
        if (String.class.isAssignableFrom(type)) return true;
        if (Boxing.isPrimitiveOrBoxedClass(type)) return true;
        return false;
    }
    protected boolean isJsonTypeName(String typename) {
        if (isJsonMarkerType(typename)) return true;
        return getSpecialKnownTypeName(typename)!=null;
    }
    protected boolean isJsonMarkerTypeExpected() {
        return isJsonMarkerType(context.getExpectedType());
    }
    protected boolean isJsonMarkerType(String typeName) {
        return YormlUtils.TYPE_JSON.equals(typeName);
    }
    protected Class<?> getSpecialKnownTypeName(String typename) {
        if (YormlUtils.TYPE_STRING.equals(typename)) return String.class;
        if (YormlUtils.TYPE_LIST.equals(typename)) return List.class;
        if (YormlUtils.TYPE_SET.equals(typename)) return Set.class;
        if (YormlUtils.TYPE_MAP.equals(typename)) return Map.class;
        return Boxing.boxedType( Boxing.getPrimitiveType(typename).orNull() );
    }
    
    protected boolean canDoRead() {
        if (!context.isPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return false;
        if (hasJavaObject()) return false;
        return true;
    }
    
    protected boolean canDoWrite() {
        if (!context.isPhase(YormlContext.StandardPhases.HANDLING_TYPE)) return false;
        if (hasYamlObject()) return false;
        if (!hasJavaObject()) return false;
        if (JavaFieldsOnBlackboard.isPresent(blackboard)) return false;
        return true;
    }
    
    protected void addSerializers(String type) {
        if (!type.equals(context.getExpectedType())) {
            Set<YormlSerializer> serializers = MutableSet.of();
            config.typeRegistry.collectSerializers(type, serializers, MutableSet.<String>of());
            SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(serializers);
        }
    }

    protected void storeReadObjectAndAdvance(Object result, boolean addPhases) {
        if (addPhases) {
            context.phaseInsert(YormlContext.StandardPhases.MANIPULATING, YormlContext.StandardPhases.HANDLING_FIELDS);
        }
        context.setJavaObject(result);
        context.phaseAdvance();
    }
    
    protected Maybe<?> tryCoerceAndNoteError(Object value, Class<?> expectedJavaType) {
        if (expectedJavaType==null) return Maybe.of(value);
        Maybe<?> coerced = config.getCoercer().tryCoerce(value, expectedJavaType);
        if (coerced.isAbsent()) {
            // type present but not coercible - error
            ReadingTypeOnBlackboard.get(blackboard).addNote("Cannot interpret '"+value+"' as primitive "+expectedJavaType);
        }
        return coerced;
    }

    protected void storeWriteObjectAndAdvance(Object jo) {
        context.setYamlObject(jo);
        context.phaseAdvance();
    }
    
    protected String readingTypeFromFieldOrExpected() {
        String type = null;
        if (isYamlMap()) {
            YamlKeysOnBlackboard.getOrCreate(blackboard, getYamlMap());
            type = peekFromYamlKeysOnBlackboard("type", String.class).orNull();
        }
        if (type==null) type = context.getExpectedType();
        return type;
    }
    protected Maybe<Object> readingValueFromTypeValueMap() {
        return readingValueFromTypeValueMap(null);
    }
    protected <T> Maybe<T> readingValueFromTypeValueMap(Class<T> requiredType) {
        if (!isYamlMap()) return Maybe.absent();
        if (YamlKeysOnBlackboard.peek(blackboard).yamlKeysToReadToJava.size()>2) return Maybe.absent();
        if (!MutableSet.of("type", "value").containsAll(YamlKeysOnBlackboard.peek(blackboard).yamlKeysToReadToJava.keySet())) {
            return Maybe.absent();
        }
        return peekFromYamlKeysOnBlackboard("value", requiredType);
    }
    protected void removeTypeAndValueKeys() {
        removeFromYamlKeysOnBlackboard("type", "value");
    }

    protected MutableMap<Object, Object> writingMapWithType(String typeName) {
        JavaFieldsOnBlackboard.create(blackboard).fieldsToWriteFromJava = MutableList.of();
        MutableMap<Object, Object> map = MutableMap.of();
        
        if (typeName!=null) {
            map.put("type", typeName);
            addSerializers(typeName);
        }
        return map;
    }
    protected MutableMap<Object, Object> writingMapWithTypeAndLiteralValue(String typeName, Object value) {
        MutableMap<Object, Object> map = writingMapWithType(typeName);
        if (value!=null) {
            map.put("value", value);
        }
        return map;
    }

    protected void warn(String message) {
        ReadingTypeOnBlackboard.get(blackboard).addNote(message);
    }
}