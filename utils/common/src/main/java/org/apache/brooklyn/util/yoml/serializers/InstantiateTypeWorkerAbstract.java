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

import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.serializers.YomlSerializerComposition.YomlSerializerWorker;

public abstract class InstantiateTypeWorkerAbstract extends YomlSerializerWorker {
    
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
        return YomlUtils.TYPE_JSON.equals(typeName);
    }
    protected Class<?> getSpecialKnownTypeName(String typename) {
        if (YomlUtils.TYPE_STRING.equals(typename)) return String.class;
        if (YomlUtils.TYPE_LIST.equals(typename)) return List.class;
        if (YomlUtils.TYPE_SET.equals(typename)) return Set.class;
        if (YomlUtils.TYPE_MAP.equals(typename)) return Map.class;
        return Boxing.boxedType( Boxing.getPrimitiveType(typename).orNull() );
    }
    
    protected boolean canDoRead() {
        if (!context.isPhase(YomlContext.StandardPhases.HANDLING_TYPE)) return false;
        if (hasJavaObject()) return false;
        return true;
    }
    
    protected boolean canDoWrite() {
        if (!context.isPhase(YomlContext.StandardPhases.HANDLING_TYPE)) return false;
        if (hasYamlObject()) return false;
        if (!hasJavaObject()) return false;
        if (JavaFieldsOnBlackboard.isPresent(blackboard)) return false;
        return true;
    }
    
    /** invoked on read and write to apply the appropriate serializers one the real type is known,
     * e.g. by looking up in registry. name of type will not be null but if it equals the java type
     * that may mean that annotation-scanning is appropriate. */
    protected boolean addSerializersForDiscoveredRealType(@Nullable String type) {
        if (type!=null) {
            // (if null, we were writing what was expected, and we'll have added from expected type serializers)
            if (!type.equals(context.getExpectedType())) {
                return SerializersOnBlackboard.get(blackboard).addInstantiatedTypeSerializers(config.getTypeRegistry().getSerializersForType(type));
            }
        }
        return false;
    }

    protected void storeReadObjectAndAdvance(Object result, boolean addPhases) {
        if (addPhases) {
            context.phaseInsert(YomlContext.StandardPhases.MANIPULATING, YomlContext.StandardPhases.HANDLING_FIELDS);
        }
        context.setJavaObject(result);
        context.phaseAdvance();
    }
    
    protected Maybe<?> tryCoerceAndNoteError(Object value, Class<?> expectedJavaType) {
        if (expectedJavaType==null) return Maybe.of(value);
        Maybe<?> coerced = config.getCoercer().tryCoerce(value, expectedJavaType);
        if (coerced.isAbsent()) {
            // type present but not coercible - error
            ReadingTypeOnBlackboard.get(blackboard).addNote("Cannot interpret or coerce '"+value+"' as "+
                config.getTypeRegistry().getTypeNameOfClass(expectedJavaType));
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

    /** null type-name means we are writing the expected type */
    protected MutableMap<Object, Object> writingMapWithType(@Nullable String typeName) {
        JavaFieldsOnBlackboard.create(blackboard).fieldsToWriteFromJava = MutableList.of();
        MutableMap<Object, Object> map = MutableMap.of();
        
        if (typeName!=null) {
            map.put("type", typeName);
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
    protected void warn(Throwable message) {
        ReadingTypeOnBlackboard.get(blackboard).addNote(message);
    }
}
