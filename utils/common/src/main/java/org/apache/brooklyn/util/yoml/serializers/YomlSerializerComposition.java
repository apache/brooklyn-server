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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.internal.YomlConverter;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.internal.YomlUtils.JsonMarker;

public abstract class YomlSerializerComposition implements YomlSerializer {

    protected abstract YomlSerializerWorker newWorker();
    
    public abstract static class YomlSerializerWorker {

        protected YomlConverter converter;
        protected YomlContext context;
        protected YomlContextForRead readContext;
        protected YomlConfig config;
        protected Map<Object, Object> blackboard;
        
        
        protected void warn(String message) {
            ReadingTypeOnBlackboard.get(blackboard).addNote(message);
        }
        protected void warn(Throwable message) {
            ReadingTypeOnBlackboard.get(blackboard).addNote(message);
        }

        
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
        protected boolean isJsonComplexType(Class<?> t) {
            if (t==null) return false;
            // or could be equals, used as response of the above
            if (Map.class.isAssignableFrom(t)) return true;
            if (Set.class.isAssignableFrom(t)) return true;
            if (List.class.isAssignableFrom(t)) return true;
            return false;
        }
        protected boolean isGeneric(String typename) {
            if (typename==null) return false;
            return typename.contains("<");
        }

        /** true iff the object is a string or java primitive type */ 
        protected boolean isJsonPrimitiveObject(Object o) {
            if (o==null) return true;
            if (o instanceof String) return true;
            if (Boxing.isPrimitiveOrBoxedObject(o)) return true;
            return false;
        }
        
        /** true iff the object is a map or collection (not recursing; for that see {@link #isJsonPureObject(Object)}  */ 
        protected boolean isJsonComplexObject(Object o) {
            return (o instanceof Map || o instanceof Collection);
        }

        /** true iff the object is a primitive type or a map or collection of pure objects;
         * see {@link JsonMarker#isPureJson(Object)} (which this simply proxies for convenience) */ 
        protected boolean isJsonPureObject(Object o) {
            return YomlUtils.JsonMarker.isPureJson(o);
        }
        
        private void initRead(YomlContextForRead context, YomlConverter converter) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.readContext = context;
            this.converter = converter;
            this.config = converter.getConfig();
            this.blackboard = context.getBlackboard();
        }
        
        private void initWrite(YomlContextForWrite context, YomlConverter converter) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.converter = converter;
            this.config = converter.getConfig();
            this.blackboard = context.getBlackboard();
        }

        /** If there is an expected type -- other than "Object"! -- return the java instance. Otherwise null. */ 
        public Class<?> getExpectedTypeJava() { 
            String et = context.getExpectedType();
            if (Strings.isBlank(et)) return null;
            Class<?> ett = config.getTypeRegistry().getJavaTypeMaybe(et).orNull();
            if (Object.class.equals(ett)) return null;
            return ett;
        }

        public boolean hasJavaObject() { return context.getJavaObject()!=null; }
        public boolean hasYamlObject() { return context.getYamlObject()!=null; }
        public Object getJavaObject() { return context.getJavaObject(); }
        public Object getYamlObject() { return context.getYamlObject(); }

        /** Reports whether the YAML object is a map
         * (or on read whether it has now been transformed to a map). */
        public boolean isYamlMap() { return getYamlObject() instanceof Map; }
        
        protected void assertReading() { assert context instanceof YomlContextForRead; }
        protected void assertWriting() { assert context instanceof YomlContextForWrite; }
        
        @SuppressWarnings("unchecked")
        public Map<Object,Object> getOutputYamlMap() {
            assertWriting();
            return (Map<Object,Object>)context.getYamlObject(); 
        }
        
        @SuppressWarnings("unchecked")
        public Map<Object,Object> getRawInputYamlMap() {
            assertReading();
            return (Map<Object,Object>)context.getYamlObject(); 
        }
        
        /** Returns the value of the given key if it is present in the output map and is of the given type. 
         * If the YAML is not a map, or the key is not present, or the type is different, this returns an absent.
         * <p>
         * Read serializers or anything interested in the state of the map should use 
         * {@link #peekFromYamlKeysOnBlackboardRemaining(String, Class)} and other methods here or
         * {@link YamlKeysOnBlackboard} directly. */
        @SuppressWarnings("unchecked")
        public <T> Maybe<T> getFromOutputYamlMap(String key, Class<T> type) {
            if (!isYamlMap()) return Maybe.absent("not a yaml map");
            if (!getOutputYamlMap().containsKey(key)) return Maybe.absent("key `"+key+"` not in yaml map");
            Object v = getOutputYamlMap().get(key);
            if (v==null) return Maybe.ofAllowingNull(null);
            if (!type.isInstance(v)) return Maybe.absent("value of key `"+key+"` is not a "+type);
            return Maybe.of((T) v);
        }
        /** Writes directly to the yaml map which will be returned from a write.
         * Read serializers should not use as per the comments on {@link #getFromOutputYamlMap(String, Class)}. */
        protected void setInOutputYamlMap(String key, Object value) {
            ((Map<Object,Object>)getOutputYamlMap()).put(key, value);
        }
        
        /** creates a YKB instance. fails if the raw yaml input is not a map. */
        protected YamlKeysOnBlackboard getYamlKeysOnBlackboardInitializedFromYamlMap() {
            return YamlKeysOnBlackboard.getOrCreate(blackboard, getRawInputYamlMap());
        }

        @SuppressWarnings("unchecked")
        protected <T> Maybe<T> peekFromYamlKeysOnBlackboardRemaining(String key, Class<T> expectedType) {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            if (ykb==null) return Maybe.absent();
            Maybe<Object> v = ykb.peekKeyLeft(key);
            if (v.isAbsent()) return Maybe.absent();
            if (expectedType!=null && !expectedType.isInstance(v.get())) return Maybe.absent();
            return Maybe.of((T)v.get());
        }
        protected boolean hasYamlKeysOnBlackboardRemaining() {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            return (ykb!=null && ykb.size()>0);
        }
        protected void removeFromYamlKeysOnBlackboardRemaining(String ...keys) {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            for (String key: keys) {
                ykb.removeKey(key);
            }
        }
        /** looks for all keys in {@link YamlKeysOnBlackboard} which can be mangled/ignore-case 
         * to match the given key */
        protected Set<String> findAllYamlKeysOnBlackboardRemainingMangleMatching(String targetKey) {
            Set<String> result = MutableSet.of();
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            for (Object k: ykb.keysLeft()) {
                if (k instanceof String && YomlUtils.mangleable(targetKey, (String)k)) {
                    result.add((String)k);
                }
            }
            return result;
        }
        
        public abstract void read();
        public abstract void write();
    }
    
    @Override
    public void read(YomlContextForRead context, YomlConverter converter) {
        YomlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initRead(context, converter);
        worker.read();
    }

    @Override
    public void write(YomlContextForWrite context, YomlConverter converter) {
        YomlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initWrite(context, converter);
        worker.write();
    }

    @Override
    public String document(String type, YomlConverter converter) {
        return null;
    }
}
