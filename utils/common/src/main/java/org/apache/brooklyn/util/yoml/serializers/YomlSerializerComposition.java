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
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.YomlContextForRead;
import org.apache.brooklyn.util.yoml.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.internal.YomlConfig;
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

        
        private void initRead(YomlContextForRead context, YomlConverter converter, Map<Object, Object> blackboard) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.readContext = context;
            this.converter = converter;
            this.config = converter.getConfig();
            this.blackboard = blackboard;
        }
        
        private void initWrite(YomlContextForWrite context, YomlConverter converter, Map<Object,Object> blackboard) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.converter = converter;
            this.config = converter.getConfig();
            this.blackboard = blackboard;
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

        public boolean isYamlMap() { return context.getYamlObject() instanceof Map; }
        @SuppressWarnings("unchecked")
        public Map<Object,Object> getYamlMap() { return (Map<Object,Object>)context.getYamlObject(); }
        /** Returns the value of the given key if present in the map and of the given type. 
         * If the YAML is not a map, or the key is not present, or the type is different, this returns null.
         * <p>
         * See also {@link #peekFromYamlKeysOnBlackboard(String, Class)} which most read serializers should use. */
        @SuppressWarnings("unchecked")
        public <T> Maybe<T> getFromYamlMap(String key, Class<T> type) {
            if (!isYamlMap()) return Maybe.absent("not a yaml map");
            if (!getYamlMap().containsKey(key)) return Maybe.absent("key `"+key+"` not in yaml map");
            Object v = getYamlMap().get(key);
            if (v==null) return Maybe.ofAllowingNull(null);
            if (!type.isInstance(v)) return Maybe.absent("value of key `"+key+"` is not a "+type);
            return Maybe.of((T) v);
        }
        protected void setInYamlMap(String key, Object value) {
            ((Map<Object,Object>)getYamlMap()).put(key, value);
        }
        @SuppressWarnings("unchecked")
        protected <T> Maybe<T> peekFromYamlKeysOnBlackboard(String key, Class<T> expectedType) {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            if (ykb==null || ykb.yamlKeysToReadToJava==null || !ykb.yamlKeysToReadToJava.containsKey(key)) {
                return Maybe.absent();
            }
            Object v = ykb.yamlKeysToReadToJava.get(key);
            if (expectedType!=null && !expectedType.isInstance(v)) return Maybe.absent();
            return Maybe.of((T)v);
        }
        protected boolean hasYamlKeysOnBlackboard() {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            if (ykb==null || ykb.yamlKeysToReadToJava==null || ykb.yamlKeysToReadToJava.isEmpty()) return false;
            return true;
        }
        protected void removeFromYamlKeysOnBlackboard(String ...keys) {
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            for (String key: keys) {
                ykb.yamlKeysToReadToJava.remove(key);
            }
        }
        /** looks for all keys in {@link YamlKeysOnBlackboard} which can be mangled/ignore-case 
         * to match the given key */
        protected Set<String> findAllKeyManglesYamlKeys(String targetKey) {
            Set<String> result = MutableSet.of();
            YamlKeysOnBlackboard ykb = YamlKeysOnBlackboard.peek(blackboard);
            for (Object k: ykb.yamlKeysToReadToJava.keySet()) {
                if (k instanceof String && YomlUtils.mangleable(targetKey, (String)k)) {
                    result.add((String)k);
                }
            }
            return result;
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

        public abstract void read();
        public abstract void write();
    }
    
    @Override
    public void read(YomlContextForRead context, YomlConverter converter, Map<Object,Object> blackboard) {
        YomlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initRead(context, converter, blackboard);
        worker.read();
    }

    @Override
    public void write(YomlContextForWrite context, YomlConverter converter, Map<Object,Object> blackboard) {
        YomlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initWrite(context, converter, blackboard);
        worker.write();
    }

    @Override
    public String document(String type, YomlConverter converter) {
        return null;
    }
}
