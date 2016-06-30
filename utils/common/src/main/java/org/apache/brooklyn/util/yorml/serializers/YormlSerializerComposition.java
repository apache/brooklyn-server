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

import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.apache.brooklyn.util.yorml.internal.YormlConfig;
import org.apache.brooklyn.util.yorml.internal.YormlConverter;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;

public abstract class YormlSerializerComposition implements YormlSerializer {

    protected abstract YormlSerializerWorker newWorker();
    
    public abstract static class YormlSerializerWorker {

        protected YormlConverter converter;
        protected YormlContext context;
        protected YormlContextForRead readContext;
        protected YormlConfig config;
        protected Map<Object, Object> blackboard;

        private void initRead(YormlContextForRead context, YormlConverter converter, Map<Object, Object> blackboard) {
            if (this.context!=null) throw new IllegalStateException("Already initialized, for "+context);
            this.context = context;
            this.readContext = context;
            this.converter = converter;
            this.config = converter.getConfig();
            this.blackboard = blackboard;
        }
        
        private void initWrite(YormlContextForWrite context, YormlConverter converter, Map<Object,Object> blackboard) {
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
            Class<?> ett = config.getTypeRegistry().getJavaType(et);
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
                if (k instanceof String && YormlUtils.mangleable(targetKey, (String)k)) {
                    result.add((String)k);
                }
            }
            return result;
        }
        
        protected boolean isJsonPrimitiveObject(Object o) {
            if (o==null) return true;
            if (o instanceof String) return true;
            if (Boxing.isPrimitiveOrBoxedObject(o)) return true;
            return false;
        }

        public abstract void read();
        public abstract void write();
    }
    
    @Override
    public void read(YormlContextForRead context, YormlConverter converter, Map<Object,Object> blackboard) {
        YormlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initRead(context, converter, blackboard);
        worker.read();
    }

    @Override
    public void write(YormlContextForWrite context, YormlConverter converter, Map<Object,Object> blackboard) {
        YormlSerializerWorker worker;
        try {
            worker = newWorker();
        } catch (Exception e) { throw Exceptions.propagate(e); }
        worker.initWrite(context, converter, blackboard);
        worker.write();
    }

    @Override
    public String document(String type, YormlConverter converter) {
        return null;
    }
}
