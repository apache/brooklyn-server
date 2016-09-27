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

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

import com.google.common.reflect.TypeToken;

public class ConfigInMapUnderConfigSerializer extends FieldsInMapUnderFields {

    final String keyNameForConfigWhenSerialized;

    public ConfigInMapUnderConfigSerializer(String keyNameForConfigWhenSerialized) {
        this.keyNameForConfigWhenSerialized = keyNameForConfigWhenSerialized;
    }
    
    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    @Override
    protected String getExpectedPhaseRead() {
        return YomlContext.StandardPhases.MANIPULATING;
    }

    @Override
    protected String getKeyNameForMapOfGeneralValues() {
        return keyNameForConfigWhenSerialized;
    }

    public class Worker extends FieldsInMapUnderFields.Worker {

        @Override
        public void read() {
            if (!context.willDoPhase(
                    InstantiateTypeFromRegistryUsingConfigMap.PHASE_INSTANTIATE_TYPE_DEFERRED)) return;
            if (JavaFieldsOnBlackboard.peek(blackboard, getKeyNameForMapOfGeneralValues())==null) return;
            
            super.read();
        }
        
        protected boolean shouldHaveJavaObject() { return false; }
        
        @Override
        protected boolean setKeyValueForJavaObjectOnRead(String key, Object value, String optionalTypeConstraint) throws IllegalAccessException {
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard, getKeyNameForMapOfGeneralValues());
            String optionalType = getType(key, null);
            if (optionalTypeConstraint!=null) {
                throw new YomlException("Types from other types not supported for config keys");
                // TODO see behaviour in super to override
            }
            Object v2;
            try {
                v2 = converter.read( new YomlContextForRead(value, context.getJsonPath()+"/"+key, optionalType, context) );
            } catch (Exception e) {
                // for config we try with the optional type, but don't insist
                Exceptions.propagateIfFatal(e);
                if (optionalType!=null) optionalType = null;
                try {
                    v2 = converter.read( new YomlContextForRead(value, context.getJsonPath()+"/"+key, optionalType, context) );
                } catch (Exception e2) {
                    Exceptions.propagateIfFatal(e2);
                    throw e;
                }
            }
            fib.fieldsFromReadToConstructJava.put(key, v2);
            return true;
        }
        
        protected Map<String, Object> writePrepareGeneralMap() {
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null || fib.configToWriteFromJava==null) return null;
            Map<String,Object> configMap = MutableMap.of();
            
            for (Map.Entry<String,Object> entry: fib.configToWriteFromJava.entrySet()) {
                // NB: won't normally have a type, the explicit config keys will take those
                String optionalType = getType(entry.getKey(), entry.getValue());
                Object v = converter.write(new YomlContextForWrite(entry.getValue(), context.getJsonPath()+"/"+entry.getKey(), optionalType, context) );
                configMap.put(entry.getKey(), v);
            }
            for (String key: configMap.keySet()) fib.configToWriteFromJava.remove(key);

            return configMap;
        }

        protected String getType(String key, Object value) {
            TopLevelFieldsBlackboard efb = TopLevelFieldsBlackboard.get(blackboard, getKeyNameForMapOfGeneralValues());
            ConfigKey<?> typeKey = efb.getConfigKey(key);
            TypeToken<?> type = typeKey==null ? null : typeKey.getTypeToken();
            String optionalType = null;
            if (type!=null && (value==null || type.getRawType().isInstance(value))) 
                optionalType = YomlUtils.getTypeNameWithGenerics(type, config.getTypeRegistry());
            return optionalType;
        }

    }
    
}
