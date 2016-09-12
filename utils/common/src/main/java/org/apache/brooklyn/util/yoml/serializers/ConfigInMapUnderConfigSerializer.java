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

import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlContext;

public class ConfigInMapUnderConfigSerializer extends FieldsInMapUnderFields {

    String keyNameForConfigWhenSerialized;

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
            if (JavaFieldsOnBlackboard.peek(blackboard, "config")==null) return;
            
            super.read();
        }
        
        protected boolean shouldHaveJavaObject() { return false; }
        
        @Override
        protected boolean setKeyValueForJavaObjectOnRead(Object key, Object value) throws IllegalAccessException {
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard, "config");
            fib.fieldsFromReadToConstructJava.put(Strings.toString(key), value);
            return true;
        }
        
        protected Map<String, Object> writePrepareGeneralMap() {
            JavaFieldsOnBlackboard fib = JavaFieldsOnBlackboard.peek(blackboard);
            if (fib==null) return null;
            return fib.configToWriteFromJava;
        }

    }
    
}
