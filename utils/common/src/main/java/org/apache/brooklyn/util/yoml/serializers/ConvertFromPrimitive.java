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

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlFromPrimitive;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

@YomlAllFieldsTopLevel
@Alias("convert-from-primitive")
public class ConvertFromPrimitive extends YomlSerializerComposition {

    public final static String DEFAULT_DEFAULT_KEY = ConvertSingletonMap.DEFAULT_KEY_FOR_VALUE;
    
    public ConvertFromPrimitive() { }

    public ConvertFromPrimitive(YomlFromPrimitive ann) { 
        this(ann.keyToInsert(), YomlUtils.extractDefaultMap(ann.defaults()));
    }

    public ConvertFromPrimitive(String keyToInsert, Map<String,? extends Object> defaults) {
        super();
        this.keyToInsert = keyToInsert;
        this.defaults = defaults;
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    @Alias("key")
    String keyToInsert = DEFAULT_DEFAULT_KEY;
    Map<String,? extends Object> defaults;
        
    public class Worker extends YomlSerializerWorker {
        public void read() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            // runs before type instantiated
            if (hasJavaObject()) return;
            // only runs on primitives/lists
            if (!isJsonPrimitiveObject(getYamlObject()) && !isJsonList(getYamlObject())) return;

            Map<String,Object> newYamlMap = MutableMap.of(keyToInsert, getYamlObject());
            
            YomlUtils.addDefaults(defaults, newYamlMap);
            
            context.setYamlObject(newYamlMap);
            context.phaseRestart();
        }

        public void write() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            if (!isYamlMap()) return;
            // don't run if we're only added after instantiating the type (because then we couldn't read back!)
            if (SerializersOnBlackboard.isAddedByTypeInstantiation(blackboard, ConvertFromPrimitive.this)) return;

            Object value = getOutputYamlMap().get(keyToInsert);
            if (value==null) return;
            if (!isJsonPrimitiveObject(value) && !isJsonList(value)) return;
            
            Map<Object, Object> yamlMap = MutableMap.copyOf(getOutputYamlMap());
            yamlMap.remove(keyToInsert);
            YomlUtils.removeDefaults(defaults, yamlMap);
            if (!yamlMap.isEmpty()) return;
            
            context.setYamlObject(value);
            context.phaseRestart();
        }
    }
    
}
