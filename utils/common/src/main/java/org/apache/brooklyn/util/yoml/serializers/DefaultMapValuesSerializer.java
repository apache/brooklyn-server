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

import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;
import org.apache.brooklyn.util.yoml.annotations.YomlDefaultMapValues;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

@YomlAllFieldsTopLevel
@YomlConfigMapConstructor("defaults")
@Alias("default-map-values")
public class DefaultMapValuesSerializer extends YomlSerializerComposition {

    DefaultMapValuesSerializer() { }

    public DefaultMapValuesSerializer(YomlDefaultMapValues ann) { 
        this(YomlUtils.extractDefaultMap(ann.value()));
    }

    public DefaultMapValuesSerializer(Map<String,? extends Object> defaults) {
        super();
        this.defaults = defaults;
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    Map<String,? extends Object> defaults;
        
    public class Worker extends YomlSerializerWorker {
        public void read() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            // runs before type instantiated
            if (hasJavaObject()) return;
            if (!isYamlMap()) return;

            if (getYamlKeysOnBlackboardInitializedFromYamlMap().addDefaults(defaults)==0) return;
            
            context.phaseRestart();
        }

        public void write() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            if (!isYamlMap()) return;

            if (YomlUtils.removeDefaults(defaults, getOutputYamlMap())==0) return;
            
            context.phaseRestart();
        }
    }
    
}
