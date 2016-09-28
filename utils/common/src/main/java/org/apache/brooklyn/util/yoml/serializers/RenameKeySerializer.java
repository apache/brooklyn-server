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

import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultKey;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultValue;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@YomlAllFieldsTopLevel
@Alias("rename-key")
public class RenameKeySerializer extends YomlSerializerComposition {

    private static final Logger log = LoggerFactory.getLogger(RenameKeySerializer.class);
    
    RenameKeySerializer() { }

    public RenameKeySerializer(YomlRenameKey ann) { 
        this(ann.oldKeyName(), ann.newKeyName(), YomlUtils.extractDefaultMap(ann.defaults()));
    }

    public RenameKeySerializer(String oldKeyName, String newKeyName, Map<String,? extends Object> defaults) {
        super();
        this.oldKeyName = oldKeyName;
        this.newKeyName = newKeyName;
        this.defaults = defaults;
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    @Alias("from")
    String oldKeyName;
    @Alias("to")
    String newKeyName;
    Map<String,? extends Object> defaults;

    public class Worker extends YomlSerializerWorker {
        public void read() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            // runs before type instantiated
            if (hasJavaObject()) return;
            if (!isYamlMap()) return;

            YamlKeysOnBlackboard ym = getYamlKeysOnBlackboardInitializedFromYamlMap();
            
            if (!ym.hasKeysLeft(oldKeyName)) return;
            if (ym.hadKeysEver(newKeyName)) return;
            
            ym.putNewKey(newKeyName, ym.removeKey(oldKeyName).get());
            ym.addDefaults(defaults);
            
            if (log.isTraceEnabled()) {
                log.trace(this+" read, keys left now: "+ym);
            }
            
            context.phaseRestart();
        }

        public void write() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            if (!isYamlMap()) return;

            // reverse order
            if (!getOutputYamlMap().containsKey(newKeyName)) return;
            if (getOutputYamlMap().containsKey(oldKeyName)) return;
            
            getOutputYamlMap().put(oldKeyName, getOutputYamlMap().remove(newKeyName));
            YomlUtils.removeDefaults(defaults, getOutputYamlMap());

            if (log.isTraceEnabled()) {
                log.trace(this+" write, output now: "+getOutputYamlMap());
            }

            context.phaseRestart();
        }
    }

    @Override
    public String toString() {
        return JavaClassNames.simpleClassName(getClass())+"["+oldKeyName+"->"+newKeyName+"]";
    }
    
    @YomlAllFieldsTopLevel
    @Alias("rename-default-key")
    public static class RenameDefaultKey extends RenameKeySerializer {
        public RenameDefaultKey(YomlRenameDefaultKey ann) { 
            this(ann.value(), YomlUtils.extractDefaultMap(ann.defaults()));
        }

        public RenameDefaultKey(String newKeyName, Map<String,? extends Object> defaults) {
            super(".key", newKeyName, defaults);
        }        
    }
    
    @YomlAllFieldsTopLevel
    @Alias("rename-default-value")
    public static class RenameDefaultValue extends RenameKeySerializer {
        public RenameDefaultValue(YomlRenameDefaultValue ann) { 
            this(ann.value(), YomlUtils.extractDefaultMap(ann.defaults()));
        }

        public RenameDefaultValue(String newKeyName, Map<String,? extends Object> defaults) {
            super(".value", newKeyName, defaults);
        }        
    }
    
}
