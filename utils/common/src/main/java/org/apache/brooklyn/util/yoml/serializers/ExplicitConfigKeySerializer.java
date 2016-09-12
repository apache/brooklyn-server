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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExplicitConfigKeySerializer extends ExplicitField {

    private static final Logger log = LoggerFactory.getLogger(ExplicitConfigKeySerializer.class);
    
    String keyNameForConfigWhenSerialized = null;
    
    public ExplicitConfigKeySerializer(String keyNameForConfigWhenSerialized, ConfigKey<?> configKey, Field optionalFieldForAnnotations) {
        super(configKey.getName(), optionalFieldForAnnotations);
        this.keyNameForConfigWhenSerialized = keyNameForConfigWhenSerialized;
        this.configKey = configKey;
        if (configKey.hasDefaultValue()) {
            defaultValue = configKey.getDefaultValue();
        }
        
        // TODO yoml config key:
        // - constraints
        // - description
    }
    
    /** The {@link ConfigKey#getName()} this serializer acts on */ 
    protected final ConfigKey<?> configKey;
    
    @Override
    protected String getKeyNameForMapOfGeneralValues() {
        return keyNameForConfigWhenSerialized;
    }

    public static Set<ConfigKey<?>> findConfigKeys(Class<?> clazz) {
        MutableMap<String, ConfigKey<?>> result = MutableMap.of();
        
        for (Field f: YomlUtils.getAllNonTransientStaticFields(clazz).values()) {
            try {
                f.setAccessible(true);
                Object ckO = f.get(null);
                
                ConfigKey<?> ck = null;
                if (ckO instanceof ConfigKey) ck = (ConfigKey<?>)ckO;
                else if (ckO instanceof HasConfigKey) ck = ((HasConfigKey<?>)ckO).getConfigKey();
                
                if (ck==null) continue;
                if (result.containsKey(ck.getName())) continue;
                
                result.put(ck.getName(), ck);
                
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.warn("Unable to access static config key "+f+" (ignoring): "+e, e);
            }
        }
        
        return MutableSet.copyOf(result.values());
    }
    
    /** only useful in conjuction with {@link InstantiateTypeFromRegistryUsingConfigMap} static serializer factory methods */
    public static Map<String,YomlSerializer> findExplicitConfigKeySerializers(String keyNameForConfigWhenSerialized, Class<?> clazz) {
        MutableMap<String, YomlSerializer> result = MutableMap.of();
        
        for (Field f: YomlUtils.getAllNonTransientStaticFields(clazz).values()) {
            try {
                f.setAccessible(true);
                Object ckO = f.get(null);
                
                ConfigKey<?> ck = null;
                if (ckO instanceof ConfigKey) ck = (ConfigKey<?>)ckO;
                else if (ckO instanceof HasConfigKey) ck = ((HasConfigKey<?>)ckO).getConfigKey();
                
                if (ck==null) continue;
                if (result.containsKey(ck.getName())) continue;
                
                result.put(ck.getName(), new ExplicitConfigKeySerializer(keyNameForConfigWhenSerialized, ck, f));
                
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.warn("Unable to access static config key "+f+" (ignoring): "+e, e);
            }
            
        }
        
        return result;
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }
    
    public class Worker extends ExplicitField.Worker {
        protected boolean canDoRead() { 
            return !hasJavaObject() && context.willDoPhase(InstantiateTypeFromRegistryUsingConfigMap.PHASE_INSTANTIATE_TYPE_DEFERRED);
        }
    }

    
}
