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

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;

/*
 * key-for-key: type
 * key-for-primitive-value: type || key-for-any-value: ... || key-for-list-value: || key-for-map-value
 *    || merge-with-map-value
 * defaults: { type: explicit-field }
 */
public class ConvertSingletonMap extends YormlSerializerComposition {

    public ConvertSingletonMap() { }

    public ConvertSingletonMap(String keyForKey, String keyForAnyValue, String keyForPrimitiveValue, String keyForListValue,
        String keyForMapValue, Boolean mergeWithMapValue, Map<String, ? extends Object> defaults) {
        super();
        this.keyForKey = keyForKey;
        this.keyForAnyValue = keyForAnyValue;
        this.keyForPrimitiveValue = keyForPrimitiveValue;
        this.keyForListValue = keyForListValue;
        this.keyForMapValue = keyForMapValue;
        this.mergeWithMapValue = mergeWithMapValue;
        this.defaults = defaults;
    }

    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }

    public final static String DEFAULT_KEY_FOR_KEY = ".key";
    public final static String DEFAULT_KEY_FOR_VALUE = ".value";
    
    String keyForKey = DEFAULT_KEY_FOR_KEY;
    String keyForAnyValue = DEFAULT_KEY_FOR_VALUE;
    String keyForPrimitiveValue;
    String keyForListValue;
    String keyForMapValue;
    Boolean mergeWithMapValue;
    Map<String,? extends Object> defaults;
    
    public static class ConvertSingletonApplied {}
    
    public class Worker extends YormlSerializerWorker {
        public void read() {
            if (!context.isPhase(YormlContext.StandardPhases.MANIPULATING)) return;
            // runs before type instantiated
            if (hasJavaObject()) return;
            
            if (!isYamlMap()) return;
            if (getYamlMap().size()!=1) return;
            // don't run multiple times
            if (blackboard.put(ConvertSingletonMap.class.getName(), new ConvertSingletonApplied())!=null) return;
            
            // it *is* a singleton map
            Object key = getYamlMap().keySet().iterator().next();
            Object value = getYamlMap().values().iterator().next();
            
            // key should always be primitive
            if (!isJsonPrimitiveObject(key)) return;
            
            Map<Object,Object> newYamlMap = MutableMap.of();
            
            newYamlMap.put(keyForKey, key);
            
            if (isJsonPrimitiveObject(value) && keyForPrimitiveValue!=null) {
                newYamlMap.put(keyForPrimitiveValue, value);
            } else if (value instanceof Map) {
                boolean merge;
                if (mergeWithMapValue==null) merge = ((Map<?,?>)value).containsKey(keyForKey) || keyForMapValue==null;
                else merge = mergeWithMapValue;
                if (merge) {
                    newYamlMap.putAll((Map<?,?>)value);
                } else {
                    newYamlMap.put(keyForMapValue != null ? keyForMapValue : keyForAnyValue, value);
                }
            } else if (value instanceof Iterable && keyForListValue!=null) {
                newYamlMap.put(keyForListValue, value);
            } else {
                newYamlMap.put(keyForAnyValue, value);
            }
            
            YormlUtils.addDefaults(defaults, newYamlMap);
            
            context.setYamlObject(newYamlMap);
            context.phaseRestart();
        }

        String OUR_PHASE = "manipulate-convert-singleton";
        
        public void write() {
            if (!isYamlMap()) return;
            // don't run if we're only added after instantiating the type (because then we couldn't read back!)
            if (SerializersOnBlackboard.isAddedByTypeInstantiation(blackboard, ConvertSingletonMap.this)) return;
            
            if (context.isPhase(YormlContext.StandardPhases.MANIPULATING) && !context.seenPhase(OUR_PHASE)) {
                // finish manipulations before seeking to apply this
                context.phaseInsert(OUR_PHASE);
                return;
            }
            if (!context.isPhase(OUR_PHASE)) return;
            
            // don't run multiple times
            if (blackboard.put(ConvertSingletonMap.class.getName(), new ConvertSingletonApplied())!=null) return;

            if (!getYamlMap().containsKey(keyForKey)) return;
            Object newKey = getYamlMap().get(keyForKey);
            if (!isJsonPrimitiveObject(newKey)) {
                // NB this is potentially irreversible - 
                // e.g. if given say we want for { color: red, xxx: yyy } to write { red: { xxx: yyy } }
                // but if we have { color: { type: string, value: red } } and don't rewrite
                // user will end up with   { color: color, type: string, value: red }
                // ... so keyForKey should probably be reserved for things which are definitely primitives
                return;
            }

            Map<Object, Object> yamlMap = MutableMap.copyOf(getYamlMap());
            yamlMap.remove(keyForKey);
            
            YormlUtils.removeDefaults(defaults, yamlMap);
            
            Object newValue = null;
            
            if (yamlMap.size()==1) {
                Object remainingKey = yamlMap.keySet().iterator().next();
                if (remainingKey!=null) {
                    Object remainingObject = yamlMap.values().iterator().next();

                    if (remainingObject instanceof Map) {
                        // NB can only merge to map if merge false or merge null and map key specified
                        if (!Boolean.TRUE.equals(mergeWithMapValue)) {
                            if (remainingKey.equals(keyForMapValue)) {
                                newValue = remainingObject;
                            } else if (keyForMapValue==null && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                                newValue = remainingObject;
                            }
                        }
                    } else if (remainingObject instanceof Iterable) {
                        if (remainingKey.equals(keyForListValue)) {
                            newValue = remainingObject;
                        } else if (keyForListValue==null && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                            newValue = remainingObject;
                        }
                    } else if (isJsonPrimitiveObject(remainingObject)) {
                        if (remainingKey.equals(keyForPrimitiveValue)) {
                            newValue = remainingObject;
                        } else if (keyForPrimitiveValue==null && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                            newValue = remainingObject;
                        }                        
                    }
                }
            }
            
            if (newValue==null && !Boolean.FALSE.equals(mergeWithMapValue)) {
                if (keyForMapValue==null && keyForAnyValue==null) {
                    // if keyFor{Map,Any}Value was supplied it will steal what we want to merge,
                    // so only apply if those are both null
                    newValue = yamlMap;
                }
            }
            if (newValue==null) return; // doesn't apply
            
            context.setYamlObject(MutableMap.of(newKey, newValue));
            context.phaseRestart();
        }
    }
    
}
