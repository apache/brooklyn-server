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
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlSingletonMap;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

/*
 * key-for-key: type
 * key-for-primitive-value: type || key-for-any-value: ... || key-for-list-value: || key-for-map-value
 *    || merge-with-map-value
 * defaults: { type: top-level-field }
 */
@YomlAllFieldsTopLevel
@Alias("convert-singleton-map")
public class ConvertSingletonMap extends YomlSerializerComposition {

    public ConvertSingletonMap() { }

    public ConvertSingletonMap(YomlSingletonMap ann) { 
        this(ann.keyForKey(), ann.keyForAnyValue(), ann.keyForPrimitiveValue(), ann.keyForListValue(), ann.keyForMapValue(),
            null, YomlUtils.extractDefaultMap(ann.defaults()));
    }

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

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    public final static String DEFAULT_KEY_FOR_KEY = ".key";
    public final static String DEFAULT_KEY_FOR_VALUE = ".value";
    
    String keyForKey = DEFAULT_KEY_FOR_KEY;
    String keyForAnyValue = DEFAULT_KEY_FOR_VALUE;
    String keyForPrimitiveValue;
    String keyForListValue;
    /** if the value against the single key is itself a map, treat it as a value for a key wit this name */
    String keyForMapValue;
    /** if the value against the single key is itself a map, should we put the {@link #keyForKey} as another entry in the map 
     * to get the result; the default (if null) and usual behaviour is to do so but not if {@link #keyForMapValue} is set (because we'd use that), and not if there would be a collision
     * (ie {@link #keyForKey} is already present) and we have a value for {@link #keyForAnyValue} (so we can use that);
     * however we can set true/false to say always merge (which will ignore {@link #keyForMapValue}) or never merge
     * (which will prevent this serializer from applying unless {@link #keyForMapValue} or {@link #keyForAnyValue} is set) */
    Boolean mergeWithMapValue;
    Map<String,? extends Object> defaults;
    
    public static class ConvertSingletonApplied {}
    
    public class Worker extends YomlSerializerWorker {
        public void read() {
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
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
            
            if (isJsonPrimitiveObject(value) && Strings.isNonBlank(keyForPrimitiveValue)) {
                newYamlMap.put(keyForPrimitiveValue, value);
            } else if (value instanceof Map) {
                boolean merge = isForMerging(value);
                if (merge) {
                    newYamlMap.putAll((Map<?,?>)value);
                } else {
                    String keyForThisMap = Strings.isNonBlank(keyForMapValue) ? keyForMapValue : keyForAnyValue;
                    if (Strings.isBlank(keyForThisMap)) {
                        // we can't apply
                        return;
                    }
                    newYamlMap.put(keyForThisMap, value);
                }
            } else if (value instanceof Iterable && Strings.isNonBlank(keyForListValue)) {
                newYamlMap.put(keyForListValue, value);
            } else {
                newYamlMap.put(keyForAnyValue, value);
            }
            
            YomlUtils.addDefaults(defaults, newYamlMap);
            
            context.setYamlObject(newYamlMap);
            context.phaseRestart();
        }

        protected boolean isForMerging(Object value) {
            boolean merge;
            if (mergeWithMapValue==null) {
                // default merge logic (if null):
                // * merge if there is no key-for-map-value AND 
                // * either
                //   * it's safe, ie there is no collision at the key-for-key key, OR
                //   * we have to, ie there is no key-for-any-value (default is overridden)
                merge = Strings.isBlank(keyForMapValue) && 
                    ( (!((Map<?,?>)value).containsKey(keyForKey)) || (Strings.isBlank(keyForAnyValue)) );
            } else {
                merge = mergeWithMapValue;
            }
            return merge;
        }

        String OUR_PHASE = "manipulate-convert-singleton";
        
        public void write() {
            if (!isYamlMap()) return;
            // don't run if we're only added after instantiating the type (because then we couldn't read back!)
            if (SerializersOnBlackboard.isAddedByTypeInstantiation(blackboard, ConvertSingletonMap.this)) return;
            
            if (context.isPhase(YomlContext.StandardPhases.MANIPULATING) && !context.seenPhase(OUR_PHASE)) {
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
            
            YomlUtils.removeDefaults(defaults, yamlMap);
            
            Object newValue = null;
            
            if (yamlMap.size()==1) {
                // if after removing the keyForKey and defaults there is just one entry, see if we can abbreviate further
                // eg using keyForPrimitiveValue
                // generally we can do this if the remaining key equals the explicit key for that category,
                // or if there is no key for that category but the any-value key is set and matches the remaining key
                // and merging is not disallowed 
                Object remainingKey = yamlMap.keySet().iterator().next();
                if (remainingKey!=null) {
                    Object remainingObject = yamlMap.values().iterator().next();

                    if (remainingObject instanceof Map) {
                        // cannot promote if merge is true
                        if (!Boolean.TRUE.equals(mergeWithMapValue)) {
                            if (remainingKey.equals(keyForMapValue)) {
                                newValue = remainingObject;
                            } else if (Strings.isBlank(keyForMapValue) && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                                newValue = remainingObject;
                            }
                        }
                    } else if (remainingObject instanceof Iterable) {
                        if (remainingKey.equals(keyForListValue)) {
                            newValue = remainingObject;
                        } else if (Strings.isBlank(keyForListValue) && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                            newValue = remainingObject;
                        }
                    } else if (isJsonPrimitiveObject(remainingObject)) {
                        if (remainingKey.equals(keyForPrimitiveValue)) {
                            newValue = remainingObject;
                        } else if (Strings.isBlank(keyForPrimitiveValue) && remainingKey.equals(keyForAnyValue) && Boolean.FALSE.equals(mergeWithMapValue)) {
                            newValue = remainingObject;
                        }                        
                    }
                }
            }
            
            // if we couldn't simplify above, we might still be able to proceed, if:
            // * merging is forced; OR
            // * merging isn't disallowed, and
            // * there is no keyForMapValue, and
            // * either there is no keyForAnyValue or it wouldn't cause a collision
            // (if keyFor{Map,Any}Value is in effect it will steal what we want to merge)
            if (newValue==null && isForMerging(yamlMap)) {
                newValue = yamlMap;
            }
            if (newValue==null) return; // this serializer was cancelled, it doesn't apply
            
            context.setYamlObject(MutableMap.of(newKey, newValue));
            context.phaseRestart();
        }
    }
    
}
