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

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlSingletonMap;
import org.apache.brooklyn.util.yoml.internal.SerializersOnBlackboard;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.internal.YomlUtils.GenericsParse;

import com.google.common.collect.Iterables;

/*
 * key-for-key: type
 * key-for-primitive-value: type || key-for-any-value: ... || key-for-list-value: || key-for-map-value
 *    || merge-with-map-value
 * defaults: { type: top-level-field }
 */
@YomlAllFieldsTopLevel
@Alias("convert-singleton-map")
public class ConvertSingletonMap extends YomlSerializerComposition {

    public static enum SingletonMapMode { LIST_AS_MAP, LIST_AS_LIST, NON_LIST }
    
    public ConvertSingletonMap() { }

    public ConvertSingletonMap(YomlSingletonMap ann) { 
        this(ann.keyForKey(), ann.keyForAnyValue(), ann.keyForPrimitiveValue(), ann.keyForListValue(), ann.keyForMapValue(),
            null, null, YomlUtils.extractDefaultMap(ann.defaults()));
    }

    public ConvertSingletonMap(String keyForKey, String keyForAnyValue, String keyForPrimitiveValue, String keyForListValue, String keyForMapValue, 
        Collection<SingletonMapMode> modes, Boolean mergeWithMapValue, Map<String, ? extends Object> defaults) {
        super();
        this.keyForKey = keyForKey;
        this.keyForAnyValue = keyForAnyValue;
        this.keyForPrimitiveValue = keyForPrimitiveValue;
        this.keyForListValue = keyForListValue;
        this.keyForMapValue = keyForMapValue;
        this.onlyInModes = MutableSet.copyOf(modes);
        this.mergeWithMapValue = mergeWithMapValue;
        this.defaults = defaults;
    }

    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    public final static String DEFAULT_KEY_FOR_KEY = ".key";
    public final static String DEFAULT_KEY_FOR_VALUE = ".value";
    
    String keyForKey = DEFAULT_KEY_FOR_KEY;
    /** key to use if type-specific key is not known;
     * only applies to map if {@link #mergeWithMapValue} is set */
    String keyForAnyValue = DEFAULT_KEY_FOR_VALUE;
    String keyForPrimitiveValue;
    String keyForListValue;
    /** if the value against the single key is itself a map, treat it as a value for a key wit this name */
    String keyForMapValue;
    /** conveniences for {@link #onlyInModes} when just supplying one */
    SingletonMapMode onlyInMode = null;
    /** if non-empty, restrict the modes where this serializer can run */
    Set<SingletonMapMode> onlyInModes = null;
    /** if the value against the single key is itself a map, should we put the {@link #keyForKey} as another entry in the map 
     * to get the result; the default (if null) and usual behaviour is to do so but not if {@link #keyForMapValue} is set (because we'd use that), and not if there would be a collision
     * (ie {@link #keyForKey} is already present) and we have a value for {@link #keyForAnyValue} (so we can use that);
     * however we can set true/false to say always merge (which will ignore {@link #keyForMapValue}) or never merge
     * (which will prevent this serializer from applying unless {@link #keyForMapValue} or {@link #keyForAnyValue} is set) */
    Boolean mergeWithMapValue;
    Map<String,? extends Object> defaults;

    public class Worker extends YomlSerializerWorker {
        public void read() {
            // runs before type instantiated
            if (hasJavaObject()) return;
            
            if (context.isPhase(InstantiateTypeList.MANIPULATING_TO_LIST)) {
                if (isYamlMap() && enterModeRead(SingletonMapMode.LIST_AS_MAP)) {
                    readManipulatingMapToList();
                } else if (getYamlObject() instanceof Collection && enterModeRead(SingletonMapMode.LIST_AS_LIST)) {
                    // this would also be done by instantiate-type-list, but it wouldn't know to
                    // pass this serializer through
                    readManipulatingInList();
                }
                
                return;
            }
            
            if (!context.isPhase(YomlContext.StandardPhases.MANIPULATING)) return;
            
            if (!isYamlMap()) return;
            if (getYamlMap().size()!=1) return;
            
            if (!enterModeRead(SingletonMapMode.NON_LIST)) return;
            
            Object key = Iterables.getOnlyElement(getYamlMap().keySet());
            Object value = Iterables.getOnlyElement(getYamlMap().values());
            
            // key should always be primitive
            if (!isJsonPrimitiveObject(key)) return;
            
            Map<Object,Object> newYamlMap = MutableMap.of();
            
            newYamlMap.put(keyForKey, key);
            
            if (isJsonPrimitiveObject(value) && Strings.isNonBlank(keyForPrimitiveValue)) {
                newYamlMap.put(keyForPrimitiveValue, value);
            } else if (value instanceof Map) {
                Boolean merge = isForMerging(value);
                if (merge==null) return;
                if (merge) {
                    newYamlMap.putAll((Map<?,?>)value);
                } else {
                    String keyForThisMap = Strings.isNonBlank(keyForMapValue) ? keyForMapValue : keyForAnyValue;
                    if (Strings.isBlank(keyForThisMap)) {
                        throw new IllegalStateException("Error in isForMergingLogic");
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
            // TODO should the above apply to YamlKeysOnBlackboard?  Or clear it?  Or does this happen early enough that isn't an issue?
            context.phaseRestart();
        }

        protected boolean enterModeRead(SingletonMapMode newMode) {
            SingletonMapMode currentMode = (SingletonMapMode) blackboard.get(ConvertSingletonMap.this);
            if (currentMode==null) {
                if (!allowInMode(newMode)) return false;
            } else if (currentMode == SingletonMapMode.NON_LIST) {
                // cannot transition from non-list mode others
                return false;
            } else {
                // current mode is one of the list modes;
                // only allowed to transition to non-list (in recursive call)
                if (newMode == SingletonMapMode.NON_LIST) /* fine */;
                else if (currentMode == SingletonMapMode.LIST_AS_MAP && newMode == SingletonMapMode.LIST_AS_LIST) ;
                else {
                    return false;
                } 
            }
            // set the new mode
            blackboard.put(ConvertSingletonMap.this, newMode);
            return true;
        }

        protected void readManipulatingInList() {
            // go through a list, applying to each
            List<Object> result = readManipulatingInList((Collection<?>)getYamlObject(), SingletonMapMode.LIST_AS_LIST);
            if (result==null) return;
            
            context.setYamlObject(result);
            context.phaseAdvance();
        }

        protected List<Object> readManipulatingInList(Collection<?> list, SingletonMapMode mode) {
            // go through a list, applying to each
            GenericsParse gp = new GenericsParse(context.getExpectedType());
            if (gp.warning!=null) {
                warn(gp.warning);
                return null;
            }
            String genericSubType = null;
            if (gp.isGeneric()) {
                if (gp.subTypeCount()!=1) {
                    // not a list
                    return null;
                }
                genericSubType = Iterables.getOnlyElement(gp.subTypes);
            }

            List<Object> result = MutableList.of();
            int index = 0;
            for (Object item: list) {
                YomlContextForRead newContext = new YomlContextForRead(item, context.getJsonPath()+"["+index+"]", genericSubType, context);
                // add this serializer and set mode in the new context
                SerializersOnBlackboard.create(newContext.getBlackboard()).addExpectedTypeSerializers(MutableList.of((YomlSerializer) ConvertSingletonMap.this));
                newContext.getBlackboard().put(ConvertSingletonMap.this, mode);
                
                Object newItem = converter.read(newContext);
                result.add( newItem );
                index++;
            }
            return result;
        }

        protected void readManipulatingMapToList() {
            // convert from a map to a list; then manipulate in list
            List<Object> result = MutableList.of();
            for (Map.Entry<Object,Object> entry: getYamlMap().entrySet()) {
                result.add(MutableMap.of(entry.getKey(), entry.getValue()));
            }
            result = readManipulatingInList(result, SingletonMapMode.LIST_AS_MAP);
            if (result==null) return;
            
            context.setYamlObject(result);
            YamlKeysOnBlackboard.getOrCreate(blackboard, null).yamlKeysToReadToJava.clear();
            context.phaseAdvance();
        }

        /** return true/false whether to merge, or null if need to bail out */
        protected Boolean isForMerging(Object value) {
            if (mergeWithMapValue==null) {
                // default merge logic (if null):
                // * merge if there is no key-for-map-value AND 
                // * it's safe, ie there is no collision at the key-for-key key
                // if not safe, we bail out (collisions may be used by clients to suppress)
                if (Strings.isNonBlank(keyForMapValue)) return false;
                if (((Map<?,?>)value).containsKey(keyForKey)) return null;
                return true;
            } else {
                return mergeWithMapValue;
            }
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
            if (blackboard.put(ConvertSingletonMap.this, SingletonMapMode.NON_LIST)!=null) return;
            
            if (!allowInMode(SingletonMapMode.NON_LIST)) {
                if (!allowInMode(SingletonMapMode.LIST_AS_LIST)) {
                    // we can only write in one of the above two modes currently
                    // (list-as-map is difficult to reverse-engineer, and not necessary)
                    return;
                }
                YomlContext parent = context.getParent();
                if (parent==null || !(parent.getJavaObject() instanceof Collection)) {
                    // parent is not a list; disallow
                    return;
                }
            }

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
            // * it wouldn't cause a collision
            // (if keyForMapValue is in effect, it will steal what we want to merge;
            // but keyForAnyValue will only be used if merge is set true)
            if (newValue==null && Boolean.TRUE.equals(isForMerging(yamlMap))) {
                newValue = yamlMap;
            }
            if (newValue==null) return; // this serializer was cancelled, it doesn't apply
            
            context.setYamlObject(MutableMap.of(newKey, newValue));
            context.phaseRestart();
        }
    }

    public boolean allowInMode(SingletonMapMode currentMode) {
        return getAllowedModes().contains(currentMode);
    }

    protected Collection<SingletonMapMode> getAllowedModes() {
        MutableSet<SingletonMapMode> modes = MutableSet.of();
        modes.addIfNotNull(onlyInMode);
        modes.putAll(onlyInModes);
        if (modes.isEmpty()) return Arrays.asList(SingletonMapMode.values());
        return modes;
    }
    
}
