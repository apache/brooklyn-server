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

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.internal.YomlUtils.GenericsParse;

import com.google.common.base.Objects;


public class InstantiateTypeMap extends YomlSerializerComposition {

    private static final String MAP = YomlUtils.TYPE_MAP;
    
    // these mapping class settings are slightly OTT but keeping for consistency with list and in case we have 'alias'
    
    @SuppressWarnings("rawtypes")
    Map<String,Class<? extends Map>> typeAliases = MutableMap.<String,Class<? extends Map>>of(
        MAP, MutableMap.class
    );
    
    @SuppressWarnings("rawtypes")
    Map<Class<? extends Map>, String> typesAliased = MutableMap.<Class<? extends Map>,String>of(
        MutableMap.class, MAP,
        LinkedHashMap.class, MAP
    );
    
    @SuppressWarnings("rawtypes")
    Set<Class<? extends Map>> typesAllowed = MutableSet.<Class<? extends Map>>of(
        // TODO does anything fit this category? we serialize as a json map, including the type, and use xxx.put(...) to read in
    );
    
    protected YomlSerializerWorker newWorker() {
        return new Worker();
    }

    public class Worker extends InstantiateTypeWorkerAbstract {
        
        String genericKeySubType = null, genericValueSubType = null;
        Class<?> expectedJavaType;
        String expectedBaseTypeName;
        
        public void read() {
            if (!canDoRead()) return;
            Object yo = getYamlObject();
            
            expectedJavaType = getExpectedTypeJava();
            if (context.getExpectedType()!=null && !parseExpectedTypeAndDetermineIfNoBadProblems(context.getExpectedType())) return;

            // if expected type is json then we just dump what we have / read what's there, end
            // if expected type in the allows list or compatible with "map" and j.u.Map, look at that value
            // else try read/write as  { type: mapxxx, value: valuexxx }
            // else end, it isn't for us
            // then for value
            //   if all keys are primitives then write as map { keyxxx: valuexxx }
            //   else write as list of kv pairs - [ { key: keyxxx , value: valuexxx } ]
            //   (in both cases using generics if available)

            if (isJsonMarkerTypeExpected()) {
                // json is pass-through
                context.setJavaObject( context.getYamlObject() );
                context.phaseAdvance();
                YamlKeysOnBlackboard.getOrCreate(blackboard, null).clear();
                return;
            }
            
            if (expectedJavaType!=null && !Map.class.isAssignableFrom(expectedJavaType)) return; // not a map expected
            
            if (isJsonPrimitiveObject(yo)) return;
            
            String actualBaseTypeName;
            Class<?> actualType;
            Object value;
            String alias = getAlias(expectedJavaType);
            if (alias!=null || typesAllowed.contains(expectedJavaType)) {
                // type would not have been written
                actualBaseTypeName = expectedBaseTypeName;
                value = getYamlObject();
                
            } else {
                // the type must have been made explicit
                if (!isYamlMap()) return;
                actualBaseTypeName = readingTypeFromFieldOrExpected();
                Maybe<Object> valueM = readingValueFromTypeValueMap();
                if (actualBaseTypeName==null && valueM.isAbsent()) return;
                
                Class<?> oldExpectedJavaType = expectedJavaType;  expectedJavaType = null;
                String oldExpectedBaseTypeName = expectedBaseTypeName; expectedBaseTypeName = null;
                parseExpectedTypeAndDetermineIfNoBadProblems(actualBaseTypeName);
                // above will overwrite baseType
                alias = getAlias(expectedJavaType);
                actualType = expectedJavaType;
                actualBaseTypeName = expectedBaseTypeName;
                expectedJavaType = oldExpectedJavaType;
                expectedBaseTypeName = oldExpectedBaseTypeName;
                if (actualType==null) actualType = config.getTypeRegistry().getJavaTypeMaybe(actualBaseTypeName, context).orNull();
                if (actualType==null) return; //we don't recognise the type
                if (!Map.class.isAssignableFrom(actualType)) return; //it's not a map
                
                value = valueM.get();
            }

            // create actualType, then populate from value
            Map<?,?> jo = null;
            if (typeAliases.get(alias)!=null) {
                jo = (Map<?,?>) Reflections.invokeConstructorFromArgsIncludingPrivate(typeAliases.get(alias)).get();
            } else {
                try {
                    jo = (Map<?, ?>) config.getTypeRegistry().newInstance(actualBaseTypeName, Yoml.newInstance(config));
                } catch (Exception e) {
                    throw new IllegalStateException("Cannot instantiate "+actualBaseTypeName, e);
                }
            }
            @SuppressWarnings("unchecked")
            Map<Object, Object> jom = (Map<Object,Object>)jo;
            
            if (value instanceof Iterable) {
                int i=0;
                for (Object o: (Iterable<?>) value) {
                    String newPath = context.getJsonPath()+"["+i+"]";
                    if (o instanceof Map) {
                        Map<?,?> m = (Map<?, ?>) o;
                        if (m.isEmpty() || m.size()>2) {
                            warn("Invalid map-entry in list for map at "+newPath);
                            return;
                        }
                        Object ek1=null, ev1, ek2=null, ev2;
                        if (m.size()==1) {
                            ek2 = m.keySet().iterator().next();
                            ev1 = m.values().iterator().next();
                        } else {
                            if (!MutableSet.of("key", "value").containsAll(m.keySet())) {
                                warn("Invalid key-value entry in list for map at "+newPath);
                                return;
                            }
                            ek1 = m.get("key");
                            ev1 = m.get("value");
                        }
                        if (ek2==null) {
                            ek2 = converter.read( ((YomlContextForRead)context).subpath("/@key["+i+"]", ek1, genericKeySubType) );
                        }
                        ev2 = converter.read( ((YomlContextForRead)context).subpath("/@value["+i+"]", ev1, genericValueSubType) );
                        jom.put(ek2, ev2);
                    } else {
                        // must be an entry set, so invalid
                        // however at this point we are committed to it being a map, so throw
                        warn("Invalid non-map map-entry in list for map at "+newPath);
                        return;
                    }
                    i++;
                }
                
            } else if (value instanceof Map) {
                for (Map.Entry<?,?> me: ((Map<?,?>)value).entrySet()) {
                    Object v = converter.read( ((YomlContextForRead)context).subpath("/"+me.getKey(), me.getValue(), genericValueSubType) );
                    jom.put(me.getKey(), v);
                }
                
            } else {
                // can't deal with primitive - but should have exited earlier 
                return;
            }
                
            context.setJavaObject(jo);
            context.phaseAdvance();
            YamlKeysOnBlackboard.getOrCreate(blackboard, null).clear();
        }

        private String getAlias(Class<?> type) {
            if (type==null || !Map.class.isAssignableFrom(type)) return null;
            for (Class<?> t: typesAliased.keySet()) {
                if (type.isAssignableFrom(t)) {
                    return typesAliased.get(t);
                }
            }
            return null;
        }

        protected boolean parseExpectedTypeAndDetermineIfNoBadProblems(String type) {
            if (isJsonMarkerType(type)) {
                genericKeySubType = YomlUtils.TYPE_JSON; 
                genericValueSubType = YomlUtils.TYPE_JSON; 
            } else {
                GenericsParse gp = new GenericsParse(type);
                if (gp.warning!=null) {
                    warn(gp.warning);
                    return false;
                }
                if (gp.isGeneric()) {
                    if (gp.subTypeCount()!=2) {
                        // not a list
                        return false;
                    }
                    genericKeySubType = gp.subTypes.get(0);
                    genericValueSubType = gp.subTypes.get(1);
                    
                    if ("?".equals(genericKeySubType)) genericKeySubType = null;
                    if ("?".equals(genericValueSubType)) genericValueSubType = null;
                }
                if (expectedBaseTypeName==null) {
                    expectedBaseTypeName = gp.baseType;
                }
                if (expectedJavaType==null) {
                    expectedJavaType = typeAliases.get(gp.baseType);
                }
            }
            return true;
        }

        public void write() {
            if (!canDoWrite()) return;
            if (!(getJavaObject() instanceof Map)) return;
            Map<?,?> jo = (Map<?,?>)getJavaObject();
            
            expectedJavaType = getExpectedTypeJava();
            if (context.getExpectedType()!=null && !parseExpectedTypeAndDetermineIfNoBadProblems(context.getExpectedType())) return;
            String expectedGenericKeySubType = genericKeySubType;
            String expectedGenericValueSubType = genericValueSubType;
            
            boolean isPureJson = YomlUtils.JsonMarker.isPureJson(getJavaObject());
            
            // if expecting json then
            if (isJsonMarkerTypeExpected()) {
                if (!isPureJson) {
                    warn("Cannot write "+getJavaObject()+" as pure JSON");
                    return;
                }
                @SuppressWarnings("unchecked")
                Map<Object,Object> m = Reflections.invokeConstructorFromArgsIncludingPrivate(typesAliased.keySet().iterator().next()).get();
                m.putAll((Map<?,?>)getJavaObject());
                storeWriteObjectAndAdvance(m);
                return;                    
            }
            
            // if expected type in the allows list or compatible with "map" and j.u.Map, look at that value
            // else try read/write as  { type: mapxxx, value: valuexxx }
            // else end, it isn't for us
            String alias = getAlias(getJavaObject().getClass());
            if (alias==null && !typesAllowed.contains(getJavaObject().getClass())) {
                // actual type should not be written this way
                return;
            }
            String aliasOfExpected = getAlias(expectedJavaType);
            boolean writeWithoutTypeInformation;
            if (alias!=null) writeWithoutTypeInformation = alias.equals(aliasOfExpected);
            else writeWithoutTypeInformation = getJavaObject().getClass().equals(expectedJavaType);

            String declaredType = alias;
            if (declaredType==null) declaredType = config.getTypeRegistry().getTypeName(getJavaObject());

            // then for value
            //   if all keys are primitives then write as map { keyxxx: valuexxx }
            //   else write as list of singleton maps as above or kv pairs - [ { key: keyxxx , value: valuexxx } ]
            //   (in both cases using generics if available)
            boolean allKeysString = true;
            for (Object k: jo.keySet()) {
                if (!(k instanceof String)) { allKeysString = false; break; }
            }
            
            Object result;
            boolean isEmpty;
            if (allKeysString) {
                if (isPureJson && genericValueSubType==null) {
                    genericValueSubType = YomlUtils.TYPE_JSON;
                }
                
                MutableMap<Object, Object> out = MutableMap.of();
                for (Map.Entry<?,?> me: jo.entrySet()) {
                    Object v = converter.write( ((YomlContextForWrite)context).subpath("/"+me.getKey(), me.getValue(), genericValueSubType) );
                    out.put(me.getKey(), v);
                }
                isEmpty = out.isEmpty();
                result = out;
                
            } else {
                int i=0;
                MutableList<Object> out = MutableList.of();
                for (Map.Entry<?,?> me: jo.entrySet()) {
                    Object v = converter.write( ((YomlContextForWrite)context).subpath("/@value["+i+"]", me.getValue(), genericValueSubType) );
                    
                    if (me.getKey() instanceof String) {
                        out.add(MutableMap.of(me.getKey(), v));
                    } else {
                        Object k = converter.write( ((YomlContextForWrite)context).subpath("/@key["+i+"]", me.getKey(), genericKeySubType) );
                        out.add(MutableMap.of("key", k, "value", v));
                    }
                    i++;
                    
                }
                isEmpty = out.isEmpty();
                result = out;
            }
            
            if (!isEmpty && ((!allKeysString && genericKeySubType!=null) || genericValueSubType!=null)) {
                // if relying on generics we must include the types
                if (writeWithoutTypeInformation) {
                    boolean mustWrap = false;
                    mustWrap |= (!allKeysString && genericKeySubType!=null && !Objects.equal(expectedGenericKeySubType, genericKeySubType));
                    mustWrap |= (genericValueSubType!=null && !Objects.equal(expectedGenericValueSubType, genericValueSubType));
                    if (mustWrap) {
                        writeWithoutTypeInformation = false;
                    }
                }
                declaredType = declaredType + "<" + (allKeysString ? "string" : genericKeySubType!=null ? genericKeySubType : "object") + "," +
                    (genericValueSubType!=null ? genericValueSubType : "object") + ">";
            }

            if (!writeWithoutTypeInformation) {
                result = MutableMap.of("type", declaredType, "value", result);
            }

            
            storeWriteObjectAndAdvance(result);
        }
    }

}
