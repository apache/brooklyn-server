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

import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yorml.Yorml;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlContextForRead;
import org.apache.brooklyn.util.yorml.YormlContextForWrite;
import org.apache.brooklyn.util.yorml.internal.YormlUtils;
import org.apache.brooklyn.util.yorml.internal.YormlUtils.GenericsParse;
import org.apache.brooklyn.util.yorml.internal.YormlUtils.JsonMarker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.Iterables;

/*
 * if expecting a coll
 *   and it's not a coll
 *     if primitive, try as type
 *     if map, new conversion phase
 *   and it is a coll
 *     instantiate then go through coll
 * if not expecting a coll
 *   and it's not a coll
 *     do nothing
 *   and it is a coll
 *     try conversion to map
 *     
 * repeat with value
 */
public class InstantiateTypeList extends YormlSerializerComposition {

    private static final Logger log = LoggerFactory.getLogger(InstantiateTypeList.class);
    
    private static final String LIST = YormlUtils.TYPE_LIST;
    private static final String SET = YormlUtils.TYPE_SET;
    
    @SuppressWarnings("rawtypes")
    Map<String,Class<? extends Collection>> basicCollectionTypes = MutableMap.<String,Class<? extends Collection>>of(
        LIST, MutableList.class,
        SET, MutableSet.class
    );
    
    @SuppressWarnings("rawtypes")
    Map<Class<? extends Collection>, String> typesMappedToBasic = MutableMap.<Class<? extends Collection>,String>of(
        MutableList.class, LIST,
        ArrayList.class, LIST,
        List.class, LIST,
        MutableSet.class, SET,
        LinkedHashSet.class, SET,
        Set.class, SET
    );
    
    @SuppressWarnings("rawtypes")
    Set<Class<? extends Collection>> typesAllowedAsCollections = MutableSet.<Class<? extends Collection>>of(
        // TODO does anything fit this category? we serialize as a json list, including the type, and use xxx.add(...) to read in
    );
    
    protected YormlSerializerWorker newWorker() {
        return new Worker();
    }

    public class Worker extends InstantiateTypeWorkerAbstract {
        
        String genericSubType = null;
        Class<?> expectedJavaType;
        
        public void read() {
            if (!canDoRead()) return;
            Object yo = getYamlObject();
            expectedJavaType = getExpectedTypeJava();
            
            if (context.getExpectedType()!=null && !parseExpectedTypeAndDetermineIfNoBadProblems(context.getExpectedType())) return;
            
            if (expectedJavaType!=null && !Iterable.class.isAssignableFrom(expectedJavaType)) {
                // expecting something other than a collection
                if (!(yo instanceof Iterable)) {
                    // and not given a collection -- just exit
                    return;
                } else {
                    // but we have a collection
                    // spawn manipulate-convert-from-list phase
                    context.phaseInsert(YormlContext.StandardPhases.MANIPULATING_FROM_LIST, YormlContext.StandardPhases.HANDLING_TYPE);
                    context.phaseAdvance();
                    return;
                }
            } else if (!(yo instanceof Iterable)) {
                // no expectation or expecting a collection, but not given a collection
                if (yo instanceof Map) {
                    String type = readingTypeFromFieldOrExpected();
                    Object value = readingValueFromTypeValueMap().orNull();
                    Class<?> oldExpectedType = expectedJavaType;
                    
                    // get any new generic type set - slightly messy
                    if (!parseExpectedTypeAndDetermineIfNoBadProblems(type)) return;
                    Class<?> javaType = config.getTypeRegistry().getJavaType(type);
                    if (javaType==null) { javaType = expectedJavaType; }
                    expectedJavaType = oldExpectedType;
                    
                    if (javaType==null || value==null || !Collection.class.isAssignableFrom(javaType) || !Iterable.class.isInstance(value)) {
                        // only apply if it's a list type and a list value
                        return;
                    }
                    // looks like a list in a type-value map
                    Object jo = newInstance(expectedJavaType, type);
                    if (jo==null) return;
                    
                    context.setJavaObject(jo);
                    
                    readIterableInto((Collection<?>)jo, (Iterable<?>)value);
                    context.phaseAdvance();
                    removeTypeAndValueKeys();
                    return;
                }
                if (expectedJavaType!=null) {
                    // collection definitely expected but not received
                    context.phaseInsert(YormlContext.StandardPhases.MANIPULATING_TO_LIST, YormlContext.StandardPhases.HANDLING_TYPE);
                    context.phaseAdvance();
                    return;
                }
                // otherwise standard InstantiateType will do it
                return;
            } else {
                // given a collection, when expecting a collection or no expectation -- read as list
                Object jo;
                if (hasJavaObject()) {
                    // populating previous java object
                    jo = context.getJavaObject();
                    
                } else {
                    jo = newInstance(expectedJavaType, null);
                    if (jo==null) return;
                    
                    context.setJavaObject(jo);
                }

                readIterableInto((Collection<?>)jo, (Iterable<?>)yo);
                context.phaseAdvance();
            }
        }

        protected boolean parseExpectedTypeAndDetermineIfNoBadProblems(String type) {
            if (isJsonMarkerType(type)) {
                genericSubType = YormlUtils.TYPE_JSON; 
            } else {
                GenericsParse gp = new GenericsParse(type);
                if (gp.warning!=null) {
                    warn(gp.warning);
                    return false;
                }
                if (gp.isGeneric()) {
                    if (gp.subTypeCount()!=1) {
                        // not a list
                        return false;
                    }
                    genericSubType = Iterables.getOnlyElement(gp.subTypes);
                }
                if (expectedJavaType==null) {
                    expectedJavaType = basicCollectionTypes.get(gp.baseType);
                }
            }
            return true;
        }

        private Object newInstance(Class<?> javaType, String explicitTypeName) {
            if (explicitTypeName!=null) {
                GenericsParse gp = new GenericsParse(explicitTypeName);
                if (gp.warning!=null) {
                    warn(gp.warning);
                    return null;
                }
                
                Class<?> locallyWantedType = basicCollectionTypes.get(gp.baseType);
                
                if (locallyWantedType==null) {
                    // rely on type registry
                    return config.getTypeRegistry().newInstance(explicitTypeName, Yorml.newInstance(config));
                }
                
                // create it ourselves, but first assert it matches expected
                if (javaType!=null) {
                    if (locallyWantedType.isAssignableFrom(javaType)) {
                        // prefer the java type
                    } else if (javaType.isAssignableFrom(locallyWantedType)) {
                        // prefer locally wanted
                        javaType = locallyWantedType;
                    }
                } else {
                    javaType = locallyWantedType;
                }
            
                // and set the subtype 
                if (gp.subTypeCount()!=1) return null;
                
                String subType = Iterables.getOnlyElement(gp.subTypes);
                if (genericSubType!=null && !genericSubType.equals(subType)) {
                    log.debug("Got different generic subtype, expected "+context.getExpectedType()+" but declared "+explicitTypeName+"; preferring declared");
                }
                genericSubType = subType;
            }
            
            Class<?> concreteJavaType = null;
            if (javaType==null || javaType.isInterface() || Modifier.isAbstract(javaType.getModifiers())) {
                // take first from default types that matches
                for (Class<?> candidate : basicCollectionTypes.values()) {
                    if (javaType==null || javaType.isAssignableFrom(candidate)) {
                        concreteJavaType = candidate;
                        break;
                    }
                }
                if (concreteJavaType==null) {
                    // fallback, if given interface create as list
                    warn("No information to instantiate list "+javaType);
                    return null;
                }
                
            } else {
                concreteJavaType = javaType;
            }
            if (!Collection.class.isAssignableFrom(concreteJavaType)) {
                warn("No information to add items to list "+concreteJavaType);
                return null;
            }
            
            try {
                return concreteJavaType.newInstance();
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }

        protected void readIterableInto(Collection<?> joq, Iterable<?> yo) {
            // go through collection, creating from children
            
            @SuppressWarnings("unchecked")
            Collection<Object> jo = (Collection<Object>) joq;
            int index = 0;
            
            for (Object yi: yo) {
                YormlContextForRead subcontext = new YormlContextForRead(context.getJsonPath()+"["+index+"]", genericSubType);
                subcontext.setYamlObject(yi);
                jo.add(converter.read(subcontext));

                index++;
            }
        }

        public void write() {
            if (!canDoWrite()) return;
            if (!(getJavaObject() instanceof Iterable)) return;
            
            boolean isPureJson = YormlUtils.JsonMarker.isPureJson(getJavaObject());
            
            // if expecting json then:
            if (isJsonMarkerTypeExpected()) {
                if (!isPureJson) {
                    warn("Cannot write "+getJavaObject()+" as pure JSON");
                    return;
                }
                storeWriteObjectAndAdvance(getJavaObject());
                return;                    
            }
            
            Class<?> expectedJavaType = getExpectedTypeJava();
            GenericsParse gp = new GenericsParse(context.getExpectedType());
            if (gp.warning!=null) {
                warn(gp.warning);
                return;
            }
            if (gp.isGeneric()) {
                if (gp.subTypeCount()!=1) {
                    // not a list
                    return;
                }
                genericSubType = Iterables.getOnlyElement(gp.subTypes);
            }
            if (expectedJavaType==null) {
                expectedJavaType = basicCollectionTypes.get(gp.baseType);
            }

            String actualTypeName = typesMappedToBasic.get(getJavaObject().getClass());
            boolean isBasicCollectionType = (actualTypeName!=null);
            if (actualTypeName==null) actualTypeName = config.getTypeRegistry().getTypeName(getJavaObject());
            if (actualTypeName==null) return;
            boolean isAllowedCollectionType = isBasicCollectionType || typesAllowedAsCollections.contains(getJavaObject().getClass());
            
            Class<?> reconstructedJavaType = basicCollectionTypes.get(actualTypeName);
            if (reconstructedJavaType==null) reconstructedJavaType = getJavaObject().getClass();
            
            Object result;
            Collection<Object> list = MutableList.of();

            boolean writeWithoutTypeInformation = Objects.equal(reconstructedJavaType, expectedJavaType);
            if (!writeWithoutTypeInformation) {
                @SuppressWarnings("rawtypes")
                Class<? extends Collection> defaultCollectionType = basicCollectionTypes.isEmpty() ? null : basicCollectionTypes.values().iterator().next();
                if (Objects.equal(reconstructedJavaType, defaultCollectionType)) {
                    // actual type is the default - typically can omit saying the type
                    if (context.getExpectedType()==null) writeWithoutTypeInformation = true;
                    else if (expectedJavaType!=null && expectedJavaType.isAssignableFrom(defaultCollectionType)) writeWithoutTypeInformation = true;
                    else {
                        // possibly another problem -- expecting something different to default
                        // don't fret, just include the type specifically
                        // likely they're just expecting an explicit collection type other than our default
                        actualTypeName = config.getTypeRegistry().getTypeName(getJavaObject());
                    }
                }
            }
            if ((YormlUtils.TYPE_LIST.equals(actualTypeName) || (YormlUtils.TYPE_SET.equals(actualTypeName))) && genericSubType==null) {
                if (JsonMarker.isPureJson(getJavaObject()) && !Iterables.isEmpty((Iterable<?>)getJavaObject())) {
                    writeWithoutTypeInformation = false;
                    actualTypeName = actualTypeName+"<"+YormlUtils.TYPE_JSON+">";
                    genericSubType = YormlUtils.TYPE_JSON;
                }
            }
            
            if (writeWithoutTypeInformation) {
                // add directly if we are expecting this
                result = list;
            } else if (!isAllowedCollectionType) {
                // not to be written with this serializer
                return;
            } else {
                // need to include the type name
                if (actualTypeName==null) return;
                result = MutableMap.of("type", actualTypeName, "value", list);
            }

            int index = 0;
            for (Object ji: (Iterable<?>)getJavaObject()) {
                YormlContextForWrite subcontext = new YormlContextForWrite(context.getJsonPath()+"["+index+"]", genericSubType);
                subcontext.setJavaObject(ji);
                list.add(converter.write(subcontext));

                index++;
            }
            
            storeWriteObjectAndAdvance(result);
        }
    }

}
