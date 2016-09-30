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
package org.apache.brooklyn.util.yoml.internal;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/** Stores serializers that should be used */
public class SerializersOnBlackboard {
    
    private static final Logger log = LoggerFactory.getLogger(SerializersOnBlackboard.class);
    
    private static String KEY = SerializersOnBlackboard.class.getName();
    
    public static boolean isPresent(Map<Object,Object> blackboard) {
        return blackboard.containsKey(KEY);
    }
    public static SerializersOnBlackboard get(Map<Object, Object> blackboard) {
        return Preconditions.checkNotNull(peek(blackboard), "Not yet available");
    }
    public static SerializersOnBlackboard peek(Map<Object,Object> blackboard) {
        return (SerializersOnBlackboard) blackboard.get(KEY);
    }
    public static SerializersOnBlackboard create(Map<Object,Object> blackboard) {
        if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
        blackboard.put(KEY, new SerializersOnBlackboard());
        return peek(blackboard);
    }
    public static SerializersOnBlackboard getOrCreate(Map<Object,Object> blackboard) {
        SerializersOnBlackboard result = peek(blackboard);
        if (result!=null) return result;
        result = new SerializersOnBlackboard();
        blackboard.put(KEY, result);
        return result;
    }
    
    private List<YomlSerializer> preSerializers = MutableList.of();
    private List<YomlSerializer> instantiatedTypeSerializers = MutableList.of();
    private List<YomlSerializer> expectedTypeSerializers = MutableList.of();
    private List<YomlSerializer> postSerializers = MutableList.of();

    public void addInstantiatedTypeSerializers(Iterable<? extends YomlSerializer> newInstantiatedTypeSerializers) {
        addNewSerializers(instantiatedTypeSerializers, newInstantiatedTypeSerializers, "instantiated type");
    }
    public void addExpectedTypeSerializers(Iterable<YomlSerializer> newExpectedTypeSerializers) {
        addNewSerializers(expectedTypeSerializers, newExpectedTypeSerializers, "expected type");
        
    }
    public void addPostSerializers(List<YomlSerializer> newPostSerializers) {
        addNewSerializers(postSerializers, newPostSerializers, "post");
    }
    protected static void addNewSerializers(List<YomlSerializer> addTo, Iterable<? extends YomlSerializer> elementsToAddIfNotPresent, String description) {
        MutableSet<YomlSerializer> newOnes = MutableSet.copyOf(elementsToAddIfNotPresent);
        int sizeBefore = newOnes.size();
        // removal isn't expected to work as hashCode and equals aren't typically implemented;
        // callers should make sure only to add when needed
        newOnes.removeAll(addTo);
        if (log.isTraceEnabled())
            log.trace("Adding "+newOnes.size()+" serializers ("+sizeBefore+" initially requested) for "+description+" (had "+addTo.size()+"): "+newOnes);
        addTo.addAll(newOnes);
    }
    
    public Iterable<YomlSerializer> getSerializers() {
        return Iterables.concat(preSerializers, instantiatedTypeSerializers, expectedTypeSerializers, postSerializers); 
    }
    
    public static boolean isAddedByTypeInstantiation(Map<Object, Object> blackboard, YomlSerializer serializer) {
        SerializersOnBlackboard sb = get(blackboard);
        if (sb!=null && sb.instantiatedTypeSerializers.contains(serializer)) return true;
        return false;
    }

    public String toString() {
        return super.toString()+"["+preSerializers.size()+" pre,"+instantiatedTypeSerializers.size()+" inst,"+
            expectedTypeSerializers.size()+" exp,"+postSerializers.size()+" post]";
    }
}
