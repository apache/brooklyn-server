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
package org.apache.brooklyn.util.yorml.internal;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yorml.YormlSerializer;

import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;

/** Stores serializers that should be used */
public class SerializersOnBlackboard {
    
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
    
    List<YormlSerializer> preSerializers = MutableList.of();
    List<YormlSerializer> instantiatedTypeSerializers = MutableList.of();
    List<YormlSerializer> expectedTypeSerializers = MutableList.of();
    List<YormlSerializer> postSerializers = MutableList.of();

    public void addInstantiatedTypeSerializers(Iterable<? extends YormlSerializer> instantiatedTypeSerializers) {
        Iterables.addAll(this.instantiatedTypeSerializers, instantiatedTypeSerializers);
    }
    
    public Iterable<YormlSerializer> getSerializers() {
        return Iterables.concat(preSerializers, instantiatedTypeSerializers, expectedTypeSerializers, postSerializers); 
    }
    
}
