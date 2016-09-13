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
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlRequirement;
import org.apache.brooklyn.util.yoml.internal.YomlContext;

/** Keys from a YAML map that still need to be handled */
public class YamlKeysOnBlackboard implements YomlRequirement {
    
    private static String KEY = YamlKeysOnBlackboard.class.getName();
    
    public static boolean isPresent(Map<Object,Object> blackboard) {
        return blackboard.containsKey(KEY);
    }
    public static YamlKeysOnBlackboard peek(Map<Object,Object> blackboard) {
        return (YamlKeysOnBlackboard) blackboard.get(KEY);
    }
    public static YamlKeysOnBlackboard getOrCreate(Map<Object,Object> blackboard, Map<Object,Object> keys) {
        if (!isPresent(blackboard)) { 
            YamlKeysOnBlackboard ykb = new YamlKeysOnBlackboard();
            blackboard.put(KEY, ykb);
            ykb.yamlKeysToReadToJava = MutableMap.copyOf(keys);
        }
        return peek(blackboard);
    }
    public static YamlKeysOnBlackboard create(Map<Object,Object> blackboard) {
        if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
        blackboard.put(KEY, new YamlKeysOnBlackboard());
        return peek(blackboard);
    }

    Map<Object,Object> yamlKeysToReadToJava;

    @Override
    public void checkCompletion(YomlContext context) {
        if (!yamlKeysToReadToJava.isEmpty()) {
            // TODO limit toString to depth 2 ?
            throw new YomlException("Incomplete read of YAML keys: "+yamlKeysToReadToJava, context);
        }
    }

    @Override
    public String toString() {
        return super.toString()+"("+yamlKeysToReadToJava+")";
    }
}
