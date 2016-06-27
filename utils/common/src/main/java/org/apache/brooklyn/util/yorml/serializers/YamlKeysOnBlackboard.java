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

import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlException;
import org.apache.brooklyn.util.yorml.YormlRequirement;

import com.google.common.annotations.Beta;

/** Keys from a YAML map that still need to be handled */
public class YamlKeysOnBlackboard implements YormlRequirement {
    
    private static String KEY = YamlKeysOnBlackboard.class.getName();
    
    public static boolean isPresent(Map<Object,Object> blackboard) {
        return blackboard.containsKey(KEY);
    }
    public static YamlKeysOnBlackboard peek(Map<Object,Object> blackboard) {
        return (YamlKeysOnBlackboard) blackboard.get(KEY);
    }
    public static YamlKeysOnBlackboard getOrCreate(Map<Object,Object> blackboard) {
        if (!isPresent(blackboard)) { blackboard.put(KEY, new YamlKeysOnBlackboard()); }
        return peek(blackboard);
    }
    public static YamlKeysOnBlackboard create(Map<Object,Object> blackboard) {
        if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
        blackboard.put(KEY, new YamlKeysOnBlackboard());
        return peek(blackboard);
    }

    Map<Object,Object> yamlKeysToReadToJava;

    @Override
    public void checkCompletion(YormlContext context) {
        if (!yamlKeysToReadToJava.isEmpty()) {
            // TODO limit toString to depth 2 ?
            throw new YormlException("Incomplete read of YAML keys: "+yamlKeysToReadToJava, context);
        }
    }
    
    /** true iff k1 and k2 are case-insensitively equal after removing all - and _.
     * Note that the definition of mangling may change.
     * TODO it should be stricter so that "ab" and "a-b" don't match but "aB" and "a-b" and "a_b" do */
    @Beta
    public static boolean mangleable(String k1, String k2) {
        if (k1==null || k2==null) return k1==k2;
        k1 = Strings.replaceAllNonRegex(k1, "-", "");
        k1 = Strings.replaceAllNonRegex(k1, "_", "");
        k2 = Strings.replaceAllNonRegex(k2, "-", "");
        k2 = Strings.replaceAllNonRegex(k2, "_", "");
        return k1.toLowerCase().equals(k2.toLowerCase());
    }
}