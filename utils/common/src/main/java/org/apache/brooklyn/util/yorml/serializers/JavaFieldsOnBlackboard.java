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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.yorml.YormlContext;
import org.apache.brooklyn.util.yorml.YormlException;
import org.apache.brooklyn.util.yorml.YormlRequirement;

/** Indicates that something has handled the type 
 * (on read, creating the java object, and on write, setting the `type` field in the yaml object)
 * and made a determination of what fields need to be handled */
public class JavaFieldsOnBlackboard implements YormlRequirement {
    
    private static String KEY = JavaFieldsOnBlackboard.class.getName();
    
    public static boolean isPresent(Map<Object,Object> blackboard) {
        return blackboard.containsKey(KEY);
    }
    public static JavaFieldsOnBlackboard peek(Map<Object,Object> blackboard) {
        return (JavaFieldsOnBlackboard) blackboard.get(KEY);
    }
    public static JavaFieldsOnBlackboard getOrCreate(Map<Object,Object> blackboard) {
        if (!isPresent(blackboard)) { blackboard.put(KEY, new JavaFieldsOnBlackboard()); }
        return peek(blackboard);
    }
    public static JavaFieldsOnBlackboard create(Map<Object,Object> blackboard) {
        if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
        blackboard.put(KEY, new JavaFieldsOnBlackboard());
        return peek(blackboard);
    }
    
    List<String> fieldsToWriteFromJava;

    @Override
    public void checkCompletion(YormlContext context) {
        if (!fieldsToWriteFromJava.isEmpty()) {
            throw new YormlException("Incomplete write of Java object data: "+fieldsToWriteFromJava, context);
        }
    }
}