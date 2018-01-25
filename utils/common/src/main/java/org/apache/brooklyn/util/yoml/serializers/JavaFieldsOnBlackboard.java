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

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlRequirement;
import org.apache.brooklyn.util.yoml.internal.YomlContext;

/** Indicates that something has handled the type 
 * (on read, creating the java object, and on write, setting the `type` field in the yaml object)
 * and made a determination of what fields need to be handled */
public class JavaFieldsOnBlackboard implements YomlRequirement {
    
    private static String KEY = JavaFieldsOnBlackboard.class.getName();
    
    public static boolean isPresent(Map<Object,Object> blackboard) {
        return isPresent(blackboard, null);
    }
    public static JavaFieldsOnBlackboard peek(Map<Object,Object> blackboard) {
        return peek(blackboard, null);
    }
    public static JavaFieldsOnBlackboard getOrCreate(Map<Object,Object> blackboard) {
        return getOrCreate(blackboard, null);
    }
    public static JavaFieldsOnBlackboard create(Map<Object,Object> blackboard) {
        return create(blackboard, null);
    }
    
    private static String key(String label) {
        return KEY + (Strings.isNonBlank(label) ? ":"+label : "");
    }
    public static boolean isPresent(Map<Object,Object> blackboard, String label) {
        return blackboard.containsKey(key(label));
    }
    public static JavaFieldsOnBlackboard peek(Map<Object,Object> blackboard, String label) {
        return (JavaFieldsOnBlackboard) blackboard.get(key(label));
    }
    public static JavaFieldsOnBlackboard getOrCreate(Map<Object,Object> blackboard, String label) {
        if (!isPresent(blackboard)) { blackboard.put(key(label), new JavaFieldsOnBlackboard()); }
        return peek(blackboard, label);
    }
    public static JavaFieldsOnBlackboard create(Map<Object,Object> blackboard, String label) {
        if (isPresent(blackboard)) { throw new IllegalStateException("Already present"); }
        blackboard.put(key(label), new JavaFieldsOnBlackboard());
        return peek(blackboard, label);
    }
    
    List<String> fieldsToWriteFromJava;
    
    String typeNameFromReadToConstructJavaLater;
    Class<?> typeFromReadToConstructJavaLater;
    Map<String,Object> fieldsFromReadToConstructJava;
    Map<String,Object> configToWriteFromJava;


    @Override
    public void checkCompletion(YomlContext context) {
        if (fieldsToWriteFromJava!=null && !fieldsToWriteFromJava.isEmpty()) {
            throw new YomlException("Incomplete write of Java object data: "+fieldsToWriteFromJava, context);
        }
        if (fieldsFromReadToConstructJava!=null && !fieldsFromReadToConstructJava.isEmpty()) {
            throw new YomlException("Incomplete use of constructor fields creating Java object: "+fieldsFromReadToConstructJava, context);
        }
        if (configToWriteFromJava!=null && !configToWriteFromJava.isEmpty()) {
            throw new YomlException("Incomplete write of config keys: "+configToWriteFromJava, context);
        }
    }
    
    @Override
    public String toString() {
        return super.toString()+"("+fieldsToWriteFromJava+"; "+typeNameFromReadToConstructJavaLater+": "+fieldsFromReadToConstructJava+")";
    }

}
