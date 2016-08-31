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
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.YomlContext;
import org.apache.brooklyn.util.yoml.YomlContextForRead;
import org.apache.brooklyn.util.yoml.YomlContextForWrite;
import org.apache.brooklyn.util.yoml.YomlException;
import org.apache.brooklyn.util.yoml.YomlRequirement;

public class ReadingTypeOnBlackboard implements YomlRequirement {

    Set<String> errorNotes = MutableSet.of();

    public static final String KEY = ReadingTypeOnBlackboard.class.getCanonicalName();
        
    public static ReadingTypeOnBlackboard get(Map<Object,Object> blackboard) {
        Object v = blackboard.get(KEY);
        if (v==null) {
            v = new ReadingTypeOnBlackboard();
            blackboard.put(KEY, v);
        }
        return (ReadingTypeOnBlackboard) v;
    }
    
    @Override
    public void checkCompletion(YomlContext context) {
        if (context instanceof YomlContextForRead && context.getJavaObject()!=null) return;
        if (context instanceof YomlContextForWrite && context.getYamlObject()!=null) return;
        if (errorNotes.isEmpty()) throw new YomlException("No means to identify type to instantiate", context);
        throw new YomlException(Strings.join(errorNotes, "; "), context);
    }
    
    public void addNote(String message) {
        errorNotes.add(message);
    }

}
