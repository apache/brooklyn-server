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
package org.apache.brooklyn.util.yorml.tests;

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.YormlTypeRegistry;

public class MockYormlTypeRegistry implements YormlTypeRegistry {

    Map<String,Class<?>> types = MutableMap.of();
    
    @Override
    public Object newInstance(String typeName) {
        Class<?> type = getJavaType(typeName);
        if (type==null) {
            return null;
        }
        try {
            return type.newInstance();
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }
    
    @Override
    public java.lang.Class<?> getJavaType(String typeName) {
        Class<?> type = types.get(typeName);
        if (type==null) type = Boxing.getPrimitiveType(typeName).orNull();
        if (type==null && "string".equals(typeName)) type = String.class;
        if (type==null && typeName.startsWith("java:")) {
            typeName = Strings.removeFromStart(typeName, "java:");
            try {
                // TODO use injected loader?
                type = Class.forName(typeName);
            } catch (ClassNotFoundException e) {
                // ignore, this isn't a java type
            }
        }
        return type;
    }
    
    public void put(String typeName, Class<?> type) {
        types.put(typeName, type);
    }

    @Override
    public String getTypeName(Object obj) {
        return getTypeNameOfClass(obj.getClass());
    }

    @Override
    public <T> String getTypeNameOfClass(Class<T> type) {
        for (Map.Entry<String,Class<?>> t: types.entrySet()) {
            if (t.getValue().equals(type)) return t.getKey();
        }
        Maybe<String> primitive = Boxing.getPrimitiveName(type);
        if (primitive.isPresent()) return primitive.get();
        if (String.class.equals(type)) return "string";
        // TODO map and list?
        
        return "java:"+type.getName();
    }
}
