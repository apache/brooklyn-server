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
package org.apache.brooklyn.util.yoml.tests;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

import com.google.common.collect.Iterables;

public class MockYomlTypeRegistry implements YomlTypeRegistry {

    static class MockRegisteredType {
        final String id;
        final String parentType;
        final Set<String> interfaceTypes;
        
        final Class<?> javaType;
        final List<YomlSerializer> serializers;
        final Object yamlDefinition;
        
        public MockRegisteredType(String id, String parentType, Class<?> javaType, Collection<String> interfaceTypes, List<? extends YomlSerializer> serializers, Object yamlDefinition) {
            super();
            this.id = id;
            this.parentType = parentType;
            this.javaType = javaType;
            this.interfaceTypes = MutableSet.copyOf(interfaceTypes);
            this.serializers = MutableList.copyOf(serializers);
            this.yamlDefinition = yamlDefinition;
        }
    }
    
    Map<String,MockRegisteredType> types = MutableMap.of();
    
    @Override
    public Object newInstance(String typeName, Yoml yoml) {
        return newInstanceMaybe(typeName, yoml).get();
    }
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml) {
        MockRegisteredType type = types.get(typeName);
        if (type!=null && type.yamlDefinition!=null) {
            String parentTypeName = type.parentType;
            if (type.parentType==null && type.javaType!=null) parentTypeName = getDefaultTypeNameOfClass(type.javaType);
            return Maybe.of(yoml.readFromYamlObject(type.yamlDefinition, parentTypeName));
        }
        Class<?> javaType = getJavaType(type, typeName); 
        if (javaType==null) {
            if (type==null) return Maybe.absent("Unknown type `"+typeName+"`");
            throw new IllegalStateException("Incomplete hierarchy for `"+typeName+"`");
        }
        return Reflections.invokeConstructorFromArgsIncludingPrivate(javaType);
    }
    
    @Override
    public Class<?> getJavaType(String typeName) {
        if (typeName==null) return null;
        // string generics here
        if (typeName.indexOf('<')>0) typeName = typeName.substring(0, typeName.indexOf('<'));
        return getJavaType(types.get(typeName), typeName);
    }
    
    protected Class<?> getJavaType(MockRegisteredType registeredType, String typeName) {
        Class<?> result = null;
            
        if (result==null && registeredType!=null) result = registeredType.javaType;
        if (result==null && registeredType!=null) result = getJavaType(registeredType.parentType);
        
        if (result==null && typeName==null) return null;
        
        if (result==null) result = Boxing.boxedType(Boxing.getPrimitiveType(typeName).orNull());
        if (result==null && YomlUtils.TYPE_STRING.equals(typeName)) result = String.class;
        
        if (result==null && typeName.startsWith("java:")) {
            typeName = Strings.removeFromStart(typeName, "java:");
            try {
                // TODO use injected loader?
                result = Class.forName(typeName);
            } catch (ClassNotFoundException e) {
                // ignore, this isn't a java type
            }
        }
        return result;
    }
    
    /** simplest type def -- an alias for a java class */
    public void put(String typeName, Class<?> javaType) {
        put(typeName, javaType, null);
    }
    public void put(String typeName, Class<?> javaType, List<? extends YomlSerializer> serializers) {
        types.put(typeName, new MockRegisteredType(typeName, "java:"+javaType.getName(), javaType, MutableSet.<String>of(), serializers, null));
    }
    
    /** takes a simplified yaml definition supporting a map with a key `type` and optionally other keys */
    public void put(String typeName, String yamlDefinition) {
        put(typeName, yamlDefinition, null);
    }
    @SuppressWarnings("unchecked")
    public void put(String typeName, String yamlDefinition, List<? extends YomlSerializer> serializers) {
        Object yamlObject = Iterables.getOnlyElement( Yamls.parseAll(yamlDefinition) );
        if (!(yamlObject instanceof Map)) throw new IllegalArgumentException("Mock only supports map definitions");
        
        Object type = ((Map<?,?>)yamlObject).remove("type");
        if (!(type instanceof String)) throw new IllegalArgumentException("Mock requires key `type` with string value");
        
        Class<?> javaType = getJavaType((String)type);
        if (javaType==null) throw new IllegalArgumentException("Mock cannot resolve parent type `"+type+"` in definition of `"+typeName+"`");
        
        Object interfaceTypes = ((Map<?,?>)yamlObject).remove("interfaceTypes");
        if (((Map<?,?>)yamlObject).isEmpty()) yamlObject = null;
        
        types.put(typeName, new MockRegisteredType(typeName, (String)type, javaType, (Collection<String>)interfaceTypes, serializers, yamlObject));
    }

    @Override
    public String getTypeName(Object obj) {
        return getTypeNameOfClass(obj.getClass());
    }

    @Override
    public <T> String getTypeNameOfClass(Class<T> type) {
        if (type==null) return null;
        for (Map.Entry<String,MockRegisteredType> t: types.entrySet()) {
            if (type.equals(t.getValue().javaType) && t.getValue().yamlDefinition==null) return t.getKey();
        }
        return getDefaultTypeNameOfClass(type);
    }
    
    protected <T> String getDefaultTypeNameOfClass(Class<T> type) {
        Maybe<String> primitive = Boxing.getPrimitiveName(type);
        if (primitive.isPresent()) return primitive.get();
        if (String.class.equals(type)) return "string";
        // map and list handled by those serializers
        return "java:"+type.getName();
    }
    
    @Override
    public void collectSerializers(String typeName, Collection<YomlSerializer> serializers, Set<String> typesVisited) {
        if (typeName==null || !typesVisited.add(typeName)) return;
        MockRegisteredType rt = types.get(typeName);
        if (rt==null || rt.serializers==null) return;
        serializers.addAll(rt.serializers);
        if (rt.parentType!=null) {
            collectSerializers(rt.parentType, serializers, typesVisited);
        }
        if (rt.interfaceTypes!=null) {
            for (String interfaceType: rt.interfaceTypes) {
                collectSerializers(interfaceType, serializers, typesVisited);
            }
        }
    }
}
