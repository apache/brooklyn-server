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

import javax.annotation.Nullable;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Boxing;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.YomlTypeRegistry;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstruction;
import org.apache.brooklyn.util.yoml.internal.ConstructionInstructions;
import org.apache.brooklyn.util.yoml.internal.YomlContext;
import org.apache.brooklyn.util.yoml.internal.YomlContextForRead;
import org.apache.brooklyn.util.yoml.internal.YomlConverter;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;

import com.google.common.collect.Iterables;

public class MockYomlTypeRegistry implements YomlTypeRegistry {

    static class MockRegisteredType {
        final String id;
        final String parentType;
        final Set<String> interfaceTypes;
        
        final Class<?> javaType;
        final Collection<YomlSerializer> serializers;
        final Object yamlDefinition;
        
        public MockRegisteredType(String id, String parentType, Class<?> javaType, Collection<String> interfaceTypes, Collection<? extends YomlSerializer> serializers, Object yamlDefinition) {
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
        return newInstanceMaybe(typeName, yoml, null);
    }
    
    @Override
    public Maybe<Object> newInstanceMaybe(String typeName, Yoml yoml, @Nullable YomlContext yomlContext) {
        MockRegisteredType type = types.get(typeName);
        if (type!=null && type.yamlDefinition!=null) {
            String parentTypeName = type.parentType;
            if (type.parentType==null && type.javaType!=null) parentTypeName = getDefaultTypeNameOfClass(type.javaType);
            return Maybe.of(new YomlConverter(yoml.getConfig()).read( new YomlContextForRead(type.yamlDefinition, "", parentTypeName, null).constructionInstruction(yomlContext.getConstructionInstruction()) ));
            // have to do the above instead of below to ensure construction instruction is passed
            // return Maybe.of(yoml.readFromYamlObject(type.yamlDefinition, parentTypeName));
        }
        
        Maybe<Class<?>> javaType = getJavaTypeInternal(type, typeName); 
        ConstructionInstruction constructor = yomlContext==null ? null : yomlContext.getConstructionInstruction();
        if (javaType.isAbsent() && constructor==null) {
            if (type==null) return Maybe.absent("Unknown type `"+typeName+"`");
            return Maybe.absent(new IllegalStateException("Incomplete hierarchy for "+type, ((Maybe.Absent<?>)javaType).getException()));
        }
        
        return ConstructionInstructions.Factory.newDefault(javaType.get(), constructor).create();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected static Maybe<Class<?>> maybeClass(Class<?> clazz) {
        // restrict unchecked wildcard generic warning/suppression to here
        return (Maybe) Maybe.ofDisallowingNull(clazz);
    }
    
    @Override
    public Maybe<Class<?>> getJavaTypeMaybe(String typeName, @Nullable YomlContext contextIgnoredInMock) {
        if (typeName==null) return Maybe.absent();
        // strip generics here
        if (typeName.indexOf('<')>0) typeName = typeName.substring(0, typeName.indexOf('<'));
        return getJavaTypeInternal(types.get(typeName), typeName);
    }
    
    protected Maybe<Class<?>> getJavaTypeInternal(MockRegisteredType registeredType, String typeName) {
        Maybe<Class<?>> result = Maybe.absent();
            
        if (result.isAbsent() && registeredType!=null) result = maybeClass(registeredType.javaType);
        if (result.isAbsent() && registeredType!=null) result = getJavaTypeMaybe(registeredType.parentType, null);
        
        if (result.isAbsent()) {
            result = Maybe.absent("Unknown type '"+typeName+"' (no match available in mock library)");
            if (typeName==null) return result; 
        }
        
        if (result.isAbsent()) result = maybeClass(Boxing.boxedType(Boxing.getPrimitiveType(typeName).orNull())).or(result);
        if (result.isAbsent() && YomlUtils.TYPE_STRING.equals(typeName)) result = maybeClass(String.class);
        
        if (result.isAbsent() && typeName.startsWith("java:")) {
            typeName = Strings.removeFromStart(typeName, "java:");
            try {
                // IRL use injected loader
                result = maybeClass(Class.forName(typeName));
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
    public void put(String typeName, Class<?> javaType, Collection<? extends YomlSerializer> serializers) {
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
        
        Maybe<Class<?>> javaType = getJavaTypeMaybe((String)type, null);
        if (javaType.isAbsent()) throw new IllegalArgumentException("Mock cannot resolve parent type `"+type+"` in definition of `"+typeName+"`: "+
            ((Maybe.Absent<?>)javaType).getException());
        
        Object interfaceTypes = ((Map<?,?>)yamlObject).remove("interfaceTypes");
        if (((Map<?,?>)yamlObject).isEmpty()) yamlObject = null;
        
        types.put(typeName, new MockRegisteredType(typeName, (String)type, javaType.get(), (Collection<String>)interfaceTypes, serializers, yamlObject));
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
    public Iterable<YomlSerializer> getSerializersForType(String typeName, YomlContext context) {
        Set<YomlSerializer> result = MutableSet.of();
        collectSerializers(typeName, result, MutableSet.<String>of());
        return result;
    }
    
    protected void collectSerializers(String typeName, Collection<YomlSerializer> serializers, Set<String> typesVisited) {
        if (typeName==null || !typesVisited.add(typeName)) return;
        MockRegisteredType rt = types.get(typeName);
        if (rt==null) return;
        if (rt.serializers!=null) serializers.addAll(rt.serializers);
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
