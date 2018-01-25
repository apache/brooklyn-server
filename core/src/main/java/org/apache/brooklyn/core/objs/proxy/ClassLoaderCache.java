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
package org.apache.brooklyn.core.objs.proxy;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.brooklyn.util.javalang.AggregateClassLoader;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * For getting/creating {@link AggregateClassLoader} for an entity proxy. The returned class loader
 * can be used when calling {@link java.lang.reflect.Proxy#newProxyInstance(ClassLoader, Class[], java.lang.reflect.InvocationHandler)}.
 * 
 * The classloader is created with references to ClassLoaders for the given class and interfaces
 * (including all super-classes and super-interfaces of those). Also see comment in
 * {@link InternalEntityFactory#createEntityProxy(Iterable, org.apache.brooklyn.api.entity.Entity)}.
 * 
 * If a classloader has already been created for the given class+interfaces, then it guarantees
 * to return that same classloader instance. This is very important when using {@code newProxyInstance}
 * because if a different ClassLoader instance is used then it will reflectively create a new Proxy class.
 * See https://issues.apache.org/jira/browse/BROOKLYN-528.
 */
class ClassLoaderCache {

    // TODO Should TypesKey use WeakReferences for the classes (e.g. so that if bundle is unloaded, 
    // karaf can eventually get rid of it)? But not as simple as that - the map's value 
    // (the AggregateClassLoader) will also reference the class.

    private final ConcurrentMap<TypesKey, AggregateClassLoader> cache = Maps.newConcurrentMap();
    
    public AggregateClassLoader getClassLoaderForProxy(Class<?> clazz, Set<Class<?>> interfaces) {
        TypesKey typesKey = new TypesKey(clazz, interfaces);
        AggregateClassLoader result = cache.get(typesKey);
        if (result == null) {
            result = newClassLoader(clazz, interfaces);
            AggregateClassLoader oldVal = cache.putIfAbsent(typesKey, result);
            if (oldVal != null) result = oldVal;
        }
        return result;
    }
    
    private AggregateClassLoader newClassLoader(Class<?> clazz, Set<Class<?>> interfaces) {
        Set<ClassLoader> loaders = Sets.newLinkedHashSet();
        addClassLoaders(clazz, loaders);
        for (Class<?> iface : interfaces) {
            loaders.add(iface.getClassLoader());
        }

        AggregateClassLoader aggregateClassLoader =  AggregateClassLoader.newInstanceWithNoLoaders();
        for (ClassLoader cl : loaders) {
            aggregateClassLoader.addLast(cl);
        }
        
        return aggregateClassLoader;
    }
    
    private void addClassLoaders(Class<?> type, Set<ClassLoader> loaders) {
        ClassLoader cl = type.getClassLoader();

        //java.lang.Object.getClassLoader() == null
        if (cl != null) {
            loaders.add(cl);
        }

        Class<?> superType = type.getSuperclass();
        if (superType != null) {
            addClassLoaders(superType, loaders);
        }
        for (Class<?> iface : type.getInterfaces()) {
            addClassLoaders(iface, loaders);
        }
    }
    
    /**
     * Uses weak references for the class/interface references.
     */
    private static class TypesKey {
        final Class<?> type;
        final Set<Class<?>> interfaces;
        
        public TypesKey(Class<?> type, Set<Class<?>> interfaces) {
            this.type = checkNotNull(type, "type");
            this.interfaces = ImmutableSet.copyOf(checkNotNull(interfaces, "interfaces"));
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(type, interfaces);
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TypesKey)) return false;
            TypesKey o = (TypesKey) obj;
            return Objects.equal(type, o.type) && Objects.equal(interfaces, o.interfaces);
        }
    }
}
