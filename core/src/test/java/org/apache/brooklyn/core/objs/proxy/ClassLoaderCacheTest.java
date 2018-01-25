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

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNotSame;
import static org.testng.Assert.assertSame;

import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.entity.stock.BasicEntityImpl;
import org.apache.brooklyn.util.javalang.AggregateClassLoader;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class ClassLoaderCacheTest {

    private ClassLoaderCache cache;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        cache = new ClassLoaderCache();
    }
    
    @Test
    public void testSameLoader() throws Exception {
        Class<BasicEntityImpl> clazz = BasicEntityImpl.class;
        ImmutableSet<Class<? extends Object>> interfaces = ImmutableSet.of(EntityProxy.class, Entity.class, EntityInternal.class, BasicEntity.class);
        
        AggregateClassLoader loader = cache.getClassLoaderForProxy(clazz, interfaces);
        assertLoader(loader, Iterables.concat(ImmutableList.of(clazz), interfaces));
        
        AggregateClassLoader loader2 = cache.getClassLoaderForProxy(clazz, interfaces);
        assertSame(loader, loader2);
    }
    
    @Test
    public void testDifferentLoader() throws Exception {
        Class<BasicEntityImpl> clazz = BasicEntityImpl.class;
        Set<Class<?>> interfaces = ImmutableSet.of(EntityProxy.class, Entity.class, EntityInternal.class, BasicEntity.class);
        AggregateClassLoader loader = cache.getClassLoaderForProxy(clazz, interfaces);
        assertLoader(loader, Iterables.concat(ImmutableList.of(clazz), interfaces));

        Class<TestEntityImpl> clazz2 = TestEntityImpl.class;
        Set<Class<?>> interfaces2 = ImmutableSet.of(EntityProxy.class, Entity.class, EntityInternal.class, TestEntity.class);
        AggregateClassLoader loader2 = cache.getClassLoaderForProxy(clazz2, interfaces2);
        assertLoader(loader2, Iterables.concat(ImmutableList.of(clazz2), interfaces2));
        assertNotSame(loader, loader2);
    }
    
    private void assertLoader(ClassLoader loader, Iterable<? extends Class<?>> clazzes) throws Exception {
        assertNotNull(loader);
        for (Class<?> clazz : clazzes) {
            loader.loadClass(clazz.getName());
        }
    }
}
