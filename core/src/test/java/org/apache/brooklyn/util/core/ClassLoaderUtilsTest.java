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
package org.apache.brooklyn.util.core;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.dom4j.tree.AbstractEntity;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class ClassLoaderUtilsTest {

    private LocalManagementContext mgmt;
    
    private String origWhiteListKey;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        origWhiteListKey = System.getProperty(ClassLoaderUtils.WHITE_LIST_KEY);
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (origWhiteListKey != null) {
            System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, origWhiteListKey);
        } else {
            System.clearProperty(ClassLoaderUtils.WHITE_LIST_KEY);
        }
        if (mgmt != null) {
            Entities.destroyAll(mgmt);
        }
    }
    
    @Test
    public void testLoadClassNotInOsgi() throws Exception {
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass());
        assertEquals(clu.loadClass(getClass().getName()), getClass());
        assertEquals(clu.loadClass(Entity.class.getName()), Entity.class);
        assertEquals(clu.loadClass(AbstractEntity.class.getName()), AbstractEntity.class);
        assertLoadFails(clu, "org.apache.brooklyn.this.name.does.not.Exist");
    }
    
    @Test
    public void testLoadClassInOsgi() throws Exception {
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = installBundle(mgmt, bundleUrl);
        
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        
        assertLoadFails(clu, classname);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + classname).getName(), classname);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + bundle.getVersion()+":" + classname).getName(), classname);
    }
    
    @Test
    public void testLoadClassInOsgiWhiteList() throws Exception {
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = installBundle(mgmt, bundleUrl);
        
        String whileList = bundle.getSymbolicName()+":"+bundle.getVersion();
        System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, whileList);
        
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        assertEquals(clu.loadClass(classname).getName(), classname);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + classname).getName(), classname);
    }
    
    @Test
    public void testLoadClassInOsgiCore() throws Exception {
        Class<?> clazz = AbstractEntity.class;
        String classname = clazz.getName();
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = getBundle(mgmt, "org.apache.brooklyn.core");
        
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        assertEquals(clu.loadClass(classname), clazz);
        assertEquals(clu.loadClass(classname), clazz);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + classname), clazz);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + bundle.getVersion() + ":" + classname), clazz);
    }
    
    @Test
    public void testLoadClassInOsgiApi() throws Exception {
        Class<?> clazz = Entity.class;
        String classname = clazz.getName();
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = getBundle(mgmt, "org.apache.brooklyn.api");
        
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        assertEquals(clu.loadClass(classname), clazz);
        assertEquals(clu.loadClass(classname), clazz);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + classname), clazz);
        assertEquals(clu.loadClass(bundle.getSymbolicName() + ":" + bundle.getVersion() + ":" + classname), clazz);
    }
    
    @Test
    public void testIsBundleWhiteListed() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        
        assertTrue(clu.isBundleWhiteListed(getBundle(mgmt, "org.apache.brooklyn.core")));
        assertTrue(clu.isBundleWhiteListed(getBundle(mgmt, "org.apache.brooklyn.api")));
        assertFalse(clu.isBundleWhiteListed(getBundle(mgmt, "com.google.guava")));
    }
    
    /**
     * When two guava versions installed, want us to load from the *brooklyn* version rather than 
     * a newer version that happens to be in Karaf.
     */
    @Test(groups={"Integration"})
    public void testLoadsFromRightGuavaVersion() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);
        
        String bundleUrl = "http://search.maven.org/remotecontent?filepath=com/google/guava/guava/18.0/guava-18.0.jar";
        Bundle bundle = installBundle(mgmt, bundleUrl);
        String bundleName = bundle.getSymbolicName();
        
        String classname = bundleName + ":" + ImmutableList.class.getName();
        Class<?> clazz = clu.loadClass(classname);
        assertEquals(clazz, ImmutableList.class);
    }
    
    private Bundle installBundle(ManagementContext mgmt, String bundleUrl) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        return Osgis.install(framework, bundleUrl);
    }

    private Bundle getBundle(ManagementContext mgmt, final String symbolicName) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        Maybe<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName(symbolicName)
                .find();
        return result.get();
    }

    private void assertLoadFails(ClassLoaderUtils clu, String className) {
        try {
            clu.loadClass(className);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassNotFoundException e) {
            Asserts.expectedFailureContains(e, className, "not found on the application class path, nor in the bundle white list");
        }
    }
}
