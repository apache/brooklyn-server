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
package org.apache.brooklyn.core.mgmt.persist;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializer.OsgiClassLoader;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;

public class XmlMementoSerializerDelegatingClassLoaderTest {

    private LocalManagementContext mgmt;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (mgmt != null) {
            Entities.destroyAll(mgmt);
        }
    }
    
    @Test
    public void testLoadClassFromBundle() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_PATH);

        ClassLoader classLoader = getClass().getClassLoader();
        Bundle apiBundle = getBundle(mgmt, "org.apache.brooklyn.api");
        Bundle coreBundle = getBundle(mgmt, "org.apache.brooklyn.core");
        
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        Bundle otherBundle = installBundle(mgmt, bundleUrl);
        
        assertLoads(classLoader, Entity.class, Optional.of(apiBundle));
        assertLoads(classLoader, AbstractEntity.class, Optional.of(coreBundle));
        assertLoads(classLoader, OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY, Optional.of(otherBundle));
    }
    
    @Test
    public void testLoadClassVanilla() throws Exception {
        ClassLoader classLoader = getClass().getClassLoader();
        
        assertLoads(classLoader, Entity.class, Optional.<Bundle>absent());
        assertLoads(classLoader, AbstractEntity.class, Optional.<Bundle>absent());
    }
    
    // Tests we can do funny stuff, like return a differently named class from that expected!
    @Test
    public void testLoadClassReturningDifferentlyNamedClass() throws Exception {
        final String specialClassName = "my.madeup.Clazz";
        
        ClassLoader classLoader = new ClassLoader() {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                if (name != null && name.equals(specialClassName)) {
                    return Entity.class;
                }
                return getClass().getClassLoader().loadClass(name);
            }
        };
        
        OsgiClassLoader ocl = new XmlMementoSerializer.OsgiClassLoader(classLoader);
        ocl.setManagementContext(mgmt);
        assertEquals(ocl.loadClass(specialClassName), Entity.class);
        
        // TODO The line below fails: java.lang.ClassNotFoundException: my/madeup/Clazz
        //assertEquals(Class.forName(specialClassName, false, ocl).getName(), Entity.class.getName());
    }
    
    private void assertLoads(ClassLoader delegateClassLoader, Class<?> clazz, Optional<Bundle> bundle) throws Exception {
        OsgiClassLoader ocl = new XmlMementoSerializer.OsgiClassLoader(delegateClassLoader);
        ocl.setManagementContext(mgmt);
        String classname = (bundle.isPresent() ? bundle.get().getSymbolicName() + ":" : "") + clazz.getName();
        assertEquals(ocl.loadClass(classname), clazz);
        
        // TODO The line below fails, e.g.: java.lang.ClassNotFoundException: org/apache/brooklyn/api:org/apache/brooklyn/api/entity/Entity
        //assertEquals(Class.forName(classname, false, ocl), clazz);
        
    }

    private void assertLoads(ClassLoader delegateClassLoader, String clazz, Optional<Bundle> bundle) throws Exception {
        OsgiClassLoader ocl = new XmlMementoSerializer.OsgiClassLoader(delegateClassLoader);
        ocl.setManagementContext(mgmt);
        String classname = (bundle.isPresent() ? bundle.get().getSymbolicName() + ":" : "") + clazz;
        assertEquals(ocl.loadClass(classname).getName(), clazz);
        
        // TODO The line below fails, e.g.: java.lang.ClassNotFoundException: org/apache/brooklyn/test/resources/osgi/brooklyn-test-osgi-entities:org/apache/brooklyn/test/osgi/entities/SimpleEntity
        //assertEquals(Class.forName(classname, false, ocl).getName(), clazz);
    }

    private Bundle getBundle(ManagementContext mgmt, final String symbolicName) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        Maybe<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName(symbolicName)
                .find();
        return result.get();
    }
    
    private Bundle installBundle(ManagementContext mgmt, String bundleUrl) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        return Osgis.install(framework, bundleUrl);
    }
}
