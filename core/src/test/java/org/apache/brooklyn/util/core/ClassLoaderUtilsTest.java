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

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.net.URL;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.catalog.CatalogItem.CatalogBundle;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.BrooklynVersion;
import org.apache.brooklyn.core.catalog.internal.CatalogBundleDto;
import org.apache.brooklyn.core.catalog.internal.CatalogEntityItemDto;
import org.apache.brooklyn.core.catalog.internal.CatalogItemBuilder;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.maven.MavenArtifact;
import org.apache.brooklyn.util.maven.MavenRetriever;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.osgi.VersionedName;
import org.apache.commons.io.output.ByteArrayOutputStream;
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
        assertLoadSucceeds(clu, getClass().getName(), getClass());
        assertLoadSucceeds(clu, Entity.class.getName(), Entity.class);
        assertLoadSucceeds(clu, AbstractEntity.class.getName(), AbstractEntity.class);
        assertLoadFails(clu, "org.apache.brooklyn.this.name.does.not.Exist");
    }

    @Test
    public void testLoadClassInOsgi() throws Exception {
        String bundlePath = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;
        
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = installBundle(mgmt, bundleUrl, false /* this works without starting it */);
        @SuppressWarnings("unchecked")
        Class<? extends Entity> clazz = (Class<? extends Entity>) bundle.loadClass(classname);
        Entity entity = createSimpleEntity(bundleUrl, clazz);

        System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, "do.not.match.whitelist:1.0.0");

        ClassLoaderUtils cluMgmt = new ClassLoaderUtils(getClass(), mgmt);
        ClassLoaderUtils cluClass = new ClassLoaderUtils(clazz);
        ClassLoaderUtils cluEntity = new ClassLoaderUtils(getClass(), entity);

        assertLoadFails(classname, cluMgmt);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + classname, clazz, cluMgmt, cluClass, cluEntity);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + bundle.getVersion()+":" + classname, clazz, cluMgmt, cluClass, cluEntity);

        // we can also load classpath url in the scope of the entity
        ResourceUtils.create(entity).getResourceAsString("classpath://"+classname.replaceAll("\\.", "/")+".class");
        // (but not in global scope)
        Asserts.assertFailsWith(() -> ResourceUtils.create(null).getResourceAsString("classpath://"+classname.replaceAll("\\.", "/")+".class"),
            e -> Asserts.expectedFailureContainsIgnoreCase(e, "not found on classpath", "SimpleEntity"));

        // test load still works when we have the item in the search path only but not a catalog item id on the entity
        ((EntityInternal)entity).setCatalogItemIdAndSearchPath(null, MutableList.of(entity.getCatalogItemId()));
        ResourceUtils.create(entity).getResourceAsString("classpath://"+classname.replaceAll("\\.", "/")+".class");

        Entity e2 = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicEntity.class).addSearchPath(MutableList.of(bundle.getSymbolicName()+":"+bundle.getVersion())));
        ResourceUtils.create(e2).getResourceAsString("classpath://"+classname.replaceAll("\\.", "/")+".class");
    }

    @Test
    public void testLoadJustOneClassInOsgiWhiteList() throws Exception {
        String bundlePath = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;

        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = installBundle(mgmt, bundleUrl, false /* this works without starting it */);
        Class<?> clazz = bundle.loadClass(classname);
        Entity entity = createSimpleEntity(bundleUrl, clazz);

        String whiteList = bundle.getSymbolicName()+":"+bundle.getVersion();
        System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, whiteList);

        ClassLoaderUtils cluEntity = new ClassLoaderUtils(getClass(), entity);

        BundledName resource = new BundledName(classname).toResource();
        BundledName bn = new BundledName(resource.bundle, resource.version, "/" + resource.name);
        Asserts.assertSize(cluEntity.getResources(bn.toString()), 2);  // should no longer mask the other available option
    }


    @Test
    public void testVariousLoadersLoadClassInOsgiWhiteList() throws Exception {
        String bundlePath = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;

        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = installBundle(mgmt, bundleUrl, true);

        Class<?> clazz = bundle.loadClass(classname);
        Entity entity = createSimpleEntity(bundleUrl, clazz);

        String whiteList = bundle.getSymbolicName()+":"+bundle.getVersion();
        System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, whiteList);

        ClassLoaderUtils cluMgmt = new ClassLoaderUtils(getClass(), mgmt);
        ClassLoaderUtils cluClass = new ClassLoaderUtils(clazz);
        ClassLoaderUtils cluEntity = new ClassLoaderUtils(getClass(), entity);

        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluEntity);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + classname, clazz, cluMgmt, cluClass, cluEntity);
    }

    @Test
    public void testLoadClassInOsgiWhiteListWithInvalidBundlePresent() throws Exception {
        String bundlePath = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_ENTITY;

        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = installBundle(mgmt, bundleUrl, true);

        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        JarOutputStream target = new JarOutputStream(buffer, manifest);
        target.close();

        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        Bundle installedBundle = framework.getBundleContext().installBundle("stream://invalid", new ByteArrayInputStream(buffer.toByteArray()));
        assertNotNull(installedBundle);

        Class<?> clazz = bundle.loadClass(classname);
        Entity entity = createSimpleEntity(bundleUrl, clazz);

        String whileList = bundle.getSymbolicName()+":"+bundle.getVersion();
        System.setProperty(ClassLoaderUtils.WHITE_LIST_KEY, whileList);

        ClassLoaderUtils cluMgmt = new ClassLoaderUtils(getClass(), mgmt);
        ClassLoaderUtils cluClass = new ClassLoaderUtils(clazz);
        ClassLoaderUtils cluEntity = new ClassLoaderUtils(getClass(), entity);

        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluEntity);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + classname, clazz, cluMgmt, cluClass, cluEntity);
    }

    @Test
    public void testLoadClassInOsgiCore() throws Exception {
        Class<?> clazz = BasicEntity.class;
        String classname = clazz.getName();

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = getBundle(mgmt, "org.apache.brooklyn.core");
        String url = bundle.getLocation();
        // NB: the above will be a system:file: url when running tests against target/classes/ -- but
        // OSGi manager will accept that if running in dev mode
        Entity entity = createSimpleEntity(url, clazz);

        ClassLoaderUtils cluMgmt = new ClassLoaderUtils(getClass(), mgmt);
        ClassLoaderUtils cluClass = new ClassLoaderUtils(clazz);
        ClassLoaderUtils cluNone = new ClassLoaderUtils(getClass());
        ClassLoaderUtils cluEntity = new ClassLoaderUtils(getClass(), entity);

        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluNone, cluEntity);
        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluNone, cluEntity);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + classname, clazz, cluMgmt, cluClass, cluNone, cluEntity);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + bundle.getVersion() + ":" + classname, clazz, cluMgmt, cluClass, cluNone, cluEntity);
    }

    @Test
    public void testLoadClassInOsgiApi() throws Exception {
        Class<?> clazz = Entity.class;
        String classname = clazz.getName();

        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        Bundle bundle = getBundle(mgmt, "org.apache.brooklyn.api");

        ClassLoaderUtils cluMgmt = new ClassLoaderUtils(getClass(), mgmt);
        ClassLoaderUtils cluClass = new ClassLoaderUtils(clazz);
        ClassLoaderUtils cluNone = new ClassLoaderUtils(getClass());

        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluNone);
        assertLoadSucceeds(classname, clazz, cluMgmt, cluClass, cluNone);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + classname, clazz, cluMgmt, cluClass, cluNone);
        assertLoadSucceeds(bundle.getSymbolicName() + ":" + bundle.getVersion() + ":" + classname, clazz, cluMgmt, cluClass, cluNone);
    }

    @Test
    public void testIsBundleWhiteListed() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
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
        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        ClassLoaderUtils clu = new ClassLoaderUtils(getClass(), mgmt);

        String bundleUrl = MavenRetriever.localUrl(MavenArtifact.fromCoordinate("com.google.guava:guava:jar:18.0"));
        Bundle bundle = installBundle(mgmt, bundleUrl, false /* this works without starting */);
        String bundleName = bundle.getSymbolicName();

        String classname = bundleName + ":" + ImmutableList.class.getName();
        assertLoadSucceeds(clu, classname, ImmutableList.class);
    }

    @Test
    public void testLoadBrooklynClass() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).enableOsgiReusable().build();
        new ClassLoaderUtils(this, mgmt).loadClass(
                "org.apache.brooklyn.api",
                BrooklynVersion.get(),
                Entity.class.getName());
        new ClassLoaderUtils(this, mgmt).loadClass(
                "org.apache.brooklyn.api",
                BrooklynVersion.getOsgiVersion(),
                Entity.class.getName());
        try {
            new ClassLoaderUtils(this, mgmt).loadClass(
                    "org.apache.brooklyn.api",
                    "100.100.100-alpha-version_wth.tags",
                    "org.apache.brooklyn.NonExistentClass");  // actual classes may be found on the classpath
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            IllegalStateException nested = Exceptions.getFirstThrowableOfType(e, IllegalStateException.class);
            assertNotNull(nested);
            Asserts.expectedFailureContains(nested, "not found to load", "org.apache.brooklyn.api", "100.100.100");
        }
        try {
            new ClassLoaderUtils(this, mgmt).loadClass(
                    "org.apache.brooklyn.api",
                    "100.100.100-SNAPSHOT",
                    "org.apache.brooklyn.NonExistentClass");  // actual classes may be found on the classpath
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            IllegalStateException nested = Exceptions.getFirstThrowableOfType(e, IllegalStateException.class);
            assertNotNull(nested);
            Asserts.expectedFailureContains(nested, "not found to load", "org.apache.brooklyn.api", "100.100.100");
        }
    }

    private Bundle installBundle(ManagementContext mgmt, String bundleUrl, boolean start) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        Bundle result = Osgis.install(framework, bundleUrl);
        if (start) result.start();
        return result;
    }

    private Bundle getBundle(ManagementContext mgmt, final String symbolicName) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        Maybe<Bundle> result = Osgis.bundleFinder(framework)
                .symbolicName(symbolicName)
                .find();
        return result.get();
    }

    private void assertLoadSucceeds(String bundledClassName, Class<?> expectedClass, ClassLoaderUtils... clua) throws ClassNotFoundException {
        for (ClassLoaderUtils clu : clua) {
            try {
                assertLoadSucceeds(clu, bundledClassName, expectedClass);
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                throw new IllegalStateException("Load failed for " + clu, e);
            }
        }
    }

    private void assertLoadSucceeds(ClassLoaderUtils clu, String bundledClassName, Class<?> expectedClass) throws ClassNotFoundException {
        BundledName className = new BundledName(bundledClassName);

        Class<?> cls = clu.loadClass(bundledClassName);
        assertEquals(cls.getName(), className.name);
        if (expectedClass != null) {
            assertEquals(cls, expectedClass);
        }

        ClassLoader cl = cls.getClassLoader();
        BundledName resource = className.toResource();
        String bundledResource = resource.toString();
        URL resourceUrl = cl.getResource(resource.name);
        assertEquals(clu.getResource(bundledResource), resourceUrl);
        Iterable<URL> allMatches = clu.getResources(bundledResource);
        assertEquals(allMatches.iterator().next(), resourceUrl, "Loading with "+clu);
        // may have others, eg if the bundle provides it via multiple sources

        BundledName rootResource = new BundledName(resource.bundle, resource.version, "/" + resource.name);
        String rootBundledResource = rootResource.toString();
        assertEquals(clu.getResource(rootBundledResource), resourceUrl);
        allMatches = clu.getResources(rootBundledResource);
        assertEquals(allMatches.iterator().next(), resourceUrl);
    }

    private void assertLoadFails(String bundledClassName, ClassLoaderUtils... clua) {
        for (ClassLoaderUtils clu : clua) {
            assertLoadFails(clu, bundledClassName);
        }
    }

    private void assertLoadFails(ClassLoaderUtils clu, String bundledClassName) {
        BundledName className = new BundledName(bundledClassName);

        try {
            clu.loadClass(bundledClassName);
            Asserts.shouldHaveFailedPreviously("Using loader " + clu);
        } catch (ClassNotFoundException e) {
            Asserts.expectedFailureContains(e, bundledClassName, "not found on the application class path, nor in the bundle white list");
        }

        BundledName resource = className.toResource();
        String bundledResource = resource.toString();
        assertNull(clu.getResource(bundledResource), resource + " is supposed to fail resource loading, but it was successful");
        assertEquals(clu.getResources(bundledResource), ImmutableList.of(), resource + " is supposed to fail resource loading, but it was successful");
    }

    protected Entity createSimpleEntity(String bundleUrl, Class<?> clazz) {
        @SuppressWarnings("unchecked")
        Class<? extends Entity> entityClass = (Class<? extends Entity>) clazz;
        EntitySpec<?> spec = EntitySpec.create(entityClass);
        Entity entity = mgmt.getEntityManager().createEntity(spec);
        CatalogEntityItemDto item = CatalogItemBuilder.newEntity(clazz.getName(), "1.0.0")
                .libraries(ImmutableList.<CatalogBundle>of(new CatalogBundleDto(null, null, bundleUrl)))
                .plan("{\"services\":[{\"type\": \"" + clazz.getName() + "\"}]}")
                .build();
        mgmt.getCatalog().addItem(item);
        final EntityInternal entityInternal = (EntityInternal) entity;
        entityInternal.setCatalogItemIdAndSearchPath(item.getCatalogItemId(), item.getCatalogItemIdSearchPath());
        return entity;
    }

    private static class BundledName {
        String bundle;
        String version;
        String name;
        BundledName(String bundledName) {
            String[] arr = bundledName.split(":");
            if (arr.length == 1) {
                bundle = null;
                version = null;
                name = arr[0];
            } else if (arr.length == 2) {
                bundle = arr[0];
                version = null;
                name = arr[1];
            } else if (arr.length == 3) {
                bundle = arr[0];
                version = arr[1];
                name = arr[2];
            } else {
                throw new IllegalStateException("Invalid bundled name " + bundledName);
            }
        }
        BundledName(@Nullable String bundle, @Nullable String version, String name) {
            this.bundle = bundle;
            this.version = version;
            this.name = checkNotNull(name, "name");
        }

        @Override
        public String toString() {
            return (bundle != null ? bundle + ":" : "") +
                    (version != null ? version + ":" : "") +
                    name;
        }

        BundledName toResource() {
            return new BundledName(bundle, version, name.replace(".", "/") + ".class");
        }
    }

}
