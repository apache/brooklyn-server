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

import java.net.InetAddress;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoPersister.LookupContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.core.catalog.internal.CatalogItemBuilder;
import org.apache.brooklyn.core.catalog.internal.CatalogItemDtoAbstract;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiStandaloneTest;
import org.apache.brooklyn.core.mgmt.osgi.OsgiVersionMoreEntityTest;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.osgi.Osgis;
import org.apache.brooklyn.util.core.task.BasicTask;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.osgi.framework.Bundle;
import org.osgi.framework.launch.Framework;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import ch.qos.logback.classic.spi.ILoggingEvent;

public class XmlMementoSerializerTest {

    private static final Logger LOG = LoggerFactory.getLogger(XmlMementoSerializerTest.class);

    private XmlMementoSerializer<Object> serializer;
    private ManagementContext mgmt;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        serializer = new XmlMementoSerializer<Object>(XmlMementoSerializerTest.class.getClassLoader());
    }

    @AfterMethod(alwaysRun=true)
    private void tearDown() {
        if (mgmt != null) {
            Entities.destroyAllCatching(mgmt);
            mgmt = null;
        }
    }
    
    @Test
    public void testRenamedClass() throws Exception {
        serializer = new XmlMementoSerializer<Object>(XmlMementoSerializerTest.class.getClassLoader(),
                ImmutableMap.of("old.package.name.UserAndHostAndPort", UserAndHostAndPort.class.getName()));
        
        String serializedForm = Joiner.on("\n").join(
                "<org.apache.brooklyn.util.net.UserAndHostAndPort>",
                "<user>myuser</user>",
                "<hostAndPort>",
                "<host>myhost</host>",
                "<port>1234</port>",
                "<hasBracketlessColons>false</hasBracketlessColons>",
                "</hostAndPort>",
                "</org.apache.brooklyn.util.net.UserAndHostAndPort>");
        UserAndHostAndPort obj = UserAndHostAndPort.fromParts("myuser", "myhost", 1234);
        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                UserAndHostAndPort.class.getName(), "old.package.name.UserAndHostAndPort"));
    }

    @Test
    public void testRenamedStaticInner() throws Exception {
        serializer = new XmlMementoSerializer<Object>(XmlMementoSerializerTest.class.getClassLoader(),
                ImmutableMap.of("old.package.name.XmlMementoSerializerTest", XmlMementoSerializerTest.class.getName()));
        
        String serializedForm = Joiner.on("\n").join(
                "<org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MyStaticInner>",
                "<myStaticInnerField>myStaticInnerVal</myStaticInnerField>",
                "</org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MyStaticInner>");
        MyStaticInner obj = new MyStaticInner("myStaticInnerVal");
        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                XmlMementoSerializerTest.class.getName(), "old.package.name.XmlMementoSerializerTest"));
    }

    @Test
    public void testRenamedNonStaticInner() throws Exception {
        serializer = new XmlMementoSerializer<Object>(XmlMementoSerializerTest.class.getClassLoader(),
                ImmutableMap.of("old.package.name.XmlMementoSerializerTest", XmlMementoSerializerTest.class.getName()));
        
        String serializedForm = Joiner.on("\n").join(
                "<org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MyStaticInner_-MyNonStaticInner>",
                "<myNonStaticInnerField>myNonStaticInnerVal</myNonStaticInnerField>",
                "<this_-1>",
                "<myStaticInnerField>myStaticInnerVal</myStaticInnerField>",
                "</this_-1>",
                "</org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MyStaticInner_-MyNonStaticInner>");
        MyStaticInner outer = new MyStaticInner("myStaticInnerVal");
        MyStaticInner.MyNonStaticInner obj = outer.new MyNonStaticInner("myNonStaticInnerVal");
        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                XmlMementoSerializerTest.class.getName(), "old.package.name.XmlMementoSerializerTest"));
    }

    @Test
    public void testRenamedAnonymousInner() throws Exception {
        serializer = new XmlMementoSerializer<Object>(XmlMementoSerializerTest.class.getClassLoader(),
                ImmutableMap.of("old.package.name.XmlMementoSerializerTest", XmlMementoSerializerTest.class.getName()));
        
        String serializedForm = Joiner.on("\n").join(
                "<org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MySuper_-1>",
                "<mySuperField>mySuperVal</mySuperField>",
                "<myAnonymousInnerField>mySubVal</myAnonymousInnerField>",
                "</org.apache.brooklyn.core.mgmt.persist.XmlMementoSerializerTest_-MySuper_-1>");
        MySuper obj = MySuper.newAnonymousInner("mySuperVal", "mySubVal");
        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                XmlMementoSerializerTest.class.getName(), "old.package.name.XmlMementoSerializerTest"));
    }

    protected void runRenamed(String serializedForm, Object obj, Map<String, String> transforms) throws Exception {
        assertSerializeAndDeserialize(obj);
        
        assertEquals(serializer.fromString(serializedForm), obj, "serializedForm="+serializedForm);
        
        String transformedForm = serializedForm;
        for (Map.Entry<String, String> entry : transforms.entrySet()) {
            transformedForm = transformedForm.replaceAll(entry.getKey(), entry.getValue());
        }
        assertEquals(serializer.fromString(transformedForm), obj, "serializedForm="+transformedForm);
    }

    @Test
    public void testInetAddress() throws Exception {
        InetAddress obj = Networking.getInetAddressWithFixedName("1.2.3.4");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testMutableSet() throws Exception {
        Set<?> obj = MutableSet.of("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testLinkedHashSet() throws Exception {
        Set<String> obj = new LinkedHashSet<String>();
        obj.add("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testImmutableSet() throws Exception {
        Set<String> obj = ImmutableSet.of("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testMutableList() throws Exception {
        List<?> obj = MutableList.of("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testLinkedList() throws Exception {
        List<String> obj = new LinkedList<String>();
        obj.add("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testArraysAsList() throws Exception {
        // For some reason Arrays.asList used in the catalog's libraries can't be deserialized correctly,
        // but here works perfectly - the generated catalog xml contains
        //    <libraries class="list">
        //      <a ...>
        //        <bundle....>
        // which is deserialized as an ArrayList with a single member array of bundles.
        // The cause is the class="list" type which should be java.util.Arrays$ArrayList instead.
        Collection<String> obj = Arrays.asList("a", "b");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testImmutableList() throws Exception {
        List<String> obj = ImmutableList.of("123");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testMutableMap() throws Exception {
        Map<?,?> obj = MutableMap.of("mykey", "myval");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testLinkedHashMap() throws Exception {
        Map<String,String> obj = new LinkedHashMap<String,String>();
        obj.put("mykey", "myval");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testImmutableMap() throws Exception {
        Map<?,?> obj = ImmutableMap.of("mykey", "myval");
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testSensorWithTypeToken() throws Exception {
        AttributeSensor<Map<String, Object>> obj = Attributes.SERVICE_NOT_UP_DIAGNOSTICS;
        assertSerializeAndDeserialize(obj);
    }

    @Test
    public void testClass() throws Exception {
        Class<?> t = XmlMementoSerializer.class;
        assertSerializeAndDeserialize(t);
    }

    @Test
    public void testEntity() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        ManagementContext managementContext = app.getManagementContext();
        try {
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, true).add(app));
            assertSerializeAndDeserialize(app);
        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    private LookupContextImpl newEmptyLookupManagementContext(ManagementContext managementContext, boolean failOnDangling) {
        return new LookupContextImpl(managementContext,
                ImmutableList.<Entity>of(), ImmutableList.<Location>of(), ImmutableList.<Policy>of(),
                ImmutableList.<Enricher>of(), ImmutableList.<Feed>of(), ImmutableList.<CatalogItem<?, ?>>of(), ImmutableList.<ManagedBundle>of(), failOnDangling);
    }

    @Test
    public void testLocation() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        ManagementContext managementContext = app.getManagementContext();
        try {
            final Location loc = managementContext.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, true).add(loc));
            assertSerializeAndDeserialize(loc);
        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    @Test
    public void testCatalogItem() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        ManagementContext managementContext = app.getManagementContext();
        try {
            CatalogItem<?, ?> catalogItem = CatalogItemBuilder.newEntity("symbolicName", "0.0.1")
                    .displayName("test catalog item")
                    .description("description")
                    .plan("yaml plan")
                    .iconUrl("iconUrl")
                    .libraries(CatalogItemDtoAbstract.parseLibraries(ImmutableList.of("library-url")))
                    .build();
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, true).add(catalogItem));
            assertSerializeAndDeserialize(catalogItem);
        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    @Test
    public void testEntitySpec() throws Exception {
        EntitySpec<?> obj = EntitySpec.create(TestEntity.class);
        assertSerializeAndDeserialize(obj);
    }
    
    @Test
    public void testEntitySpecNested() throws Exception {
        EntitySpec<?> obj = EntitySpec.create(TestEntity.class)
                .configure("nest1", EntitySpec.create(TestEntity.class)
                        .configure("nest2", EntitySpec.create(TestEntity.class)));
        assertSerializeAndDeserialize(obj);
    }
    
    @Test
    public void testEntitySpecManyConcurrently() throws Exception {
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        List<ListenableFuture<Void>> futures = Lists.newArrayList();
        try {
            for (int i = 0; i < 100; i++) {
                futures.add(executor.submit(new Callable<Void>() {
                    @Override public Void call() throws Exception {
                        EntitySpec<?> obj = EntitySpec.create(TestEntity.class);
                        assertSerializeAndDeserialize(obj);
                        return null;
                    }}));
            }
            Futures.allAsList(futures).get();
            
        } finally {
            executor.shutdownNow();
        }
    }
    
    @Test
    public void testEntitySpecFromOsgi() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_V1_PATH);
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        
        RegisteredType ci = OsgiVersionMoreEntityTest.addMoreEntityV1(mgmt, "1.0");
            
        EntitySpec<DynamicCluster> spec = EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.INITIAL_SIZE, 1)
            .configure(DynamicCluster.MEMBER_SPEC, mgmt.getTypeRegistry().createSpec(ci, null, EntitySpec.class));
        
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));
        assertSerializeAndDeserialize(spec);
    }
    
    @Test
    public void testOsgiBundleNameNotIncludedForWhiteListed() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();

        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));
        
        Object obj = PersistMode.AUTO;
        
        assertSerializeAndDeserialize(obj);

        // i.e. not pre-pended with "org.apache.brooklyn.core:"
        String expectedForm = "<"+PersistMode.class.getName()+">AUTO</"+PersistMode.class.getName()+">";
        String serializedForm = serializer.toString(obj);
        assertEquals(serializedForm.trim(), expectedForm.trim());
    }

    @Test
    public void testOsgiBundleNamePrefixIncluded() throws Exception {
        String bundlePath = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_PATH;
        String bundleUrl = OsgiStandaloneTest.BROOKLYN_TEST_OSGI_ENTITIES_URL;
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));
        
        Bundle bundle = installBundle(mgmt, bundleUrl);
        
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_SIMPLE_OBJECT;
        Class<?> osgiObjectClazz = bundle.loadClass(classname);
        Object obj = Reflections.invokeConstructorFromArgs(osgiObjectClazz, "myval").get();

        assertSerializeAndDeserialize(obj);

        // i.e. prepended with bundle name
        String expectedForm = Joiner.on("\n").join(
                "<"+bundle.getSymbolicName()+":"+classname+">",
                "  <val>myval</val>",
                "</"+bundle.getSymbolicName()+":"+classname+">");
        String WHITE_LIST_KEY = "org.apache.brooklyn.classloader.fallback.bundles";
        String origWhiteList = System.getProperty(WHITE_LIST_KEY);
        System.setProperty(WHITE_LIST_KEY, "some.none.existend.whitelist:1.0.0");
        try {
            String serializedForm = serializer.toString(obj);
            assertEquals(serializedForm.trim(), expectedForm.trim());
        } finally {
            if (origWhiteList != null) {
                System.setProperty(WHITE_LIST_KEY, origWhiteList);
            } else {
                System.clearProperty(WHITE_LIST_KEY);
            }
        }
    }

    // A sanity-check, to confirm that normal loading works (before we look at success/failure of rename tests)
    @Test
    public void testNoRenameOsgiClass() throws Exception {
        String bundlePath = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH;
        String bundleUrl = "classpath:" + bundlePath;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_OBJECT;
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = installBundle(mgmt, bundleUrl);

        String bundlePrefix = bundle.getSymbolicName();
        Class<?> osgiObjectClazz = bundle.loadClass(classname);
        Object obj = Reflections.invokeConstructorFromArgs(osgiObjectClazz, "myval").get();

        serializer = new XmlMementoSerializer<Object>(mgmt.getCatalogClassLoader(),
                ImmutableMap.<String,String>of());
        
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));

        // i.e. prepended with bundle name
        String serializedForm = Joiner.on("\n").join(
                "<"+bundlePrefix+":"+classname+">",
                "  <val>myval</val>",
                "</"+bundlePrefix+":"+classname+">");

        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of());
    }

    @Test
    public void testRenamedOsgiClassMovedBundle() throws Exception {
        String bundlePath = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH;
        String bundleUrl = "classpath:" + bundlePath;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_OBJECT;
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = installBundle(mgmt, bundleUrl);
        
        String oldBundlePrefix = "com.old.symbolicname";
        
        String bundlePrefix = bundle.getSymbolicName();
        Class<?> osgiObjectClazz = bundle.loadClass(classname);
        Object obj = Reflections.invokeConstructorFromArgs(osgiObjectClazz, "myval").get();

        serializer = new XmlMementoSerializer<Object>(mgmt.getCatalogClassLoader(),
                ImmutableMap.of(oldBundlePrefix + ":" + classname, bundlePrefix + ":" + classname));
        
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));

        // i.e. prepended with bundle name
        String serializedForm = Joiner.on("\n").join(
                "<"+bundlePrefix+":"+classname+">",
                "  <val>myval</val>",
                "</"+bundlePrefix+":"+classname+">");

        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                bundlePrefix + ":" + classname, oldBundlePrefix + ":" + classname));
    }

    @Test
    public void testRenamedOsgiClassWithoutBundlePrefixInRename() throws Exception {
        String bundlePath = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH;
        String bundleUrl = "classpath:" + bundlePath;
        String classname = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_OBJECT;
        String oldClassname = "com.old.package.name.OldClassName";
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), bundlePath);
        
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        Bundle bundle = installBundle(mgmt, bundleUrl);
        
        String bundlePrefix = bundle.getSymbolicName();
        
        Class<?> osgiObjectClazz = bundle.loadClass(classname);
        Object obj = Reflections.invokeConstructorFromArgs(osgiObjectClazz, "myval").get();

        serializer = new XmlMementoSerializer<Object>(mgmt.getCatalogClassLoader(),
                ImmutableMap.of(oldClassname, classname));
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));

        // i.e. prepended with bundle name
        String serializedForm = Joiner.on("\n").join(
                "<"+bundlePrefix+":"+classname+">",
                "  <val>myval</val>",
                "</"+bundlePrefix+":"+classname+">");

        runRenamed(serializedForm, obj, ImmutableMap.<String, String>of(
                bundlePrefix + ":" + classname, bundlePrefix + ":" + oldClassname));
    }

    // TODO This doesn't get the bundleName - should we expect it to? Is this because of 
    // how we're using Felix? Would it also be true in Karaf?
    @Test(groups="Broken")
    public void testOsgiBundleNamePrefixIncludedForDownstreamDependency() throws Exception {
        mgmt = LocalManagementContextForTests.builder(true).disableOsgi(false).build();
        serializer.setLookupContext(newEmptyLookupManagementContext(mgmt, true));
        
        // Using a guava type (which is a downstream dependency of Brooklyn)
        String bundleName = "com.goole.guava";
        String classname = "com.google.common.base.Predicates_-ObjectPredicate";
        Object obj = Predicates.alwaysTrue();

        assertSerializeAndDeserialize(obj);

        // i.e. prepended with bundle name
        String expectedForm = "<"+bundleName+":"+classname+">ALWAYS_TRUE</"+bundleName+":"+classname+">";
        String serializedForm = serializer.toString(obj);
        assertEquals(serializedForm.trim(), expectedForm.trim());
    }
    
    @Test
    public void testImmutableCollectionsWithDanglingEntityRef() throws Exception {
        // If there's a dangling entity in an ImmutableList etc, then discard it entirely.
        // If we try to insert null then it fails, breaking the deserialization of that entire file.
        
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        ManagementContext managementContext = app.getManagementContext();
        try {
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, false).add(app));
            
            List<?> resultList = serializeAndDeserialize(ImmutableList.of(app, entity));
            assertEquals(resultList, ImmutableList.of(app));
            
            Set<?> resultSet = serializeAndDeserialize(ImmutableSet.of(app, entity));
            assertEquals(resultSet, ImmutableSet.of(app));
            
            Map<?, ?> resultMap = serializeAndDeserialize(ImmutableMap.of(app, "appval", "appkey", app, entity, "entityval", "entityKey", entity));
            assertEquals(resultMap, ImmutableMap.of(app, "appval", "appkey", app));

        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    @Test
    public void testFieldReffingEntity() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        ReffingEntity reffer = new ReffingEntity(app);
        ManagementContext managementContext = app.getManagementContext();
        try {
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, true).add(app));
            ReffingEntity reffer2 = assertSerializeAndDeserialize(reffer);
            assertEquals(reffer2.entity, app);
        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    @Test
    public void testUntypedFieldReffingEntity() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        ReffingEntity reffer = new ReffingEntity((Object)app);
        ManagementContext managementContext = app.getManagementContext();
        try {
            serializer.setLookupContext(newEmptyLookupManagementContext(managementContext, true).add(app));
            ReffingEntity reffer2 = assertSerializeAndDeserialize(reffer);
            assertEquals(reffer2.obj, app);
        } finally {
            Entities.destroyAll(managementContext);
        }
    }

    @Test
    public void testTask() throws Exception {
        final TestApplication app = TestApplication.Factory.newManagedInstanceForTests();
        mgmt = app.getManagementContext();
        Task<String> completedTask = app.getExecutionContext().submit(Callables.returning("myval"));
        completedTask.get();
        
        String loggerName = UnwantedStateLoggingMapper.class.getName();
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.WARN;
        Predicate<ILoggingEvent> filter = EventPredicates.containsMessage("Task object serialization is not supported or recommended"); 
        LogWatcher watcher = new LogWatcher(loggerName, logLevel, filter);
        
        String serializedForm;
        watcher.start();
        try {
            serializedForm = serializer.toString(completedTask);
            watcher.assertHasEvent();
        } finally {
            watcher.close();
        }

        assertEquals(serializedForm.trim(), "<"+BasicTask.class.getName()+">myval</"+BasicTask.class.getName()+">");
        Object deserialized = serializer.fromString(serializedForm);
        assertEquals(deserialized, null, "serializedForm="+serializedForm+"; deserialized="+deserialized);
    }

    public static class ReffingEntity {
        public Entity entity;
        public Object obj;
        public ReffingEntity(Entity entity) {
            this.entity = entity;
        }
        public ReffingEntity(Object obj) {
            this.obj = obj;
        }
        @Override
        public boolean equals(Object o) {
            return (o instanceof ReffingEntity) && Objects.equal(entity, ((ReffingEntity)o).entity) && Objects.equal(obj, ((ReffingEntity)o).obj);
        }
        @Override
        public int hashCode() {
            return Objects.hashCode(entity, obj);
        }
    }

    @SuppressWarnings("unchecked")
    private <T> T assertSerializeAndDeserialize(T obj) throws Exception {
        String serializedForm = serializer.toString(obj);
        LOG.info("serializedForm=" + serializedForm);
        Object deserialized = serializer.fromString(serializedForm);
        assertEquals(deserialized, obj, "serializedForm="+serializedForm);
        return (T) deserialized;
    }

    @SuppressWarnings("unchecked")
    private <T> T serializeAndDeserialize(T obj) throws Exception {
        String serializedForm = serializer.toString(obj);
        LOG.info("serializedForm=" + serializedForm);
        return (T) serializer.fromString(serializedForm);
    }
    
    static class LookupContextImpl implements LookupContext {
        private final ManagementContext mgmt;
        private final Map<String, Entity> entities;
        private final Map<String, Location> locations;
        private final Map<String, Policy> policies;
        private final Map<String, Enricher> enrichers;
        private final Map<String, Feed> feeds;
        private final Map<String, CatalogItem<?, ?>> catalogItems;
        private final Map<String, ManagedBundle> bundles;
        private final boolean failOnDangling;

        LookupContextImpl(ManagementContext mgmt, Iterable<? extends Entity> entities, Iterable<? extends Location> locations,
                Iterable<? extends Policy> policies, Iterable<? extends Enricher> enrichers, Iterable<? extends Feed> feeds,
                Iterable<? extends CatalogItem<?, ?>> catalogItems, Iterable<? extends ManagedBundle> bundles,
                    boolean failOnDangling) {
            this.mgmt = mgmt;
            this.entities = Maps.newLinkedHashMap();
            this.locations = Maps.newLinkedHashMap();
            this.policies = Maps.newLinkedHashMap();
            this.enrichers = Maps.newLinkedHashMap();
            this.feeds = Maps.newLinkedHashMap();
            this.catalogItems = Maps.newLinkedHashMap();
            this.bundles = Maps.newLinkedHashMap();
            for (Entity entity : entities) this.entities.put(entity.getId(), entity);
            for (Location location : locations) this.locations.put(location.getId(), location);
            for (Policy policy : policies) this.policies.put(policy.getId(), policy);
            for (Enricher enricher : enrichers) this.enrichers.put(enricher.getId(), enricher);
            for (Feed feed : feeds) this.feeds.put(feed.getId(), feed);
            for (CatalogItem<?, ?> catalogItem : catalogItems) this.catalogItems.put(catalogItem.getId(), catalogItem);
            for (ManagedBundle bundle : bundles) this.bundles.put(bundle.getId(), bundle);
            this.failOnDangling = failOnDangling;
        }
        LookupContextImpl(ManagementContext mgmt, Map<String,? extends Entity> entities, Map<String,? extends Location> locations,
                Map<String,? extends Policy> policies, Map<String,? extends Enricher> enrichers, Map<String,? extends Feed> feeds,
                Map<String, ? extends CatalogItem<?, ?>> catalogItems, Map<String,? extends ManagedBundle> bundles,
                boolean failOnDangling) {
            this.mgmt = mgmt;
            this.entities = ImmutableMap.copyOf(entities);
            this.locations = ImmutableMap.copyOf(locations);
            this.policies = ImmutableMap.copyOf(policies);
            this.enrichers = ImmutableMap.copyOf(enrichers);
            this.feeds = ImmutableMap.copyOf(feeds);
            this.catalogItems = ImmutableMap.copyOf(catalogItems);
            this.bundles = ImmutableMap.copyOf(bundles);
            this.failOnDangling = failOnDangling;
        }
        @Override public ManagementContext lookupManagementContext() {
            return mgmt;
        }
        @Override public Entity lookupEntity(String id) {
            if (entities.containsKey(id)) {
                return entities.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no entity with id "+id+"; contenders are "+entities.keySet());
            }
            return null;
        }
        @Override public Location lookupLocation(String id) {
            if (locations.containsKey(id)) {
                return locations.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no location with id "+id+"; contenders are "+locations.keySet());
            }
            return null;
        }
        @Override public Policy lookupPolicy(String id) {
            if (policies.containsKey(id)) {
                return policies.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no policy with id "+id+"; contenders are "+policies.keySet());
            }
            return null;
        }
        @Override public Enricher lookupEnricher(String id) {
            if (enrichers.containsKey(id)) {
                return enrichers.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no enricher with id "+id+"; contenders are "+enrichers.keySet());
            }
            return null;
        }
        @Override public Feed lookupFeed(String id) {
            if (feeds.containsKey(id)) {
                return feeds.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no feed with id "+id+"; contenders are "+feeds.keySet());
            }
            return null;
        }
        @Override public CatalogItem<?, ?> lookupCatalogItem(String id) {
            if (catalogItems.containsKey(id)) {
                return catalogItems.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no catalog item with id "+id+"; contenders are "+catalogItems.keySet());
            }
            return null;
        }
        @Override public ManagedBundle lookupBundle(String id) {
            if (bundles.containsKey(id)) {
                return bundles.get(id);
            }
            if (failOnDangling) {
                throw new NoSuchElementException("no bundle with id "+id+"; contenders are "+bundles.keySet());
            }
            return null;
        }
        
        @Override
        public BrooklynObject lookup(BrooklynObjectType type, String id) {
            if (type==null) {
                BrooklynObject result = peek(null, id);
                if (result==null) {
                    if (failOnDangling) {
                        throw new NoSuchElementException("no brooklyn object with id "+id+"; type not specified");
                    }                    
                }
                type = BrooklynObjectType.of(result);
            }
            
            switch (type) {
            case CATALOG_ITEM: return lookupCatalogItem(id);
            case MANAGED_BUNDLE: return lookupBundle(id);
            case ENRICHER: return lookupEnricher(id);
            case ENTITY: return lookupEntity(id);
            case FEED: return lookupFeed(id);
            case LOCATION: return lookupLocation(id);
            case POLICY: return lookupPolicy(id);
            case UNKNOWN: return null;
            }
            throw new IllegalStateException("Unexpected type "+type+" / id "+id);
        }
        
        private Map<String,? extends BrooklynObject> getMapFor(BrooklynObjectType type) {
            switch (type) {
            case CATALOG_ITEM: return catalogItems;
            case MANAGED_BUNDLE: return bundles;
            case ENRICHER: return enrichers;
            case ENTITY: return entities;
            case FEED: return feeds;
            case LOCATION: return locations;
            case POLICY: return policies;
            case UNKNOWN: return null;
            }
            throw new IllegalStateException("Unexpected type "+type);
        }
        
        @Override
        public BrooklynObject peek(BrooklynObjectType type, String id) {
            if (type==null) {
                for (BrooklynObjectType typeX: BrooklynObjectType.values()) {
                    BrooklynObject result = peek(typeX, id);
                    if (result!=null) return result;
                }
                return null;
            }
            
            Map<String, ? extends BrooklynObject> map = getMapFor(type);
            if (map==null) return null;
            return map.get(id);
        }
        
        @SuppressWarnings("unchecked")
        @VisibleForTesting
        public LookupContextImpl add(BrooklynObject object) {
            if (object!=null) {
                ((Map<String,BrooklynObject>) getMapFor(BrooklynObjectType.of(object))).put(object.getId(), object);
            }
            return this;
        }
    };

    public static class MySuper {
        public String mySuperField;
        
        public static MySuper newAnonymousInner(final String superVal, final String subVal) {
            MySuper result = new MySuper() {
                public String myAnonymousInnerField = subVal;
                
                @Override public boolean equals(Object obj) {
                    return (obj != null) && (obj.getClass() == getClass()) && super.equals(obj);
                }
                @Override public int hashCode() {
                    return Objects.hashCode(super.hashCode(), myAnonymousInnerField);
                }
            };
            result.mySuperField = superVal;
            return result;
        }
        @Override public boolean equals(Object obj) {
            return (obj instanceof MySuper) 
                    && mySuperField.equals(((MySuper)obj).mySuperField);
        }
        @Override public int hashCode() {
            return Objects.hashCode(mySuperField);
        }
    }
    
    public static class MyStaticInner {
        public class MyNonStaticInner {
            public String myNonStaticInnerField;
            
            public MyNonStaticInner(String val) {
                this.myNonStaticInnerField = val;
            }
            @Override public boolean equals(Object obj) {
                return (obj instanceof MyNonStaticInner) 
                        && myNonStaticInnerField.equals(((MyNonStaticInner)obj).myNonStaticInnerField);
            }
            @Override public int hashCode() {
                return Objects.hashCode(myNonStaticInnerField);
            }
        }

        public String myStaticInnerField;
        
        public MyStaticInner(String val) {
            this.myStaticInnerField = val;
        }
        @Override public boolean equals(Object obj) {
            return (obj instanceof MyStaticInner) 
                    && myStaticInnerField.equals(((MyStaticInner)obj).myStaticInnerField);
        }
        @Override public int hashCode() {
            return Objects.hashCode(myStaticInnerField);
        }
    }
    
    private Bundle installBundle(ManagementContext mgmt, String bundleUrl) throws Exception {
        OsgiManager osgiManager = ((ManagementContextInternal)mgmt).getOsgiManager().get();
        Framework framework = osgiManager.getFramework();
        return Osgis.install(framework, bundleUrl);
    }
    
    @Test
    public void testConfigInheritanceVals() throws Exception {
        ConfigInheritance val = BasicConfigInheritance.NEVER_INHERITED;

        ConfigInheritance newVal = assertSerializeAndDeserialize(val);
        Assert.assertSame(val, newVal);
    }
    
}