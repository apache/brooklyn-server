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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.mgmt.ha.OsgiBundleInstallationResult;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiVersionMoreEntityTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.apache.brooklyn.util.text.Strings;
import org.osgi.framework.Bundle;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/** Many of the same tests as per {@link OsgiVersionMoreEntityTest} but using YAML for catalog and entities, so catalog item ID is set automatically */
public class CatalogOsgiVersionMoreEntityRebindTest extends AbstractYamlRebindTest implements OsgiTestResources {
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CatalogOsgiVersionMoreEntityRebindTest.class);

    @Override
    protected boolean useOsgi() {
        return true;
    }

    @Test
    public void testRebindAppIncludingBundle() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V1_URL) );
        
        createAndStartApplication("services: [ { type: "+BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY+" } ]");
        
        StartableApplication newApp = rebind();

        // bundles installed
        Map<String, ManagedBundle> bundles = ((ManagementContextInternal)mgmt()).getOsgiManager().get().getManagedBundles();
        Asserts.assertSize(bundles.keySet(), 1);
        
        // types installed
        RegisteredType t = mgmt().getTypeRegistry().get("org.apache.brooklyn.test.osgi.entities.more.MoreEntity");
        Assert.assertNotNull(t);
        
        Assert.assertNotNull(newApp);
    }
    
    @Test
    public void testPolicyInBundleReferencedByStockCatalogItem() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String policyType = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_POLICY;
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: wrapped-entity",
                "  version: 1.0",
                "  item:",
                "    services:",
                "    - type: " + TestEntity.class.getName());
    
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: with-policy-from-library",
                "  version: 1.0",
                "  brooklyn.libraries:",
                "  - classpath:" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH,
                "  item:",
                "    services:",
                "    - type: " + BasicApplication.class.getName(),
                "      brooklyn.children:",
                "      - type: wrapped-entity:1.0",
                "        brooklyn.policies:",
                "        - type: " + policyType);
        
        Entity app = createAndStartApplication("services: [ { type: 'with-policy-from-library:1.0' } ]");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Policy policy = Iterables.getOnlyElement(entity.policies());
        assertEquals(policy.getPolicyType().getName(), policyType);
        
        StartableApplication newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        Policy newPolicy = Iterables.getOnlyElement(newEntity.policies());
        assertEquals(newPolicy.getPolicyType().getName(), policyType);
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-410
    @Test
    @SuppressWarnings("unchecked")
    public void testRebindsLocationFromBundle() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String locationType = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_LOCATION;
        String locationTypeWithBundlePrefix = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_SYMBOLIC_NAME_FULL + ":" + locationType;

        addCatalogItems(
                "brooklyn.catalog:",
                "  id: with-library",
                "  version: 1.0",
                "  brooklyn.libraries:",
                "  - classpath:" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH,
                "  item:",
                "    services:",
                "    - type: " + BasicApplication.class.getName(),
                "      brooklyn.children:",
                "      - type: " + TestEntity.class.getName());
        
        Entity app = createAndStartApplication("services: [ { type: 'with-library:1.0' } ]");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        // Add a location that can only be classloaded from the `brooklyn.libraries` bundle
        Reflections reflections = new Reflections(CatalogOsgiVersionMoreEntityRebindTest.class.getClassLoader());
        Class<? extends Location> locationClazz = (Class<? extends Location>) new ClassLoaderUtils(reflections.getClassLoader(), mgmt()).loadClass(locationTypeWithBundlePrefix);
        Location loc = mgmt().getLocationManager().createLocation(LocationSpec.create(locationClazz));
        ((EntityInternal)entity).addLocations(ImmutableList.of(loc));

        // Confirm that we can rebind, and thus instantiate that location
        StartableApplication newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        Location newLoc = Iterables.getOnlyElement(newEntity.getLocations());
        assertEquals(newLoc.getClass().getName(), locationType);
    }
    
    @Test
    public void testEffectorInBundleReferencedByStockCatalogItem() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH);
        
        String effectorType = OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_EFFECTOR;
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: wrapped-entity",
                "  version: 1.0",
                "  item:",
                "    services:",
                "    - type: " + TestEntity.class.getName());
    
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: with-effector-from-library",
                "  version: 1.0",
                "  brooklyn.libraries:",
                "  - classpath:" + OsgiTestResources.BROOKLYN_TEST_OSGI_ENTITIES_COM_EXAMPLE_PATH,
                "  item:",
                "    services:",
                "    - type: " + BasicApplication.class.getName(),
                "      brooklyn.children:",
                "      - type: wrapped-entity:1.0",
                "        brooklyn.initializers:",
                "        - type: " + effectorType);

        Entity app = createAndStartApplication("services: [ { type: 'with-effector-from-library:1.0' } ]");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myEffector").get();
        entity.invoke(effector, ImmutableMap.<String, Object>of()).get();
        
        StartableApplication newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        Effector<?> newEffector = newEntity.getEntityType().getEffectorByName("myEffector").get();
        newEntity.invoke(newEffector, ImmutableMap.<String, Object>of()).get();
    }
    
    @Test
    public void testClassAccessAfterUninstall() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), BROOKLYN_TEST_OSGI_MORE_ENTITIES_0_1_0_PATH);
        
        // install dependency
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_OSGI_ENTITIES_URL) );

        // now the v2 bundle
        OsgiBundleInstallationResult b = ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_URL) ).get();

        Assert.assertEquals(b.getVersionedName().toString(), BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL+":"+"0.2.0");
        
        String yaml = Strings.lines("name: simple-app-yaml",
                "services:",
                "- type: " + BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        Entity app = createAndStartApplication(yaml);
        Entity more = Iterables.getOnlyElement( app.getChildren() );
        
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Bob")).get(),
            "HI BOB FROM V2");
        
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().uninstallUploadedBundle(b.getMetadata());
        Assert.assertEquals(b.getBundle().getState(), Bundle.UNINSTALLED);

        // can still call things
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Claudia")).get(),
            "HI CLAUDIA FROM V2");
        
        // but still uninstalled, and attempt to create makes error 
        Assert.assertEquals(b.getBundle().getState(), Bundle.UNINSTALLED);
        try {
            Entity app2 = createAndStartApplication(yaml);
            Asserts.shouldHaveFailedPreviously("Expected deployment to fail after uninstall; instead got "+app2);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "unable to match", BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        }
        
        try {
            StartableApplication app2 = rebind();
            Asserts.shouldHaveFailedPreviously("Expected deployment to fail rebind; instead got "+app2);
        } catch (Exception e) {
            // should fail to rebind this entity
            Asserts.expectedFailureContainsIgnoreCase(e, more.getId(), "unable to load", BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        }
    }
    
    @Test
    public void testClassAccessAfterUpgrade() throws Exception {
        TestResourceUnavailableException.throwIfResourceUnavailable(getClass(), BROOKLYN_TEST_OSGI_MORE_ENTITIES_0_1_0_PATH);
        
        // install dependency
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_OSGI_ENTITIES_URL) ).checkNoError();

        // now the v2 bundle
        OsgiBundleInstallationResult b2a = ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_URL) ).get();

        Assert.assertEquals(b2a.getVersionedName().toString(), BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL+":"+"0.2.0");
        Assert.assertEquals(b2a.getCode(), OsgiBundleInstallationResult.ResultCode.INSTALLED_NEW_BUNDLE);
        
        String yaml = Strings.lines("name: simple-app-yaml",
                "services:",
                "- type: " + BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        Entity app = createAndStartApplication(yaml);
        Entity more = Iterables.getOnlyElement( app.getChildren() );
        
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Bob")).get(),
            "HI BOB FROM V2");
        
        // unforced upgrade should report already installed
        Assert.assertEquals( ((ManagementContextInternal)mgmt()).getOsgiManager().get().install(
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_EVIL_TWIN_URL) ).get().getCode(),
            OsgiBundleInstallationResult.ResultCode.IGNORING_BUNDLE_AREADY_INSTALLED);
        
        // force upgrade
        OsgiBundleInstallationResult b2b = ((ManagementContextInternal)mgmt()).getOsgiManager().get().install(b2a.getMetadata(), 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_EVIL_TWIN_URL), true, true, true).get();
        Assert.assertEquals(b2a.getBundle(), b2b.getBundle());
        Assert.assertEquals(b2b.getCode(), OsgiBundleInstallationResult.ResultCode.UPDATED_EXISTING_BUNDLE);

        // calls to things previously instantiated get the old behaviour
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Claudia")).get(),
            "HI CLAUDIA FROM V2");
        
        // but new deployment gets the new behaviour 
        StartableApplication app2 = (StartableApplication) createAndStartApplication(yaml);
        Entity more2 = Iterables.getOnlyElement( app2.getChildren() );
        Assert.assertEquals(
            more2.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Daphne")).get(),
            "HO DAPHNE FROM V2 EVIL TWIN");
        app2.stop();
        
        // and after rebind on the old we get new behaviour
        StartableApplication app1 = rebind();
        Entity more1 = Iterables.getOnlyElement( app1.getChildren() );
        Assert.assertEquals(
            more1.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Eric")).get(),
            "HO ERIC FROM V2 EVIL TWIN");
    }
    
    @Test
    public void testClusterWithEntitySpecFromOsgi() throws Exception {
        // install dependencies
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_OSGI_ENTITIES_URL) ).checkNoError();
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_URL) ).get();
        
        RegisteredType ci = Preconditions.checkNotNull( mgmt().getTypeRegistry().get(BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY) );
        EntitySpec<DynamicCluster> clusterSpec = EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.INITIAL_SIZE, 1)
            .configure(DynamicCluster.MEMBER_SPEC, origManagementContext.getTypeRegistry().createSpec(ci, null, EntitySpec.class));
        
        final Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class).child(clusterSpec));
        app.invoke(Startable.START, MutableMap.of()).get();
        
        rebind();
    }

    protected RegisteredType installWrappedMoreEntity() {
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_OSGI_ENTITIES_URL) ).checkNoError();
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().install( 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_URL) ).get();
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: wrapped-more-entity",
            "  version: 1.0",
            "  item:",
            "    services:",
            "    - type: " + BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        
        RegisteredType ci = Preconditions.checkNotNull( mgmt().getTypeRegistry().get("wrapped-more-entity") );
        return ci;
    }
    
    @Test
    public void testRebindsClusterWithEntitySpecWrappingOsgi() throws Exception {
        RegisteredType ci = installWrappedMoreEntity();
        EntitySpec<DynamicCluster> clusterSpec = EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.INITIAL_SIZE, 1)
            .configure(DynamicCluster.MEMBER_SPEC, origManagementContext.getTypeRegistry().createSpec(ci, null, EntitySpec.class));
        
        final Entity app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class).child(clusterSpec));
        app.invoke(Startable.START, MutableMap.of()).get();
        
        rebind();
    }

    /** Does the class loader for the wrapped-more-entity type inherit more-entity's class loader?
     * Was tempting to say yes but the implementation is hard as we need to add API methods to find the
     * supertype (or supertypes).  We might do that at which point we could change these semantics if we wished.
     * However there is also an argument that the instantiation engine determines where inherited loaders
     * behave transitively and where they don't, so we shouldn't have a blanket rule that you can always
     * see someone else's loaders just by extending them. In any case the "compelling use case" for 
     * considering this (and noticing it, and adding comments to {@link RegisteredType#getLibraries()})
     * is xml deserialization of entity specs in persisted state, and: 
     * (a) there are other ways to do that, and 
     * (b) we'd like to move away from that and use the same yaml-based instantiation engine used for initial construction. */
    @Test
    public void testWrappedEntityClassLoaderDoesntHaveAncestorClassLoader() throws Exception {
        RegisteredType ci = installWrappedMoreEntity();
        BrooklynClassLoadingContext clc = CatalogUtils.newClassLoadingContext(mgmt(), ci);
        try {
            clc.loadClass(BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "unable to load", BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        }
    }

    @Test(groups="Broken")  // AH think not going to support this; see notes in BasicBrooklynCatalog.scanAnnotationsInBundle
    // it's hard to get the JAR for scanning, and doesn't fit with the OSGi way
    public void testRebindJavaScanningBundleInCatalog() throws Exception {
        CatalogScanOsgiTest.installJavaScanningMoreEntitiesV2(mgmt(), this);
        rebind();
        RegisteredType item = mgmt().getTypeRegistry().get(OsgiTestResources.BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        Assert.assertNotNull(item, "Scanned item should have been available after rebind");
    }
}
