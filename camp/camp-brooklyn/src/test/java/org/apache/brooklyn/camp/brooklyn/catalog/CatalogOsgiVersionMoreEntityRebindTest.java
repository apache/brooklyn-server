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
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.osgi.OsgiVersionMoreEntityTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicManagedBundle;
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
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().installUploadedBundle(new BasicManagedBundle(), 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V1_URL), true);
        
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
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().installUploadedBundle(new BasicManagedBundle(), 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_OSGI_ENTITIES_URL), true);

        // now the v2 bundle
        BasicManagedBundle mb = new BasicManagedBundle();
        Bundle b2a = ((ManagementContextInternal)mgmt()).getOsgiManager().get().installUploadedBundle(mb, 
            new ResourceUtils(getClass()).getResourceFromUrl(BROOKLYN_TEST_MORE_ENTITIES_V2_URL), true);

        Assert.assertEquals(mb.getVersionedName().toString(), BROOKLYN_TEST_MORE_ENTITIES_SYMBOLIC_NAME_FULL+":"+"0.2.0");
        
        String yaml = Strings.lines("name: simple-app-yaml",
                "services:",
                "- type: " + BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY);
        Entity app = createAndStartApplication(yaml);
        Entity more = Iterables.getOnlyElement( app.getChildren() );
        
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Bob")).get(),
            "HI BOB FROM V2");
        
        ((ManagementContextInternal)mgmt()).getOsgiManager().get().uninstallUploadedBundle(mb);
        Assert.assertEquals(b2a.getState(), Bundle.UNINSTALLED);

        // can still call things
        Assert.assertEquals(
            more.invoke(Effectors.effector(String.class, "sayHI").buildAbstract(), MutableMap.of("name", "Claudia")).get(),
            "HI CLAUDIA FROM V2");
        
        // but still uninstalled, and attempt to create makes error 
        Assert.assertEquals(b2a.getState(), Bundle.UNINSTALLED);
        try {
            Entity app2 = createAndStartApplication(yaml);
            Asserts.shouldHaveFailedPreviously("Expected deployment to fail after uninstall; instead got "+app2);
        } catch (Exception e) {
            // org.apache.brooklyn.util.exceptions.CompoundRuntimeException: Unable to instantiate item; 2 errors including: 
            // Transformer for brooklyn-camp gave an error creating this plan: Transformer for catalog gave an error creating this plan: 
            // Unable to instantiate org.apache.brooklyn.test.osgi.entities.more.MoreEntity; 
            // 2 errors including: Error in definition of org.apache.brooklyn.test.osgi.entities.more.MoreEntity:0.12.0-SNAPSHOT: 
            // Unable to create spec for type brooklyn:org.apache.brooklyn.test.osgi.entities.more.MoreEntity. 
            // The reference brooklyn:org.apache.brooklyn.test.osgi.entities.more.MoreEntity looks like a URL 
            // (running the CAMP Brooklyn assembly-template instantiator) but 
            // the protocol brooklyn isn't white listed ([classpath, http, https]). 
            // It's also neither a catalog item nor a java type.
            // TODO different error after catalog item uninstalled
            Asserts.expectedFailureContainsIgnoreCase(e, BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY, "not", "registered");
        }
        
        try {
            StartableApplication app2 = rebind();
            Asserts.shouldHaveFailedPreviously("Expected deployment to fail rebind; instead got "+app2);
        } catch (Exception e) {
            Asserts.expectedFailure(e);
            // TODO should fail to rebind this app
            // (currently fails to load the catalog item, since it wasn't removed)
            // Asserts.expectedFailureContainsIgnoreCase(e, BROOKLYN_TEST_MORE_ENTITIES_MORE_ENTITY, "simple-app-yaml");
        }
    }
    

}
