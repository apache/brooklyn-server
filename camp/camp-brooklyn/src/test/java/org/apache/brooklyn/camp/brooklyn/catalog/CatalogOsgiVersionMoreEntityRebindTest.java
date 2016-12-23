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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlRebindTest;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.mgmt.osgi.OsgiVersionMoreEntityTest;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.support.TestResourceUnavailableException;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.osgi.OsgiTestResources;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

/** Many of the same tests as per {@link OsgiVersionMoreEntityTest} but using YAML for catalog and entities, so catalog item ID is set automatically */
public class CatalogOsgiVersionMoreEntityRebindTest extends AbstractYamlRebindTest {
    
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(CatalogOsgiVersionMoreEntityRebindTest.class);

    @Override
    protected boolean useOsgi() {
        return true;
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-409
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
}
