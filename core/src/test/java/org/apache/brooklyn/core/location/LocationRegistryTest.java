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
package org.apache.brooklyn.core.location;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationDefinition;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.localhost.LocalhostLocationResolver;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.Test;

public class LocationRegistryTest {
    
    private static final Logger log = LoggerFactory.getLogger(LocationRegistryTest.class);
    
    private LocalManagementContext mgmt;
    private LocationDefinition locdef;

    @AfterMethod(alwaysRun = true)
    public void tearDown(){
        if (mgmt != null) Entities.destroyAll(mgmt);
    }

    @Test
    public void testNamedLocationsPropertyDefinedLocations() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.foo", "byon:(hosts=\"root@192.168.1.{1,2,3,4}\")");
        properties.put("brooklyn.location.named.foo.privateKeyFile", "~/.ssh/foo.id_rsa");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        log.info("foo properties gave defined locations: "+mgmt.getLocationRegistry().getDefinedLocations());
        locdef = mgmt.getLocationRegistry().getDefinedLocationByName("foo");
        Assert.assertNotNull(locdef, "Expected 'foo' location; but had "+mgmt.getLocationRegistry().getDefinedLocations());
        Assert.assertEquals(locdef.getConfig().get("privateKeyFile"), "~/.ssh/foo.id_rsa");
    }
    
    @Test(dependsOnMethods="testNamedLocationsPropertyDefinedLocations")
    public void testResolvesByNamedAndId() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.foo", "byon:(hosts=\"root@192.168.1.{1,2,3,4}\")");
        properties.put("brooklyn.location.named.foo.privateKeyFile", "~/.ssh/foo.id_rsa");
        mgmt = LocalManagementContextForTests.newInstance(properties);

        locdef = mgmt.getLocationRegistry().getDefinedLocationByName("foo");
        log.info("testResovlesBy has defined locations: "+mgmt.getLocationRegistry().getDefinedLocations());
        
        LocationSpec<?> ls = mgmt.getLocationRegistry().getLocationSpec("named:foo").get();
        Location l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertNotNull(l);
        Assert.assertEquals(l.getConfig(LocationConfigKeys.PRIVATE_KEY_FILE), "~/.ssh/foo.id_rsa");
        
        ls = mgmt.getLocationRegistry().getLocationSpec("foo").get();
        l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertNotNull(l);
        Assert.assertEquals(l.getConfig(LocationConfigKeys.PRIVATE_KEY_FILE), "~/.ssh/foo.id_rsa");
        
        ls = mgmt.getLocationRegistry().getLocationSpec("id:"+locdef.getId()).get();
        l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertNotNull(l);
        Assert.assertEquals(l.getConfig(LocationConfigKeys.PRIVATE_KEY_FILE), "~/.ssh/foo.id_rsa");
        
        ls = mgmt.getLocationRegistry().getLocationSpec(locdef.getId()).get();
        l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertNotNull(l);
        Assert.assertEquals(l.getConfig(LocationConfigKeys.PRIVATE_KEY_FILE), "~/.ssh/foo.id_rsa");
    }

    @Test
    public void testLocationGetsDisplayName() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.foo", "byon:(hosts=\"root@192.168.1.{1,2,3,4}\")");
        properties.put("brooklyn.location.named.foo.displayName", "My Foo");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        LocationSpec<?> ls = mgmt.getLocationRegistry().getLocationSpec("foo").get();
        Location l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertEquals(l.getDisplayName(), "My Foo");
    }
    
    @Test
    public void testLocationGetsDefaultDisplayName() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.foo", "byon:(hosts=\"root@192.168.1.{1,2,3,4}\")");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        LocationSpec<? extends Location> ls = mgmt.getLocationRegistry().getLocationSpec("foo").get();
        Location l = mgmt.getLocationManager().createLocation(ls);
        Assert.assertNotNull(l.getDisplayName());
        Assert.assertTrue(l.getDisplayName().startsWith(FixedListMachineProvisioningLocation.class.getSimpleName()), "name="+l.getDisplayName());
        // TODO currently it gives default name; it would be nice to use 'foo', 
        // or at least to have access to the spec (and use it e.g. in places such as DynamicFabric)
        // Assert.assertEquals(l.getDisplayName(), "foo");
    }
    
    @Test
    public void testSetupForTesting() {
        mgmt = LocalManagementContextForTests.newInstance();
        BasicLocationRegistry.addNamedLocationLocalhost(mgmt);
        Assert.assertNotNull(mgmt.getLocationRegistry().getDefinedLocationByName("localhost"));
    }

    @Test
    public void testCircularReference() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.bar", "named:bar");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        log.info("bar properties gave defined locations: "+mgmt.getLocationRegistry().getDefinedLocations());
        try {
            mgmt.getLocationRegistry().getLocationSpec("bar").get();
            Asserts.shouldHaveFailedPreviously("Circular reference gave a location");
        } catch (IllegalStateException e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "bar");
        }
    }

    protected boolean findLocationMatching(String regex) {
        for (LocationDefinition d: mgmt.getLocationRegistry().getDefinedLocations().values()) {
            if (d.getName()!=null && d.getName().matches(regex)) return true;
        }
        return false;
    }
    
    @Test
    public void testLocalhostEnabled() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.localhost.enabled", true);
        mgmt = LocalManagementContextForTests.newInstance(properties);
        BasicLocationRegistry.addNamedLocationLocalhost(mgmt);

        Assert.assertTrue( findLocationMatching("localhost") );
    }

    @Test
    public void testLocalhostNotPresentByDefault() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put(LocalhostLocationResolver.LOCALHOST_ENABLED.getName(), false);
        mgmt = LocalManagementContextForTests.newInstance(properties);
        
        log.info("RESOLVERS: "+mgmt.getLocationRegistry().getDefinedLocations());
        log.info("DEFINED LOCATIONS: "+mgmt.getLocationRegistry().getDefinedLocations());
        Assert.assertFalse( findLocationMatching("localhost") );
    }

    @Test
    public void testLocalhostAllowedByDefault() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put("brooklyn.location.named.localhost_allowed", "localhost");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        
        Assert.assertTrue( findLocationMatching("localhost_allowed") );
        Maybe<LocationSpec<?>> l = mgmt.getLocationRegistry().getLocationSpec("localhost_allowed");
        Assert.assertTrue( l.isPresent(), "Should have resolved: "+l );
        l.get();
    }

    @Test
    public void testNonsenseParentSupported() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put(LocalhostLocationResolver.LOCALHOST_ENABLED.getName(), false);
        properties.put("brooklyn.location.named.bogus_will_fail_eventually", "totally_bogus");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        
        Assert.assertTrue( findLocationMatching("bogus_will_fail_eventually") );
        Maybe<LocationSpec<?>> l = mgmt.getLocationRegistry().getLocationSpec("bogus_will_fail_eventually");
        Assert.assertTrue( l.isAbsent(), "Should not have resolved: "+l );
        try {
            l.get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "bogus_will_fail", "totally_bogus");
        }
    }
    
    @Test
    public void testLocalhostDisallowedIfDisabled() {
        BrooklynProperties properties = BrooklynProperties.Factory.newEmpty();
        properties.put(LocalhostLocationResolver.LOCALHOST_ENABLED.getName(), false);
        properties.put("brooklyn.location.named.local_host_not_allowed", "localhost");
        mgmt = LocalManagementContextForTests.newInstance(properties);
        
        Assert.assertTrue( findLocationMatching("local_host_not_allowed") );
        Maybe<LocationSpec<?>> l = mgmt.getLocationRegistry().getLocationSpec("local_host_not_allowed");
        Assert.assertTrue( l.isAbsent(), "Should not have resolved: "+l );
        try {
            l.get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "local_host", "localhost");
        }
    }
    
}
