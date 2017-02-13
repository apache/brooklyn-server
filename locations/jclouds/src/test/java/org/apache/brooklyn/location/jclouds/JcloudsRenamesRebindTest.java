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
package org.apache.brooklyn.location.jclouds;

import static org.testng.Assert.assertTrue;

import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests rebind (i.e. restarting Brooklyn server) when using the jclouds-provider renames.
 */
public class JcloudsRenamesRebindTest extends RebindTestFixtureWithApp {

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        AbstractJcloudsLiveTest.stripBrooklynProperties(origManagementContext.getBrooklynProperties());
    }

    @Test(expectedExceptions=NoSuchElementException.class)
    public void testProviderDoesNotExist() throws Exception {
        resolve("wrongprovider");
    }
    
    @Test
    public void testProviderNotRenamed() throws Exception {
        JcloudsLocation loc = resolve("aws-ec2:us-east-1", ImmutableMap.of("identity", "dummy", "credential", "dummy"));
        assertComputeServiceType(loc, "aws-ec2");
        
        rebind();
        
        JcloudsLocation newLoc = (JcloudsLocation) mgmt().getLocationManager().getLocation(loc.getId());
        assertComputeServiceType(newLoc, "aws-ec2");
    }

    @Test
    public void testProviderRenamed() throws Exception {
        JcloudsLocation loc = resolve("openstack-mitaka-nova:http://hostdoesnotexist.com:5000", ImmutableMap.of("identity", "dummy", "credential", "dummy"));
        assertComputeServiceType(loc, "openstack-nova");
        
        rebind();
        
        JcloudsLocation newLoc = (JcloudsLocation) mgmt().getLocationManager().getLocation(loc.getId());
        assertComputeServiceType(newLoc, "openstack-nova");
    }

    private JcloudsLocation resolve(String spec) {
        return resolve(spec, ImmutableMap.of());
    }
    
    private JcloudsLocation resolve(String spec, Map<?,?> flags) {
        return (JcloudsLocation) mgmt().getLocationRegistry().getLocationManaged(spec, flags);
    }
    
    private void assertComputeServiceType(JcloudsLocation loc, String expectedType) {
        // TODO Would be nice to do this more explicitly, rather than relying on toString.
        // But this is good enough.
        ComputeService computeService = loc.getComputeService();
        ComputeServiceContext context = computeService.getContext();
        assertTrue(context.toString().contains("id="+expectedType), "computeService="+computeService+"; context="+computeService.getContext());
    }
    
}
