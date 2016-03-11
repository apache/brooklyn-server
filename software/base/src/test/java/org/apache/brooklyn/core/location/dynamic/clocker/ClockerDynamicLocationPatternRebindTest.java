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
package org.apache.brooklyn.core.location.dynamic.clocker;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.location.BasicLocationRegistry;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class ClockerDynamicLocationPatternRebindTest extends RebindTestFixtureWithApp {

    @Override
    protected LocalManagementContext createOrigManagementContext() {
        LocalManagementContext result = super.createOrigManagementContext();
        StubResolver stubResolver = new StubResolver();
        ((BasicLocationRegistry)result.getLocationRegistry()).registerResolver(stubResolver);
        return result;
    }
    
    @Override
    protected LocalManagementContext createNewManagementContext(File mementoDir) {
        LocalManagementContext result = super.createNewManagementContext(mementoDir);
        StubResolver stubResolver = new StubResolver();
        ((BasicLocationRegistry)result.getLocationRegistry()).registerResolver(stubResolver);
        return result;
    }
    
    // To make this fail (with Clocker code as at 2016-03-11) requires several apps - there's a bug
    // in rebind that only happens when there are several entities, so the order that they rebind
    // is more interleaved.
    @Test
    public void testRebind() throws Exception {
        final int NUM_INFRAS = 10;
        final int HOST_CLUSTER_SIZE = 1;
        Location loc = mgmt().getLocationRegistry().resolve("localhost");
        
        // Maps from the infrastructure locSpec to the (potentially many) host locSpecs
        Map<String, List<String>> locSpecs = Maps.newLinkedHashMap();
        
        for (int i = 0; i < NUM_INFRAS; i++) {
            StubInfrastructure infra = mgmt().getEntityManager().createEntity(EntitySpec.create(StubInfrastructure.class)
                    .configure(StubInfrastructure.LOCATION_NAME, "myname"+i));
            infra.start(ImmutableList.of(loc));
            infra.getStubHostCluster().resize(HOST_CLUSTER_SIZE);
            assertEquals(infra.getStubHostCluster().getMembers().size(), HOST_CLUSTER_SIZE);
            
            String infraLocSpec = infra.sensors().get(StubInfrastructure.LOCATION_SPEC);
            List<String> hostLocSpecs = Lists.newArrayList();
            for (Entity host : infra.getStubHostCluster().getMembers()) {
                hostLocSpecs.add(host.sensors().get(StubInfrastructure.LOCATION_SPEC));
            }
    
            locSpecs.put(infraLocSpec, hostLocSpecs);
        }
        assertEquals(locSpecs.size(), NUM_INFRAS); // in case the infrastructures all used the same loc name!

        rebind();

        for (Map.Entry<String, List<String>> entry : locSpecs.entrySet()) {
            String infraLocSpec = entry.getKey();
            List<String> hostLocSpecs = entry.getValue();
            
            StubInfrastructureLocation newInfraLoc = (StubInfrastructureLocation) mgmt().getLocationRegistry().resolve(infraLocSpec);
            assertNotNull(newInfraLoc);
            for (String hostLocSpec: hostLocSpecs) {
                StubHostLocation newHostLoc = (StubHostLocation) mgmt().getLocationRegistry().resolve(hostLocSpec);
                assertNotNull(newHostLoc);
            }
    
            // Confirm that it still functions
            StubContainerLocation containerLoc = (StubContainerLocation) newInfraLoc.obtain(ImmutableMap.of());
            StubContainer container = containerLoc.getOwner();
            
            newInfraLoc.release(containerLoc);
            assertFalse(Entities.isManaged(container));
            assertFalse(Locations.isManaged(containerLoc));
        }
    }
}
