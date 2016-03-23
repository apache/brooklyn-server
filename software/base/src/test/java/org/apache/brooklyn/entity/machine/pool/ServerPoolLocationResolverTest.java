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
package org.apache.brooklyn.entity.machine.pool;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.dynamic.LocationOwner;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class ServerPoolLocationResolverTest extends BrooklynAppUnitTestSupport {

    private ServerPool serverPool;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        serverPool = app.createAndManageChild(EntitySpec.create(ServerPool.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution())
                .configure(ServerPool.INITIAL_SIZE, 0)
                .configure(ServerPool.MEMBER_SPEC, EntitySpec.create(EmptySoftwareProcess.class)));
        Location localhostLoc = mgmt.getLocationManager()
                .createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class));
        app.start(ImmutableList.of(localhostLoc));
    }

    @Test
    public void testResolve() {
        String spec = "pool:" + serverPool.getDynamicLocation().getId();
        ServerPoolLocation loc = (ServerPoolLocation) mgmt.getLocationRegistry().getLocationManaged(spec);
        assertEquals(loc, serverPool.getDynamicLocation());
        
        String spec2 = serverPool.sensors().get(LocationOwner.LOCATION_SPEC);
        ServerPoolLocation loc2 = (ServerPoolLocation) mgmt.getLocationRegistry().getLocationManaged(spec2);
        assertEquals(loc2, serverPool.getDynamicLocation());
    }
}
