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
package org.apache.brooklyn.location.multi;

import java.util.List;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.location.cloud.AvailabilityZoneExtension;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.net.Networking;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class MultiLocationRebindTest extends RebindTestFixtureWithApp {

    private SshMachineLocation mac1a;
    private SshMachineLocation mac2a;
    private FixedListMachineProvisioningLocation<SshMachineLocation> loc1;
    private FixedListMachineProvisioningLocation<SshMachineLocation> loc2;
    private MultiLocation<SshMachineLocation> multiLoc;
    
    @SuppressWarnings("unchecked")
    @Test
    public void testRebindsMultiLocation() throws Exception {
        mac1a = origManagementContext.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .displayName("mac1a")
                .configure("address", Networking.getInetAddressWithFixedName("1.1.1.1")));
        mac2a = origManagementContext.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .displayName("mac2a")
                .configure("address", Networking.getInetAddressWithFixedName("1.1.1.3")));
        loc1 = origManagementContext.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .displayName("loc1")
                .configure("machines", MutableSet.of(mac1a)));
        loc2 = origManagementContext.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .displayName("loc2")
                .configure("machines", MutableSet.of(mac2a)));
        multiLoc = origManagementContext.getLocationManager().createLocation(LocationSpec.create(MultiLocation.class)
                        .displayName("multiLoc")
                        .configure("subLocations", ImmutableList.of(loc1, loc2)));
        
        newApp = rebind();
        newManagementContext = newApp.getManagementContext();
        
        MultiLocation<?> newMultiLoc = (MultiLocation<?>) Iterables.find(newManagementContext.getLocationManager().getLocations(), Predicates.instanceOf(MultiLocation.class));
        AvailabilityZoneExtension azExtension = newMultiLoc.getExtension(AvailabilityZoneExtension.class);
        List<Location> newSublLocs = azExtension.getAllSubLocations();
        Iterable<String> newSubLocNames = Iterables.transform(newSublLocs, new Function<Location, String>() {
            @Override public String apply(Location input) {
                return (input == null) ? null : input.getDisplayName();
            }});
        Asserts.assertEqualsIgnoringOrder(newSubLocNames, ImmutableList.of("loc1", "loc2"));
    }
}
