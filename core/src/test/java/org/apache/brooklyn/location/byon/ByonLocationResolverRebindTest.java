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
package org.apache.brooklyn.location.byon;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class ByonLocationResolverRebindTest extends RebindTestFixtureWithApp {

    @Test
    public void testRebindByon() throws Exception {
        String spec = "byon(hosts=\"1.1.1.1\")";
        MachineProvisioningLocation<MachineLocation> provisioner = resolve(spec);
        
        rebind();
        
        @SuppressWarnings("unchecked")
        MachineProvisioningLocation<MachineLocation> newProvisioner = (MachineProvisioningLocation<MachineLocation>) mgmt().getLocationManager().getLocation(provisioner.getId());
        MachineLocation newLocation = newProvisioner.obtain(ImmutableMap.of());
        assertTrue(newLocation instanceof SshMachineLocation, "Expected location to be SshMachineLocation, found " + newLocation);
    }

    @Test
    public void testRebindWhenOnlyByonLocationSpec() throws Exception {
        int before = mgmt().getLocationManager().getLocations().size();
        String spec = "byon(hosts=\"1.1.1.1\")";
        getLocationSpec(spec);
        
        rebind();

        int after = mgmt().getLocationManager().getLocations().size();
        assertEquals(after, before);
    }

    @SuppressWarnings("unchecked")
    private LocationSpec<FixedListMachineProvisioningLocation<MachineLocation>> getLocationSpec(String val) {
        return (LocationSpec<FixedListMachineProvisioningLocation<MachineLocation>>) mgmt().getLocationRegistry().getLocationSpec(val).get();
    }

    @SuppressWarnings("unchecked")
    private FixedListMachineProvisioningLocation<MachineLocation> resolve(String val) {
        return (FixedListMachineProvisioningLocation<MachineLocation>) mgmt().getLocationRegistry().getLocationManaged(val);
    }
}
