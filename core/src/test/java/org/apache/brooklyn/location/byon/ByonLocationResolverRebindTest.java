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
import static org.testng.Assert.assertNull;

import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class ByonLocationResolverRebindTest extends RebindTestFixtureWithApp {

    @Test
    public void testRebindByon() throws Exception {
        String spec = "byon(hosts=\"1.1.1.1,1.1.1.2\")";
        MachineProvisioningLocation<MachineLocation> provisioner = resolve(spec);
        MachineLocation machine1 = provisioner.obtain(ImmutableMap.of());
        machine1.config().set(ConfigKeys.newStringConfigKey("mykey1"), "myval1");

        rebind();
        
        @SuppressWarnings("unchecked")
        MachineProvisioningLocation<MachineLocation> newProvisioner = (MachineProvisioningLocation<MachineLocation>) mgmt().getLocationManager().getLocation(provisioner.getId());
        
        SshMachineLocation newMachine1 = (SshMachineLocation) mgmt().getLocationManager().getLocation(machine1.getId());
        assertEquals(newMachine1.config().get(ConfigKeys.newStringConfigKey("mykey1")), "myval1");
        
        SshMachineLocation newMachine2 = (SshMachineLocation) newProvisioner.obtain(ImmutableMap.of());
        
        // See https://issues.apache.org/jira/browse/BROOKLYN-396 (though didn't fail for byon, only localhost)
        ((LocationInternal)newProvisioner).config().getLocalBag().toString();
        ((LocationInternal)newMachine1).config().getLocalBag().toString();
        ((LocationInternal)newMachine2).config().getLocalBag().toString();
        
        // Confirm when machine is released that we get it back (without the modifications to config)
        newMachine1.config().set(ConfigKeys.newStringConfigKey("mykey2"), "myval2");
        newProvisioner.release(newMachine1);
        
        MachineLocation newMachine1b = newProvisioner.obtain(ImmutableMap.of());
        assertEquals(newMachine1b.getAddress(), machine1.getAddress());
        assertNull(newMachine1b.config().get(ConfigKeys.newStringConfigKey("mykey1")));
        assertNull(newMachine1b.config().get(ConfigKeys.newStringConfigKey("mykey2")));
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
