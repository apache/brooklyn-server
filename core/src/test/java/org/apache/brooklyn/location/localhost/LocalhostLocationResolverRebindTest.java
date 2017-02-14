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
package org.apache.brooklyn.location.localhost;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class LocalhostLocationResolverRebindTest extends RebindTestFixtureWithApp {

    // Test is motivated by https://issues.apache.org/jira/browse/BROOKLYN-396, to
    // compare behaviour with and without rebind.
    @Test
    public void testWithoutRebind() throws Exception {
        runTest(false);
    }

    @Test
    public void testRebind() throws Exception {
        runTest(true);
    }
    
    protected void runTest(boolean doRebind) throws Exception {
        LocalhostMachineProvisioningLocation provisioner = resolve("localhost");
        MachineLocation machine1 = provisioner.obtain(ImmutableMap.of());
        machine1.config().set(ConfigKeys.newStringConfigKey("mykey1"), "myval1");

        if (doRebind) {
            rebind();
        }
        
        LocalhostMachineProvisioningLocation newProvisioner = (LocalhostMachineProvisioningLocation) mgmt().getLocationManager().getLocation(provisioner.getId());

        SshMachineLocation newMachine1 = (SshMachineLocation) mgmt().getLocationManager().getLocation(machine1.getId());
        assertEquals(newMachine1.config().get(ConfigKeys.newStringConfigKey("mykey1")), "myval1");
        
        SshMachineLocation newMachine2 = newProvisioner.obtain(ImmutableMap.of());
        
        // See https://issues.apache.org/jira/browse/BROOKLYN-396
        ((LocationInternal)newProvisioner).config().getLocalBag().toString();
        ((LocationInternal)newMachine1).config().getLocalBag().toString();
        ((LocationInternal)newMachine2).config().getLocalBag().toString();
        
        // Release a machine, and get a new one.
        // With a normal BYON, it would give us the same machine.
        // For localhost, we don't care if it's the same or different - as long as it doesn't
        // have the specific config set on the original MachineLocation
        newMachine1.config().set(ConfigKeys.newStringConfigKey("mykey2"), "myval2");
        newProvisioner.release(newMachine1);
        
        MachineLocation newMachine1b = newProvisioner.obtain(ImmutableMap.of());
        assertNull(newMachine1b.config().get(ConfigKeys.newStringConfigKey("mykey1")));
        assertNull(newMachine1b.config().get(ConfigKeys.newStringConfigKey("mykey2")));
    }

    @Test
    public void testRebindWhenOnlyByonLocationSpec() throws Exception {
        int before = mgmt().getLocationManager().getLocations().size();
        getLocationSpec("localhost");
        
        rebind();

        int after = mgmt().getLocationManager().getLocations().size();
        assertEquals(after, before);
    }

    @SuppressWarnings("unchecked")
    private LocationSpec<LocalhostMachineProvisioningLocation> getLocationSpec(String val) {
        return (LocationSpec<LocalhostMachineProvisioningLocation>) mgmt().getLocationRegistry().getLocationSpec(val).get();
    }

    private LocalhostMachineProvisioningLocation resolve(String val) {
        return (LocalhostMachineProvisioningLocation) mgmt().getLocationRegistry().getLocationManaged(val);
    }
}
