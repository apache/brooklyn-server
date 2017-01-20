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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation.LocalhostMachine;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class MachinesTest extends BrooklynAppUnitTestSupport {

    protected String publicAddr = "1.2.3.4";
    protected String privateAddr = "10.1.2.3";
    
    protected LocationSpec<SshMachineLocation> sshMachineSpec;
    protected LocationSpec<SshMachineLocation> sshMachineWithoutPrivateSpec;
    protected LocationSpec<LocalhostMachineProvisioningLocation> otherLocSpec;
    protected LocationSpec<LocalhostMachine> localMachineSpec;
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        sshMachineSpec = LocationSpec.create(SshMachineLocation.class)
                .configure("address", publicAddr)
                .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableList.of(privateAddr));
        sshMachineWithoutPrivateSpec = LocationSpec.create(SshMachineLocation.class)
                .configure("address", publicAddr);
        otherLocSpec = TestApplication.LOCALHOST_PROVISIONER_SPEC;
        localMachineSpec = TestApplication.LOCALHOST_MACHINE_SPEC;
    }
    
    private <T extends Location> T create(LocationSpec<T> spec) {
        return mgmt.getLocationManager().createLocation(spec);
    }
    
    @Test
    public void testFindUniqueMachineLocation() throws Exception {
        SshMachineLocation l1 = create(sshMachineSpec);
        LocalhostMachineProvisioningLocation l2 = create(otherLocSpec);
        assertEquals(Machines.findUniqueMachineLocation(ImmutableList.of(l1, l2)).get(), l1);
        assertFalse(Machines.findUniqueMachineLocation(ImmutableList.of(l2)).isPresent());
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testFindUniqueSshMachineLocation() throws Exception {
        SshMachineLocation l1 = create(sshMachineSpec);
        LocalhostMachineProvisioningLocation l2 = create(otherLocSpec);
        assertEquals(Machines.findUniqueSshMachineLocation(ImmutableList.of(l1, l2)).get(), l1);
        assertFalse(Machines.findUniqueSshMachineLocation(ImmutableList.of(l2)).isPresent());
    }
    
    @Test
    public void testFindUniqueMachineLocationOfType() throws Exception {
        SshMachineLocation l1 = create(sshMachineSpec);
        LocalhostMachineProvisioningLocation l2 = create(otherLocSpec);
        assertEquals(Machines.findUniqueMachineLocation(ImmutableList.of(l1, l2), SshMachineLocation.class).get(), l1);
        assertFalse(Machines.findUniqueMachineLocation(ImmutableList.of(l2), LocalhostMachine.class).isPresent());
    }

    @Test
    public void testFindSubnetIpFromAttribute() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "myaddr");
        assertEquals(Machines.findSubnetIp(entity).get(), "myaddr");
    }

    @Test
    public void testFindSubnetIpFromLocation() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        assertEquals(Machines.findSubnetIp(entity).get(), privateAddr);
    }

    @Test
    public void testFindSubnetIpFromLocationWithoutPrivate() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineWithoutPrivateSpec));
        assertEquals(Machines.findSubnetIp(entity).get(), publicAddr);
    }

    @Test
    public void testFindSubnetHostnameFromAttribute() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        entity.sensors().set(Attributes.SUBNET_HOSTNAME, "myval");
        assertEquals(Machines.findSubnetHostname(entity).get(), "myval");
    }

    @Test
    public void testFindSubnetHostnameFromLocation() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        assertEquals(Machines.findSubnetHostname(entity).get(), privateAddr);
    }

    @Test
    public void testFindSubnetHostnameFromLocationWithoutPrivate() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineWithoutPrivateSpec));
        assertEquals(Machines.findSubnetHostname(entity).get(), publicAddr);
    }

    @Test
    public void testFindSubnetOrPrivateIpWithAddressAttributePrefersLocationPrivateIp() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        entity.sensors().set(Attributes.ADDRESS, "myval");
        
        assertEquals(Machines.findSubnetOrPrivateIp(entity).get(), privateAddr);
    }
    
    // TODO Why do we only return the "myval" (rather than publicAddr) if Attributes.ADDRESS is set?
    @Test
    public void testFindSubnetOrPrivateIpFromAttribute() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        entity.sensors().set(Attributes.ADDRESS, "ignored-val");
        entity.sensors().set(Attributes.SUBNET_ADDRESS, "myval");
        
        assertEquals(Machines.findSubnetOrPrivateIp(entity).get(), "myval");
    }
    
    // TODO Why do we only return the privateAddr (rather than publicAddr) if Attributes.ADDRESS is set?
    @Test
    public void testFindSubnetOrPrivateIpFromLocation() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineSpec));
        entity.sensors().set(Attributes.ADDRESS, "ignored-val");
        
        assertEquals(Machines.findSubnetOrPrivateIp(entity).get(), privateAddr);
    }
    
    @Test
    public void testFindSubnetOrPrivateIpFromLocationWithoutPrivate() throws Exception {
        TestEntity entity = app.addChild(EntitySpec.create(TestEntity.class)
                .location(sshMachineWithoutPrivateSpec));
        entity.sensors().set(Attributes.ADDRESS, "ignored-val");
        
        assertEquals(Machines.findSubnetOrPrivateIp(entity).get(), publicAddr);
    }
    
    @Test
    public void testWarnIfLocalhost() throws Exception {
        assertFalse(Machines.warnIfLocalhost(ImmutableList.of(create(sshMachineSpec)), "my message"));
        
        // Visual inspection test - expect a log.warn
        assertTrue(Machines.warnIfLocalhost(ImmutableList.of(create(localMachineSpec)), "my message"));
    }
}
