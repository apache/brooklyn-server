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
package org.apache.brooklyn.core.location.access;

import java.net.URI;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

public class PublicNetworkFaceEnricherRebindTest extends RebindTestFixtureWithApp {

    private TestEntity origEntity;
    private SshMachineLocation origMachine;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        origEntity = origApp.createAndManageChild(EntitySpec.create(TestEntity.class));
        origMachine = origApp.newLocalhostProvisioningLocation().obtain();
    }
    
    @Test
    public <T> void testRebind() throws Exception {
        origEntity.sensors().set(Attributes.SUBNET_ADDRESS, "127.0.0.1");
        
        origEntity.enrichers().add(EnricherSpec.create(PublicNetworkFaceEnricher.class)
                .configure(PublicNetworkFaceEnricher.SENSOR, Attributes.MAIN_URI));

        rebind();
        TestEntity newEntity = (TestEntity) Iterables.getOnlyElement(newApp.getChildren());
        PortForwardManager newPortForwardManager = (PortForwardManager) mgmt().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        SshMachineLocation newMachine = (SshMachineLocation) mgmt().getLocationManager().getLocation(origMachine.getId());
        
        newEntity.sensors().set(Attributes.MAIN_URI, URI.create("http://127.0.0.1:1234/my/path"));
        newPortForwardManager.associate("myPublicIp", HostAndPort.fromParts("mypublichost", 5678), newMachine, 1234);
        newEntity.addLocations(ImmutableList.of(newMachine));
        
        EntityAsserts.assertAttributeEqualsEventually(newEntity, Sensors.newStringSensor(Attributes.MAIN_URI.getName()+".mapped.public"), "http://mypublichost:5678/my/path");
    }
}
