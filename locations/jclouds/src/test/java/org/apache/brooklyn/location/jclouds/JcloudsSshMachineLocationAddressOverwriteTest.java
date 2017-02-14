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

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;

import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;

public class JcloudsSshMachineLocationAddressOverwriteTest extends AbstractJcloudsStubbedUnitTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsImageChoiceStubbedLiveTest.class);
    
    private List<String> privateAddresses;
    private List<String> publicAddresses;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        privateAddresses = ImmutableList.of("172.168.10.11");
        publicAddresses = ImmutableList.of("173.194.32.123");
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(JcloudsLocationConfig.USE_MACHINE_PUBLIC_ADDRESS_AS_PRIVATE_ADDRESS.getName(), true));
    }
    
    @Override
    protected NodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myid")
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(publicAddresses)
                        .privateAddresses(privateAddresses)
                        .build();
                return result;
            }
        };
    }


    @Test
    public void testSetPrivateIpToPublicIp() throws Exception {
        JcloudsSshMachineLocation machine = obtainMachine(ImmutableMap.of());

        assertEquals(publicAddresses, machine.getPublicAddresses());

        assertEquals(machine.getPublicAddresses().size(), 1);
        String publicAddress = machine.getPublicAddresses().iterator().next();

        assertEquals(machine.getPrivateAddress().get(), publicAddress);
        assertEquals(machine.getSubnetHostname(), machine.getHostname());
        assertEquals(machine.getSubnetIp(), publicAddress);

        assertEquals(Machines.getSubnetHostname(machine).get(), machine.getHostname());
        assertEquals(Machines.getSubnetIp(machine).get(), publicAddress);
        assertEquals(machine.getPrivateAddresses().size(), 1);
        assertEquals(machine.getPrivateAddresses().iterator().next(), publicAddress);

    }
}
