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

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class JcloudsSshMachineLocationStubbedTest extends AbstractJcloudsStubbedUnitTest {

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
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
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
    public void testWithNoPrivateAddress() throws Exception {
        privateAddresses = ImmutableList.of();
        JcloudsSshMachineLocation machine = obtainMachine();
        assertEquals(machine.getPrivateAddresses(), ImmutableSet.of());
        assertEquals(machine.getPrivateAddress(), Optional.absent());
        assertEquals(machine.getSubnetIp(), publicAddresses.get(0));
        assertEquals(machine.getSubnetHostname(), publicAddresses.get(0));
    }
    
    @Test
    public void testWithPrivateAddress() throws Exception {
        JcloudsSshMachineLocation machine = obtainMachine();
        assertEquals(machine.getPrivateAddresses(), privateAddresses);
        assertEquals(machine.getPrivateAddress(), Optional.of(privateAddresses.get(0)));
        assertEquals(machine.getSubnetIp(), privateAddresses.get(0));
        assertEquals(machine.getSubnetHostname(), privateAddresses.get(0));
    }
    
    @Test
    public void testSshConfigPassedToMachine() throws Exception {
        JcloudsSshMachineLocation machine = obtainMachine(ImmutableMap.of(
                SshMachineLocation.LOCAL_TEMP_DIR.getName(), "/my/local/temp/dir",
                SshMachineLocation.LOG_PREFIX.getName(), "myLogPrefix",
                SshTool.PROP_SSH_TRIES, 123));
        assertEquals(machine.config().get(SshMachineLocation.LOCAL_TEMP_DIR), "/my/local/temp/dir");
        assertEquals(machine.config().get(SshMachineLocation.LOG_PREFIX), "myLogPrefix");
        assertEquals(machine.config().get(SshTool.PROP_SSH_TRIES), Integer.valueOf(123));
    }
    
    @Test
    public void testWinrmConfigPassedToMachine() throws Exception {
        JcloudsWinRmMachineLocation machine = obtainWinrmMachine(ImmutableMap.of(
                JcloudsLocation.OS_FAMILY_OVERRIDE.getName(), OsFamily.WINDOWS,
//                JcloudsLocation.WAIT_FOR_WINRM_AVAILABLE.getName(), "false",
                WinRmMachineLocation.COPY_FILE_CHUNK_SIZE_BYTES.getName(), 123,
                WinRmTool.PROP_EXEC_TRIES.getName(), 456));
        assertEquals(machine.config().get(WinRmMachineLocation.COPY_FILE_CHUNK_SIZE_BYTES), Integer.valueOf(123));
        assertEquals(machine.config().get(WinRmTool.PROP_EXEC_TRIES), Integer.valueOf(456));
    }
}
