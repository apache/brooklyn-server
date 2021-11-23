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

import static org.apache.brooklyn.location.jclouds.JcloudsLocationConfig.JCLOUDS_LOCATION_CUSTOMIZERS;
import static org.apache.brooklyn.util.core.internal.ssh.SshTool.ADDITIONAL_CONNECTION_METADATA;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Nullable;

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.jclouds.compute.ComputeService;
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
    
    @SuppressWarnings("serial")
    private static class FailObtainOnPurposeException extends RuntimeException {
        public FailObtainOnPurposeException(String message) {
            super(message);
        }
    }
    
    private List<String> privateAddresses;
    private List<String> publicAddresses;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        privateAddresses = ImmutableList.of(PRIVATE_IP_ADDRESS);
        publicAddresses = ImmutableList.of(PUBLIC_IP_ADDRESS);
        jcloudsLocation = initStubbedJcloudsLocation(ImmutableMap.of());
    }
    
    @Test(enabled = false)
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
        JcloudsSshMachineLocation machine = obtainMachine(ImmutableMap.of(JcloudsLocationConfig.ACCESS_IDENTITY, "testWithPrivateAddress"));
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

    @Test
    public void testNodeSetupCustomizer() throws Exception {
        final String testMetadata = "test-metadata";
        obtainMachine(ImmutableMap.of(JCLOUDS_LOCATION_CUSTOMIZERS, ImmutableList.of(new BasicJcloudsLocationCustomizer(){
            @Override
            public void customize(JcloudsLocation location, NodeMetadata node, ConfigBag setup) {
                assertNotNull(node, "node");
                assertNotNull(location, "location");
                setup.configure(ADDITIONAL_CONNECTION_METADATA, testMetadata);
            }
        })));
        Map<?, ?> lastConstructorProps = RecordingSshTool.getLastConstructorProps();
        assertEquals(lastConstructorProps.get(ADDITIONAL_CONNECTION_METADATA.getName()), testMetadata);
    }
    
    @Test
    public void testNodeObtainErrorCustomizer() throws Exception {
        final AtomicBoolean calledPreRelease = new AtomicBoolean();
        final AtomicBoolean calledPostRelease = new AtomicBoolean();
        try {
            obtainMachine(ImmutableMap.of(
                    JcloudsLocationConfig.MACHINE_CREATE_ATTEMPTS, 1,
                    JCLOUDS_LOCATION_CUSTOMIZERS, ImmutableList.of(new BasicJcloudsLocationCustomizer(){

                @Override
                public void customize(JcloudsLocation location, ComputeService computeService,
                        JcloudsMachineLocation machine) {
                    // failing with a node already created
                    throw new FailObtainOnPurposeException("testing obtain failure customizer callbacks");
                }
                @Override
                public void preReleaseOnObtainError(JcloudsLocation jcloudsLocation,
                        @Nullable JcloudsMachineLocation machineLocation,
                        Exception cause) {
                    calledPreRelease.set(true);
                }
                @Override
                public void postReleaseOnObtainError(JcloudsLocation jcloudsLocation,
                        @Nullable JcloudsMachineLocation machineLocation,
                        Exception cause) {
                    calledPostRelease.set(true);
                }
            })));
            Asserts.shouldHaveFailedPreviously("Expected exception of type " + FailObtainOnPurposeException.class.getSimpleName());
        } catch (FailObtainOnPurposeException e) {
            // expected
        }
        assertTrue(calledPreRelease.get(), "preReleaseOnObtainError not called on failed onObtain");
        assertTrue(calledPostRelease.get(), "postReleaseOnObtainError not called on failed onObtain");
    }

    @Test
    public void testNodeObtainErrorCustomizerNoDestroy() throws Exception {
        final AtomicBoolean calledPreRelease = new AtomicBoolean();
        final AtomicBoolean calledPostRelease = new AtomicBoolean();
        try {
            obtainMachine(ImmutableMap.of(
                    JcloudsLocationConfig.MACHINE_CREATE_ATTEMPTS, 1,
                    JcloudsLocationConfig.DESTROY_ON_FAILURE, false,
                    JCLOUDS_LOCATION_CUSTOMIZERS, ImmutableList.of(new BasicJcloudsLocationCustomizer(){

                @Override
                public void customize(JcloudsLocation location, ComputeService computeService,
                        JcloudsMachineLocation machine) {
                    // failing with a node already created
                    throw new FailObtainOnPurposeException("testing obtain failure customizer callbacks");
                }
                @Override
                public void preReleaseOnObtainError(JcloudsLocation jcloudsLocation,
                        @Nullable JcloudsMachineLocation machineLocation,
                        Exception cause) {
                    calledPreRelease.set(true);
                }
                @Override
                public void postReleaseOnObtainError(JcloudsLocation jcloudsLocation,
                        @Nullable JcloudsMachineLocation machineLocation,
                        Exception cause) {
                    calledPostRelease.set(true);
                }
            })));
            Asserts.shouldHaveFailedPreviously("Expected exception of type " + FailObtainOnPurposeException.class.getSimpleName());
        } catch (FailObtainOnPurposeException e) {
            // expected
        }
        assertTrue(calledPreRelease.get(), "preReleaseOnObtainError not called on failed onObtain");
        assertFalse(calledPostRelease.get(), "postReleaseOnObtainError not to be called on failed onObtain when destroyOnFailure=false");
    }
}
