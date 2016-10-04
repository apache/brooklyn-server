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
package org.apache.brooklyn.location.jclouds.networking;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerImpl;
import org.apache.brooklyn.location.jclouds.AbstractJcloudsStubbedLiveTest;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.networking.JcloudsPortForwardingStubbedLiveTest.RecordingJcloudsPortForwarderExtension;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponseGenerator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.net.Cidr;
import org.apache.brooklyn.util.net.Protocol;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * The VM creation is stubbed out, but it still requires live access (i.e. real account credentials)
 * to generate the template etc.
 * 
 * Simulates the creation of a VM that has multiple IPs. Checks that we choose the right address.
 * 
 */
public class JcloudsReachableAddressStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    // TODO Aim is to test the various situations/permutations, where we pass in different config.
    // More tests still need to be added.

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JcloudsReachableAddressStubbedLiveTest.class);

    protected String reachableIp;
    protected List<String> publicAddresses;
    protected List<String> privateAddresses;
    protected AddressChooser addressChooser;
    protected CustomResponseGeneratorImpl customResponseGenerator;

    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        reachableIp = null; // expect test method to set this
        publicAddresses = null; // expect test method to set this
        privateAddresses = null; // expect test method to set this
        addressChooser = new AddressChooser();
        customResponseGenerator = new CustomResponseGeneratorImpl();
        RecordingSshTool.clear();
        super.setUp();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingSshTool.clear();
        }
    }
    
    @Override
    protected AbstractNodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            int nextIpSuffix = 2;
            @Override
            protected NodeMetadata newNode(String group, Template template) {
                int ipSuffix = nextIpSuffix++;
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myid-"+ipSuffix)
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

    protected AbstractNodeCreator getNodeCreator() {
        return (AbstractNodeCreator) nodeCreator;
    }
    
    /**
     * Only one public and one private; public is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test(groups = {"Live", "Live-sanity"})
    protected void testMachineUsesVanillaPublicAddress() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = "1.1.1.1";

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .build());
        
        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    /**
     * Only one public and one private; private is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test(groups = {"Live", "Live-sanity"})
    protected void testMachineUsesVanillaPrivateAddress() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = "2.1.1.1";

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .build());
        
        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), reachableIp);
    }

    /**
     * Multiple public addresses; chooses the reachable one;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test(enabled=false, groups = {"Live", "Live-sanity"})
    protected void testMachineUsesReachablePublicAddress() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.2");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = "1.1.1.2";

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .build());
        
        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    /**
     * Multiple private addresses; chooses the reachable one;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test(enabled=false, groups = {"Live", "Live-sanity"})
    protected void testMachineUsesReachablePrivateAddress() throws Exception {
        publicAddresses = ImmutableList.<String>of("1.1.1.1");
        privateAddresses = ImmutableList.of("2.1.1.1", "2.1.1.2", "2.1.1.2");
        reachableIp = "2.1.1.2";

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .build());
        
        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), reachableIp);
    }

    /**
     * No waitForSshable: should not try to ssh.
     * Therefore will also not search for reachable address (as that expects loginPort to be reachable).
     */
    @Test(groups = {"Live", "Live-sanity"})
    protected void testNoWaitFroSshable() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.2");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = "1.1.1.2";

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, "false")
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .build());
        
        addressChooser.assertNotCalled();
        assertTrue(RecordingSshTool.getExecCmds().isEmpty());

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    /**
     * No pollForFirstReachableAddress: should use first public IP.
     */
    @Test(groups = {"Live", "Live-sanity"})
    protected void testNoPollForFirstReachable() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.2");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = null;

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, "false")
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")
                .build());
        
        addressChooser.assertNotCalled();
        assertTrue(RecordingSshTool.getExecCmds().isEmpty());

        assertEquals(machine.getAddress().getHostAddress(), "1.1.1.1");
        assertEquals(machine.getHostname(), "1.1.1.1");
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    // TODO What is the right behaviour for getHostname?
    @Test(groups = {"Live", "Live-sanity"})
    protected void testReachabilityChecksWithPortForwarding() throws Exception {
        publicAddresses = ImmutableList.of("1.1.1.1");
        privateAddresses = ImmutableList.<String>of("2.1.1.1");
        reachableIp = "1.2.3.4";
                
        PortForwardManager pfm = new PortForwardManagerImpl();
        RecordingJcloudsPortForwarderExtension portForwarder = new RecordingJcloudsPortForwarderExtension(pfm);
        
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.USE_PORT_FORWARDING, true)
                .put(JcloudsLocation.PORT_FORWARDER, portForwarder)
                .build());
        
        addressChooser.assertNotCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_PORT.getName()), 12345);

        assertEquals(machine.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(machine.getPort(), 12345);
        assertEquals(machine.getSshHostAndPort(), HostAndPort.fromParts("1.2.3.4", 12345));
        assertEquals(machine.getHostname(), "1.1.1.1");
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }
    
    protected JcloudsSshMachineLocation newMachine() throws Exception {
        return newMachine(ImmutableMap.<ConfigKey<?>, Object>of());
    }
    
    protected JcloudsSshMachineLocation newMachine(Map<? extends ConfigKey<?>, ?> additionalConfig) throws Exception {
        return obtainMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, addressChooser)
                .putAll(additionalConfig)
                .build());
    }
    
    protected class AddressChooser implements Predicate<HostAndPort> {
        final List<HostAndPort> calls = Lists.newCopyOnWriteArrayList();
        
        @Override public boolean apply(HostAndPort input) {
            calls.add(input);
            return reachableIp != null && reachableIp.equals(input.getHostText());
        }
        
        public void assertCalled() {
            assertTrue(calls.size() > 0, "no calls to "+this);
        }
        
        public void assertNotCalled() {
            assertTrue(calls.isEmpty(), "unexpected calls to "+this+": "+calls);
        }
    }
    
    protected class CustomResponseGeneratorImpl implements CustomResponseGenerator {
        @Override public CustomResponse generate(ExecParams execParams) throws Exception {
            System.out.println("ssh call: "+execParams);
            Object host = execParams.constructorProps.get(SshTool.PROP_HOST.getName());
            if (reachableIp != null && reachableIp.equals(host)) {
                return new CustomResponse(0, "", "");
            } else {
                throw new IOException("Simulate VM not reachable for host '"+host+"'");
            }
        }
    }
}
