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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerImpl;
import org.apache.brooklyn.location.jclouds.AbstractJcloudsStubbedUnitTest;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.location.jclouds.JcloudsWinRmMachineLocation;
import org.apache.brooklyn.location.jclouds.DefaultConnectivityResolver;
import org.apache.brooklyn.location.jclouds.ConnectivityResolver;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.SingleNodeCreator;
import org.apache.brooklyn.location.jclouds.networking.JcloudsPortForwardingStubbedTest.RecordingJcloudsPortForwarderExtension;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponseGenerator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.WinRmTool;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * Simulates the creation of a VM that has multiple IPs. Checks that we choose the right address.
 */
public class JcloudsReachableAddressStubbedTest extends AbstractJcloudsStubbedUnitTest {

    // TODO Aim is to test the various situations/permutations, where we pass in different config.
    // More tests still need to be added.

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JcloudsReachableAddressStubbedTest.class);

    protected String reachableIp;
    protected ImmutableSet<String> additionalReachableIps;
    protected AddressChooser addressChooser;
    protected CustomResponseGeneratorImpl customResponseGenerator;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        reachableIp = null; // expect test method to set this
        additionalReachableIps = null;
        addressChooser = new AddressChooser();
        customResponseGenerator = new CustomResponseGeneratorImpl();
    }
    
    protected AbstractNodeCreator newNodeCreator(final List<String> publicAddresses, final List<String> privateAddresses) {
        NodeMetadata node = new NodeMetadataBuilder()
                .id("myid-1")
                .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                .loginPort(22)
                .status(Status.RUNNING)
                .publicAddresses(publicAddresses)
                .privateAddresses(privateAddresses)
                .build();
        return new SingleNodeCreator(node);
    }

    /**
     * Only one public and one private; public is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesVanillaPublicAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "1.1.1.1";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        // Expect an SSH connection to have been made when determining readiness.
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    /**
     * Similar to {@link #testMachineUsesVanillaPublicAddress()}, except for a windows machine.
     */
    @Test
    public void testWindowsMachineUsesVanillaPublicAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "1.1.1.1";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsWinRmMachineLocation machine = newWinrmMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), reachableIp);
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    /**
     * Only one public and one private; private is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesVanillaPrivateAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "2.1.1.1";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), "1.1.1.1"); // preferes public, even if that was not reachable
        assertEquals(machine.getSubnetIp(), reachableIp);
    }

    /**
     * Multiple public addresses; chooses the reachable one;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesReachablePublicAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.3");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "1.1.1.2";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
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
    @Test
    public void testMachineUsesReachablePrivateAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1", "2.1.1.2", "2.1.1.3");
        reachableIp = "2.1.1.2";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), reachableIp);

        assertEquals(machine.getAddress().getHostAddress(), reachableIp);
        assertEquals(machine.getHostname(), "1.1.1.1"); // prefers public, even if that was not reachable
        assertEquals(machine.getSubnetIp(), "2.1.1.1"); // TODO uses first, rather than "reachable"; is that ok?
    }

    /**
     * No waitForSshable: should not try to ssh.
     * Therefore will also not search for reachable address (as that expects loginPort to be reachable).
     */
    @Test
    public void testNoWaitForSshable() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.3");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = null;
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

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

    /**
     * No pollForFirstReachableAddress: should use first public IP.
     */
    @Test
    public void testNoPollForFirstReachable() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.3");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = null;
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")
                .build());

        addressChooser.assertNotCalled();

        assertEquals(machine.getAddress().getHostAddress(), "1.1.1.1");
        assertEquals(machine.getHostname(), "1.1.1.1");
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    @Test
    public void testReachabilityChecksWithPortForwarding() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "1.2.3.4";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        PortForwardManager pfm = new PortForwardManagerImpl();
        RecordingJcloudsPortForwarderExtension portForwarder = new RecordingJcloudsPortForwarderExtension(pfm);

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
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

    /**
     * Similar to {@link #testMachineUsesVanillaPublicAddress()}, except for a windows machine.
     */
    @Test
    public void testWindowsReachabilityChecksWithPortForwarding() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1");
        reachableIp = "1.2.3.4";
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        PortForwardManager pfm = new PortForwardManagerImpl();
        RecordingJcloudsPortForwarderExtension portForwarder = new RecordingJcloudsPortForwarderExtension(pfm);

        JcloudsWinRmMachineLocation machine = newWinrmMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .put(JcloudsLocation.USE_PORT_FORWARDING, true)
                .put(JcloudsLocation.PORT_FORWARDER, portForwarder)
                .build());

        addressChooser.assertNotCalled();
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_HOST.getName()), reachableIp);
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_PORT.getName()), 12345);

        assertEquals(machine.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(machine.getPort(), 12345);
        assertEquals(machine.getHostname(), "1.2.3.4"); // TODO Different impl from JcloudsSshMachineLocation!
        assertEquals(machine.getSubnetIp(), "2.1.1.1");
    }

    @Test
    public void testMachineUsesFirstPublicAddress() throws Exception {
        List<String> publicAddresses = ImmutableList.of("1.1.1.1", "1.1.1.2");
        List<String> privateAddresses = ImmutableList.of("2.1.1.1", "2.1.1.2", "2.1.1.3");
        reachableIp = null;
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, false)
                .build());

        assertEquals(machine.getAddress().getHostAddress(), "1.1.1.1");
    }

    @DataProvider(name = "publicPrivateProvider")
    public Object[][] publicPrivateProvider() {
        List<String> pub = ImmutableList.of("1.1.1.1", "1.1.1.2");
        List<String> pri = ImmutableList.of("2.1.1.1", "2.1.1.2", "2.1.1.3");
        return new Object[][]{
                // First available public address chosen.
                {pub, pri, DefaultConnectivityResolver.NetworkMode.PREFER_PUBLIC, "1.1.1.1", ImmutableSet.of("1.1.1.2")},
                // public desired and reachable. private address ignored.
                {pub, pri, DefaultConnectivityResolver.NetworkMode.PREFER_PUBLIC, "1.1.1.2", ImmutableSet.of("2.1.1.1")},
                // public desired but unreachable. first reachable private chosen.
                {pub, pri, DefaultConnectivityResolver.NetworkMode.PREFER_PUBLIC, "2.1.1.2", ImmutableSet.of("2.1.1.3")},
                // private desired and reachable. public addresses ignored.
                {pub, pri, DefaultConnectivityResolver.NetworkMode.PREFER_PRIVATE, "2.1.1.2", ImmutableSet.of("1.1.1.1", "1.1.1.2")},
                // private desired but unreachable.
                {pub, pri, DefaultConnectivityResolver.NetworkMode.PREFER_PRIVATE, "1.1.1.1", ImmutableSet.of()},
        };
    }

    @Test(dataProvider = "publicPrivateProvider")
    public void testPublicPrivatePreference(
            List<String> publicAddresses, List<String> privateAddresses,
            DefaultConnectivityResolver.NetworkMode networkMode, String preferredIp, ImmutableSet<String> otherReachableIps) throws Exception {
        LOG.info("Checking {} preferred when mode={}, other reachable IPs={}, public addresses={} and private={}",
                new Object[]{preferredIp, networkMode, otherReachableIps, publicAddresses, privateAddresses});
        this.reachableIp = preferredIp;
        this.additionalReachableIps = otherReachableIps;
        initNodeCreatorAndJcloudsLocation(newNodeCreator(publicAddresses, privateAddresses), ImmutableMap.of());

        ConnectivityResolver customizer =
                new DefaultConnectivityResolver(ImmutableMap.of(DefaultConnectivityResolver.NETWORK_MODE, networkMode));

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>, Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.ONE_SECOND.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.ONE_SECOND.toString())
                .put(JcloudsLocation.CONNECTIVITY_RESOLVER, customizer)
                .build());

        assertEquals(machine.getAddress().getHostAddress(), preferredIp);
    }

    protected JcloudsSshMachineLocation newMachine() throws Exception {
        return newMachine(ImmutableMap.<ConfigKey<?>, Object>of());
    }
    
    protected JcloudsSshMachineLocation newMachine(Map<? extends ConfigKey<?>, ?> additionalConfig) throws Exception {
        return obtainMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, addressChooser)
                .putAll(additionalConfig)
                .build());
    }
    
    protected JcloudsWinRmMachineLocation newWinrmMachine() throws Exception {
        return newWinrmMachine(ImmutableMap.<ConfigKey<?>, Object>of());
    }
    
    protected JcloudsWinRmMachineLocation newWinrmMachine(Map<? extends ConfigKey<?>, ?> additionalConfig) throws Exception {
        return obtainWinrmMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, addressChooser)
                .put(JcloudsLocation.OS_FAMILY_OVERRIDE, OsFamily.WINDOWS)
                .putAll(additionalConfig)
                .build());
    }
    
    protected class AddressChooser implements Predicate<HostAndPort> {
        final List<HostAndPort> calls = Lists.newCopyOnWriteArrayList();
        
        @Override public boolean apply(HostAndPort input) {
            calls.add(input);
            return isReachable(input.getHostText());
        }
        
        public void assertCalled() {
            assertFalse(calls.isEmpty(), "no calls to "+this);
        }
        
        public void assertNotCalled() {
            assertTrue(calls.isEmpty(), "unexpected calls to "+this+": "+calls);
        }

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("reachableIp", reachableIp)
                    .toString();
        }
    }
    
    protected class CustomResponseGeneratorImpl implements CustomResponseGenerator {
        @Override public CustomResponse generate(ExecParams execParams) throws Exception {
            System.out.println("ssh call: "+execParams);
            Object host = execParams.constructorProps.get(SshTool.PROP_HOST.getName());
            if (isReachable(host.toString())) {
                return new CustomResponse(0, "", "");
            } else {
                throw new IOException("Simulate VM not reachable for host '"+host+"'");
            }
        }
    }

    protected boolean isReachable(String address) {
        return (reachableIp != null && reachableIp.equals(address)) ||
                (additionalReachableIps != null && additionalReachableIps.contains(address));
    }
}
