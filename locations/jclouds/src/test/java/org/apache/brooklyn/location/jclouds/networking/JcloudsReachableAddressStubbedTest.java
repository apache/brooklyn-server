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
import org.jclouds.compute.domain.OsFamily;
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

    protected ImmutableSet<String> additionalReachableIps;
    protected AddressChooser addressChooser;
    protected CustomResponseGeneratorImpl customResponseGenerator;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        additionalReachableIps = null;
        addressChooser = new AddressChooser();
        customResponseGenerator = new CustomResponseGeneratorImpl();
        jcloudsLocation =  initStubbedJcloudsLocation(ImmutableMap.of());
    }

    /**
     * Only one public and one private; public is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesVanillaPublicAddress() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestMachineUsesVanillaPublicAddress")
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        // Expect an SSH connection to have been made when determining readiness.
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);

        assertEquals(machine.getAddress().getHostAddress(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getHostname(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * Similar to {@link #testMachineUsesVanillaPublicAddress()}, except for a windows machine.
     */
    @Test
    public void testWindowsMachineUsesVanillaPublicAddress() throws Exception {
        JcloudsWinRmMachineLocation machine = newWinrmMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestWindowsMachineUsesVanillaPublicAddress")
                .put(JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);

        assertEquals(machine.getAddress().getHostAddress(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getHostname(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * Only one public and one private; private is reachable;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesVanillaPrivateAddress() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestMachineUsesVanillaPrivateAddress")
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);

        assertEquals(machine.getAddress().getHostAddress(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getHostname(), PUBLIC_IP_ADDRESS); // preferes public, even if that was not reachable
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * Multiple public addresses; chooses the reachable one;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesReachablePublicAddress() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestMachineUsesReachablePublicAddress")

                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);

        assertEquals(machine.getAddress().getHostAddress(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getHostname(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * Multiple private addresses; chooses the reachable one;
     * With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
     */
    @Test
    public void testMachineUsesReachablePrivateAddress() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestMachineUsesReachablePrivateAddress")

                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .build());

        addressChooser.assertCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);

        assertEquals(machine.getAddress().getHostAddress(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getHostname(), "144.175.1.1"); // prefers public, even if that was not reachable
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS); // TODO uses first, rather than "reachable"; is that ok?
    }

    /**
     * No waitForSshable: should not try to ssh.
     * Therefore will also not search for reachable address (as that expects loginPort to be reachable).
     */
    @Test
    public void testNoWaitForSshable() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestNoWaitForSshable")

                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, "false")
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")
                .build());

        addressChooser.assertNotCalled();
        assertTrue(RecordingSshTool.getExecCmds().isEmpty());

        assertEquals(machine.getAddress().getHostAddress(), "144.175.1.1");
        assertEquals(machine.getHostname(), "144.175.1.1");
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * No pollForFirstReachableAddress: should use first public IP.
     */
    @Test
    public void testNoPollForFirstReachable() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityFortestNoPollForFirstReachable")

                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")
                .build());

        addressChooser.assertNotCalled();

        assertEquals(machine.getAddress().getHostAddress(), "144.175.1.1");
        assertEquals(machine.getHostname(), "144.175.1.1");
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    @Test
    public void testReachabilityChecksWithPortForwarding() throws Exception {
        PortForwardManager pfm = new PortForwardManagerImpl();
        RecordingJcloudsPortForwarderExtension portForwarder = new RecordingJcloudsPortForwarderExtension(pfm);

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestReachabilityChecksWithPortForwarding")

                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .put(JcloudsLocation.USE_PORT_FORWARDING, true)
                .put(JcloudsLocation.PORT_FORWARDER, portForwarder)
                .build());

        addressChooser.assertNotCalled();
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);
        assertEquals(RecordingSshTool.getLastExecCmd().constructorProps.get(SshTool.PROP_PORT.getName()), 12345);

        assertEquals(machine.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(machine.getPort(), 12345);
        assertEquals(machine.getSshHostAndPort(), HostAndPort.fromParts("1.2.3.4", 12345));
        assertEquals(machine.getHostname(), PUBLIC_IP_ADDRESS);
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    /**
     * Similar to {@link #testMachineUsesVanillaPublicAddress()}, except for a windows machine.
     */
    @Test
    public void testWindowsReachabilityChecksWithPortForwarding() throws Exception {
        PortForwardManager pfm = new PortForwardManagerImpl();
        RecordingJcloudsPortForwarderExtension portForwarder = new RecordingJcloudsPortForwarderExtension(pfm);

        JcloudsWinRmMachineLocation machine = newWinrmMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "identityForTestWindowsReachabilityChecksWithPortForwarding")

                .put(JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.millis(50).toString())
                .put(JcloudsLocation.USE_PORT_FORWARDING, true)
                .put(JcloudsLocation.PORT_FORWARDER, portForwarder)
                .build());

        addressChooser.assertNotCalled();
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_HOST.getName()), PUBLIC_IP_ADDRESS);
        assertEquals(RecordingWinRmTool.getLastExec().constructorProps.get(WinRmTool.PROP_PORT.getName()), 12345);

        assertEquals(machine.getAddress().getHostAddress(), "1.2.3.4");
        assertEquals(machine.getPort(), 12345);
        // FIXME JcloudsWinRmMachineLocation has a different behavior compare to JcloudsSshMachineLocation
        // assertEquals(machine.getHostname(), Strings.format("brooklyn-p1l6i7-%s", System.getProperty("user.name")));
        assertEquals(machine.getSubnetIp(), PRIVATE_IP_ADDRESS);
    }

    @Test
    public void testMachineUsesFirstPublicAddress() throws Exception {
        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.millis(50).toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, false)
                .build());

        assertEquals(machine.getAddress().getHostAddress(), "144.175.1.1");
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
        this.additionalReachableIps = otherReachableIps;

        ConnectivityResolver customizer =
                new DefaultConnectivityResolver(ImmutableMap.of(DefaultConnectivityResolver.NETWORK_MODE, networkMode));

        JcloudsSshMachineLocation machine = newMachine(ImmutableMap.<ConfigKey<?>, Object>builder()
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Duration.ONE_SECOND.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Duration.ONE_SECOND.toString())
                .put(JcloudsLocation.CONNECTIVITY_RESOLVER, customizer)
                .build());

        assertEquals(machine.getAddress().getHostAddress(), preferredIp);
    }

    protected JcloudsSshMachineLocation newMachine(Map<? extends ConfigKey<?>, ?> additionalConfig) throws Exception {
        return obtainMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, addressChooser)
                .putAll(additionalConfig)
                .build());
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
                    .add("PUBLIC_IP_ADDRESS", PUBLIC_IP_ADDRESS)
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
        return (PUBLIC_IP_ADDRESS != null && PUBLIC_IP_ADDRESS.equals(address)) ||
                (additionalReachableIps != null && additionalReachableIps.contains(address));
    }
}
