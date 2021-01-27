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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Set;

import org.apache.brooklyn.location.jclouds.DefaultConnectivityResolver.NetworkMode;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.domain.LoginCredentials;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;

public class DefaultConnectivityResolverTest extends AbstractJcloudsStubbedUnitTest {

    private final LoginCredentials credential = LoginCredentials.builder().user("AzureDiamond").password("hunter2").build();

    @Test
    public void testRespectsHostAndPortOverride() throws Exception {
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
        // ideally would confirm that no credentials are tested either.
        ConnectivityResolverOptions options = newResolveOptions()
                .portForwardSshOverride(HostAndPort.fromParts("10.1.1.4", 4361))
                .build();
        DefaultConnectivityResolver customizer = new DefaultConnectivityResolver();
        ConfigBag configBag = jcloudsLocation.config().getBag();
        ManagementAddressResolveResult result = customizer.resolve(jcloudsLocation, newNodeMetadata(), configBag, options);
        assertEquals(result.hostAndPort().getHostText(), "10.1.1.4");
        assertEquals(result.hostAndPort().getPort(), 4361);
    }

    @Test
    public void testObtainsHostnameFromAwsMachine() throws Exception {
        final String expectedHostname = "ec2-awshostname";
        RecordingSshTool.setCustomResponse(".*curl.*169.254.169.254.*", new RecordingSshTool.CustomResponse(0, expectedHostname, ""));
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(
                JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME, true));
        ConnectivityResolverOptions options = newResolveOptions()
                .waitForConnectable(true)
                .pollForReachableAddresses(Predicates.<HostAndPort>alwaysTrue(), Duration.millis(1), true)
                .userCredentials(credential)
                .build();
        DefaultConnectivityResolver customizer = new DefaultConnectivityResolver();
        ConfigBag configBag = jcloudsLocation.config().getBag();
        ManagementAddressResolveResult result = customizer.resolve(
                jcloudsLocation, newNodeMetadata(), configBag, options);
        assertEquals(result.hostAndPort().getHostText(), expectedHostname);
    }

    @Test
    public void testTestCredentialWithLinuxMachine() throws Exception {
        final String allowedUser = "Mr. Big";
        // Match every command.
        RecordingSshTool.setCustomResponse(".*", new RecordingSshTool.CustomResponseGenerator() {
            @Override
            public RecordingSshTool.CustomResponse generate(RecordingSshTool.ExecParams execParams) throws Exception {
                boolean valid = allowedUser.equals(execParams.constructorProps.get("user"));
                return new RecordingSshTool.CustomResponse(valid ? 0 : 1, "", "");
            }
        });
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
        DefaultConnectivityResolver customizer = new DefaultConnectivityResolver();
        final ConfigBag config = ConfigBag.newInstanceExtending(jcloudsLocation.config().getBag(), ImmutableMap.of(
                JcloudsLocationConfig.WAIT_FOR_SSHABLE, "1ms",
                JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS, "1ms"));
        assertTrue(customizer.checkCredential(
                jcloudsLocation, HostAndPort.fromParts("10.0.0.234", 22),
                LoginCredentials.builder().user(allowedUser).password("password1").build(), config, false));
        assertFalse(customizer.checkCredential(
                jcloudsLocation, HostAndPort.fromParts("10.0.0.234", 22), credential, config, false));
    }

    @Test
    public void testTestCredentialWithWindowsMachine() throws Exception {
        final String allowedUser = "Mr. Big";
        // Match every command.
        RecordingWinRmTool.setCustomResponse(".*", new RecordingWinRmTool.CustomResponseGenerator() {
            @Override
            public RecordingWinRmTool.CustomResponse generate(RecordingWinRmTool.ExecParams execParams) {
                boolean valid = allowedUser.equals(execParams.constructorProps.get("user"));
                return new RecordingWinRmTool.CustomResponse(valid ? 0 : 1, "", "");
            }
        });
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
        DefaultConnectivityResolver customizer = new DefaultConnectivityResolver();
        final ConfigBag config = ConfigBag.newInstanceExtending(jcloudsLocation.config().getBag(), ImmutableMap.of(
                JcloudsLocationConfig.WAIT_FOR_WINRM_AVAILABLE, "1ms",
                JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS, "1ms"));
        assertTrue(customizer.checkCredential(
                jcloudsLocation, HostAndPort.fromParts("10.0.0.234", 22),
                LoginCredentials.builder().user(allowedUser).password("password1").build(), config, true));
        assertFalse(customizer.checkCredential(
                jcloudsLocation, HostAndPort.fromParts("10.0.0.234", 22), credential, config, true));
    }

    /**
     * e.g. case when brooklyn on laptop provisions in one location then rebinds later
     * from another location where the ip happens to resolve to a different machine.
     */
    @Test
    public void testResolveChecksCredentials() throws Exception {
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
        final HostAndPort authorisedHostAndPort = HostAndPort.fromParts("10.0.0.2", 22);
        final HostAndPort otherHostAndPort = HostAndPort.fromParts("10.0.0.1", 22);
        final Set<HostAndPort> reachableIps = Sets.newHashSet(authorisedHostAndPort, otherHostAndPort);

        // Checks arguments and exits 0 if host+port match authorised.
        RecordingSshTool.setCustomResponse(".*", new RecordingSshTool.CustomResponseGenerator() {
            @Override
            public RecordingSshTool.CustomResponse generate(RecordingSshTool.ExecParams execParams) throws Exception {
                HostAndPort hap = HostAndPort.fromParts(
                        (String) execParams.constructorProps.get("host"),
                        (Integer) execParams.constructorProps.get("port"));
                int exitCode = authorisedHostAndPort.equals(hap) ? 0 : 1;
                return new RecordingSshTool.CustomResponse(exitCode, "", "");
            }
        });
        ConfigBag config = ConfigBag.newInstanceExtending(jcloudsLocation.config().getBag(), ImmutableMap.of(
                JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME, false,
                JcloudsLocationConfig.WAIT_FOR_SSHABLE, "1ms",
                JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS, "1ms",
                JcloudsLocation.CUSTOM_CREDENTIALS, credential));
        ConnectivityResolverOptions options = newResolveOptionsForIps(reachableIps, Duration.millis(100)).build();

        // Chooses authorisedHostAndPort when credentials are tested.
        DefaultConnectivityResolver customizer = new DefaultConnectivityResolver(ImmutableMap.of(
                DefaultConnectivityResolver.CHECK_CREDENTIALS, true));

        ManagementAddressResolveResult result = customizer.resolve(jcloudsLocation, newNodeMetadata(), config, options);
        assertEquals(result.hostAndPort(), authorisedHostAndPort);
        assertFalse(RecordingSshTool.getExecCmds().isEmpty(), "expected ssh connection to be made when testing credentials");

        // Chooses otherHostAndPort when credentials aren't tested.
        RecordingSshTool.clear();
        customizer = new DefaultConnectivityResolver(ImmutableMap.of(
                DefaultConnectivityResolver.CHECK_CREDENTIALS, false));
        result = customizer.resolve(jcloudsLocation, newNodeMetadata(), config, options);
        assertEquals(result.hostAndPort(), otherHostAndPort);
        assertTrue(RecordingSshTool.getExecCmds().isEmpty(),
                "expected no ssh connection to be made when not testing credentials: " + Iterables.toString(RecordingSshTool.getExecCmds()));
    }

    @DataProvider(name = "testModeDataProvider")
    public Object[][] testModeDataProvider() throws Exception {
        return new Object[][]{
                new Object[]{NetworkMode.ONLY_PUBLIC, haps("10.0.0.2:22", "192.168.0.1:22", "192.168.0.2:22"), "10.0.0.2"},
                new Object[]{NetworkMode.ONLY_PRIVATE, haps("10.0.0.1:22", "10.0.0.2:22", "192.168.0.1:22"), "192.168.0.1"},
                new Object[]{NetworkMode.PREFER_PRIVATE, haps("10.0.0.2:22", "192.168.0.2:22"), "192.168.0.2"},
                new Object[]{NetworkMode.PREFER_PUBLIC, haps("10.0.0.2:22", "192.168.0.1:22", "192.168.0.2:22"), "10.0.0.2"},
                // Preference unavailable so falls back to next best.
                new Object[]{NetworkMode.PREFER_PRIVATE, haps("10.0.0.1:22"), "10.0.0.1"},
                new Object[]{NetworkMode.PREFER_PUBLIC, haps("192.168.0.1:22"), "192.168.0.1"},
        };
    }

    @Test(dataProvider = "testModeDataProvider")
    public void testMode(NetworkMode mode, Set<HostAndPort> reachableIps, String expectedIp) throws Exception {
        final DefaultConnectivityResolver customizer = new DefaultConnectivityResolver(ImmutableMap.of(
                DefaultConnectivityResolver.NETWORK_MODE, mode,
                DefaultConnectivityResolver.CHECK_CREDENTIALS, false));

        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(
                JcloudsLocationConfig.CONNECTIVITY_RESOLVER, customizer));

        ConnectivityResolverOptions options = newResolveOptionsForIps(reachableIps, Duration.millis(100)).build();
        ConfigBag configBag = jcloudsLocation.config().getBag();

        ManagementAddressResolveResult result = customizer.resolve(jcloudsLocation, newNodeMetadata(), configBag, options);
        assertEquals(result.hostAndPort().getHostText(), expectedIp);
    }

    @DataProvider(name = "fallibleModes")
    public Object[][] testFallibleModesDataProvider() throws Exception {
        return new Object[][]{
                new Object[]{NetworkMode.ONLY_PUBLIC, haps("192.168.0.1:22")},
                new Object[]{NetworkMode.ONLY_PRIVATE, haps("10.0.0.1:22")},
                new Object[]{NetworkMode.PREFER_PUBLIC, haps()},
                new Object[]{NetworkMode.PREFER_PRIVATE, haps()}
        };
    }

    /**
     * Tests behaviour when no desired addresses are available.
     */
    @Test(dataProvider = "fallibleModes", expectedExceptions = IllegalStateException.class)
    public void testModeUnavailable(NetworkMode mode, Set<HostAndPort> reachableIps) throws Exception {
        final DefaultConnectivityResolver customizer = new DefaultConnectivityResolver(ImmutableMap.of(
                DefaultConnectivityResolver.NETWORK_MODE, mode,
                DefaultConnectivityResolver.CHECK_CREDENTIALS, false));

        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(
                JcloudsLocationConfig.CONNECTIVITY_RESOLVER, customizer));

        ConnectivityResolverOptions options = newResolveOptionsForIps(reachableIps, Duration.ONE_MILLISECOND).build();
        ConfigBag configBag = jcloudsLocation.config().getBag();
        customizer.resolve(jcloudsLocation, newNodeMetadata(), configBag, options);
    }

    private ConnectivityResolverOptions.Builder newResolveOptions() {
        return ConnectivityResolverOptions.builder()
                .initialCredentials(credential);
    }
    
    private ConnectivityResolverOptions.Builder newResolveOptionsForIps(Set<HostAndPort> reachableIps, Duration timeout) {
        // It's important to not use a tiny timeout (e.g. 1ms) if you expect it to succeed, 
        // because that can fail on apache jenkins. We execute the check in a background
        // thread, and then wait for this timeout for it to succeed. On a slow machine, we 
        // might not have finished executing the predicate, so might abort. 
        // (see `ReachableSocketFinder.tryReachable()`, and its use of `timeout`).
        return newResolveOptions().
            pollForReachableAddresses(Predicates.in(reachableIps), timeout, true);
    }

    private NodeMetadata newNodeMetadata() {
        return new NodeMetadataBuilder()
                .id("id")
                .backendStatus("backendStatus")
                .credentials(credential)
                .group("group")
                .hostname("hostname")
                .loginPort(22)
                .name("DefaultConnectivityResolverTest")
                .publicAddresses(ImmutableList.of("10.0.0.1", "10.0.0.2"))
                .privateAddresses(ImmutableList.of("192.168.0.1", "192.168.0.2"))
                .status(NodeMetadata.Status.RUNNING)
                .tags(ImmutableList.<String>of())
                .userMetadata(ImmutableMap.<String, String>of())
                .build();
    }

    private Set<HostAndPort> haps(String... s) {
        Set<HostAndPort> hap = Sets.newHashSet();
        for (String str : s) {
            hap.add(HostAndPort.fromString(str));
        }
        return hap;
    }
}