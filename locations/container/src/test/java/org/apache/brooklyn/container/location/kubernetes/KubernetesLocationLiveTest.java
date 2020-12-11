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
package org.apache.brooklyn.container.location.kubernetes;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesMachineLocation;
import org.apache.brooklyn.container.location.kubernetes.machine.KubernetesSshMachineLocation;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.net.HostAndPort;

/**
 * /**
 * Live tests for deploying simple containers. Particularly useful during dev, but not so useful
 * after that (because assumes the existence of a kubernetes endpoint). It needs configured with
 * something like:
 * <p>
 * {@code -Dtest.brooklyn-container-service.kubernetes.endpoint=http://10.104.2.206:8080}
 */
public class KubernetesLocationLiveTest extends BrooklynAppLiveTestSupport {

    public static final String KUBERNETES_ENDPOINT = System.getProperty("test.brooklyn-container-service.kubernetes.endpoint", "");
    public static final String IDENTITY = System.getProperty("test.brooklyn-container-service.kubernetes.identity", "");
    public static final String CREDENTIAL = System.getProperty("test.brooklyn-container-service.kubernetes.credential", "");
    private static final Logger LOG = LoggerFactory.getLogger(KubernetesLocationLiveTest.class);
    protected KubernetesLocation loc;
    protected List<KubernetesMachineLocation> machines;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        machines = Lists.newCopyOnWriteArrayList();
    }

    // FIXME: Clear up properly: Test leaves deployment, replicas and pods behind if obtain fails.
    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        for (KubernetesMachineLocation machine : machines) {
            try {
                loc.release(machine);
            } catch (Exception e) {
                LOG.error("Error releasing machine " + machine + " in location " + loc, e);
            }
        }
        super.tearDown();
    }

    protected KubernetesLocation newKubernetesLocation(Map<String, ?> flags) throws Exception {
        Map<String, ?> allFlags = MutableMap.<String, Object>builder()
                .put("identity", IDENTITY)
                .put("credential", CREDENTIAL)
                .put("endpoint", KUBERNETES_ENDPOINT)
                .putAll(flags)
                .build();
        return (KubernetesLocation) mgmt.getLocationRegistry().getLocationManaged("kubernetes", allFlags);
    }

    @Test(groups = {"Live"})
    public void testDefault() throws Exception {
        // Default is "brooklyncentral/centos:7"
        runImage(ImmutableMap.<String, Object>of(), "centos", "7");
    }

    @Test(groups = {"Live"})
    public void testMatchesCentos() throws Exception {
        runImage(ImmutableMap.<String, Object>of(KubernetesLocationConfig.OS_FAMILY.getName(), "centos"), "centos", "7");
    }

    @Test(groups = {"Live"})
    public void testMatchesCentos7() throws Exception {
        ImmutableMap<String, Object> conf = ImmutableMap.<String, Object>of(
                KubernetesLocationConfig.OS_FAMILY.getName(), "centos",
                KubernetesLocationConfig.OS_VERSION_REGEX.getName(), "7.*");
        runImage(conf, "centos", "7");
    }

    @Test(groups = {"Live"})
    public void testMatchesUbuntu() throws Exception {
        runImage(ImmutableMap.<String, Object>of(KubernetesLocationConfig.OS_FAMILY.getName(), "ubuntu"), "ubuntu", "14.04");
    }

    @Test(groups = {"Live"})
    public void testMatchesUbuntu16() throws Exception {
        ImmutableMap<String, Object> conf = ImmutableMap.<String, Object>of(
                KubernetesLocationConfig.OS_FAMILY.getName(), "ubuntu",
                KubernetesLocationConfig.OS_VERSION_REGEX.getName(), "16.*");
        runImage(conf, "ubuntu", "16.04");
    }

    @Test(groups = {"Live"})
    public void testCentos7Image() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "brooklyncentral/centos:7"), "centos", "7");
    }

    @Test(groups = {"Live"})
    public void testUbuntu14Image() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "brooklyncentral/ubuntu:14.04"), "ubuntu", "14.04");
    }

    @Test(groups = {"Live"})
    public void testUbuntu16Image() throws Exception {
        runImage(ImmutableMap.of(KubernetesLocationConfig.IMAGE.getName(), "brooklyncentral/ubuntu:16.04"), "ubuntu", "16.04");
    }

    @Test(groups = {"Live"})
    public void testFailsForNonMatching() throws Exception {
        ImmutableMap<String, Object> conf = ImmutableMap.<String, Object>of(
                KubernetesLocationConfig.OS_FAMILY.getName(), "weirdOsFamiliy");
        try {
            runImage(conf, null, null);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No matching image found");
        }
    }

    protected void runImage(Map<String, ?> config, String expectedOs, String expectedVersion) throws Exception {
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .putAll(config)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable(), "not sshable machine=" + machine);
        assertOsNameContains(machine, expectedOs, expectedVersion);
        assertMachinePasswordSecure(machine);
    }

    @Test(groups = {"Live"})
    protected void testUsesSuppliedLoginPassword() throws Exception {
        // Because defaulting to "brooklyncentral/centos:7", it knows to set the loginUserPassword
        // on container creation.
        String password = "myCustomP4ssword";
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), password)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());

        assertTrue(machine.isSshable(), "not sshable machine=" + machine);
        assertEquals(machine.config().get(SshMachineLocation.PASSWORD), password);
    }

    @Test(groups = {"Live"})
    public void testOpenPorts() throws Exception {
        List<Integer> inboundPorts = ImmutableList.of(22, 443, 8000, 8081);
        loc = newKubernetesLocation(ImmutableMap.<String, Object>of());
        SshMachineLocation machine = newContainerMachine(loc, ImmutableMap.<String, Object>builder()
                .put(KubernetesLocationConfig.IMAGE.getName(), "brooklyncentral/centos:7")
                .put(KubernetesLocationConfig.LOGIN_USER_PASSWORD.getName(), "p4ssw0rd")
                .put(KubernetesLocationConfig.INBOUND_PORTS.getName(), inboundPorts)
                .put(LocationConfigKeys.CALLER_CONTEXT.getName(), app)
                .build());
        assertTrue(machine.isSshable());

        String publicHostText = machine.getSshHostAndPort().getHost();
        PortForwardManager pfm = (PortForwardManager) mgmt.getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        for (int targetPort : inboundPorts) {
            HostAndPort mappedPort = pfm.lookup(machine, targetPort);
            assertNotNull(mappedPort, "no mapping for targetPort " + targetPort);
            assertEquals(mappedPort.getHost(), publicHostText);
            assertTrue(mappedPort.hasPort(), "no port-part in " + mappedPort + " for targetPort " + targetPort);
        }
    }

    protected void assertOsNameContains(SshMachineLocation machine, String expectedNamePart, String expectedVersionPart) {
        MachineDetails machineDetails = app.getExecutionContext()
                .submit(BasicMachineDetails.taskForSshMachineLocation(machine))
                .getUnchecked();
        OsDetails osDetails = machineDetails.getOsDetails();
        String osName = osDetails.getName();
        String osVersion = osDetails.getVersion();
        assertTrue(osName != null && osName.toLowerCase().contains(expectedNamePart), "osDetails=" + osDetails);
        assertTrue(osVersion != null && osVersion.toLowerCase().contains(expectedVersionPart), "osDetails=" + osDetails);
    }

    protected SshMachineLocation newContainerMachine(KubernetesLocation loc, Map<?, ?> flags) throws Exception {
        KubernetesMachineLocation result = loc.obtain(flags);
        machines.add(result);
        assertTrue(result instanceof KubernetesSshMachineLocation);
        return (SshMachineLocation) result;
    }

    protected void assertMachinePasswordSecure(SshMachineLocation machine) {
        String password = machine.config().get(SshMachineLocation.PASSWORD);
        assertTrue(password.length() > 10, "password=" + password);
        boolean hasUpper = false;
        boolean hasLower = false;
        boolean hasNonAlphabetic = false;
        for (char c : password.toCharArray()) {
            if (Character.isUpperCase(c)) hasUpper = true;
            if (Character.isLowerCase(c)) hasLower = true;
            if (!Character.isAlphabetic(c)) hasNonAlphabetic = true;
        }
        assertTrue(hasUpper && hasLower && hasNonAlphabetic, "password=" + password);
    }
}
