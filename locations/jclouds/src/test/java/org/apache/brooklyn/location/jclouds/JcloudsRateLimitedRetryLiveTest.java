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

import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.location.jclouds.networking.SecurityGroupEditor;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.domain.Location;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests provisioning machines, where it causes a lot of activity (in an effort to be
 * rate-limited!). We expect the retry to do suitable exponential backoff that the retries
 * eventually succeed, provisioning all the machines without error.
 */
public class JcloudsRateLimitedRetryLiveTest extends AbstractJcloudsLiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsRateLimitedRetryLiveTest.class);

    public static final String LOCATION_SPEC = "jclouds:" + AWS_EC2_PROVIDER + ":" + AWS_EC2_USEAST_REGION_NAME;

    // Image: {id=us-east-1/ami-7d7bfc14, providerId=ami-7d7bfc14, name=RightImage_CentOS_6.3_x64_v5.8.8.5, location={scope=REGION, id=us-east-1, description=us-east-1, parent=aws-ec2, iso3166Codes=[US-VA]}, os={family=centos, arch=paravirtual, version=6.0, description=rightscale-us-east/RightImage_CentOS_6.3_x64_v5.8.8.5.manifest.xml, is64Bit=true}, description=rightscale-us-east/RightImage_CentOS_6.3_x64_v5.8.8.5.manifest.xml, version=5.8.8.5, status=AVAILABLE[available], loginUser=root, userMetadata={owner=411009282317, rootDeviceType=instance-store, virtualizationType=paravirtual, hypervisor=xen}}
    public static final String AWS_EC2_CENTOS_IMAGE_ID = "us-east-1/ami-7d7bfc14";

    protected ListeningExecutorService executor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (executor != null) executor.shutdownNow();
        }
    }

    @Test(groups = {"Live", "Acceptance"})
    public void testCreateOne() throws Exception {
        doMany(1);
    }

    @Test(groups = {"Live", "Acceptance"})
    public void testCreateMany() throws Exception {
        doMany(20);
    }

    @Test(groups = {"Live", "Acceptance"})
    public void testSecurityEditorOne() throws Exception {
        doSecurityEditor(1);
    }

    @Test(groups = {"Live", "Acceptance"})
    public void testSecurityEditorMany() throws Exception {
        doSecurityEditor(50);
    }


    protected void doMany(int num) throws Exception {
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(LOCATION_SPEC);

        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (int i = 0; i < num; i++) {
            final int startingPort = 1024 + i;
            final Runnable task = new Runnable() {
                public void run() {
                    doOnce(startingPort);
                }
            };
            ListenableFuture<?> future = executor.submit(task);
            futures.add(future);
        }
        assertTasksDoneSuccessfully(futures);
    }

    private void assertTasksDoneSuccessfully(List<ListenableFuture<?>> futures) throws InterruptedException,
            ExecutionException {

        // Wait for all to to be done
        Futures.successfulAsList(futures).get();

        // Fail if any of the the tasks failed
        Futures.allAsList(futures).get();
    }

    protected void doOnce(int startingPort) {
        // Use a non-trivial security group, to increase the amount of work done by AWS.
        // Each VM's group has slightly different ports, to avoid any optimisations/sharing.
        final List<Integer> inboundPorts = getPorts(startingPort);

        JcloudsSshMachineLocation machine;
        try {
            // Don't want for ssh'able - we are testing the aws-ec2 API, rather than the subsequent
            // machines that are provisioned.
            machine = obtainMachine(inboundPorts);
        } catch (Exception e) {
            LOG.error("Problem obtaining machine", e);
            throw Exceptions.propagate(e);
        }
        try {
            releaseMachine(machine);
        } catch (Exception e) {
            LOG.error("Problem releasing machine", e);
            throw Exceptions.propagate(e);
        }
    }


    protected void doSecurityEditor(int n) throws Exception {
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(LOCATION_SPEC);

        final List<Integer> inboundPorts = ImmutableList.of(9999);

        JcloudsSshMachineLocation machine;
        try {
            // Don't want for ssh'able - we are testing the aws-ec2 API, rather than the subsequent
            // machines that are provisioned.
            machine = obtainMachine(inboundPorts);
            testSecurityEditorOnMachine(n, machine);
        } catch (Exception e) {
            LOG.error("Problem obtaining machine", e);
            throw Exceptions.propagate(e);
        }

        try {
            releaseMachine(machine);
        } catch (Exception e) {
            LOG.error("Problem releasing machine", e);
            throw Exceptions.propagate(e);
        }
    }

    private void testSecurityEditorOnMachine(int n, final JcloudsSshMachineLocation machine) throws ExecutionException,
            InterruptedException {

        final Location location = machine.getNode().getLocation();
        ComputeService computeService = machine.getParent().getComputeService();
        final Optional<SecurityGroupExtension> securityApi = computeService.getSecurityGroupExtension();
        final SecurityGroupEditor editor = new SecurityGroupEditor(location, securityApi.get());

        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (int c = 0; c < n; c++) {
            final String id = "sgtest-" + Identifiers.makeRandomLowercaseId(6) + "-" + String.valueOf(c);
            final Runnable task = new Runnable() {
                public void run() {
                    doOneSecurityEditorOperationCycle(id, editor, machine);
                }
            };
            ListenableFuture<?> future = executor.submit(task);
            futures.add(future);
        }
        assertTasksDoneSuccessfully(futures);
    }

    private void doOneSecurityEditorOperationCycle(String id, SecurityGroupEditor editor,
             JcloudsSshMachineLocation machine) {

        SecurityGroup securityGroup = editor.createSecurityGroup(id);
        final String groupId = securityGroup.getId();
        final IpPermission permission = aPermission();

        securityGroup = editor.addPermission(securityGroup, permission);
        assertTrue(securityGroup.getIpPermissions().contains(permission));

        securityGroup = editor.removePermission(securityGroup, permission);
        assertFalse(securityGroup.getIpPermissions().contains(permission));

        assertTrue(editor.removeSecurityGroup(securityGroup));
        final Set<SecurityGroup> securityGroups = editor.listSecurityGroupsForNode(machine.getNode().getId());
        for (SecurityGroup s: securityGroups) {
            assertFalse(s.getId().equals(groupId));
        }
    }

    private IpPermission aPermission() {
        return IpPermission.builder()
            .ipProtocol(IpProtocol.TCP)
            .fromPort(22)
            .toPort(22)
            .cidrBlock("0.0.0.0/0")
            .build();
    }

    private List<Integer> getPorts(int startingPort) {
        final List<Integer> inboundPorts = Lists.newArrayList();
        inboundPorts.add(22);
        for (int i = 0; i < 10; i++) {
            inboundPorts.add(startingPort + (i * 2));
        }
        return inboundPorts;
    }

    private JcloudsSshMachineLocation obtainMachine(List<Integer> inboundPorts) throws Exception {
        JcloudsSshMachineLocation machine;
        machine = obtainMachine(MutableMap.<String, Object>builder()
            .put(JcloudsLocation.IMAGE_ID.getName(), AWS_EC2_CENTOS_IMAGE_ID)
            .put(JcloudsLocation.HARDWARE_ID.getName(), AWS_EC2_MEDIUM_HARDWARE_ID)
            .put(JcloudsLocation.INBOUND_PORTS.getName(), inboundPorts)
            .put(JcloudsLocation.WAIT_FOR_SSHABLE.getName(), false)
            .build());
        return machine;
    }
}
