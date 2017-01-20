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
package org.apache.brooklyn.entity.software.base;

import static org.apache.brooklyn.test.Asserts.assertEquals;
import static org.apache.brooklyn.test.Asserts.assertNotNull;
import static org.apache.brooklyn.test.Asserts.fail;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.ProvisioningLocation;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.internal.AttributesInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.AbstractEc2LiveTest;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class SoftwareProcessStopsDuringStartJcloudsLiveTest extends BrooklynAppLiveTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessStopsDuringStartJcloudsLiveTest.class);

    // same image as in AbstractEc2LiveTest.test_CentOS_6_3
    // Image: {id=us-east-1/ami-a96b01c0, providerId=ami-a96b01c0, name=CentOS-6.3-x86_64-GA-EBS-02-85586466-5b6c-4495-b580-14f72b4bcf51-ami-bb9af1d2.1, location={scope=REGION, id=us-east-1, description=us-east-1, parent=aws-ec2, iso3166Codes=[US-VA]}, os={family=centos, arch=paravirtual, version=6.3, description=aws-marketplace/CentOS-6.3-x86_64-GA-EBS-02-85586466-5b6c-4495-b580-14f72b4bcf51-ami-bb9af1d2.1, is64Bit=true}, description=CentOS-6.3-x86_64-GA-EBS-02 on EBS x86_64 20130527:1219, version=bb9af1d2.1, status=AVAILABLE[available], loginUser=root, userMetadata={owner=679593333241, rootDeviceType=ebs, virtualizationType=paravirtual, hypervisor=xen}})
    public static final String PROVIDER = "aws-ec2";
    public static final String REGION_NAME = "us-east-1";
    public static final String IMAGE_ID = "us-east-1/ami-a96b01c0";
    public static final String HARDWARE_ID = AbstractEc2LiveTest.SMALL_HARDWARE_ID;
    public static final String LOCATION_SPEC = PROVIDER + (REGION_NAME == null ? "" : ":" + REGION_NAME);

    protected BrooklynProperties brooklynProperties;

    protected ExecutorService executor;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        brooklynProperties = BrooklynProperties.Factory.newDefault();

        // Also removes scriptHeader (e.g. if doing `. ~/.bashrc` and `. ~/.profile`, then that can cause "stdin: is not a tty")
        brooklynProperties.remove("brooklyn.ssh.config.scriptHeader");

        mgmt = new LocalManagementContextForTests(brooklynProperties);
        super.setUp();
        
        executor = Executors.newCachedThreadPool();
    }

    @Override
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
        super.tearDown();
    }
    
    // Integration because takes approx 1 seconds
    @Test(groups = "Integration")
    public void testStartStopSequentiallyIsQuickInLocalhost() throws Exception {
        LocalhostMachineProvisioningLocation localLoc = mgmt.getLocationManager().createLocation(LocationSpec.create(LocalhostMachineProvisioningLocation.class)
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()));
        runStartStopSequentiallyIsQuick(localLoc);
    }
    
    // Integration because takes approx 1 seconds
    @Test(groups = "Integration")
    public void testStartStopSequentiallyIsQuickInByon() throws Exception {
        FixedListMachineProvisioningLocation<?> byonLoc = mgmt.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.<LocationSpec<? extends MachineLocation>>of(
                        LocationSpec.create(SshMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()))));
        runStartStopSequentiallyIsQuick(byonLoc);
    }
    
    protected void runStartStopSequentiallyIsQuick(final ProvisioningLocation<?> loc) throws Exception {
        final EmptySoftwareProcess entity = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true));
        
        executeInLimitedTime(new Callable<Void>() {
            @Override
            public Void call() {
                app.start(ImmutableList.of(loc));
                return null;
            }
        }, Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        EntityAsserts.assertEntityHealthy(entity);
        assertEquals(entity.getAttribute(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE), null);
        assertEquals(entity.getAttribute(MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE), Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get());

        executeInLimitedTime(new Callable<Void>() {
            @Override
            public Void call() {
                Entities.destroy(app);
                return null;
            }
        }, Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_ACTUAL), Lifecycle.STOPPED);
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_EXPECTED).getState(), Lifecycle.STOPPED);
        assertEquals(entity.getAttribute(AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE), null);
        assertEquals(entity.getAttribute(MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE), null);
    }

    /**
     * Verifies the behavior described in
     * <a href="https://issues.apache.org/jira/browse/BROOKLYN-264">BROOKLYN-264 Stop app while VM still being provisioned: vm is left running when app is expunged</a>
     * <ul>
     *     <li>Launch the app
     *     <li>wait a few seconds (for entity internal state to indicate the provisioning is happening)
     *     <li>Expunge the app (thus calling stop on the entity)
     *     <li>assert the image is terminated (using jclouds directly to query the cloud api)
     * </ul>
     */
    @Test(groups = {"Live"})
    public void testJclousMachineIsExpungedWhenStoppedDuringStart() throws Exception {
        Map<String,?> allFlags = ImmutableMap.<String,Object>builder()
                .put("tags", ImmutableList.of(getClass().getName()))
                .put(JcloudsLocation.IMAGE_ID.getName(), IMAGE_ID)
                .put(JcloudsLocation.HARDWARE_ID.getName(), HARDWARE_ID)
                .put(LocationConfigKeys.CLOUD_MACHINE_NAMER_CLASS.getName(), "")
                .put(JcloudsLocation.MACHINE_CREATE_ATTEMPTS.getName(), 1)
                .put(JcloudsLocation.OPEN_IPTABLES.getName(), true)
                .build();
        JcloudsLocation jcloudsLocation = (JcloudsLocation)mgmt.getLocationRegistry().getLocationManaged(LOCATION_SPEC, allFlags);

        final VanillaSoftwareProcess entity = app.createAndManageChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "echo install")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "echo launch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "echo running"));

        app.addLocations(ImmutableList.of(jcloudsLocation));

        // Invoke async
        @SuppressWarnings("unused")
        Task<Void> startTask = Entities.invokeEffector(app, app, Startable.START, ImmutableMap.of("locations", MutableList.of()));
        EntityAsserts.assertAttributeEqualsEventually(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, AttributesInternal.ProvisioningTaskState.RUNNING);

        Stopwatch stopwatch = Stopwatch.createStarted();
        Entities.destroyCatching(app);
        LOG.info("Time for expunging: {}", Duration.of(stopwatch));

        NodeMetadata nodeMetadata = Iterables.getFirst(((AWSEC2ComputeService) jcloudsLocation.getComputeService()).listNodesDetailsMatching(new Predicate<ComputeMetadata>() {
                @Override public boolean apply(@Nullable ComputeMetadata computeMetadata) {
                    return ((NodeMetadata)computeMetadata).getGroup() == null 
                            ? false
                            : Pattern.matches(
                                "brooklyn-.*" + System.getProperty("user.name") + ".*vanillasoftware.*"+entity.getId().substring(0, 4),
                                ((NodeMetadata)computeMetadata).getGroup()
                                );
                }}),
            null);
        assertNotNull(nodeMetadata, "node matching node found");
        LOG.info("nodeMetadata found after app was created: {}", nodeMetadata);
        
        // If pending (e.g. "status=PENDING[shutting-down]"), wait for it to transition
        Stopwatch pendingStopwatch = Stopwatch.createStarted();
        Duration maxPendingWait = Duration.FIVE_MINUTES;
        while (maxPendingWait.isLongerThan(Duration.of(pendingStopwatch)) && nodeMetadata.getStatus() == NodeMetadata.Status.PENDING) {
            Thread.sleep(1000);
            nodeMetadata = ((AWSEC2ComputeService) jcloudsLocation.getComputeService()).getNodeMetadata(nodeMetadata.getId());
        }
        
        if (nodeMetadata.getStatus() != NodeMetadata.Status.TERMINATED) {
            // Try to terminate the VM - don't want test to leave it behind!
            String errMsg = "The application should be destroyed after stop effector was called: status="+nodeMetadata.getStatus()+"; node="+nodeMetadata;
            LOG.error(errMsg);
            jcloudsLocation.getComputeService().destroyNode(nodeMetadata.getId());
            fail(errMsg);
        }
    }

    private <T> T executeInLimitedTime(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
        Future<T> future = executor.submit(callable);
        return future.get(timeout, timeUnit);
    }
}
