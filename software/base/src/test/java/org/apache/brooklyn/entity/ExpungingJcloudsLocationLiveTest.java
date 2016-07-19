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
package org.apache.brooklyn.entity;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.mgmt.EntityManagementUtils;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.aws.ec2.compute.AWSEC2ComputeService;
import org.jclouds.compute.domain.ComputeMetadata;
import org.jclouds.compute.domain.NodeMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.apache.brooklyn.test.Asserts.*;

public class ExpungingJcloudsLocationLiveTest extends BrooklynAppLiveTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(ExpungingJcloudsLocationLiveTest.class);

    protected BrooklynProperties brooklynProperties;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        brooklynProperties = BrooklynProperties.Factory.newDefault();

        // Also removes scriptHeader (e.g. if doing `. ~/.bashrc` and `. ~/.profile`, then that can cause "stdin: is not a tty")
        brooklynProperties.remove("brooklyn.ssh.config.scriptHeader");

        mgmt = new LocalManagementContextForTests(brooklynProperties);
        super.setUp();
    }

    @Test
    public void verifyExpungingMockedEntityIsQuick() throws Exception {
        final EmptySoftwareProcess emptySoftwareProcess = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class));
        executeInLimitedTime(new Callable<Void>() {
            public Void call() {
                app.start(ImmutableList.of(mgmt.getLocationManager().createLocation(TestApplication.LOCALHOST_PROVISIONER_SPEC)));
                return null;
            }
        }, 2, TimeUnit.SECONDS);
        EntityAsserts.assertEntityHealthy(emptySoftwareProcess);
        assertEquals(emptySoftwareProcess.getAttribute(MachineLifecycleEffectorTasks.PROVISIONING_TASK_STATE), MachineLifecycleEffectorTasks.ProvisioningTaskState.DONE);

        executeInLimitedTime(new Callable<Void>() {
            public Void call() {
                Entities.destroy(app);
                return null;
            }
        }, 1, TimeUnit.SECONDS);
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_ACTUAL), Lifecycle.STOPPED);
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_EXPECTED).getState(), Lifecycle.STOPPED);
    }

    @Test(groups = "Integration")
    public void verifyExpungingByonLocationIsQuick() throws Exception {
        final VanillaSoftwareProcess entity = app.addChild(EntitySpec.create(VanillaSoftwareProcess.class)
                .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "echo install")
                .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "echo launch")
                .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "echo running"));
        app.addLocations(ImmutableList.of(mgmt.getLocationFactory().createLocation(TestApplication.LOCALHOST_PROVISIONER_SPEC)));

        EntityManagementUtils.start(app);

        succeedsEventually(ImmutableMap.of("timeout", "4s"), new Callable<Boolean>() {
            public Boolean call() {
                assertTrue(entity.sensors().get(Attributes.SERVICE_UP));
                assertEquals(entity.getAttribute(MachineLifecycleEffectorTasks.PROVISIONING_TASK_STATE), MachineLifecycleEffectorTasks.ProvisioningTaskState.DONE);
                return entity.sensors().get(Attributes.SERVICE_UP);
            }
        });

        executeInLimitedTime(new Callable<Void>() {
            public Void call() {
                Entities.destroy(app);
                return null;
            }
        }, 2, TimeUnit.SECONDS);
        // Make sure that the entity will be stopped fast. Two seconds at most.
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_ACTUAL), Lifecycle.STOPPED);
        assertEquals(app.getAttribute(Attributes.SERVICE_STATE_EXPECTED).getState(), Lifecycle.STOPPED);
    }

    public static final String PROVIDER = "aws-ec2";
    public static final String REGION_NAME = "us-west-2";
    public static final String LOCATION_SPEC = PROVIDER + (REGION_NAME == null ? "" : ":" + REGION_NAME);

    /**
     * Verifies the behavior described in
     * <a href="https://issues.apache.org/jira/browse/BROOKLYN-264">BROOKLYN-264 Stop app while VM still being provisioned: vm is left running when app is expunged</a>
     * <ul>
     *     <li>ApplicationResource.launch</li>
     *     <li>wait a few seconds and EntityResponse.expunge</li>
     *     <li>assert the image is on the cloud</li>
     * </ul>
     */
    @Test(groups = {"Live"})
    public void verifyJclousMachineIsExpungedWhenStoppedImmediatelyAfterStart() {
        Map<String,String> flags = ImmutableMap.of("imageId", "us-west-2/ami-cd715dfd", LocationConfigKeys.CLOUD_MACHINE_NAMER_CLASS.getName(), "");
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("tags", ImmutableList.of(getClass().getName()))
                .putAll(flags)
                .build();
        JcloudsLocation jcloudsLocation = (JcloudsLocation)mgmt.getLocationRegistry().getLocationManaged(LOCATION_SPEC, allFlags);

        final EmptySoftwareProcess emptySoftwareProcess = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(EmptySoftwareProcess.PROVISIONING_PROPERTIES.subKey(CloudLocationConfig.INBOUND_PORTS.getName()), ImmutableList.of(22)));

        app.addLocations(ImmutableList.of(jcloudsLocation));

        EntityManagementUtils.start(app);

        succeedsEventually(ImmutableMap.of("timeout", "16s"), new Callable<MachineLifecycleEffectorTasks.ProvisioningTaskState>() {
            public MachineLifecycleEffectorTasks.ProvisioningTaskState call() {
                assertEquals(emptySoftwareProcess.getAttribute(MachineLifecycleEffectorTasks.PROVISIONING_TASK_STATE), MachineLifecycleEffectorTasks.ProvisioningTaskState.RUNNING);
                return emptySoftwareProcess.getAttribute(MachineLifecycleEffectorTasks.PROVISIONING_TASK_STATE);
            }
        });

        long beginTime = System.currentTimeMillis();
        Entities.destroyCatching(app);
        LOG.info("Time for expunging: {}", System.currentTimeMillis() - beginTime);

        NodeMetadata nodeMetadata = Iterables.getFirst(((AWSEC2ComputeService) jcloudsLocation.getComputeService()).listNodesDetailsMatching(new Predicate<ComputeMetadata>() {
                @Override public boolean apply(@Nullable ComputeMetadata computeMetadata) {
                    return ((NodeMetadata)computeMetadata).getGroup() == null ? false
                            : Pattern.matches(
                                "brooklyn-.*" + System.getProperty("user.name") + ".*emptysoftware.*"+emptySoftwareProcess.getId().substring(0, 4),
                                ((NodeMetadata)computeMetadata).getGroup()
                                );
                }}),
            null);
        LOG.info("nodeMetadata found after app was created: {}", nodeMetadata);
        assertTrue(nodeMetadata.getStatus().equals(NodeMetadata.Status.TERMINATED), "The application should be destroyed after stop effector was called.");
    }

    private <T> T executeInLimitedTime(Callable<T> callable, long timeout, TimeUnit timeUnit) throws Exception {
        Future<T> future = Executors.newCachedThreadPool().submit(callable);
        return future.get(timeout, timeUnit);
    }
}
