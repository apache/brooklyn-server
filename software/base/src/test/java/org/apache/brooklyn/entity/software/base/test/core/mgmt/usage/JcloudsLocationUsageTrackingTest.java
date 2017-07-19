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
package org.apache.brooklyn.entity.software.base.test.core.mgmt.usage;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.fail;

import java.lang.reflect.Field;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.mgmt.internal.LocalUsageManager;
import org.apache.brooklyn.core.mgmt.usage.LocationUsage;
import org.apache.brooklyn.core.mgmt.usage.LocationUsage.LocationEvent;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest;
import org.apache.brooklyn.location.jclouds.AbstractJcloudsStubbedLiveTest;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecCmd;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.time.Time;
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

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * These tests confirm that we (correctly!) retrieve the metadata for jclouds VMs.
 * 
 * They are live tests: they talk directly to the cloud to get the image details etc,
 * but they do not provision VMs (that part is stubbed out).
 * 
 * Some tests expect it to try to ssh. This leads to some unusual configuration! We open up a 
 * server socket, listening on a random high-number port. We return a jclouds NodeMetadata that
 * claims its public IP + login port is that of the server-socket (thus hopefully avoiding the 
 * risk of risk of accidentally executing ssh commands on the local machine!). We also configure
 * the location to use the RecordingSshTool, which stubs out the ssh execution.
 */
public class JcloudsLocationUsageTrackingTest extends AbstractJcloudsStubbedLiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsLocationUsageTrackingTest.class);

    private TestApplication app;
    private SoftwareProcessEntityTest.MyService entity;
    
    /**
     * A socket that is used to simulate an ssh endpoint. The JcloudsLocation code just waits for
     * the port to be reachable, before then switching to the SshTool for executing commands. We
     * therefore need a real socket (rather than just relying on {@link RecordingSshTool}.
     */
    private ServerSocket serverSocket;

    /**
     * If true, then real hardware metadata is included in the jclouds node. If false, we leave
     * the hardware as null. This causes {@link JcloudsSshMachineLocation} to try executing an
     * ssh command to find out the OS, architecture, etc.
     */
    protected boolean includeNodeHardwareMetadata;
    
    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();

        app = managementContext.getEntityManager().createEntity(EntitySpec.create(TestApplication.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true));
        entity = app.createAndManageChild(EntitySpec.create(SoftwareProcessEntityTest.MyService.class));
        
        serverSocket = new ServerSocket();
        serverSocket.bind(new InetSocketAddress(Networking.getReachableLocalHost(), 0), 0);
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (serverSocket != null) serverSocket.close();
        }
    }
    
    @Override
    protected NodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myNodeId")
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(serverSocket.getLocalPort())
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.of(serverSocket.getInetAddress().getHostAddress()))
                        .privateAddresses(ImmutableList.of("1.2.3.4"))
                        .imageId(template.getImage().getId())
                        .tags(template.getOptions().getTags())
                        .hardware(includeNodeHardwareMetadata ? template.getHardware() : null)
                        .group(template.getOptions().getGroups().isEmpty() ? "myGroup" : Iterables.get(template.getOptions().getGroups(), 0))
                        .build();
                return result;
            }
        };
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testLocationEventGetsMetadataFromCloudProvider() throws Exception {
        includeNodeHardwareMetadata = true;
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(
                getLocationSpec(), 
                jcloudsLocationConfig(ImmutableMap.<Object, Object>builder()
                        .put(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                        .put("sshToolClass", RecordingSshTool.class.getName())
                        .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")
                        .put(JcloudsLocation.WAIT_FOR_SSHABLE.getName(), false)
                        .put(JcloudsLocation.IMAGE_ID.getName(), "UBUNTU_14_64")
                        .put(JcloudsLocation.MIN_RAM.getName(), 1024)
                        .put(JcloudsLocation.MIN_CORES.getName(), 1)
                        .build()));
        

        // Start the app; expect record of location in use (along with metadata)
        app.start(ImmutableList.of(jcloudsLocation));
        JcloudsSshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), JcloudsSshMachineLocation.class).get();

        // Expect usage information, including metadata about machine (obtained by ssh'ing)
        Set<LocationUsage> usages = managementContext.getUsageManager().getLocationUsage(Predicates.alwaysTrue());
        LocationUsage usage = findLocationUsage(usages, machine.getId());
        LOG.info("metadata="+usage.getMetadata());
        assertMetadata(usage.getMetadata(), ImmutableMap.<String, String>builder()
                .put("displayName", machine.getDisplayName())
                .put("parentDisplayName", jcloudsLocation.getDisplayName())
                .put("provider", jcloudsLocation.getProvider())
                .put("account", jcloudsLocation.getIdentity())
                .put("region", jcloudsLocation.getRegion())
                .put("serverId", "myNodeId")
                .put("imageId", "UBUNTU_14_64")
                .put("instanceTypeId", "cpu=1,memory=1024,disk=25,type=LOCAL")
                .put("ram", "1024")
                .put("cpus", "1")
                .put("osName", "ubuntu")
                .put("osArch", "x86_64")
                .put("is64bit", "true")
                .build());
    }
    
    @Test(groups={"Live", "Live-sanity"})
    public void testLocationEventMetadataObtainedOverSsh() throws Exception {
        runLocationEventTrackingSshCalls(false);
    }

    /**
     * Previously (see BROOKLYN-299), after a rebind we've lost the usage records, so if a location is
     * subsequently unmanaged we'd attempt to obtain the os-details again (by executing an ssh command).
     * But the machine had been terminated, so that ssh command would eventually timeout (e.g. after 
     * two minutes).
     */
    @Test(groups={"Live", "Live-sanity"})
    public void testLocationEventMetadataNotObtainedOverSshOnStop() throws Exception {
        runLocationEventTrackingSshCalls(true);
    }
    
    protected void runLocationEventTrackingSshCalls(boolean simulateRebind) throws Exception {
        includeNodeHardwareMetadata = false;
        
        String osDetailsResponse = Joiner.on("\n").join(
                "name:Acme OS",
                "version:10.11.5",
                "architecture:x86_64",
                "ram:16384",
                "cpus:8");
        RecordingSshTool.setCustomResponse(
                ".*os-release.*", 
                new RecordingSshTool.CustomResponse(0, osDetailsResponse, ""));

        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(
                getLocationSpec(), 
                jcloudsLocationConfig(ImmutableMap.<Object, Object>builder()
                        .put(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                        .put("sshToolClass", RecordingSshTool.class.getName())
                        .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "1m")
                        .put(JcloudsLocation.WAIT_FOR_SSHABLE.getName(), "1m")
                        .put(JcloudsLocation.IMAGE_ID.getName(), "UBUNTU_14_64")
                        .build()));
        
        SoftwareProcessEntityTest.MyService entity = app.createAndManageChild(EntitySpec.create(SoftwareProcessEntityTest.MyService.class));

        // Start the app; expect record of location in use (along with metadata)
        long preStart = System.currentTimeMillis();
        app.start(ImmutableList.of(jcloudsLocation));
        long postStart = System.currentTimeMillis();
        JcloudsSshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), JcloudsSshMachineLocation.class).get();

        // imageId=myImageId, instanceTypeId=cpu=1,memory=1024,disk=25,type=LOCAL, ram=1024, cpus=1, osName=ubuntu, osArch=x86_64, is64bit=true} expected [Acme OS] but found [ubuntu]

        // Expect usage information, including metadata about machine (obtained by ssh'ing)
        Set<LocationUsage> usages1 = managementContext.getUsageManager().getLocationUsage(Predicates.alwaysTrue());
        LocationUsage usage1 = findLocationUsage(usages1, machine.getId());
        assertEquals(usage1.getLocationId(), machine.getId(), "usage="+usage1);
        LOG.info("metadata="+usage1.getMetadata());
        assertMetadata(usage1.getMetadata(), ImmutableMap.<String, String>builder()
                .put("displayName", machine.getDisplayName())
                .put("parentDisplayName", jcloudsLocation.getDisplayName())
                .put("provider", jcloudsLocation.getProvider())
                .put("account", jcloudsLocation.getIdentity())
                .put("region", jcloudsLocation.getRegion())
                .put("serverId", "myNodeId")
                .put("imageId", "UBUNTU_14_64")
                .put("osName", "Acme OS")
                .put("osArch", "x86_64")
                .put("is64bit", "true")
                .build());
        LocationEvent event1 = usage1.getEvents().get(0);
        assertLocationEvent(event1, entity, Lifecycle.CREATED, preStart, postStart);
        
        assertCmdContains(RecordingSshTool.execScriptCmds, "os-release");
        
        // Clear the ssh-history, so we can assert again
        RecordingSshTool.clear();

        if (simulateRebind) {
            managementContext.getStorage().getMap(LocalUsageManager.APPLICATION_USAGE_KEY).clear();
            managementContext.getStorage().getMap(LocalUsageManager.LOCATION_USAGE_KEY).clear();
            Field machineDetailsField = Reflections.findField(JcloudsSshMachineLocation.class, "machineDetails");
            machineDetailsField.setAccessible(true);
            machineDetailsField.set(machine, null);
        }
        
        // Stop the app; expect location-event for "destroyed". 
        // Expect *not* to have exec'ed ssh command again.
        long preStop = System.currentTimeMillis();
        app.stop();
        long postStop = System.currentTimeMillis();

        Set<LocationUsage> usages2 = managementContext.getUsageManager().getLocationUsage(Predicates.alwaysTrue());
        LocationUsage usage2 = findLocationUsage(usages2, machine.getId());
        LOG.info("metadata="+usage2.getMetadata());
        assertEquals(usage2.getLocationId(), machine.getId(), "usage="+usage2);
        if (simulateRebind) {
            assertMetadata(usage2.getMetadata(), ImmutableMap.<String, String>builder()
                    .put("displayName", machine.getDisplayName())
                    .put("parentDisplayName", jcloudsLocation.getDisplayName())
                    .put("provider", jcloudsLocation.getProvider())
                    .put("account", jcloudsLocation.getIdentity())
                    .put("region", jcloudsLocation.getRegion())
                    .put("serverId", "myNodeId")
                    .put("imageId", "UBUNTU_14_64")
                    .build());
        } else {
            assertMetadata(usage2.getMetadata(), usage2.getMetadata());
        }
        LocationEvent event2 = usage2.getEvents().get(usage2.getEvents().size()-1);
        assertLocationEvent(event2, app.getApplicationId(), entity.getId(), entity.getEntityType().getName(), Lifecycle.DESTROYED, preStop, postStop);
        
        assertCmdNotContains(RecordingSshTool.execScriptCmds, "os-release");
    }

    // Assets everything in "expected" is in the metadata; allows additional values in the metadata
    private void assertMetadata(Map<String, String> metadata, Map<String, String> expected) {
        String errMsg = "metadata="+metadata;
        for (Map.Entry<String, String> entry : expected.entrySet()) {
            assertEquals(metadata.get(entry.getKey()), entry.getValue(), errMsg);
        }
    }

    private LocationUsage findLocationUsage(Iterable<? extends LocationUsage> usages, String locationId) {
        for (LocationUsage usage : usages) {
            if (locationId.equals(usage.getLocationId())) {
                return usage;
            }
        }
        throw new NoSuchElementException("No location-usage for "+locationId+": "+usages);
    }
    
    private void assertCmdContains(Iterable<? extends ExecCmd> cmds, String expected) {
        if (!doesCmdContain(cmds, expected)) {
            fail("'"+expected+"' not found in executed commands: "+cmds);
        }
    }
    
    private void assertCmdNotContains(Iterable<? extends ExecCmd> cmds, String expected) {
        if (doesCmdContain(cmds, expected)) {
            fail("Expected '"+expected+"' not present, but found in executed commands: "+cmds);
        }
    }

    private boolean doesCmdContain(Iterable<? extends ExecCmd> cmds, String expected) {
        for (ExecCmd cmd : cmds) {
            for (String subCmd : cmd.commands) {
                if (subCmd.contains(expected)) {
                    return true;
                }
            }
        }
        return false;
    }

    private void assertLocationEvent(LocationEvent event, Entity expectedEntity, Lifecycle expectedState, long preEvent, long postEvent) {
        assertLocationEvent(event, expectedEntity.getApplicationId(), expectedEntity.getId(), expectedEntity.getEntityType().getName(), expectedState, preEvent, postEvent);
    }
    
    private void assertLocationEvent(LocationEvent event, String expectedAppId, String expectedEntityId, String expectedEntityType, Lifecycle expectedState, long preEvent, long postEvent) {
        // Saw times differ by 1ms - perhaps different threads calling currentTimeMillis() can get out-of-order times?!
        final int TIMING_GRACE = 5;
        
        assertEquals(event.getApplicationId(), expectedAppId);
        assertEquals(event.getEntityId(), expectedEntityId);
        assertEquals(event.getEntityType(), expectedEntityType);
        assertEquals(event.getState(), expectedState);
        long eventTime = event.getDate().getTime();
        if (eventTime < (preEvent - TIMING_GRACE) || eventTime > (postEvent + TIMING_GRACE)) {
            fail("for "+expectedState+": event=" + Time.makeDateString(eventTime) + "("+eventTime + "); "
                    + "pre=" + Time.makeDateString(preEvent) + " ("+preEvent+ "); "
                    + "post=" + Time.makeDateString(postEvent) + " ("+postEvent + ")");
        }
    }
}
