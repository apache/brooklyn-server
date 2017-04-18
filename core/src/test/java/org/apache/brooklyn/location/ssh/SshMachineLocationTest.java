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
package org.apache.brooklyn.location.ssh;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;
import static org.testng.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.EffectorTaskTest;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.location.BasicHardwareDetails;
import org.apache.brooklyn.core.location.BasicMachineDetails;
import org.apache.brooklyn.core.location.BasicOsDetails;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.sshj.SshjTool;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.stream.Streams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

import ch.qos.logback.classic.spi.ILoggingEvent;

/**
 * Test the {@link SshMachineLocation} implementation of the {@link Location} interface.
 */
public class SshMachineLocationTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntity.class);

    protected SshMachineLocation host;
    
    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        host = newHost();
        RecordingSshTool.clear();
    }

    @Override
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            if (host != null) Streams.closeQuietly(host);
        } finally {
            RecordingSshTool.clear();
            super.tearDown();
        }
    }

    protected SshMachineLocation newHost() {
        return mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", Networking.getLocalHost())
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName()));
    }
    
    @Test
    public void testGetMachineDetails() throws Exception {
        String response = Joiner.on("\n").join(
                "name:Test OS Y",
                "version:1.2.3",
                "architecture:x86_64",
                "ram:1234",
                "cpus:3");
        RecordingSshTool.setCustomResponse(".*uname.*", new CustomResponse(0, response, ""));

        BasicExecutionManager execManager = new BasicExecutionManager("mycontextid");
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);
        try {
            MachineDetails details = execContext.submit(new Callable<MachineDetails>() {
                @Override
                public MachineDetails call() {
                    return host.getMachineDetails();
                }}).get();
            LOG.info("machineDetails="+details);
            assertNotNull(details);
            
            assertEquals(details.getOsDetails().getName(), "Test OS Y", "details="+details);
            assertEquals(details.getOsDetails().getVersion(), "1.2.3", "details="+details);
            assertEquals(details.getOsDetails().getArch(), "x86_64", "details="+details);
            assertEquals(details.getHardwareDetails().getCpuCount(), Integer.valueOf(3), "details="+details);
            assertEquals(details.getHardwareDetails().getRam(), Integer.valueOf(1234), "details="+details);
        } finally {
            execManager.shutdownNow();
        }
    }

    @Test
    public void testGetMachineDetailsWithExtraStdout() throws Exception {
        String response = Joiner.on("\n").join(
                "Last login: Fri Apr 14 08:01:37 2017 from 35.156.73.145",
                "Line with no colons",
                ": colon first",
                "colon last:",
                "name:MyCentOS",
                "version:6.7",
                "architecture:x86_64",
                "ram:15948",
                "cpus:4");
        RecordingSshTool.setCustomResponse(".*uname.*", new CustomResponse(0, response, ""));

        BasicExecutionManager execManager = new BasicExecutionManager("mycontextid");
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);
        try {
            MachineDetails details = execContext.submit(new Callable<MachineDetails>() {
                @Override
                public MachineDetails call() {
                    return host.getMachineDetails();
                }}).get();
            LOG.info("machineDetails="+details);
            assertNotNull(details);
            
            assertEquals(details.getOsDetails().getName(), "MyCentOS", "details="+details);
            assertEquals(details.getOsDetails().getVersion(), "6.7", "details="+details);
            assertEquals(details.getOsDetails().getArch(), "x86_64", "details="+details);
            assertEquals(details.getHardwareDetails().getCpuCount(), Integer.valueOf(4), "details="+details);
            assertEquals(details.getHardwareDetails().getRam(), Integer.valueOf(15948), "details="+details);
        } finally {
            execManager.shutdownNow();
        }
    }

    @Test
    public void testSupplyingMachineDetails() throws Exception {
        MachineDetails machineDetails = new BasicMachineDetails(new BasicHardwareDetails(1, 1024), new BasicOsDetails("myname", "myarch", "myversion"));
        SshMachineLocation host2 = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure(SshMachineLocation.MACHINE_DETAILS, machineDetails));
        
        assertSame(host2.getMachineDetails(), machineDetails);
    }
    
    @Test
    public void testConfigurePrivateAddresses() throws Exception {
        SshMachineLocation host2 = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", Networking.getLocalHost())
                .configure(SshMachineLocation.PRIVATE_ADDRESSES, ImmutableList.of("1.2.3.4"))
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true));

        assertEquals(host2.getPrivateAddresses(), ImmutableSet.of("1.2.3.4"));
        assertEquals(Machines.getSubnetIp(host2).get(), "1.2.3.4");
        assertEquals(Machines.getSubnetHostname(host2).get(), "1.2.3.4");
    }
    
    // Wow, this is hard to test (until I accepted creating the entity + effector)! Code smell?
    // Need to call getMachineDetails in a DynamicSequentialTask so that the "innessential" takes effect,
    // to not fail its caller. But to get one of those outside of an effector is non-obvious.
    @Test
    public void testGetMachineIsInessentialOnFailure() throws Exception {
        SshMachineLocation host2 = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", Networking.getLocalHost())
                .configure(SshMachineLocation.SSH_TOOL_CLASS, FailingSshTool.class.getName()));

        final Effector<MachineDetails> GET_MACHINE_DETAILS = Effectors.effector(MachineDetails.class, "getMachineDetails")
                .impl(new EffectorBody<MachineDetails>() {
                    @Override
                    public MachineDetails call(ConfigBag parameters) {
                        Maybe<MachineLocation> machine = Machines.findUniqueMachineLocation(entity().getLocations());
                        try {
                            machine.get().getMachineDetails();
                            throw new IllegalStateException("Expected failure in ssh");
                        } catch (RuntimeException e) {
                            return null;
                        }
                    }})
                .build();

        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .addInitializer(new EntityInitializer() {
                        @Override
                        public void apply(EntityLocal entity) {
                            ((EntityInternal)entity).getMutableEntityType().addEffector(EffectorTaskTest.DOUBLE_1);
                        }});

        TestApplication app = mgmt.getEntityManager().createEntity(appSpec);

        app.start(ImmutableList.of(host2));
        
        MachineDetails details = app.invoke(GET_MACHINE_DETAILS, ImmutableMap.<String, Object>of()).get();
        assertNull(details);
    }
    public static class FailingSshTool extends RecordingSshTool {
        public FailingSshTool(Map<String, ?> props) {
            super(props);
        }
        @Override public int execScript(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
            throw new RuntimeException("Simulating failure of ssh: cmds="+commands);
        }
        @Override public int execCommands(Map<String, ?> props, List<String> commands, Map<String, ?> env) {
            throw new RuntimeException("Simulating failure of ssh: cmds="+commands);
        }
    }
    
    @Test
    public void testSshExecScript() throws Exception {
        String expectedName = Os.user();
        RecordingSshTool.setCustomResponse(".*whoami.*", new CustomResponse(0, expectedName, ""));
        
        OutputStream outStream = new ByteArrayOutputStream();
        host.execScript(MutableMap.of("out", outStream), "mysummary", ImmutableList.of("whoami; exit"));
        String outString = outStream.toString();
        
        assertTrue(outString.contains(expectedName), outString);
    }
    
    @Test
    public void testSshExecCommands() throws Exception {
        String expectedName = Os.user();
        RecordingSshTool.setCustomResponse(".*whoami.*", new CustomResponse(0, expectedName, ""));
        
        OutputStream outStream = new ByteArrayOutputStream();
        host.execCommands(MutableMap.of("out", outStream), "mysummary", ImmutableList.of("whoami; exit"));
        String outString = outStream.toString();
        
        assertTrue(outString.contains(expectedName), outString);
    }
    
    @Test
    public void obtainSpecificPortGivesOutPortOnlyOnce() {
        int port = 2345;
        assertTrue(host.obtainSpecificPort(port));
        assertFalse(host.obtainSpecificPort(port));
        host.releasePort(port);
        assertTrue(host.obtainSpecificPort(port));
    }
    
    @Test
    public void obtainPortInRangeGivesBackRequiredPortOnlyIfAvailable() {
        int port = 2345;
        assertEquals(host.obtainPort(new PortRanges.LinearPortRange(port, port)), port);
        assertEquals(host.obtainPort(new PortRanges.LinearPortRange(port, port)), -1);
        host.releasePort(port);
        assertEquals(host.obtainPort(new PortRanges.LinearPortRange(port, port)), port);
    }
    
    @Test
    public void obtainPortInWideRange() {
        int lowerPort = 2345;
        int upperPort = 2350;
        PortRange range = new PortRanges.LinearPortRange(lowerPort, upperPort);
        for (int i = lowerPort; i <= upperPort; i++) {
            assertEquals(host.obtainPort(range), i);
        }
        assertEquals(host.obtainPort(range), -1);
        
        host.releasePort(lowerPort);
        assertEquals(host.obtainPort(range), lowerPort);
        assertEquals(host.obtainPort(range), -1);
    }
    
    @Test
    public void testObtainPortDoesNotUsePreReservedPorts() {
        host = new SshMachineLocation(MutableMap.of("address", Networking.getLocalHost(), "usedPorts", ImmutableSet.of(8000)));
        assertEquals(host.obtainPort(PortRanges.fromString("8000")), -1);
        assertEquals(host.obtainPort(PortRanges.fromString("8000+")), 8001);
    }
    
    @Test
    public void testDoesNotLogPasswordsInEnvironmentVariables() {
        List<String> loggerNames = ImmutableList.of(
                SshMachineLocation.class.getName(), 
                BrooklynLogging.SSH_IO, 
                SshjTool.class.getName());
        ch.qos.logback.classic.Level logLevel = ch.qos.logback.classic.Level.DEBUG;
        Predicate<ILoggingEvent> filter = Predicates.or(
                EventPredicates.containsMessage("DB_PASSWORD"), 
                EventPredicates.containsMessage("mypassword"));
        LogWatcher watcher = new LogWatcher(loggerNames, logLevel, filter);

        watcher.start();
        try {
            host.execCommands("mySummary", ImmutableList.of("true"), ImmutableMap.of("DB_PASSWORD", "mypassword"));
            watcher.assertHasEventEventually();
            
            Optional<ILoggingEvent> eventWithPasswd = Iterables.tryFind(watcher.getEvents(), EventPredicates.containsMessage("mypassword"));
            assertFalse(eventWithPasswd.isPresent(), "event="+eventWithPasswd);
        } finally {
            watcher.close();
        }
    }
}
