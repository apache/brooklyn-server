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
package org.apache.brooklyn.camp.brooklyn;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.LocationPredicates;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.entity.software.base.DoNothingSoftwareProcess;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

public class ByonLocationsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ByonLocationsYamlTest.class);

    @Test
    @SuppressWarnings("unchecked")
    public void testByonSpec() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: byon(user=myuser,mykey=myval,hosts=\"1.1.1.1\")",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);
        
        Set<SshMachineLocation> machines = loc.getAvailable();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts("myuser", "1.1.1.1",  22), ImmutableMap.of("mykey", "myval"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachine() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - ssh: 1.1.1.1:8022",
                "      privateAddresses: [10.0.0.1]",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);
        
        Set<SshMachineLocation> machines = loc.getAvailable();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts("myuser", "1.1.1.1",  8022), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval"));
        assertEquals(machine.getPrivateAddresses(), ImmutableSet.of("10.0.0.1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachineResolvesDependentConfigForHost() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - $brooklyn:config(\"ip_address\")",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    ip_address: 1.2.3.4"
                );

        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);

        Set<SshMachineLocation> machines = loc.getAvailable();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts(machine.getUser(), "1.2.3.4",  22), Collections.emptyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachineResolvesDynamicDependentConfigForHost() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - $brooklyn:attributeWhenReady(\"ip_address\")",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.DoNothingSoftwareProcess"
        );

        Entity app = createApplicationUnstarted(yaml);
        Task<Void> start = app.invoke(Startable.START, null);
        Time.sleep(Duration.millis(100));
        Asserts.assertFalse(start.isDone());

        Iterables.getOnlyElement(app.getChildren()).sensors().set(Sensors.newStringSensor("ip_address"), "1.2.3.4");
        start.getUnchecked();
        start.getUnchecked(Duration.seconds(5));

        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);

        Set<SshMachineLocation> machines = loc.getAllMachines();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts(machine.getUser(), "1.2.3.4", 22), Collections.emptyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachineResolvesDependentConfigForHosts() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts: $brooklyn:config(\"ip_addresses\")",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    ip_addresses: [ 1.2.3.4, { ssh: 1.2.3.5, user: Beth } ]"
        );

        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);

        List<SshMachineLocation> machines = MutableList.copyOf(loc.getAvailable());
        Asserts.assertSize(machines, 2);
        assertMachine(machines.get(0), UserAndHostAndPort.fromParts(machines.get(0).getUser(), "1.2.3.4",  22), Collections.emptyMap());
        assertMachine(machines.get(1), UserAndHostAndPort.fromParts("Beth", "1.2.3.5",  22), Collections.emptyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachineResolvesDependentConfigForUser() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    user: $brooklyn:config(\"uzer\")",
                "    hosts: [ 1.2.3.4 ]",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    uzer: Beth"
        );

        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);

        Set<SshMachineLocation> machines = loc.getAvailable();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts("Beth", "1.2.3.4",  22), Collections.emptyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMachineResolvesDependentConfigForHostWithUser() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "      - ssh: 1.2.3.4",
                "        user: $brooklyn:config(\"uzer\")",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication",
                "  brooklyn.config:",
                "    uzer: Beth"
        );

        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = (FixedListMachineProvisioningLocation<SshMachineLocation>) Iterables.get(app.getLocations(), 0);

        Set<SshMachineLocation> machines = loc.getAvailable();
        SshMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts("Beth", "1.2.3.4",  22), Collections.emptyMap());
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonWindowsMachine() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - winrm: 1.1.1.1:8985",
                "      privateAddresses: [10.0.0.1]",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval",
                "      osFamily: windows",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<WinRmMachineLocation> loc = (FixedListMachineProvisioningLocation<WinRmMachineLocation>) Iterables.get(app.getLocations(), 0);
        
        Set<WinRmMachineLocation> machines = loc.getAvailable();
        WinRmMachineLocation machine = Iterables.getOnlyElement(machines);
        assertMachine(machine, UserAndHostAndPort.fromParts("myuser", "1.1.1.1",  8985), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval"));
        assertEquals(machine.getPrivateAddresses(), ImmutableSet.of("10.0.0.1"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonMultiMachine() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - ssh: 1.1.1.1:8022",
                "      privateAddresses: [10.0.0.1]",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval1",
                "    - ssh: 1.1.1.2:8022",
                "      privateAddresses: [10.0.0.2]",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval2",
                "    - winrm: 1.1.1.3:8985",
                "      privateAddresses: [10.0.0.3]",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval3",
                "      osFamily: windows",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<MachineLocation> loc = (FixedListMachineProvisioningLocation<MachineLocation>) Iterables.get(app.getLocations(), 0);
        
        Set<MachineLocation> machines = loc.getAvailable();
        assertEquals(machines.size(), 3, "machines="+machines);
        SshMachineLocation machine1 = (SshMachineLocation) Iterables.find(machines, LocationPredicates.configEqualTo(ConfigKeys.newStringConfigKey("mykey"), "myval1"));
        SshMachineLocation machine2 = (SshMachineLocation) Iterables.find(machines, LocationPredicates.configEqualTo(ConfigKeys.newStringConfigKey("mykey"), "myval2"));
        WinRmMachineLocation machine3 = (WinRmMachineLocation) Iterables.find(machines, Predicates.instanceOf(WinRmMachineLocation.class));

        assertMachine(machine1, UserAndHostAndPort.fromParts("myuser", "1.1.1.1",  8022), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval1"));
        assertEquals(machine1.getPrivateAddresses(), ImmutableSet.of("10.0.0.1"));

        assertMachine(machine2, UserAndHostAndPort.fromParts("myuser", "1.1.1.2",  8022), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval2"));
        assertEquals(machine2.getPrivateAddresses(), ImmutableSet.of("10.0.0.2"));

        assertMachine(machine3, UserAndHostAndPort.fromParts("myuser", "1.1.1.3",  8985), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval3"));
        assertEquals(machine3.getPrivateAddresses(), ImmutableSet.of("10.0.0.3"));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testByonPortMapping() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - ssh: 1.1.1.1:22",
                "      privateAddresses: [10.0.0.1]",
                "      tcpPortMappings: {22: \"83.222.229.1:12001\", 8080: \"83.222.229.1:12002\"}",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval1",
                "    - winrm: 1.1.1.2:8985",
                "      privateAddresses: [10.0.0.2]",
                "      tcpPortMappings: {8985: \"83.222.229.2:12003\", 8080: \"83.222.229.2:12004\"}",
                "      password: mypassword",
                "      user: myuser",
                "      mykey: myval2",
                "      osFamily: windows",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");

        Entity app = createStartWaitAndLogApplication(yaml);
        FixedListMachineProvisioningLocation<MachineLocation> loc = (FixedListMachineProvisioningLocation<MachineLocation>) Iterables.get(app.getLocations(), 0);
        PortForwardManager pfm = (PortForwardManager) mgmt().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        
        Set<MachineLocation> machines = loc.getAvailable();
        assertEquals(machines.size(), 2, "machines="+machines);
        SshMachineLocation machine1 = (SshMachineLocation) Iterables.find(machines, LocationPredicates.configEqualTo(ConfigKeys.newStringConfigKey("mykey"), "myval1"));
        WinRmMachineLocation machine2 = (WinRmMachineLocation) Iterables.find(machines, Predicates.instanceOf(WinRmMachineLocation.class));

        assertMachine(machine1, UserAndHostAndPort.fromParts("myuser", "83.222.229.1", 12001), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval1"));
        assertEquals(machine1.getPrivateAddresses(), ImmutableSet.of("10.0.0.1"));
        assertEquals(pfm.lookup(machine1, 22), HostAndPort.fromParts("83.222.229.1", 12001));
        assertEquals(pfm.lookup(machine1, 8080), HostAndPort.fromParts("83.222.229.1", 12002));
        assertNull(pfm.lookup(machine1, 12345));
        
        assertMachine(machine2, UserAndHostAndPort.fromParts("myuser", "83.222.229.2",  12003), ImmutableMap.of(
                SshMachineLocation.PASSWORD.getName(), "mypassword",
                "mykey", "myval2"));
        assertEquals(machine2.getPrivateAddresses(), ImmutableSet.of("10.0.0.2"));
        assertEquals(pfm.lookup(machine2, 8985), HostAndPort.fromParts("83.222.229.2", 12003));
        assertEquals(pfm.lookup(machine2, 8080), HostAndPort.fromParts("83.222.229.2", 12004));
        assertNull(pfm.lookup(machine2, 12345));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testPassesInboundPortsToMachineAndRemovesOnceMachineReleased() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  byon:",
                "    hosts:",
                "    - ssh: 1.1.1.1:22",
                "      password: mypassword",
                "      user: myuser",
                "services:",
                "- type: org.apache.brooklyn.entity.software.base.DoNothingSoftwareProcess",
                "  brooklyn.config:",
                "    requiredOpenLoginPorts: [22, 1024]");

        Entity app = createStartWaitAndLogApplication(yaml);
        DoNothingSoftwareProcess entity = (DoNothingSoftwareProcess) Iterables.find(Entities.descendantsAndSelf(app), Predicates.instanceOf(DoNothingSoftwareProcess.class));
        FixedListMachineProvisioningLocation<MachineLocation> loc = (FixedListMachineProvisioningLocation<MachineLocation>) Iterables.get(app.getLocations(), 0);
        
        // Machine should have been given the inbound-ports
        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        Asserts.assertEqualsIgnoringOrder((Iterable<?>)machine.config().get(CloudLocationConfig.INBOUND_PORTS), ImmutableList.of(22, 1024));
        
        // Stop the entity; should release the machine
        entity.stop();
        MachineLocation availableMachine = Iterables.getOnlyElement(loc.getAvailable());
        assertEquals(availableMachine, machine);
        assertNull(machine.config().get(CloudLocationConfig.INBOUND_PORTS));
    }

    // See https://issues.apache.org/jira/browse/BROOKLYN-479
    @Test
    public void testByonSpecWithNoConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: byon",
                "services:",
                "- serviceType: org.apache.brooklyn.entity.stock.BasicApplication");
        
        try {
            Entity app = createStartWaitAndLogApplication(yaml);
            Asserts.shouldHaveFailedPreviously("app="+app);
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Invalid location", "byon", "hosts must be defined");
        }
    }
    
    private void assertMachine(SshMachineLocation machine, UserAndHostAndPort conn, Map<String, ?> config) {
        assertEquals(machine.getAddress().getHostAddress(), conn.getHostAndPort().getHost());
        assertEquals(machine.getPort(), conn.getHostAndPort().getPort());
        assertEquals(machine.getUser(), conn.getUser());
        for (Map.Entry<String, ?> entry : config.entrySet()) {
            Object actualVal = machine.getConfig(ConfigKeys.newConfigKey(Object.class, entry.getKey()));
            assertEquals(actualVal, entry.getValue());
        }
    }
    
    private void assertMachine(WinRmMachineLocation machine, UserAndHostAndPort conn, Map<String, ?> config) {
        assertEquals(machine.getAddress().getHostAddress(), conn.getHostAndPort().getHost());
        assertEquals(machine.getPort(), conn.getHostAndPort().getPort());
        assertEquals(machine.getUser(), conn.getUser());
        for (Map.Entry<String, ?> entry : config.entrySet()) {
            Object actualVal = machine.getConfig(ConfigKeys.newConfigKey(Object.class, entry.getKey()));
            assertEquals(actualVal, entry.getValue());
        }
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
