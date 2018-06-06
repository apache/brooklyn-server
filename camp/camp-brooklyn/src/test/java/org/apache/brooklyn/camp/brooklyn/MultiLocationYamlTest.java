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
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.core.location.cloud.AvailabilityZoneExtension;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.multi.MultiLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class MultiLocationYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(MultiLocationYamlTest.class);

    @Test
    public void testSimpleInSingleLine() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location: multi:(targets=\"localhost,localhost\")",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication");
        runSimple(yaml);
    }

    @Test
    public void testSimpleMultiLine() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  multi:",
                "    targets:",
                "    - localhost",
                "    - localhost",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication");
        runSimple(yaml);
    }

    @SuppressWarnings("unchecked")
    protected void runSimple(String yaml) throws Exception {
        Entity app = createStartWaitAndLogApplication(yaml);
        MultiLocation<MachineLocation> multiLoc = (MultiLocation<MachineLocation>) Iterables.get(app.getLocations(), 0);
        
        assertMultiLocation(multiLoc, 2, 
                Collections.nCopies(2, Predicates.instanceOf(LocalhostMachineProvisioningLocation.class)));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testComplex() throws Exception {
        String yaml = Joiner.on("\n").join(
                "location:",
                "  multi:",
                "    targets:",
                "    - byon:",
                "        hosts:",
                "        - \"myuser@1.1.1.1\"",
                "    - byon:",
                "        hosts:",
                "        - \"myuser2@2.2.2.2\"",
                "services:",
                "- type: org.apache.brooklyn.entity.stock.BasicApplication");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        MultiLocation<MachineLocation> multiLoc = (MultiLocation<MachineLocation>) Iterables.get(app.getLocations(), 0);

        assertMultiLocation(multiLoc, 2, ImmutableList.of(
                byonEqualTo(UserAndHostAndPort.fromParts("myuser", "1.1.1.1", 22)),
                byonEqualTo(UserAndHostAndPort.fromParts("myuser2", "2.2.2.2", 22))));
    }

    
    private void assertMultiLocation(MultiLocation<?> multiLoc, int expectedSize, List<? extends Predicate<? super Location>> expectedSubLocationPredicates) {
        AvailabilityZoneExtension zones = multiLoc.getExtension(AvailabilityZoneExtension.class);
        List<Location> subLocs = zones.getAllSubLocations();
        assertEquals(subLocs.size(), expectedSize, "zones="+subLocs);
        for (int i = 0; i < subLocs.size(); i++) {
            Location subLoc = subLocs.get(i);
            assertTrue(expectedSubLocationPredicates.get(i).apply(subLoc), "index="+i+"; subLocs="+subLocs);
        }
    }
    
    public static Predicate<? super Location> byonEqualTo(UserAndHostAndPort... conns) {
        return Predicates.and(
                Predicates.instanceOf(FixedListMachineProvisioningLocation.class),
                new Predicate<Location>() {
                    @SuppressWarnings("unchecked")
                    @Override public boolean apply(Location rawInput) {
                        MachineProvisioningLocation<SshMachineLocation> input = (MachineProvisioningLocation<SshMachineLocation>) rawInput;
                        List<SshMachineLocation> obtainedMachines = new ArrayList<>();
                        try {
                            for (UserAndHostAndPort conn : conns) {
                                SshMachineLocation machine = (SshMachineLocation) input.obtain(ImmutableMap.of());
                                obtainedMachines.add(machine);
                                if (!machineEqualTo(machine, conn)) {
                                    return false;
                                }
                            }
                        } catch (NoMachinesAvailableException e) {
                            throw Exceptions.propagate(e);
                        } finally {
                            for (SshMachineLocation machine : obtainedMachines) {
                                input.release(machine);
                            }
                        }
                        return true;
                    }
                    private boolean machineEqualTo(SshMachineLocation machine, UserAndHostAndPort conn) {
                        String addr = machine.getAddress().getHostAddress();
                        int port = machine.getPort();
                        String user = machine.getUser();
                        return addr != null && addr.equals(conn.getHostAndPort().getHostText())
                                && port == conn.getHostAndPort().getPortOrDefault(22)
                                && user != null && user.equals(conn.getUser());
                    }
                });
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
