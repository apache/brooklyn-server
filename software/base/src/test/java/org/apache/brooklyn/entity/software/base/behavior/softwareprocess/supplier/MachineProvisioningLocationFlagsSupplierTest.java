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
package org.apache.brooklyn.entity.software.base.behavior.softwareprocess.supplier;

import com.google.common.annotations.Beta;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Set;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

@Beta
public class MachineProvisioningLocationFlagsSupplierTest extends BrooklynAppUnitTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(MachineProvisioningLocationFlagsSupplierTest.class);

    private SshMachineLocation machine;
    private FixedListMachineProvisioningLocation<SshMachineLocation> loc;
    private MachineProvisioningLocationFlagsSupplier machineFlagsSupplier;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = getLocation();
    }

    @SuppressWarnings("unchecked")
    private FixedListMachineProvisioningLocation<SshMachineLocation> getLocation() {
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = mgmt.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class));
        machine = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "localhost"));
        loc.addMachine(machine);
        return loc;
    }

    @Test
    @SuppressWarnings("unchecked")
    public void testMachineDefaultAttributesGeneration() throws Exception {

        SoftwareProcessEntityTest.MyServiceImpl entity =
                new SoftwareProcessEntityTest.MyServiceImpl(app);
        machineFlagsSupplier = new MachineProvisioningLocationFlagsSupplier(entity);

        Map<String, Object> machineFlags = machineFlagsSupplier.obtainFlagsForLocation(loc);
        assertNotNull(machineFlags);
        assertEquals(machineFlags.size(), 2);
        assertTrue(machineFlags.containsKey("inboundPorts"));
        Set<Integer> inboundPorts = (Set<Integer>) machineFlags.get("inboundPorts");
        assertEquals(inboundPorts.size(), 2);
        Integer requiredOpenDefaultLoginPorts =
                (Integer)SoftwareProcessEntityTest.MyService.REQUIRED_OPEN_LOGIN_PORTS
                        .getDefaultValue().toArray()[0];
        assertTrue(inboundPorts.contains(requiredOpenDefaultLoginPorts));
        assertTrue(inboundPorts.contains(new Integer("8080")));
    }

}
