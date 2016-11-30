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
package org.apache.brooklyn.cm.salt;

import java.util.Map;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.drivers.DriverDependentEntity;
import org.apache.brooklyn.api.entity.drivers.EntityDriver;
import org.apache.brooklyn.api.entity.drivers.EntityDriverManager;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.drivers.BasicEntityDriverManager;
import org.apache.brooklyn.core.entity.drivers.ReflectiveEntityDriverFactory.AbstractDriverInferenceRule;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class SaltIntegrationTest extends BrooklynAppLiveTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(SaltIntegrationTest.class);
    private static final boolean USE_SIMULATED_LOCATION = true;

    private Location testLocation;

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        super.setUp();

        testLocation = USE_SIMULATED_LOCATION ? app.newSimulatedLocation() : getByonLocation();
        EntityDriverManager edm = mgmt.getEntityDriverManager();
        if (edm != null && edm instanceof BasicEntityDriverManager) {
        	BasicEntityDriverManager bedm = (BasicEntityDriverManager) edm;
        	bedm.getReflectiveDriverFactory().addRule(
        			DriverInferenceForSimulatedLocation.DEFAULT_IDENTIFIER, 
        			new DriverInferenceForSimulatedLocation());
        }
    }

    @AfterMethod(alwaysRun = true)
    public void tearDown() throws Exception {
        if (mgmt != null) {
        	Entities.destroyAll(mgmt);
        }
    }

    /**
     * Test that the broker starts up and sets SERVICE_UP correctly.
     */
    @Test(groups = "Integration")
    public void canStartupAndShutdown() throws Exception {
    	SaltEntity salt = app.createAndManageChild(EntitySpec.create(SaltEntity.class));

    	salt.start(ImmutableList.of(testLocation));
    	salt.stop();
    	
    	Assert.assertFalse(salt.getAttribute(Startable.SERVICE_UP));
    }

    public Location getByonLocation() throws NoMachinesAvailableException {
    	BrooklynProperties brooklynProperties = mgmt.getBrooklynProperties();
        brooklynProperties.put("brooklyn.location.byon.user", "hadrian");
        brooklynProperties.put("brooklyn.location.byon.password", "secret");
        String spec = "byon";
        Map<String, ?> flags = ImmutableMap.of(
            "hosts", ImmutableList.of("10.1.1.10", "10.1.1.11"),
            "osFamily", "linux");

        @SuppressWarnings("unchecked")
		MachineProvisioningLocation<MachineLocation> provisioner = 
        	(MachineProvisioningLocation<MachineLocation>) mgmt.getLocationRegistry().resolve(spec, flags);
        return provisioner.obtain(ImmutableMap.of());

    }
    public static class DriverInferenceForSimulatedLocation extends AbstractDriverInferenceRule {

        public static final String DEFAULT_IDENTIFIER = "simulated-location-driver-inference-rule";

        @Override
        public <D extends EntityDriver> String inferDriverClassName(DriverDependentEntity<D> entity, Class<D> driverInterface, Location location) {
            String driverInterfaceName = driverInterface.getName();
            if (!(location instanceof SimulatedLocation)) {
            	return null;
            }
            if (!driverInterfaceName.endsWith("Driver")) {
                throw new IllegalArgumentException(String.format("Driver name [%s] doesn't end with 'Driver'; cannot auto-detect SshDriver class name", driverInterfaceName));
            }
            return Strings.removeFromEnd(driverInterfaceName, "Driver") + "SimulatedDriver";
        }
    }

}
