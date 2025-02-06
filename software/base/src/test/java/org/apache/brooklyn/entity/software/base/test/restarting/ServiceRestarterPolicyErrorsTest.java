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
package org.apache.brooklyn.entity.software.base.test.restarting;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle.Transition;
import org.apache.brooklyn.core.location.LocationPredicates;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.policy.ha.ServiceFailureDetector;
import org.apache.brooklyn.policy.ha.ServiceRestarter;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class ServiceRestarterPolicyErrorsTest extends BrooklynAppUnitTestSupport {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ServiceRestarterPolicyErrorsTest.class);
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-534
    @Test
    public void testRestarterWhenStopFails() throws Exception {
        List<LocationSpec<? extends MachineLocation>> machineSpecs = ImmutableList.of(
                LocationSpec.create(SshMachineLocation.class).displayName("machine1").configure("address", "1.1.1.1"),
                LocationSpec.create(SshMachineLocation.class).displayName("machine2").configure("address", "1.1.1.2"));
        FailingMachineProvisioningLocation failingLoc = mgmt.getLocationManager().createLocation(LocationSpec.create(FailingMachineProvisioningLocation.class)
                .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .configure(FailingMachineProvisioningLocation.MACHINE_SPECS, machineSpecs)
                .configure(FailingMachineProvisioningLocation.FAIL_ON_RELEASE, true));
        
        EmptySoftwareProcess entity = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, true)
                .configure(EmptySoftwareProcess.USE_SSH_MONITORING, false)
                .policy(PolicySpec.create(ServiceRestarter.class))
                .enricher(EnricherSpec.create(ServiceFailureDetector.class)
                        .configure(ServiceFailureDetector.ENTITY_FAILED_STABILIZATION_DELAY, Duration.ZERO)));
        app.start(ImmutableList.<Location>of(failingLoc));

        try {
            entity.stop();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Simulating failure of release");
        }

        // As reported in https://issues.apache.org/jira/browse/BROOKLYN-534, previously the stop-failure
        // would cause the ServiceFailureDetector to emit a failure event, which would cause the ServiceRestarter
        // to call start() again, which would make the entity obtain a new machine!
        Asserts.succeedsContinually(new Runnable() {
            public void run() {
                Transition serviceStateExpected = entity.sensors().get(Attributes.SERVICE_STATE_EXPECTED);
                Iterable<MachineLocation> entityMachines = Iterables.filter(entity.getLocations(), MachineLocation.class);
                Optional<MachineLocation> availableMachine2 = Iterables.tryFind(failingLoc.getAvailable(), LocationPredicates.displayNameEqualTo("machine2"));
                String errMsg = "entityStateExpected="+serviceStateExpected+"; entityMachines="+entityMachines+"; availableMachine2="+availableMachine2;
                assertEquals(serviceStateExpected.getState(), Lifecycle.STOPPED, errMsg);
                assertTrue(Iterables.isEmpty(entityMachines), errMsg);
                assertTrue(availableMachine2.isPresent(), errMsg);
            }});
    }
    
    public static class FailingMachineProvisioningLocation extends FixedListMachineProvisioningLocation<MachineLocation> {
        public static final ConfigKey<Boolean> FAIL_ON_RELEASE = ConfigKeys.newBooleanConfigKey("failOnRelease", "Whether to throw exception on call to release", false);
        
        @SuppressWarnings("serial")
        ConfigKey<Class<? extends Exception>> EXCEPTION_CLAZZ = ConfigKeys.newConfigKey(
                new TypeToken<Class<? extends Exception>>() {},
                "exceptionClazz", 
                "Type of exception to throw", 
                IllegalStateException.class);
        
        @Override
        public void release(MachineLocation machine) {
            if (Boolean.TRUE.equals(config().get(FAIL_ON_RELEASE))) {
                throw newException("Simulating failure of release("+machine+")");
            } else {
                super.release(machine);
            }
        }
        
        private RuntimeException newException(String msg) {
            try {
                Exception result = getConfig(EXCEPTION_CLAZZ).getConstructor(String.class).newInstance(msg);
                if (!(result instanceof RuntimeException)) {
                    return new RuntimeException("wrapping", result);
                } else {
                    return (RuntimeException)result;
                }
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        }
    }
}
