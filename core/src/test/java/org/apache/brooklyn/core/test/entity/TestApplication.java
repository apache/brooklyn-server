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
package org.apache.brooklyn.core.test.entity;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.StartableApplication;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;

/**
 * Mock application for testing.
 */
//TODO Don't want to extend EntityLocal/EntityInternal, but tests want to call things like app.setAttribute
@ImplementedBy(TestApplicationImpl.class)
public interface TestApplication extends StartableApplication, EntityInternal {

    public static final AttributeSensor<String> MY_ATTRIBUTE = Sensors.newStringSensor("test.myattribute", "Test attribute sensor");
    
    public static final LocationSpec<LocalhostMachineProvisioningLocation> LOCALHOST_PROVISIONER_SPEC = LocationSpec.create(LocalhostMachineProvisioningLocation.class);
    public static final LocationSpec<LocalhostMachineProvisioningLocation.LocalhostMachine> LOCALHOST_MACHINE_SPEC = LocationSpec.create(LocalhostMachineProvisioningLocation.LocalhostMachine.class);

    public <T extends Entity> T createAndManageChild(EntitySpec<T> spec);

    /** Create a managed location instance for use in tests.
     * Note that it is typically preferred to pass a location spec to the entity spec instead,
     * e.g. using {@link #LOCALHOST_PROVISIONER_SPEC} or {@link #LOCALHOST_MACHINE_SPEC};
     * however in many tests this is simpler. */
    public LocalhostMachineProvisioningLocation newLocalhostProvisioningLocation();
    public SimulatedLocation newSimulatedLocation();
    public LocalhostMachineProvisioningLocation newLocalhostProvisioningLocation(Map<?,?> flags);

    public static class Factory {
        public static TestApplication newManagedInstanceForTests(ManagementContext mgmt) {
            return mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        }
        public static TestApplication newManagedInstanceForTests() {
            return newManagedInstanceForTests(new LocalManagementContextForTests());
        }
    }

}
