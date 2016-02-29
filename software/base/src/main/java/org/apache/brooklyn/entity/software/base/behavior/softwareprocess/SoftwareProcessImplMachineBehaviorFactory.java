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
package org.apache.brooklyn.entity.software.base.behavior.softwareprocess;


import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcessImpl;
import org.apache.brooklyn.entity.software.base.behavior.softwareprocess.supplier.LocationFlagSupplier;
import org.apache.brooklyn.entity.software.base.behavior.softwareprocess.supplier.MachineProvisioningLocationFlagsSupplier;
import org.apache.brooklyn.entity.software.base.lifecycle.LifecycleEffectorTasks;

public class SoftwareProcessImplMachineBehaviorFactory
        implements SoftwareProcessImplBehaviorFactory {

    SoftwareProcessImpl entity;

    public SoftwareProcessImplMachineBehaviorFactory(SoftwareProcessImpl entity){
        this.entity = entity;
    }

    @Override
    public LocationFlagSupplier getLocationFlagSupplier() {
        return new MachineProvisioningLocationFlagsSupplier(entity);
    }

    @Override
    public LifecycleEffectorTasks getLifecycleEffectorTasks() {
        return entity.getConfig(SoftwareProcess.LIFECYCLE_EFFECTOR_TASKS);
    }

    @Override
    public boolean isASupportedLocation(Location location) {
        return (location instanceof MachineProvisioningLocation) ||
                (location instanceof MachineLocation);
    }


}
