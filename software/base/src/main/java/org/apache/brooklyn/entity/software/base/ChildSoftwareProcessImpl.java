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
package org.apache.brooklyn.entity.software.base;

import java.util.Collection;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.guava.Maybe;

public class ChildSoftwareProcessImpl extends VanillaSoftwareProcessImpl implements ChildSoftwareProcess {

    @Override
    public void init() {
        // Set the child start mode so that we are started after the parent finishes
        getParent().config().set(CHILDREN_STARTABLE_MODE, ChildStartableMode.FOREGROUND_LATE);
    }

    /**
     * Return all the {@link Location}s the parent entity is deployed to.
     * <p>
     * Checks that at least one is an {@link SshMachineLocation}.
     */
    @Override
    public Collection<Location> getLocations() {
        Maybe<SshMachineLocation> machine = Machines.findUniqueMachineLocation(getLocations(), SshMachineLocation.class);
        if (machine.isAbsentOrNull()) {
            throw new IllegalStateException("Must be started as a child of an entity running in an existing SshMachineLocation");
        }

        return getParent().getLocations();
    }

}
