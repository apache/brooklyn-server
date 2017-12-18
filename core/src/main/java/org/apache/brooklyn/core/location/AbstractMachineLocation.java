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
package org.apache.brooklyn.core.location;

import java.util.Map;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.mutex.MutexSupport;
import org.apache.brooklyn.util.core.mutex.WithMutexes;

import com.google.common.base.Optional;

public abstract class AbstractMachineLocation extends AbstractLocation implements MachineLocation {

    public static final ConfigKey<MachineDetails> MACHINE_DETAILS = ConfigKeys.newConfigKey(
            MachineDetails.class,
            "machineDetails");

    public static final ConfigKey<Boolean> DETECT_MACHINE_DETAILS = ConfigKeys.newBooleanConfigKey("detectMachineDetails",
            "Attempt to detect machine details automatically.", true);

    protected static final MachineDetails UNKNOWN_MACHINE_DETAILS = new BasicMachineDetails(
            new BasicHardwareDetails(-1, -1),
            new BasicOsDetails("UNKNOWN", "UNKNOWN", "UNKNOWN")
    );

    private volatile MachineDetails machineDetails;
    private final Object machineDetailsLock = new Object();

    private transient WithMutexes mutexSupport;
    private transient final Object mutexSupportCreationLock = new Object();


    public AbstractMachineLocation() {
        this(MutableMap.of());
    }

    public AbstractMachineLocation(Map<?,?> properties) {
        super(properties);
    }

    /**
     * Returns the machine details only if they are already loaded, or available directly as
     * config.
     */
    protected Optional<MachineDetails> getOptionalMachineDetails() {
        MachineDetails result = machineDetails != null ? machineDetails : config().get(MACHINE_DETAILS);
        return Optional.fromNullable(result);
    }

    @Override
    public MachineDetails getMachineDetails() {
        synchronized (machineDetailsLock) {
            if (machineDetails == null) {
                machineDetails = getConfig(MACHINE_DETAILS);
            }
            if (machineDetails == null) {
                boolean detectionEnabled = getConfig(DETECT_MACHINE_DETAILS);
                if (!detectionEnabled || !isManaged()) {
                    return UNKNOWN_MACHINE_DETAILS;
                }
                machineDetails = detectMachineDetails();
            }
            return machineDetails;
        }
    }

    @Override
    public OsDetails getOsDetails() {
        return getMachineDetails().getOsDetails();
    }

    protected abstract MachineDetails detectMachineDetails();

    public WithMutexes mutexes() {
        synchronized (mutexSupportCreationLock) {
            // create on demand so that it is not null after serialization
            if (mutexSupport == null) {
                mutexSupport = new MutexSupport();
            }
            return mutexSupport;
        }
    }

}
