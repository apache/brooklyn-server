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
package org.apache.brooklyn.api.location;

import java.util.Map;

import com.google.common.annotations.Beta;

/**
 * Defines mixins for interesting locations.
 */
public class MachineManagementMixins {
    
    public interface RichMachineProvisioningLocation<T extends MachineLocation> extends
            MachineProvisioningLocation<T>, ListsMachines, GivesMachineMetadata, KillsMachines {}

    public interface ListsMachines {
        /**
         * @return A map of machine ID to metadata record for all machines known in a given cloud location.
         */
        Map<String,MachineMetadata> listMachines();
    }
    
    public interface GivesMachineMetadata {
        /**
         * @return the {@link MachineMetadata} for a given (brooklyn) machine location instance,
         * or null if not matched.
         */
        MachineMetadata getMachineMetadata(MachineLocation location);
    }
    
    public interface KillsMachines {
        /** Kills the indicated machine; throws if not recognised or possible */
        void killMachine(MachineLocation machine);
        
        /** Kills the machine indicated by the given (server-side) machine id;
         *  note, the ID is the _cloud-service_ ID,
         *  that is, pass in getMetadata(machineLocation).getId() not the machineLocation.getId() */
        void killMachine(String cloudServiceId);
    }
    
    /** very lightweight machine record */
    public interface MachineMetadata {
        /** The cloud service ID -- distinct from any Brooklyn {@link Location#getId()} */
        String getId();
        String getName();
        String getPrimaryIp();
        Boolean isRunning();
        /** original metadata object, if available; e.g. ComputeMetadata when using jclouds */ 
        Object getOriginalMetadata();
    }

    /**
     * Implement to indicate that a location can suspend and resume machines.
     */
    @Beta
    public interface SuspendResumeLocation extends SuspendsMachines, ResumesMachines {}

    @Beta
    public interface SuspendsMachines {
        /**
         * Suspend the indicated machine.
         */
        void suspendMachine(MachineLocation location);
    }

    @Beta
    public interface ResumesMachines {
        /**
         * Resume the indicated machine.
         */
        MachineLocation resumeMachine(Map<?, ?> flags);
    }

    @Beta
    public interface ShutsdownMachines {
        /**
         * Shut down (stop, but without deleting) the indicated machine, throwing if not possible.
         * Implementations may opt to suspend rather than shut down if that is usually preferred.
         */
        void shutdownMachine(MachineLocation location);

        /**
         * Ensure that a machine that might have been shutdown is running, or throw if not possible.
         * May return the original {@link MachineLocation} or may return a new {@link MachineLocation} if data might have changed.
         */
        MachineLocation startupMachine(MachineLocation location);

        /** Issues a reboot command via the machine location provider (not on-box), or does a shutdown/startup pair
         * (but only if the implementation of {@link #shutdownMachine(MachineLocation)} does a true machine stop, not a suspend).
         */
        MachineLocation rebootMachine(MachineLocation location);
    }

    @Beta
    public interface GivesMetrics {
        /**
         * Gets metrics of a machine within a location. The actual metrics supported depends on the implementation, which should advise which config keys it supports.
         */
        Map<String,Object> getMachineMetrics(MachineLocation location);

        /**
         * Returns metrics for a location. The actual metrics supported depends on the implementation, which should advise which config keys it supports.
         */
        Map<String,Object> getLocationMetrics(Map<String,Object> properties);
    }

}