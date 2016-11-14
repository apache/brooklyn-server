/*
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
package org.apache.brooklyn.core.entity.internal;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.reflect.TypeToken;

public interface AttributesInternal extends Attributes {
    @Beta
    public static final AttributeSensor<ProvisioningTaskState> INTERNAL_PROVISIONING_TASK_STATE = new BasicAttributeSensor<ProvisioningTaskState>(
            TypeToken.of(ProvisioningTaskState.class), 
            "internal.provisioning.task.state",
            "Internal transient sensor (do not use) for tracking the provisioning of a machine (to better handle aborting/rebind)");
    
    @Beta
    public static final AttributeSensor<ProvisioningTaskState> INTERNAL_TERMINATION_TASK_STATE = new BasicAttributeSensor<ProvisioningTaskState>(
            TypeToken.of(ProvisioningTaskState.class), 
            "internal.termination.task.state",
            "Internal transient sensor (do not use) for tracking the termination of a machine (to better handle aborting/rebind)");

    /**
     * Used only internally by {@link org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks}
     * to track provisioning/termination. This is used so the machine can be terminated if stopped while opaque provision
     * call is being made; and is used to report if termination was prematurely aborted (e.g. during Brooklyn restart).
     */
    @Beta
    @VisibleForTesting
    public enum ProvisioningTaskState {
        RUNNING,
        DONE;
    }
}
