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

import org.apache.brooklyn.api.location.MachineManagementMixins;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import org.apache.brooklyn.core.location.MachineLifecycleUtils.GivesMachineStatus;
import org.apache.brooklyn.core.location.MachineLifecycleUtils.MachineStatus;

public class BasicMachineMetadata implements MachineManagementMixins.MachineMetadata, GivesMachineStatus {

    final String id, name, primaryIp;
    final Boolean isRunning;
    final MachineStatus status;
    final Object originalMetadata;

    public BasicMachineMetadata(String id, String name, String primaryIp, MachineStatus status, Object originalMetadata) {
        super();
        this.id = id;
        this.name = name;
        this.primaryIp = primaryIp;
        this.status = status;
        this.isRunning = MachineStatus.RUNNING.equals(status);
        this.originalMetadata = originalMetadata;
    }
    @Deprecated /** @deprecated since 1.1, use other constructor */
    public BasicMachineMetadata(String id, String name, String primaryIp, Boolean isRunning, Object originalMetadata) {
        super();
        this.id = id;
        this.name = name;
        this.primaryIp = primaryIp;
        this.isRunning = isRunning;
        this.status = Boolean.TRUE.equals(isRunning) ? MachineStatus.RUNNING : MachineStatus.UNKNOWN;
        this.originalMetadata = originalMetadata;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getPrimaryIp() {
        return primaryIp;
    }

    @Override
    public Boolean isRunning() {
        return isRunning;
    }

    @Override
    public MachineStatus getStatus() {
        if (status!=null) return status;
        if (isRunning()) return MachineStatus.RUNNING;
        return MachineStatus.UNKNOWN;
    }

    @Override
    public Object getOriginalMetadata() {
        return originalMetadata;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id, isRunning, name, originalMetadata, primaryIp);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null) return false;
        if (getClass() != obj.getClass()) return false;
        BasicMachineMetadata other = (BasicMachineMetadata) obj;
        if (!Objects.equal(id, other.id)) return false;
        if (!Objects.equal(name, other.name)) return false;
        if (!Objects.equal(primaryIp, other.primaryIp)) return false;
        if (!Objects.equal(isRunning, other.isRunning)) return false;
        if (!Objects.equal(originalMetadata, other.originalMetadata)) return false;
        return true;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("id", id).add("name", name).add("originalMetadata", originalMetadata).toString();
    }
    
}
