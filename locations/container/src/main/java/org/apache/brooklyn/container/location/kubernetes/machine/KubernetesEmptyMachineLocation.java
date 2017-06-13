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
package org.apache.brooklyn.container.location.kubernetes.machine;

import java.net.InetAddress;
import java.util.Set;

import org.apache.brooklyn.api.location.MachineDetails;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.location.OsDetails;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.net.Networking;

import com.google.common.collect.ImmutableSet;

/**
 * A {@link MachineLocation} represemnting a Kubernetes resource that does not support SSH access.
 *
 * @see {@link KubernetesSshMachineLocation}
 */
public class KubernetesEmptyMachineLocation extends SshMachineLocation implements KubernetesMachineLocation {

    @Override
    public String getHostname() {
        return getResourceName();
    }

    @Override
    public Set<String> getPublicAddresses() {
        return ImmutableSet.of("0.0.0.0");
    }

    @Override
    public Set<String> getPrivateAddresses() {
        return ImmutableSet.of("0.0.0.0");
    }

    @Override
    public InetAddress getAddress() {
        return Networking.getInetAddressWithFixedName("0.0.0.0");
    }

    @Override
    public OsDetails getOsDetails() {
        return null;
        // throw new UnsupportedOperationException("No OS details for empty KubernetesMachineLocation");
    }

    @Override
    public MachineDetails getMachineDetails() {
        return null;
        // throw new UnsupportedOperationException("No machine details for empty KubernetesMachineLocation");
    }

    @Override
    public String getResourceName() {
        return config().get(KUBERNETES_RESOURCE_NAME);
    }

    @Override
    public String getResourceType() {
        return config().get(KUBERNETES_RESOURCE_TYPE);
    }

    @Override
    public String getNamespace() {
        return config().get(KUBERNETES_NAMESPACE);
    }

}
