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
package org.apache.brooklyn.entity.software.base.behavior.softwareprocess.supplier;


import com.google.common.collect.ImmutableList;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.entity.software.base.InboundPortsUtils;
import org.apache.brooklyn.entity.software.base.SoftwareProcessImpl;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

public class MachineProvisioningLocationFlagsSupplier extends AbstractLocationFlagsSupplier {

    private static final Logger log = LoggerFactory.getLogger(MachineProvisioningLocationFlagsSupplier.class);

    public MachineProvisioningLocationFlagsSupplier(AbstractEntity entity) {
        super(entity);
    }

    @Override
    public Map<String,Object> obtainFlagsForLocation(Location location){
        return obtainProvisioningFlags((MachineProvisioningLocation)location);
    }

    protected Map<String,Object> obtainProvisioningFlags(MachineProvisioningLocation location) {
        ConfigBag result = ConfigBag.newInstance(location.getProvisioningFlags(ImmutableList.of(getClass().getName())));
        result.putAll(entity().getConfig(SoftwareProcessImpl.PROVISIONING_PROPERTIES));
        if (result.get(CloudLocationConfig.INBOUND_PORTS) == null) {
            Collection<Integer> ports = getRequiredOpenPorts();
            Object requiredPorts = result.get(CloudLocationConfig.ADDITIONAL_INBOUND_PORTS);
            if (requiredPorts instanceof Integer) {
                ports.add((Integer) requiredPorts);
            } else if (requiredPorts instanceof Iterable) {
                for (Object o : (Iterable<?>) requiredPorts) {
                    if (o instanceof Integer) ports.add((Integer) o);
                }
            }
            if (ports != null && ports.size() > 0) result.put(CloudLocationConfig.INBOUND_PORTS, ports);
        }
        result.put(LocationConfigKeys.CALLER_CONTEXT, entity());
        return result.getAllConfigMutable();
    }

    /**
     * Returns the ports that this entity wants to be opened.
     * @see org.apache.brooklyn.entity.software.base.InboundPortsUtils#getRequiredOpenPorts(org.apache.brooklyn.api.entity.Entity, Set, Boolean, String)
     * @see org.apache.brooklyn.entity.software.base.SoftwareProcessImpl#REQUIRED_OPEN_LOGIN_PORTS
     * @see org.apache.brooklyn.entity.software.base.SoftwareProcessImpl#INBOUND_PORTS_AUTO_INFER
     * @see org.apache.brooklyn.entity.software.base.SoftwareProcessImpl#INBOUND_PORTS_CONFIG_REGEX
     */
    @SuppressWarnings("serial")
    public Collection<Integer> getRequiredOpenPorts() {
        Set<Integer> ports = MutableSet.copyOf(entity().getConfig(SoftwareProcessImpl.REQUIRED_OPEN_LOGIN_PORTS));
        Boolean portsAutoInfer = entity().getConfig(SoftwareProcessImpl.INBOUND_PORTS_AUTO_INFER);
        String portsRegex = entity().getConfig(SoftwareProcessImpl.INBOUND_PORTS_CONFIG_REGEX);
        ports.addAll(InboundPortsUtils.getRequiredOpenPorts(entity(), entity().config().getBag().getAllConfigAsConfigKeyMap().keySet(), portsAutoInfer, portsRegex));
        return ports;
    }

}
