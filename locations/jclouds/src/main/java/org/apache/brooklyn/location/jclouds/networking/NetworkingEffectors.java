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
package org.apache.brooklyn.location.jclouds.networking;

import com.google.common.annotations.Beta;
import com.google.common.base.Optional;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;

import java.util.Collection;
import java.util.List;

import static com.google.common.base.Predicates.instanceOf;
import static com.google.common.collect.Iterables.tryFind;
import static org.apache.brooklyn.core.location.Locations.getLocationsCheckingAncestors;

public class NetworkingEffectors {
    // Intentionally not use CloudLocationConfig.INBOUND_PORTS to make richer syntax and rename it to differ it from the first in a ConfigBag
    public static final ConfigKey<List<String>> INBOUND_PORTS_LIST = ConfigKeys.newConfigKey(new TypeToken<List<String>>() {}, "inbound.ports.list",
            "Ports to open from the effector", ImmutableList.<String>of());
    public static final ConfigKey<IpProtocol> INBOUND_PORTS_LIST_PROTOCOL = ConfigKeys.newConfigKey(new TypeToken<IpProtocol>() {}, "inbound.ports.list.protocol",
            "Protocol for ports to open. Possible values: TCP, UDP, ICMP.", IpProtocol.TCP);

    public static final ConfigKey<JcloudsMachineLocation> JCLOUDS_MACHINE_LOCATIN = ConfigKeys.newConfigKey(JcloudsMachineLocation.class, "jcloudsMachineLocation");

    @Beta
    @SuppressWarnings("unchecked")
    public static final Effector<Iterable<IpPermission>> OPEN_INBOUND_PORTS_IN_SECURITY_GROUP_EFFECTOR = (Effector<Iterable<IpPermission>>)(Effector<?>)Effectors.effector(Iterable.class, "openPortsInSecurityGroup")
                .parameter(INBOUND_PORTS_LIST)
                .parameter(INBOUND_PORTS_LIST_PROTOCOL)
                .description("Open ports in Cloud Security Group. If called before machine location is provisioned, it will fail.")
                .impl(new OpenPortsInSecurityGroupBody())
                .build();

    @SuppressWarnings("rawtypes")
    private static class OpenPortsInSecurityGroupBody extends EffectorBody<Iterable> {
        @Override
        public Collection<SecurityGroup> call(ConfigBag parameters) {
            List<String> rawPortRules = parameters.get(INBOUND_PORTS_LIST);
            IpProtocol ipProtocol = parameters.get(INBOUND_PORTS_LIST_PROTOCOL);
            Preconditions.checkNotNull(ipProtocol, INBOUND_PORTS_LIST_PROTOCOL.getName() + " cannot be null");
            Preconditions.checkNotNull(rawPortRules, INBOUND_PORTS_LIST.getName() + " cannot be null");

            SharedLocationSecurityGroupCustomizer locationSecurityGroupCustomizer = new SharedLocationSecurityGroupCustomizer();
            if (IpProtocol.TCP.equals(ipProtocol)) {
                locationSecurityGroupCustomizer.setTcpPortRanges(rawPortRules);
            } else if (IpProtocol.UDP.equals(ipProtocol)) {
                locationSecurityGroupCustomizer.setUdpPortRanges(rawPortRules);
            } else if (IpProtocol.ICMP.equals(ipProtocol)) {
                locationSecurityGroupCustomizer.setOpenIcmp(true);
            }

            Optional<Location> jcloudsMachineLocationOptional = tryFind(
                    (Iterable<Location>) getLocationsCheckingAncestors(null, entity()),
                    instanceOf(JcloudsMachineLocation.class));
            if (!jcloudsMachineLocationOptional.isPresent()) {
                throw new IllegalArgumentException("Tried to execute open ports effector on an entity with no JcloudsMachineLocation");
            }
            JcloudsLocation jcloudsLocation = ((JcloudsMachineLocation)jcloudsMachineLocationOptional.get()).getParent();

            return locationSecurityGroupCustomizer.applySecurityGroupCustomizations(jcloudsLocation, jcloudsLocation.getComputeService(),(JcloudsMachineLocation)jcloudsMachineLocationOptional.get());
        }
    }
}
