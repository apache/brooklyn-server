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

import java.util.List;

import javax.annotation.Nullable;

import org.apache.brooklyn.location.jclouds.BasicJcloudsLocationCustomizer;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.net.Cidr;
import org.apache.brooklyn.util.net.Networking;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Strings;
import com.google.common.base.Suppliers;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;
import com.google.common.collect.RangeSet;

/**
 * Configures a shared security group on Jclouds locations
 * <p>
 * Is based on {@link JcloudsLocationSecurityGroupCustomizer} but can be instantiated
 * in yaml e.g.
 * <p>
 * <pre>
 * {@code
 * services:
 * - type: org.apache.brooklyn.entity.software.base.EmptySoftwareProcess
 *   brooklyn.config:
 *     provisioning.properties:
 *       customizers:
 *       - $brooklyn:object:
 *         type: org.apache.brooklyn.location.jclouds.networking.SharedLocationSecurityGroupCustomizer
 *         object.fields: {locationName: "test", tcpPortRanges: ["900-910", "915", "22"], cidr: "82.40.153.101/24"}
 * }
 * </pre>
*/
public class SharedLocationSecurityGroupCustomizer extends BasicJcloudsLocationCustomizer {

    private String locationName = null;

    private String cidr = null;

    /**
     * We track inbound ports from the template to open them once we've created the new
     * security groups
     */
    private int[] inboundPorts;

    private RangeSet<Integer> tcpPortRanges;
    private RangeSet<Integer> udpPortRanges;

    /**
     * The location name is appended to the name of the shared SG - use if you need distinct shared SGs within the same location
     *
     * @param locationName appended to the name of the SG
     */
    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    /**
     * Extra ports to be opened on the entities in the SG
     *
     * @param tcpPortRanges
     */
    public void setTcpPortRanges(List<String> tcpPortRanges) {
        this.tcpPortRanges = Networking.portRulesToRanges(tcpPortRanges);
    }

    public void setUdpPortRanges(List<String> udpPortRanges) {
        this.udpPortRanges = Networking.portRulesToRanges(udpPortRanges);
    }

    /**
     * Set to restict the range that the ports that are to be opened can be accessed from
     *
     * @param cidr
     */
    public void setCidr(String cidr) {
        this.cidr = cidr;
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, Template template) {
        super.customize(location, computeService, template);

        inboundPorts = template.getOptions().getInboundPorts();

        final JcloudsLocationSecurityGroupCustomizer instance = getInstance(getSharedGroupId(location));
        if (cidr != null) instance.setSshCidrSupplier(Suppliers.ofInstance(new Cidr(cidr)));
        instance.customize(location, computeService, template);
    }

    @Override
    public void customize(JcloudsLocation location, ComputeService computeService, JcloudsMachineLocation machine) {
        super.customize(location, computeService, machine);

        final JcloudsLocationSecurityGroupCustomizer instance = getInstance(getSharedGroupId(location));

        ImmutableList.Builder<IpPermission> builder = ImmutableList.<IpPermission>builder();
        builder.addAll(getIpPermissions(instance, tcpPortRanges, IpProtocol.TCP));
        builder.addAll(getIpPermissions(instance, udpPortRanges, IpProtocol.UDP));

        if (inboundPorts != null) {
            for (int inboundPort : inboundPorts) {
                IpPermission ipPermission = IpPermission.builder()
                        .fromPort(inboundPort)
                        .toPort(inboundPort)
                        .ipProtocol(IpProtocol.TCP)
                        .cidrBlock(instance.getBrooklynCidrBlock())
                        .build();
                builder.add(ipPermission);
            }
        }
        instance.addPermissionsToLocation(machine, builder.build());
    }

    private List<IpPermission> getIpPermissions(JcloudsLocationSecurityGroupCustomizer instance, RangeSet<Integer> portRanges, IpProtocol protocol) {
        List<IpPermission> ipPermissions = ImmutableList.<IpPermission>of();
        if (portRanges != null) {
             ipPermissions =
                    FluentIterable
                            .from(portRanges.asRanges())
                            .transform(portRangeToPermission(instance, protocol))
                            .toList();
        }
        return ipPermissions;
    }

    private Function<Range<Integer>, IpPermission> portRangeToPermission(final JcloudsLocationSecurityGroupCustomizer instance, final IpProtocol protocol) {
        return new Function<Range<Integer>, IpPermission>() {
            @Nullable
            @Override
            public IpPermission apply(@Nullable Range<Integer> integerRange) {
                IpPermission extraPermission = IpPermission.builder()
                        .fromPort(integerRange.lowerEndpoint())
                        .toPort(integerRange.upperEndpoint())
                        .ipProtocol(protocol)
                        .cidrBlock(instance.getBrooklynCidrBlock())
                        .build();
                return extraPermission;
            }
        };
    }

    private String getSharedGroupId(JcloudsLocation location) {
        return (Strings.isNullOrEmpty(locationName))
                ? location.getId()
                : locationName + "-" + location.getId();
    }

    @VisibleForTesting
    JcloudsLocationSecurityGroupCustomizer getInstance(String sharedGroupId) {
        return JcloudsLocationSecurityGroupCustomizer.getInstance(sharedGroupId);
    }
}



