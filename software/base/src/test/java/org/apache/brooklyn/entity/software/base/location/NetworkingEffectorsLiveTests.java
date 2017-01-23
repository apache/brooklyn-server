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
package org.apache.brooklyn.entity.software.base.location;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.software.base.SoftwareProcess;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import static org.apache.brooklyn.location.jclouds.networking.NetworkingEffectors.INBOUND_PORTS_LIST;
import static org.apache.brooklyn.location.jclouds.networking.NetworkingEffectors.INBOUND_PORTS_LIST_PROTOCOL;
import static org.apache.brooklyn.test.Asserts.assertTrue;
import static org.jclouds.net.domain.IpProtocol.ICMP;
import static org.jclouds.net.domain.IpProtocol.TCP;
import static org.jclouds.net.domain.IpProtocol.UDP;

public abstract class NetworkingEffectorsLiveTests extends BrooklynAppLiveTestSupport {
    @Test(groups = "Live")
    public void testPassSecurityGroupParameters() {
        EmptySoftwareProcess emptySoftwareProcess = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(SoftwareProcess.ADD_OPEN_INBOUND_PORTS_EFFECTOR, true));

        JcloudsLocation jcloudsLocation = (JcloudsLocation)mgmt.getLocationRegistry().getLocationManaged(getLocationSpec(), getLocationProperties());
        app.start(ImmutableList.of(jcloudsLocation));

        Optional<Location> jcloudsMachineLocation = Iterables.tryFind(emptySoftwareProcess.getLocations(), Predicates.instanceOf(JcloudsMachineLocation.class));
        if (!jcloudsMachineLocation.isPresent()) {
            throw new IllegalArgumentException("Tried to execute open ports effector on an entity with no JcloudsMachineLocation");
        }
        ComputeService computeService = ((JcloudsMachineLocation)jcloudsMachineLocation.get()).getParent().getComputeService();
        String nodeId = ((JcloudsMachineLocation)jcloudsMachineLocation.get()).getNode().getId();
        final SecurityGroupExtension securityApi = computeService.getSecurityGroupExtension().get();

        Effector<Collection<SecurityGroup>> openPortsInSecurityGroup = (Effector<Collection<SecurityGroup>>)EffectorUtils.findEffectorDeclared(emptySoftwareProcess, "openPortsInSecurityGroup").get();
        Task<Collection<SecurityGroup>> task = EffectorUtils.invokeEffectorAsync(emptySoftwareProcess, openPortsInSecurityGroup,
                ImmutableMap.of(INBOUND_PORTS_LIST.getName(), "234,324,550-1050"));
        Collection<SecurityGroup> effectorResult = task.getUnchecked();
        for (Predicate<SecurityGroup> ipPermissionPredicate : ImmutableList.of(ruleExistsPredicate(234, 234, TCP), ruleExistsPredicate(324, 324, TCP), ruleExistsPredicate(550, 1050, TCP))) {
            assertTrue(Iterables.tryFind(effectorResult, ipPermissionPredicate).isPresent());
        }

        task = EffectorUtils.invokeEffectorAsync(emptySoftwareProcess, openPortsInSecurityGroup,
                ImmutableMap.of(INBOUND_PORTS_LIST.getName(), "234,324,550-1050", INBOUND_PORTS_LIST_PROTOCOL.getName(), "UDP"));
        effectorResult = task.getUnchecked();
        for (Predicate<SecurityGroup> ipPermissionPredicate : ImmutableList.of(ruleExistsPredicate(234, 234, UDP), ruleExistsPredicate(324, 324, UDP), ruleExistsPredicate(550, 1050, UDP))) {
            assertTrue(Iterables.tryFind(effectorResult, ipPermissionPredicate).isPresent());
        }

        Set<SecurityGroup> groupsOnNode = securityApi.listSecurityGroupsForNode(nodeId);
        SecurityGroup securityGroup = Iterables.getOnlyElement(groupsOnNode);

        assertTrue(ruleExistsPredicate(234, 234, TCP).apply(securityGroup));
        assertTrue(ruleExistsPredicate(324, 324, TCP).apply(securityGroup));
        assertTrue(ruleExistsPredicate(550, 1050, TCP).apply(securityGroup));

        assertTrue(ruleExistsPredicate(234, 234, UDP).apply(securityGroup));
        assertTrue(ruleExistsPredicate(324, 324, UDP).apply(securityGroup));
        assertTrue(ruleExistsPredicate(550, 1050, UDP).apply(securityGroup));

        task = EffectorUtils.invokeEffectorAsync(emptySoftwareProcess, openPortsInSecurityGroup,
                ImmutableMap.of(INBOUND_PORTS_LIST.getName(), "-1", INBOUND_PORTS_LIST_PROTOCOL.getName(), "ICMP"));
        effectorResult = task.getUnchecked();
        assertTrue(Iterables.tryFind(effectorResult, ruleExistsPredicate(-1, -1, ICMP)).isPresent());
        groupsOnNode = securityApi.listSecurityGroupsForNode(nodeId);
        securityGroup = Iterables.getOnlyElement(groupsOnNode);
        assertTrue(ruleExistsPredicate(-1, -1, ICMP).apply(securityGroup));
    }

    protected Predicate<SecurityGroup> ruleExistsPredicate(final int fromPort, final int toPort, final IpProtocol ipProtocol) {
        return new Predicate<SecurityGroup>() {
            @Override
            public boolean apply(SecurityGroup scipPermission) {
                for (IpPermission ipPermission : scipPermission.getIpPermissions()) {
                    if (ipPermission.getFromPort() == fromPort && ipPermission.getToPort() == toPort && ipPermission.getIpProtocol() == ipProtocol) {
                        return true;
                    }
                }
                return false;
            }
        };
    }

    public abstract String getLocationSpec();

    public abstract Map<String, Object> getLocationProperties();
}
