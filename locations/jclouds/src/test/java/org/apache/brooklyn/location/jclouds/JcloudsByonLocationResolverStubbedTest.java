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
package org.apache.brooklyn.location.jclouds;

import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class JcloudsByonLocationResolverStubbedTest extends AbstractJcloudsStubbedUnitTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsByonLocationResolverStubbedTest.class);
    
    private final String nodeId = "mynodeid";
    private final String nodePublicAddress = "173.194.32.123";
    private final String nodePrivateAddress = "172.168.10.11";

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
    }
    
    protected LocalManagementContext newManagementContext() {
        // This really is stubbed; no live access to the cloud
        LocalManagementContext result = LocalManagementContextForTests.builder(true).build();
        BrooklynProperties brooklynProperties = result.getBrooklynProperties();
        brooklynProperties.put("brooklyn.location.jclouds."+SOFTLAYER_PROVIDER+".identity", "myidentity");
        brooklynProperties.put("brooklyn.location.jclouds."+SOFTLAYER_PROVIDER+".credential", "mycredential");
        return result;

    }

    protected NodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            @Override
            public Set<? extends NodeMetadata> listNodesDetailsMatching(Predicate<? super NodeMetadata> filter) {
                NodeMetadata result = new NodeMetadataBuilder()
                        .id(nodeId)
                        .credentials(LoginCredentials.builder().identity("dummy").credential("dummy").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.of(nodePublicAddress))
                        .privateAddresses(ImmutableList.of(nodePrivateAddress))
                        .build();
                return ImmutableSet.copyOf(Iterables.filter(ImmutableList.of(result), filter));
            }
            @Override
            protected NodeMetadata newNode(String group, Template template) {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Test
    public void testResolvesHostInSpec() throws Exception {
        String spec = "jcloudsByon:(provider=\""+SOFTLAYER_PROVIDER+"\",region=\""+SOFTLAYER_AMS01_REGION_NAME+"\",user=\"myuser\",password=\"mypassword\",hosts=\""+nodeId+"\")";
        Map<?,?> specFlags = ImmutableMap.of(
                JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry,
                JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false");
        
        FixedListMachineProvisioningLocation<MachineLocation> location = getLocationManaged(spec, specFlags);
        
        JcloudsSshMachineLocation machine = (JcloudsSshMachineLocation) Iterables.getOnlyElement(location.getAllMachines());
        assertEquals(machine.getJcloudsId(), nodeId);
        assertEquals(machine.getPublicAddresses(), ImmutableSet.of(nodePublicAddress));
        assertEquals(machine.getPrivateAddresses(), ImmutableSet.of(nodePrivateAddress));
        
        // TODO what user/password should we expect? Fails below because has "dummy":
        assertEquals(machine.getUser(), "myuser");
        assertEquals(machine.config().get(JcloudsLocationConfig.PASSWORD), "mypassword");
    }

    @Test
    public void testResolvesHostStringInFlags() throws Exception {
        runResolvesHostsInFlags(nodeId);
    }

    @Test
    public void testResolvesHostsListInFlags() throws Exception {
        runResolvesHostsInFlags(ImmutableList.of(nodeId));
    }
    
    @Test
    public void testResolvesHostsListOfMapsInFlags() throws Exception {
        runResolvesHostsInFlags(ImmutableList.of(ImmutableMap.of("id", nodeId)));
    }
    
    protected void runResolvesHostsInFlags(Object hostsValInFlags) throws Exception {
        String spec = "jcloudsByon:(provider=\""+SOFTLAYER_PROVIDER+"\",region=\""+SOFTLAYER_AMS01_REGION_NAME+"\")";
        Map<?,?> specFlags = ImmutableMap.of(
                JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry,
                JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false",
                "hosts", hostsValInFlags);

        FixedListMachineProvisioningLocation<MachineLocation> location = getLocationManaged(spec, specFlags);
        
        JcloudsSshMachineLocation machine = (JcloudsSshMachineLocation) Iterables.getOnlyElement(location.getAllMachines());
        assertEquals(machine.getJcloudsId(), nodeId);
    }

    @Test
    public void testLocationSpecDoesNotCreateMachines() throws Exception {
        Collection<Location> before = managementContext.getLocationManager().getLocations();
        String spec = "jcloudsByon:(provider=\""+SOFTLAYER_PROVIDER+"\",region=\""+SOFTLAYER_AMS01_REGION_NAME+"\",user=\"myname\",hosts=\""+nodeId+"\")";
        Map<?,?> specFlags = ImmutableMap.of(
                JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry,
                JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false");

        @SuppressWarnings("unused")
        LocationSpec<FixedListMachineProvisioningLocation<MachineLocation>> locationSpec = getLocationSpec(spec, specFlags);
        
        Collection<Location> after = managementContext.getLocationManager().getLocations();
        assertEquals(after, before, "after="+after+"; before="+before);
    }

    @SuppressWarnings("unchecked")
    private LocationSpec<FixedListMachineProvisioningLocation<MachineLocation>> getLocationSpec(String val, Map<?,?> specFlags) {
        return (LocationSpec<FixedListMachineProvisioningLocation<MachineLocation>>) managementContext.getLocationRegistry().getLocationSpec(val, specFlags).get();
    }
    
    @SuppressWarnings("unchecked")
    private FixedListMachineProvisioningLocation<MachineLocation> getLocationManaged(String val, Map<?,?> specFlags) {
        return (FixedListMachineProvisioningLocation<MachineLocation>) managementContext.getLocationRegistry().getLocationManaged(val, specFlags);
    }
}
