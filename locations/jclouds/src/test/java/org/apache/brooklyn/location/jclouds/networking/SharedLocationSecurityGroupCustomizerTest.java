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

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;
import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.net.Cidr;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Location;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;
import org.mockito.Answers;
import org.mockito.ArgumentCaptor;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;

public class SharedLocationSecurityGroupCustomizerTest {

    TestSharedLocationSecurityGroupCustomizer customizer;
    JcloudsLocationSecurityGroupCustomizer sgCustomizer;
    ComputeService computeService;
    Location location;
    SecurityGroupExtension securityApi;
    private JcloudsLocation jcloudsLocation = new JcloudsLocation(MutableMap.of("deferConstruction", true));
    private Template mockTemplate;
    private TemplateOptions mockOptions;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        sgCustomizer = mock(JcloudsLocationSecurityGroupCustomizer.class);
        customizer = new TestSharedLocationSecurityGroupCustomizer();
        location = mock(Location.class);
        securityApi = mock(SecurityGroupExtension.class);
        computeService = mock(ComputeService.class, Answers.RETURNS_DEEP_STUBS.get());
        mockTemplate = mock(Template.class);
        mockOptions = mock(TemplateOptions.class);
        when(computeService.getSecurityGroupExtension()).thenReturn(Optional.of(securityApi));
        when(mockTemplate.getOptions()).thenReturn(mockOptions);
        when(mockOptions.getInboundPorts()).thenReturn(new int[]{});
    }

    @Test
    public void testLocationNameAppended() {
        customizer.setLocationName("Fred");
        customizer.customize(jcloudsLocation, computeService, mockTemplate);
        assertEquals(customizer.sharedGroupId, "Fred-" + jcloudsLocation.getId());
    }

    @Test
    public void testCidrIsSetIfAvailable() {
        // Set cidr with over specified ip range which will be fixed by Cidr object
        customizer.setCidr("1.1.1.1/24");
        customizer.customize(jcloudsLocation, computeService, mockTemplate);
        ArgumentCaptor<Supplier<Cidr>> argumentCaptor = ArgumentCaptor.forClass((Class)Supplier.class);
        verify(sgCustomizer).setSshCidrSupplier(argumentCaptor.capture());
        Cidr cidr = argumentCaptor.getValue().get();
        assertEquals(cidr.toString(), "1.1.1.0/24");
    }

    @Test
    public void testCidrNotSetIfNotavailable() {
        customizer.customize(jcloudsLocation, computeService, mockTemplate);
        verify(sgCustomizer, never()).setSshCidrSupplier(any(Supplier.class));
    }


    @Test
    public void testPermissionsSetFromPortRanges() {
        customizer.setTcpPortRanges(ImmutableList.of("99-100"));
        when(sgCustomizer.getBrooklynCidrBlock()).thenReturn("10.10.10.10/24");
        customizer.customize(jcloudsLocation, computeService, mock(JcloudsMachineLocation.class));
        assertPermissionsAdded(99, 100, IpProtocol.TCP);
    }

    @Test
    public void testUdpPermissionsSetFromPortRanges() {
        customizer.setUdpPortRanges(ImmutableList.of("55-78"));
        when(sgCustomizer.getBrooklynCidrBlock()).thenReturn("10.10.10.10/24");
        customizer.customize(jcloudsLocation, computeService, mock(JcloudsMachineLocation.class));
        assertPermissionsAdded(55, 78, IpProtocol.UDP);
    }

    @Test
    public void testInboundPortsAddedToPermissions() {
        when(mockOptions.getInboundPorts()).thenReturn(new int[]{5});
        when(sgCustomizer.getBrooklynCidrBlock()).thenReturn("10.10.10.10/24");
        customizer.customize(jcloudsLocation, computeService, mockTemplate);
        customizer.customize(jcloudsLocation, computeService, mock(JcloudsMachineLocation.class));
        assertPermissionsAdded(5, 5, IpProtocol.TCP);
    }

    @Test
    public  void testDisableFlagDisableCustomizer() {
        customizer.setEnabled(false);
        customizer.setUdpPortRanges(ImmutableList.of("55-78"));

        customizer.customize(jcloudsLocation, computeService, mockTemplate);
        customizer.customize(jcloudsLocation, computeService, mock(JcloudsMachineLocation.class));

        verify(sgCustomizer, never()).customize(jcloudsLocation, computeService, mockTemplate);
        verify(sgCustomizer, never()).addPermissionsToLocation(any(JcloudsMachineLocation.class), any(Iterable.class));
    }

    private void assertPermissionsAdded(int expectedFrom, int expectedTo, IpProtocol expectedProtocol) {
        ArgumentCaptor<List> listArgumentCaptor = ArgumentCaptor.forClass(List.class);
        verify(sgCustomizer).addPermissionsToLocation(any(JcloudsMachineLocation.class), listArgumentCaptor.capture());
        IpPermission ipPermission = (IpPermission) listArgumentCaptor.getValue().get(0);
        assertEquals(ipPermission.getFromPort(), expectedFrom);
        assertEquals(ipPermission.getToPort(), expectedTo);
        assertEquals(ipPermission.getIpProtocol(), expectedProtocol);
    }

    /**
     * Used to skip external checks in unit tests.
     */
    private static class TestCidrSupplier implements Supplier<Cidr> {
        @Override
        public Cidr get() {
            return new Cidr("192.168.10.10/32");
        }
    }

    public class TestSharedLocationSecurityGroupCustomizer extends SharedLocationSecurityGroupCustomizer {
        public String sharedGroupId;

        @Override
        JcloudsLocationSecurityGroupCustomizer getInstance(String sharedGroupId) {
            this.sharedGroupId = sharedGroupId;
            return sgCustomizer;
        }
    }
}