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
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

import java.net.URI;
import java.util.Collections;
import java.util.Set;

import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.jclouds.aws.AWSResponseException;
import org.jclouds.aws.domain.AWSError;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.Location;
import org.jclouds.net.domain.IpPermission;
import org.jclouds.net.domain.IpProtocol;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.net.Cidr;

public class JcloudsLocationSecurityGroupCustomizerTest {

    private static final String JCLOUDS_PREFIX_AWS = "jclouds#";
    JcloudsLocationSecurityGroupCustomizer customizer;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) ComputeService computeService;
    @Mock(answer = Answers.RETURNS_SMART_NULLS) Location location;
    @Mock(answer = Answers.RETURNS_SMART_NULLS) SecurityGroupExtension securityApi;
    @Mock(answer = Answers.RETURNS_DEEP_STUBS) JcloudsMachineLocation jcloudsMachineLocation;
    private static final String NODE_ID = "node";

    /** Used to skip external checks in unit tests. */
    private static class TestCidrSupplier implements Supplier<Cidr> {
        @Override public Cidr get() {
            return new Cidr("192.168.10.10/32");
        }
    }

    @BeforeMethod
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        customizer = new JcloudsLocationSecurityGroupCustomizer("testapp", new TestCidrSupplier());
        when(computeService.getSecurityGroupExtension()).thenReturn(Optional.of(securityApi));
        when(location.getParent()).thenReturn(location);
        when(location.getId()).thenReturn("aws-ec2");
        when(jcloudsMachineLocation.getNode().getId()).thenReturn(NODE_ID);
        when(jcloudsMachineLocation.getNode().getLocation()).thenReturn(location);
        when(jcloudsMachineLocation.getParent().getComputeService()).thenReturn(computeService);
    }

    @Test
    public void testSameInstanceReturnedForSameApplication() {
        assertEquals(JcloudsLocationSecurityGroupCustomizer.getInstance("a"),
                JcloudsLocationSecurityGroupCustomizer.getInstance("a"));
        assertNotEquals(JcloudsLocationSecurityGroupCustomizer.getInstance("a"),
                JcloudsLocationSecurityGroupCustomizer.getInstance("b"));
    }

    @Test
    public void testSecurityGroupAddedWhenJcloudsLocationCustomised() {
        Template template = mock(Template.class);
        TemplateOptions templateOptions = mock(TemplateOptions.class);
        when(template.getLocation()).thenReturn(location);
        when(template.getOptions()).thenReturn(templateOptions);
        SecurityGroup group = newGroup("id");
        when(securityApi.createSecurityGroup(anyString(), eq(location))).thenReturn(group);
        when(securityApi.addIpPermission(any(IpPermission.class), eq(group))).thenReturn(group);

        // Two Brooklyn.JcloudsLocations added to same Jclouds.Location
        JcloudsLocation jcloudsLocationA = new JcloudsLocation(MutableMap.of("deferConstruction", true));
        JcloudsLocation jcloudsLocationB = new JcloudsLocation(MutableMap.of("deferConstruction", true));
        customizer.customize(jcloudsLocationA, computeService, template);
        customizer.customize(jcloudsLocationB, computeService, template);

        // One group with three permissions shared by both locations.
        // Expect TCP, UDP and ICMP between members of group and SSH to Brooklyn
        verify(securityApi).createSecurityGroup(anyString(), eq(location));
        verify(securityApi, times(4)).addIpPermission(any(IpPermission.class), eq(group));
        // New groups set on options
        verify(templateOptions, times(2)).securityGroups(anyString());
    }

    @Test
    public void testSharedGroupLoadedWhenItExistsButIsNotCached() {
        Template template = mock(Template.class);
        TemplateOptions templateOptions = mock(TemplateOptions.class);
        when(template.getLocation()).thenReturn(location);
        when(template.getOptions()).thenReturn(templateOptions);
        JcloudsLocation jcloudsLocation = new JcloudsLocation(MutableMap.of("deferConstruction", true));
        SecurityGroup shared = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup irrelevant = newGroup("irrelevant");
        when(securityApi.createSecurityGroup(shared.getName(), location)).thenReturn(shared);
        when(securityApi.createSecurityGroup(irrelevant.getName(), location)).thenReturn(irrelevant);
        when(securityApi.listSecurityGroupsInLocation(location)).thenReturn(ImmutableSet.of(irrelevant, shared));
        when(securityApi.addIpPermission(any(IpPermission.class), eq(shared))).thenReturn(shared);
        when(securityApi.addIpPermission(any(IpPermission.class), eq(irrelevant))).thenReturn(irrelevant);

        customizer.customize(jcloudsLocation, computeService, template);

        verify(securityApi).listSecurityGroupsInLocation(location);
        verify(securityApi, never()).createSecurityGroup(anyString(), any(Location.class));
    }

    @Test
    public void testAddPermissionsToNode() {
        IpPermission ssh = newPermission(22);
        IpPermission jmx = newPermission(31001);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup group = newGroup("id");
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, group));
        SecurityGroup updatedSecurityGroup = newGroup("id", ImmutableSet.of(ssh, jmx));
        when(securityApi.addIpPermission(ssh, group)).thenReturn(updatedSecurityGroup);
        when(securityApi.addIpPermission(jmx, group)).thenReturn(updatedSecurityGroup);
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh, jmx));

        verify(securityApi, never()).createSecurityGroup(anyString(), any(Location.class));
        verify(securityApi, times(1)).addIpPermission(ssh, group);
        verify(securityApi, times(1)).addIpPermission(jmx, group);
    }

    @Test
    public void testRemovePermissionsFromNode() {
        IpPermission ssh = newPermission(22);
        IpPermission jmx = newPermission(31001);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup group = newGroup("id");
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, group));
        SecurityGroup updatedSecurityGroup = newGroup("id", ImmutableSet.of(ssh, jmx));
        when(securityApi.addIpPermission(ssh, group)).thenReturn(updatedSecurityGroup);
        when(securityApi.addIpPermission(jmx, group)).thenReturn(updatedSecurityGroup);
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh, jmx));
        customizer.removePermissionsFromLocation(jcloudsMachineLocation, ImmutableList.of(jmx));

        verify(securityApi, never()).removeIpPermission(ssh, group);
        verify(securityApi, times(1)).removeIpPermission(jmx, group);
    }

    @Test
    public void testRemoveMultiplePermissionsFromNode() {
        IpPermission ssh = newPermission(22);
        IpPermission jmx = newPermission(31001);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup group = newGroup("id");
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, group));
        SecurityGroup updatedSecurityGroup = newGroup("id", ImmutableSet.of(ssh, jmx));
        when(securityApi.addIpPermission(ssh, group)).thenReturn(updatedSecurityGroup);
        when(securityApi.addIpPermission(jmx, group)).thenReturn(updatedSecurityGroup);
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh, jmx));

        when(securityApi.removeIpPermission(ssh, group)).thenReturn(updatedSecurityGroup);
        when(securityApi.removeIpPermission(jmx, group)).thenReturn(updatedSecurityGroup);
        customizer.removePermissionsFromLocation(jcloudsMachineLocation, ImmutableList.of(ssh, jmx));

        verify(securityApi, times(1)).removeIpPermission(ssh, group);
        verify(securityApi, times(1)).removeIpPermission(jmx, group);
    }


    @Test
    public void testAddPermissionWhenNoExtension() {
        IpPermission ssh = newPermission(22);
        IpPermission jmx = newPermission(31001);

        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(Collections.<SecurityGroup>emptySet());

        RuntimeException exception = null;
        try {
            customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh, jmx));
        } catch(RuntimeException e){
            exception = e;
        }

        assertNotNull(exception);
    }

    @Test
    public void testAddPermissionsToNodeUsesUncachedSecurityGroup() {
        JcloudsLocation jcloudsLocation = new JcloudsLocation(MutableMap.of("deferConstruction", true));
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup uniqueGroup = newGroup("unique");

        Template template = mock(Template.class);
        TemplateOptions templateOptions = mock(TemplateOptions.class);
        when(template.getLocation()).thenReturn(location);
        when(template.getOptions()).thenReturn(templateOptions);
        when(securityApi.createSecurityGroup(anyString(), eq(location))).thenReturn(sharedGroup);
        when(securityApi.addIpPermission(any(IpPermission.class), eq(uniqueGroup))).thenReturn(uniqueGroup);
        when(securityApi.addIpPermission(any(IpPermission.class), eq(sharedGroup))).thenReturn(sharedGroup);

        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        // Call customize to cache the shared group
        customizer.customize(jcloudsLocation, computeService, template);
        reset(securityApi);
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(uniqueGroup, sharedGroup));
        IpPermission ssh = newPermission(22);
        SecurityGroup updatedSharedSecurityGroup = newGroup(sharedGroup.getId(), ImmutableSet.of(ssh));
        when(securityApi.addIpPermission(ssh, uniqueGroup)).thenReturn(updatedSharedSecurityGroup);
        SecurityGroup updatedUniqueSecurityGroup = newGroup("unique", ImmutableSet.of(ssh));
        when(securityApi.addIpPermission(ssh, sharedGroup)).thenReturn(updatedUniqueSecurityGroup);
        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableSet.of(ssh));

        // Expect the per-machine group to have been altered, not the shared group
        verify(securityApi).addIpPermission(ssh, uniqueGroup);
        verify(securityApi, never()).addIpPermission(any(IpPermission.class), eq(sharedGroup));
    }

    @Test
    public void testSecurityGroupsLoadedWhenAddingPermissionsToUncachedNode() {
        IpPermission ssh = newPermission(22);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup uniqueGroup = newGroup("unique");

        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, uniqueGroup));
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");
        SecurityGroup updatedSecurityGroup = newGroup(uniqueGroup.getId(), ImmutableSet.of(ssh));
        when(securityApi.addIpPermission(ssh, sharedGroup)).thenReturn(updatedSecurityGroup);
        SecurityGroup updatedUniqueSecurityGroup = newGroup(uniqueGroup.getId(), ImmutableSet.of(ssh));
        when(securityApi.addIpPermission(ssh, updatedUniqueSecurityGroup)).thenReturn(updatedUniqueSecurityGroup);

        // Expect first call to list security groups on nodeId, second to use cached version
        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableSet.of(ssh));
        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableSet.of(ssh));

        verify(securityApi, times(1)).listSecurityGroupsForNode(NODE_ID);
        verify(securityApi, times(2)).addIpPermission(ssh, uniqueGroup);
        verify(securityApi, never()).addIpPermission(any(IpPermission.class), eq(sharedGroup));
    }

    @Test
    public void testAddRuleNotRetriedByDefault() {
        IpPermission ssh = newPermission(22);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup uniqueGroup = newGroup("unique");
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, uniqueGroup));
        when(securityApi.addIpPermission(eq(ssh), eq(uniqueGroup)))
                .thenThrow(new RuntimeException("exception creating " + ssh));
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        try {
            customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh));
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("repeated errors from provider"), "message=" + e.getMessage());
        }
        verify(securityApi, never()).createSecurityGroup(anyString(), any(Location.class));
        verify(securityApi, times(1)).addIpPermission(ssh, uniqueGroup);
    }

    @Test
    public void testCustomExceptionRetryablePredicate() {
        final String message = "testCustomExceptionRetryablePredicate";
        Predicate<Exception> messageChecker = new Predicate<Exception>() {
            @Override
            public boolean apply(Exception input) {
                Throwable t = input;
                while (t != null) {
                    if (t.getMessage().contains(message)) {
                        return true;
                    } else {
                        t = t.getCause();
                    }
                }
                return false;
            }
        };
        customizer.setRetryExceptionPredicate(messageChecker);
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        IpPermission ssh = newPermission(22);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup uniqueGroup = newGroup("unique");
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, uniqueGroup));
        when(securityApi.addIpPermission(eq(ssh), eq(uniqueGroup)))
                .thenThrow(new RuntimeException(new Exception(message)))
                .thenThrow(new RuntimeException(new Exception(message)))
                .thenReturn(sharedGroup);

        customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh));

        verify(securityApi, never()).createSecurityGroup(anyString(), any(Location.class));
        verify(securityApi, times(3)).addIpPermission(ssh, uniqueGroup);
    }

    @Test
    public void testAddRuleRetriedOnAwsFailure() {
        IpPermission ssh = newPermission(22);
        SecurityGroup sharedGroup = newGroup(customizer.getNameForSharedSecurityGroup());
        SecurityGroup uniqueGroup = newGroup("unique");
        customizer.setRetryExceptionPredicate(JcloudsLocationSecurityGroupCustomizer.newAwsExceptionRetryPredicate());
        when(securityApi.listSecurityGroupsForNode(NODE_ID)).thenReturn(ImmutableSet.of(sharedGroup, uniqueGroup));
        when(securityApi.addIpPermission(any(IpPermission.class), eq(uniqueGroup)))
                .thenThrow(newAwsResponseExceptionWithCode("InvalidGroup.InUse"))
                .thenThrow(newAwsResponseExceptionWithCode("DependencyViolation"))
                .thenThrow(newAwsResponseExceptionWithCode("RequestLimitExceeded"))
                .thenThrow(newAwsResponseExceptionWithCode("Blocked"))
                .thenReturn(sharedGroup);
        when(computeService.getContext().unwrap().getId()).thenReturn("aws-ec2");

        try {
            customizer.addPermissionsToLocation(jcloudsMachineLocation, ImmutableList.of(ssh));
        } catch (Exception e) {
            String expected = "repeated errors from provider";
            assertTrue(e.getMessage().contains(expected), "expected exception message to contain " + expected + ", was: " + e.getMessage());
        }

        verify(securityApi, never()).createSecurityGroup(anyString(), any(Location.class));
        verify(securityApi, times(4)).addIpPermission(ssh, uniqueGroup);
    }

    private SecurityGroup newGroup(String id) {
        return newGroup(id, ImmutableSet.<IpPermission>of());
    }

    private SecurityGroup newGroup(String name, Set<IpPermission> ipPermissions) {
        String id = name;
        if (!name.startsWith(JCLOUDS_PREFIX_AWS)) {
            id = JCLOUDS_PREFIX_AWS + name;
        }
        URI uri = null;
        String ownerId = null;
        return new SecurityGroup(
            "providerId",
            id,
            id,
            location,
            uri,
            Collections.<String, String>emptyMap(),
            ImmutableSet.<String>of(),
            ipPermissions,
            ownerId);
    }

    private IpPermission newPermission(int port) {
        return IpPermission.builder()
                .ipProtocol(IpProtocol.TCP)
                .fromPort(port)
                .toPort(port)
                .cidrBlock("0.0.0.0/0")
                .build();
    }

    private AWSError newAwsErrorWithCode(String code) {
        AWSError e = new AWSError();
        e.setCode(code);
        return e;
    }

    private Exception newAwsResponseExceptionWithCode(String code) {
        AWSResponseException e = new AWSResponseException("irrelevant message", null, null, newAwsErrorWithCode(code));
        return new RuntimeException(e);
    }
}
