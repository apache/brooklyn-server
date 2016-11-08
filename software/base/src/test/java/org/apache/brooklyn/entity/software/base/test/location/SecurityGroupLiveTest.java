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
package org.apache.brooklyn.entity.software.base.test.location;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.location.jclouds.JcloudsMachineLocation;
import org.apache.brooklyn.location.jclouds.networking.SecurityGroupEditor;
import org.apache.brooklyn.location.jclouds.networking.SecurityGroupDefinition;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Identifiers;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.SecurityGroup;
import org.jclouds.compute.extensions.SecurityGroupExtension;
import org.jclouds.net.domain.IpPermission;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.brooklyn.core.entity.DependentConfigurationTest.TIMEOUT_MS;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

@Test(groups = {"Live"})
public class SecurityGroupLiveTest extends BrooklynAppLiveTestSupport  {

    private static final Logger LOG = LoggerFactory.getLogger(SecurityGroupLiveTest.class);

    public static final String PROVIDER = "aws-ec2";
    public static final String REGION_NAME = "us-east-1";
    public static final String LOCATION_SPEC = PROVIDER + (REGION_NAME == null ? "" : ":" + REGION_NAME);
    public static final String UBUNTU_12 = "us-east-1/ami-d0f89fb9";

    private BrooklynProperties brooklynProperties;

    private Location loc;
    private List<Location> locs;
    private Entity testEntity;
    private JcloudsMachineLocation jcloudsMachineLocation;
    private ComputeService computeService;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        brooklynProperties = BrooklynProperties.Factory.newDefault();
        brooklynProperties.remove("brooklyn.jclouds."+PROVIDER+".image-description-regex");
        brooklynProperties.remove("brooklyn.jclouds."+PROVIDER+".image-name-regex");
        brooklynProperties.remove("brooklyn.jclouds."+PROVIDER+".image-id");
        brooklynProperties.remove("brooklyn.jclouds."+PROVIDER+".inboundPorts");
        brooklynProperties.remove("brooklyn.jclouds."+PROVIDER+".hardware-id");

        // Also removes scriptHeader (e.g. if doing `. ~/.bashrc` and `. ~/.profile`, then that can cause "stdin: is not a tty")
        brooklynProperties.remove("brooklyn.ssh.config.scriptHeader");

        mgmt = new LocalManagementContextForTests(brooklynProperties);

        super.setUp();

        Map<String,?> allFlags = MutableMap.<String,Object>builder()
            .put("tags", ImmutableList.of(getClass().getName()))
            .putAll(ImmutableMap.of("imageId", UBUNTU_12, "loginUser", "ubuntu", "hardwareId", "m1.small"))
            .build();
        loc = mgmt.getLocationRegistry().getLocationManaged(LOCATION_SPEC, allFlags);
        testEntity = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class));
        app.start(ImmutableList.of(loc));
        EntityAsserts.assertAttributeEqualsEventually(MutableMap.of("timeout", TIMEOUT_MS),
            testEntity, Startable.SERVICE_UP, true);
        SshMachineLocation sshLoc = Locations.findUniqueSshMachineLocation(testEntity.getLocations()).get();
        jcloudsMachineLocation = (JcloudsMachineLocation)sshLoc;
        computeService = jcloudsMachineLocation.getParent().getComputeService();

    }


    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (app != null) {
                app.stop();
            }
        } catch (Exception e) {
            LOG.error("error stopping app; continuing with shutdown...", e);
        } finally {
            super.tearDown();
        }
    }

    @Test
    public void testCreateGroupAddPermissionsAndDelete() {
        SecurityGroupDefinition sgDef = new SecurityGroupDefinition()
            .allowingInternalPorts(8097, 8098)
            .allowingInternalPortRange(6000, 7999)
            .allowingPublicPort(8099);
        final String securityGroupName = Identifiers.makeRandomLowercaseId(15);
        final SecurityGroupEditor editor = makeEditor();
        final SecurityGroup testGroup = createTestGroup(securityGroupName, editor);
        assertEquals(testGroup.getName(), "jclouds#" + securityGroupName);

        final SecurityGroup updated = editor.addPermissions(testGroup, sgDef.getPermissions());

        final Optional<SecurityGroup> fromCloud = editor.findSecurityGroupByName(securityGroupName);
        assertTrue(fromCloud.isPresent());
        final SecurityGroup cloudGroup = fromCloud.get();

        assertPermissionsEqual(updated.getIpPermissions(), cloudGroup.getIpPermissions());

        editor.removeSecurityGroup(updated);

        final Optional<SecurityGroup> afterRemove = editor.findSecurityGroupByName(securityGroupName);
        assertFalse(afterRemove.isPresent());

    }

    @Test
    public void testGroupAddIsIdempotent() {
        SecurityGroupDefinition sgDef = new SecurityGroupDefinition()
            .allowingInternalPorts(8097, 8098)
            .allowingInternalPortRange(6000, 7999)
            .allowingPublicPort(8099);
        final String securityGroupName = Identifiers.makeRandomLowercaseId(15);
        final SecurityGroupEditor editor = makeEditor();

        SecurityGroup group1 = createTestGroup(securityGroupName, editor);
        assertEquals(group1.getName(), "jclouds#" + securityGroupName);
        group1 = editor.addPermissions(group1, sgDef.getPermissions());

        final SecurityGroup group2 = createTestGroup(securityGroupName, editor);
        assertEquals(group2.getName(), group1.getName());
        assertPermissionsEqual(group2.getIpPermissions(), group1.getIpPermissions());

        editor.removeSecurityGroup(group2);

        final Optional<SecurityGroup> afterRemove = editor.findSecurityGroupByName(securityGroupName);
        assertFalse(afterRemove.isPresent());
    }



    @Test
    public void testPermissionsAddIsIdempotent() {
        SecurityGroupDefinition sgDef = new SecurityGroupDefinition()
            .allowingInternalPorts(8097, 8098)
            .allowingInternalPortRange(6000, 7999)
            .allowingPublicPort(8099);
        final String securityGroupName = Identifiers.makeRandomLowercaseId(15);
        final SecurityGroupEditor editor = makeEditor();

        SecurityGroup group1 = createTestGroup(securityGroupName, editor);
        assertEquals(group1.getName(), "jclouds#" + securityGroupName);

        final SecurityGroup before = editor.addPermissions(group1, sgDef.getPermissions());
        assertPermissionsEqual(ImmutableSet.copyOf(sgDef.getPermissions()), before.getIpPermissions());

        try {
            final SecurityGroup after = editor.addPermissions(before, sgDef.getPermissions());
            assertPermissionsEqual(before.getIpPermissions(), after.getIpPermissions());
        } catch (Exception e) {
            // catch here so as to give us the chance to delete the group rather than leak it (this is a live test)
            fail("Exception repeating group permissions", e);
        }

        editor.removeSecurityGroup(group1);

        final Optional<SecurityGroup> afterRemove = editor.findSecurityGroupByName(securityGroupName);
        assertFalse(afterRemove.isPresent());
    }

    private void assertPermissionsEqual(Set<IpPermission> actuals, Set<IpPermission> expecteds) {
        assertEquals(actuals.size(), expecteds.size());
        assertTrue(actuals.containsAll(expecteds));
    }

    private SecurityGroupEditor makeEditor() {
        final org.jclouds.domain.Location nodeLocation = jcloudsMachineLocation.getNode().getLocation();
        ComputeService computeService = jcloudsMachineLocation.getParent().getComputeService();
        final Optional<SecurityGroupExtension> securityGroupExtension = computeService.getSecurityGroupExtension();
        if (securityGroupExtension.isPresent()) {
            return new SecurityGroupEditor(nodeLocation, securityGroupExtension.get());
        } else {
            throw new IllegalArgumentException("Expected SecurityGroupExtension not found in " + computeService);
        }
    }

    private SecurityGroup createTestGroup(String securityGroupName, SecurityGroupEditor editor) {

        LOG.info("Creating security group {} in {}", securityGroupName, jcloudsMachineLocation);
        return editor.createSecurityGroup(securityGroupName);
    }

}
