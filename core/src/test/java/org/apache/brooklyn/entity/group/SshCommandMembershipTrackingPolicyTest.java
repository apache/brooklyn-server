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
package org.apache.brooklyn.entity.group;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.EntityManager;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.AbstractMembershipTrackingPolicy.EventType;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.time.Duration;

public class SshCommandMembershipTrackingPolicyTest extends BrooklynAppUnitTestSupport {

    private SimulatedLocation loc;
    private EntityManager entityManager;
    private BasicGroup group;
    private BasicStartable entity;
    private LocationSpec<SshMachineLocation> machine;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();

        machine = LocationSpec.create(SshMachineLocation.class)
                .configure("address", "1.2.3.4")
                .configure("sshToolClass", RecordingSshTool.class.getName());
        loc = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        entityManager = app.getManagementContext().getEntityManager();

        group = app.createAndManageChild(EntitySpec.create(BasicGroup.class)
                .configure("childrenAsMembers", true));

        entity = app.createAndManageChild(EntitySpec.create(BasicStartable.class).location(machine));
        entity.policies().add(PolicySpec.create(SshCommandMembershipTrackingPolicy.class)
                .configure("group", group)
                .configure("shell.env.TEST", "test")
                .configure("update.command", "echo ignored"));

        app.start(ImmutableList.of(loc));
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        RecordingSshTool.clear();
    }

    @Test
    public void testCommandExecutedWithEnvironment() throws Exception {
        TestEntity member = entityManager.createEntity(EntitySpec.create(TestEntity.class).parent(group));

        assertExecSizeEventually(1);
        assertEquals(RecordingSshTool.getLastExecCmd().commands, ImmutableList.of("echo ignored"));

        Map<?, ?> env = RecordingSshTool.getLastExecCmd().env;
        assertTrue(env.containsKey(SshCommandMembershipTrackingPolicy.EVENT_TYPE));
        assertEquals(env.get(SshCommandMembershipTrackingPolicy.EVENT_TYPE), EventType.ENTITY_ADDED.name());
        assertTrue(env.containsKey(SshCommandMembershipTrackingPolicy.MEMBER_ID));
        assertEquals(env.get(SshCommandMembershipTrackingPolicy.MEMBER_ID), member.getId());
        assertTrue(env.containsKey("TEST"));
        assertEquals(env.get("TEST"), "test");

        member.sensors().set(Startable.SERVICE_UP, true);
        Duration.seconds(1).countdownTimer().waitForExpiry();

        assertExecSizeEventually(2);
        assertEquals(RecordingSshTool.getLastExecCmd().env.get(SshCommandMembershipTrackingPolicy.EVENT_TYPE), EventType.ENTITY_CHANGE.name());

        member.clearParent();

        assertExecSizeEventually(3);
        assertEquals(RecordingSshTool.getLastExecCmd().env.get(SshCommandMembershipTrackingPolicy.EVENT_TYPE), EventType.ENTITY_REMOVED.name());
    }

    protected void assertExecSizeEventually(final int expectedSize) {
        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                assertEquals(RecordingSshTool.getExecCmds().size(), expectedSize);
            }
        });
    }

}
