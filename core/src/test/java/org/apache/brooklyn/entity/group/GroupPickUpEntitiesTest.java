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

import java.util.List;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.Boxing;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

/**
 * tests that a group's membership gets updated using subscriptions
 */
public class GroupPickUpEntitiesTest extends BrooklynAppUnitTestSupport {

    private BasicGroup group;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        group = app.createAndManageChild(EntitySpec.create(BasicGroup.class));
        
        group.policies().add(PolicySpec.create(FindUpServicesWithNameBob.class));
    }

    /**
     * TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins.
     * Also fails occassionally for Aled if setting invocationCount=1000
     * 
     * org.apache.brooklyn.util.exceptions.PropagatedRuntimeException: failed succeeds-eventually, 69 attempts, 30002ms elapsed: AssertionError: entity=BasicGroupImpl{id=hcf1sr0sbo}; attribute=Sensor: group.members.count (java.lang.Integer) expected [1] but found [0]
     *     at org.apache.brooklyn.util.exceptions.Exceptions.propagate(Exceptions.java:164)
     *     at org.apache.brooklyn.util.exceptions.Exceptions.propagateAnnotated(Exceptions.java:144)
     *     at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:963)
     *     at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:854)
     *     at org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually(EntityAsserts.java:67)
     *     at org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually(EntityAsserts.java:62)
     *     at org.apache.brooklyn.entity.group.GroupPickUpEntitiesTest.testGroupFindsElement(GroupPickUpEntitiesTest.java:74)
     * Caused by: java.lang.AssertionError: entity=BasicGroupImpl{id=hcf1sr0sbo}; attribute=Sensor: group.members.count (java.lang.Integer) expected [1] but found [0]
     *     at org.apache.brooklyn.test.Asserts.fail(Asserts.java:721)
     *     ...
     * 
     * The debug log also shows:
     *   2016-06-07 00:57:25,686 DEBUG o.a.b.c.m.i.LocalEntityManager [main]: org.apache.brooklyn.core.mgmt.internal.LocalEntityManager@2326c52 starting management of entity BasicGroupImpl{id=hcf1sr0sbo}
     *   2016-06-07 00:57:25,686 DEBUG o.a.b.core.sensor.AttributeMap [brooklyn-execmanager-m7hnHEpz-1]: removing attribute service.isUp on BasicGroupImpl{id=hcf1sr0sbo}
     * 
     * This strongly suggests the problem is that another thread has cleared is service.isUp value. 
     * That is most likely being done by the enrichers added in AbstractEntity.initEnrichers.
     */
    @Test(groups="Broken")
    public void testGroupFindsElement() {
        Assert.assertEquals(group.getMembers().size(), 0);
        EntityAsserts.assertAttributeEquals(group, BasicGroup.GROUP_SIZE, 0);
        
        final TestEntity e1 = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        EntityAsserts.assertAttributeEquals(group, BasicGroup.GROUP_SIZE, 0);

        e1.sensors().set(Startable.SERVICE_UP, true);
        e1.sensors().set(TestEntity.NAME, "bob");

        EntityAsserts.assertAttributeEqualsEventually(group, BasicGroup.GROUP_SIZE, 1);
        assertGroupMemebersEqualsEventually(group, ImmutableList.of(e1));
        
        final TestEntity e2 = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        EntityAsserts.assertAttributeEquals(group, BasicGroup.GROUP_SIZE, 1);
        Assert.assertEquals(group.getMembers().size(), 1);
        Assert.assertTrue(group.getMembers().contains(e1));

        e2.sensors().set(Startable.SERVICE_UP, true);
        e2.sensors().set(TestEntity.NAME, "fred");

        EntityAsserts.assertAttributeEquals(group, BasicGroup.GROUP_SIZE, 1);

        e2.sensors().set(TestEntity.NAME, "BOB");
        EntityAsserts.assertAttributeEqualsEventually(group, BasicGroup.GROUP_SIZE, 2);
        assertGroupMemebersEqualsEventually(group, ImmutableList.of(e1, e2));
    }

    private void assertGroupMemebersEqualsEventually(final Group group, final List<? extends Entity> expected) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                // must use "succeedsEventually" because size + members attributes are set sequentially in another thread; 
                // just waiting for the first does not mean the second will have been set by the time we check in this thread.
                Asserts.assertEqualsIgnoringOrder(group.getAttribute(BasicGroup.GROUP_MEMBERS), expected);
            }});
    }

    /**
     * sets the membership of a group to be all up services;
     * callers can subclass and override {@link #checkMembership(Entity)} to add additional membership constraints,
     * and optionally {@link #init()} to apply additional subscriptions
     */
    public static class FindUpServices extends AbstractPolicy {

        protected final SensorEventListener<Object> handler = new SensorEventListener<Object>() {
            @Override
            public void onEvent(SensorEvent<Object> event) {
                updateMembership(event.getSource());
            }
        };

        @Override
        public void setEntity(EntityLocal entity) {
            assert entity instanceof Group;
            super.setEntity(entity);
            subscriptions().subscribe(null, Startable.SERVICE_UP, handler);
            for (Entity e : ((EntityInternal) entity).getManagementContext().getEntityManager().getEntities()) {
                if (Objects.equal(e.getApplicationId(), entity.getApplicationId()))
                    updateMembership(e);
            }
        }

        protected Group getGroup() {
            return (Group) entity;
        }

        protected void updateMembership(Entity e) {
            boolean isMember = checkMembership(e);
            if (isMember) getGroup().addMember(e);
            else getGroup().removeMember(e);
        }

        protected boolean checkMembership(Entity e) {
            if (!Entities.isManaged(e)) return false;
            if (!Boxing.unboxSafely(e.getAttribute(Startable.SERVICE_UP), false)) return false;
            return true;
        }
    }


    public static class FindUpServicesWithNameBob extends FindUpServices {

        @Override
        public void setEntity(EntityLocal entity) {
            super.setEntity(entity);
            subscriptions().subscribe(null, TestEntity.NAME, handler);
        }

        @Override
        protected boolean checkMembership(Entity e) {
            if (!super.checkMembership(e)) return false;
            if (!"Bob".equalsIgnoreCase(e.getAttribute(TestEntity.NAME))) return false;
            return true;
        }
    }

}
