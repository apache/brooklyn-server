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

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttribute;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEquals;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;
import static org.apache.brooklyn.test.Asserts.assertEqualsIgnoringOrder;
import static org.apache.brooklyn.test.Asserts.*;
import static org.apache.brooklyn.test.Asserts.succeedsEventually;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

public class SequenceGroupTest extends BrooklynAppUnitTestSupport {

    private SimulatedLocation loc1;
    private SequenceGroup group;
    private TestEntity e1, e2, e3;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
    }

    protected void createTestEntities() throws Exception {
        e1 = app.createAndManageChild(EntitySpec.create(TestEntity.class).displayName("test-one"));
        e2 = app.createAndManageChild(EntitySpec.create(TestEntity.class).displayName("test-two"));
        e3 = app.createAndManageChild(EntitySpec.create(TestEntity.class).displayName("test-three"));
    }

    @Test
    public void testGroupDefaults() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertAttribute(group, SequenceGroup.SEQUENCE_VALUE, Predicates.isNull());
        assertTrue(group.getMembers().isEmpty());
    }

    @Test
    public void testGroupWithMatchingFilterReturnsOnlyMatchingMembers() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.ENTITY_FILTER, EntityPredicates.displayNameEqualTo("test-one")));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableList.of(e1));
        assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 1);
        AtomicInteger state = group.sensors().get(SequenceGroup.SEQUENCE_STATE);
        assertEquals(state.get(), 2);
    }

    @Test
    public void testGroupConfiguration() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.SEQUENCE_STRING_SENSOR,  Sensors.newStringSensor("test.sequence"))
                .configure(SequenceGroup.SEQUENCE_FORMAT, "test-%02d")
                .configure(SequenceGroup.ENTITY_FILTER, EntityPredicates.displayNameEqualTo("test-one")));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableList.of(e1));
        assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 1);
        assertAttributeEquals(e1, SequenceGroup.SEQUENCE_STRING, null);
        assertAttributeEquals(e1, Sensors.newStringSensor("test.sequence"), "test-01");
    }

    @Test
    public void testAlternateGroupConfiguration() throws Exception {
        AttributeSensor<Integer> value = Sensors.newIntegerSensor("test.value");
        AttributeSensor<String> string = Sensors.newStringSensor("test.string");
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.SEQUENCE_START, 12345)
                .configure(SequenceGroup.SEQUENCE_INCREMENT, 678)
                .configure(SequenceGroup.SEQUENCE_VALUE_SENSOR, value)
                .configure(SequenceGroup.SEQUENCE_STRING_SENSOR, string)
                .configure(SequenceGroup.SEQUENCE_FORMAT, "0x%04X")
                .configure(SequenceGroup.ENTITY_FILTER, EntityPredicates.hasInterfaceMatching(".*TestEntity")));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, e3));
        assertAttributeEquals(e1, value, 12345);
        assertAttributeEquals(e1, string, "0x3039");
        assertAttributeEquals(e2, value, 13023);
        assertAttributeEquals(e2, string, "0x32DF");
        assertAttributeEquals(e3, value, 13701);
        assertAttributeEquals(e3, string, "0x3585");
    }

    @Test
    public void testGroupWithMatchingFilterReturnsEverythingThatMatches() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.ENTITY_FILTER, Predicates.alwaysTrue()));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, e3, app, group));
        assertAttributeEquals(app, SequenceGroup.SEQUENCE_VALUE, 1);
        assertAttributeEquals(group, SequenceGroup.SEQUENCE_VALUE, 2);
        assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 3);
        assertAttributeEquals(e2, SequenceGroup.SEQUENCE_VALUE, 4);
        assertAttributeEquals(e3, SequenceGroup.SEQUENCE_VALUE, 5);
        assertAttributeEquals(group, SequenceGroup.SEQUENCE_CURRENT, e3);
        AtomicInteger state = group.sensors().get(SequenceGroup.SEQUENCE_STATE);
        assertEquals(state.get(), 6);
    }

    @Test
    public void testGroupDetectsNewlyManagedMatchingMember() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.ENTITY_FILTER, EntityPredicates.displayNameEqualTo("test-four")));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        final Entity e = app.addChild(EntitySpec.create(TestEntity.class).displayName("test-four"));

        succeedsEventually(new Runnable() {
            public void run() {
                assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e));
                assertAttributeEquals(e, SequenceGroup.SEQUENCE_VALUE, 1);
                AtomicInteger state = group.sensors().get(SequenceGroup.SEQUENCE_STATE);
                assertEquals(state.get(), 2);
            }});
    }

    @Test
    public void testGroupAddsNewMatchingMember() throws Exception {
        group = app.addChild(EntitySpec.create(SequenceGroup.class)
                .configure(SequenceGroup.ENTITY_FILTER, EntityPredicates.hasInterfaceMatching(".*TestEntity")));
        createTestEntities();
        app.start(ImmutableList.of(loc1));

        assertAttributeEqualsEventually(group, SequenceGroup.RUNNING, true);

        assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, e3));
        assertAttributeEquals(e1, SequenceGroup.SEQUENCE_VALUE, 1);
        assertAttributeEquals(e2, SequenceGroup.SEQUENCE_VALUE, 2);
        assertAttributeEquals(e3, SequenceGroup.SEQUENCE_VALUE, 3);
        assertAttributeEquals(group, SequenceGroup.SEQUENCE_CURRENT, e3);
        AtomicInteger state = group.sensors().get(SequenceGroup.SEQUENCE_STATE);
        assertEquals(state.get(), 4);

        final Entity e = app.addChild(EntitySpec.create(TestEntity.class));

        succeedsEventually(new Runnable() {
            public void run() {
                assertEqualsIgnoringOrder(group.getMembers(), ImmutableSet.of(e1, e2, e3, e));
                assertAttributeEquals(e, SequenceGroup.SEQUENCE_VALUE, 4);
                assertAttributeEquals(group, SequenceGroup.SEQUENCE_CURRENT, e);
                AtomicInteger state = group.sensors().get(SequenceGroup.SEQUENCE_STATE);
                assertEquals(state.get(), 5);
            }});
    }
}
