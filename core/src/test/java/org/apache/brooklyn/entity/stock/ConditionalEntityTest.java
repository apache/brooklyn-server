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
package org.apache.brooklyn.entity.stock;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.*;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;

public class ConditionalEntityTest extends BrooklynAppUnitTestSupport {

    private SimulatedLocation loc1;
    private ConditionalEntity optional;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
    }

    @Test
    public void testAddsConditionalWhenConfigured() throws Exception {
        optional = app.addChild(EntitySpec.create(ConditionalEntity.class)
                .configure(ConditionalEntity.CREATE_CONDITIONAL_ENTITY, true)
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(optional.getChildren());
        assertTrue(child instanceof TestEntity);
        assertEquals(child, optional.sensors().get(ConditionalEntity.CONDITIONAL_ENTITY));

        // The service.isUp sensor will have been propagated by default
        EntityAsserts.assertAttributeEqualsEventually(child, Startable.SERVICE_UP, true);
        EntityAsserts.assertAttributeEqualsEventually(optional, Startable.SERVICE_UP, true);
    }

    @Test
    public void testAddsConditionalAndPropagatesSensors() throws Exception {
        optional = app.addChild(EntitySpec.create(ConditionalEntity.class)
                .configure(ConditionalEntity.CREATE_CONDITIONAL_ENTITY, true)
                .configure(ConditionalEntity.PROPAGATE_CONDITIONAL_ENTITY_SENSORS, true)
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SENSOR_LIST, ImmutableList.<AttributeSensor<?>>of(TestEntity.SEQUENCE))
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(optional.getChildren());
        assertTrue(child instanceof TestEntity);
        assertEquals(child, optional.sensors().get(ConditionalEntity.CONDITIONAL_ENTITY));

        // Check that the configured sensors are propagated
        child.sensors().set(TestEntity.SEQUENCE, 123);
        EntityAsserts.assertAttributeEqualsEventually(child, TestEntity.SEQUENCE, 123);
        EntityAsserts.assertAttributeEqualsEventually(optional, TestEntity.SEQUENCE, 123);
        child.sensors().set(TestEntity.NAME, "frog");
        EntityAsserts.assertAttributeEqualsEventually(child, TestEntity.NAME, "frog");
        EntityAsserts.assertAttribute(optional, TestEntity.NAME, Predicates.isNull());
    }

    @Test
    public void testConditionalSurvivesRestart() {
        optional = app.addChild(EntitySpec.create(ConditionalEntity.class)
                .configure(ConditionalEntity.CREATE_CONDITIONAL_ENTITY, true)
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));
        app.restart();

        assertEquals(optional.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(optional.getChildren());
        assertTrue(child instanceof TestEntity);
        assertEquals(child, optional.sensors().get(ConditionalEntity.CONDITIONAL_ENTITY));
    }

    @Test
    public void testDoesNotAddConditionalWhenConfigured() throws Exception {
        optional = app.addChild(EntitySpec.create(ConditionalEntity.class)
                .configure(ConditionalEntity.CREATE_CONDITIONAL_ENTITY, false)
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 0);
    }

    @Test
    public void testDoesNotAddConditionalWhenOnlySpecConfigured() throws Exception {
        optional = app.addChild(EntitySpec.create(ConditionalEntity.class)
                .configure(ConditionalEntity.CONDITIONAL_ENTITY_SPEC, EntitySpec.create(TestEntity.class)));
        app.start(ImmutableList.of(loc1));

        assertEquals(optional.getChildren().size(), 0);
    }

}
