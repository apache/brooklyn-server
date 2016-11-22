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
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class LocationEntityTest extends BrooklynAppUnitTestSupport {

    private SimulatedLocation loc1, loc2, loc3;
    private LocationEntity entity;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        loc2 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class).configure(LocationConfigKeys.CLOUD_PROVIDER, "cloud"));
        loc3 = mgmt.getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class).configure(LocationConfigKeys.ISO_3166, ImmutableSet.of("UK", "US")));
    }

    @Test
    public void testLocationEntityConfigurationWithType() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("OtherLocation", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Other"))
                        .put("SimulatedLocation", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Simulated"))
                        .put("default", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Default"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc1));

        assertEquals(entity.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        assertTrue(child instanceof TestEntity);

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, child);
        EntityAsserts.assertConfigEquals(child, TestEntity.CONF_NAME, "Simulated");
        EntityAsserts.assertEntityHealthy(child);
        EntityAsserts.assertEntityHealthy(entity);
    }

    @Test
    public void testLocationEntityConfigurationWithDefault() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("default", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Default"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc1));

        assertEquals(entity.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        assertTrue(child instanceof TestEntity);

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, child);
        EntityAsserts.assertConfigEquals(child, TestEntity.CONF_NAME, "Default");
        EntityAsserts.assertEntityHealthy(child);
        EntityAsserts.assertEntityHealthy(entity);
    }

    @Test
    public void testLocationEntityConfigurationWithWrongTypeAndDefault() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("OtherLocation", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Other"))
                        .put("default", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Default"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc1));

        assertEquals(entity.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        assertTrue(child instanceof TestEntity);

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, child);
        EntityAsserts.assertConfigEquals(child, TestEntity.CONF_NAME, "Default");
        EntityAsserts.assertEntityHealthy(child);
        EntityAsserts.assertEntityHealthy(entity);
    }

    @Test
    public void testLocationEntityConfigurationWithWrongTypeAndNoDefault() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("OtherLocation", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Other"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc1));

        assertTrue(entity.getChildren().isEmpty());

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, null);
        EntityAsserts.assertEntityHealthy(entity);
    }

    @Test
    public void testLocationEntityConfigurationWithProvider() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("cloud", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Cloud"))
                        .put("other", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Other"))
                        .put("default", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Default"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc2));

        assertEquals(entity.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        assertTrue(child instanceof TestEntity);

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_PROVIDER, "cloud");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, child);
        EntityAsserts.assertConfigEquals(child, TestEntity.CONF_NAME, "Cloud");
        EntityAsserts.assertEntityHealthy(child);
        EntityAsserts.assertEntityHealthy(entity);
    }

    @Test
    public void testLocationEntityConfigurationWithCountry() throws Exception {
        Map<String, EntitySpec<?>> map = ImmutableMap.<String, EntitySpec<?>>builder()
                        .put("UK", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "United Kingdom"))
                        .put("DE", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "Germany"))
                        .put("default", EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, "default"))
                        .build();
        entity = app.addChild(EntitySpec.create(LocationEntity.class)
                .configure(LocationEntity.LOCATION_ENTITY_SPEC_MAP, map));
        app.start(ImmutableList.of(loc3));

        assertEquals(entity.getChildren().size(), 1);
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        assertTrue(child instanceof TestEntity);

        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_TYPE, "SimulatedLocation");
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_COUNTRY_CODES, ImmutableSet.of("UK", "US"));
        EntityAsserts.assertAttributeEqualsEventually(entity, LocationEntity.LOCATION_ENTITY, child);
        EntityAsserts.assertConfigEquals(child, TestEntity.CONF_NAME, "United Kingdom");
        EntityAsserts.assertEntityHealthy(child);
        EntityAsserts.assertEntityHealthy(entity);
    }

}
