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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.trait.Changeable;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.BasicGroup;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class EntityPredicatesTest extends BrooklynAppUnitTestSupport {

    private TestEntity entity;
    private BasicGroup group;
    private Location loc;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class).displayName("mydisplayname"));
        group = app.createAndManageChild(EntitySpec.create(BasicGroup.class));
        loc = app.getManagementContext().getLocationRegistry().getLocationManaged("localhost");
    }

    @Test
    public void testApplicationIdEqualTo() throws Exception {
        assertTrue(EntityPredicates.applicationIdEqualTo(app.getId()).apply(entity));
        assertFalse(EntityPredicates.applicationIdEqualTo("wrongid").apply(entity));
    }

    @Test
    public void testIdEqualTo() throws Exception {
        assertTrue(EntityPredicates.idEqualTo(entity.getId()).apply(entity));
        assertFalse(EntityPredicates.idEqualTo("wrongid").apply(entity));
    }

    @Test
    public void testAttributeNotNull() throws Exception {
        entity.sensors().set(TestEntity.NAME, "myname");
        assertTrue(EntityPredicates.attributeNotNull(TestEntity.NAME).apply(entity));
        assertTrue(EntityPredicates.attributeNotNull(TestEntity.NAME.getName()).apply(entity));
        assertFalse(EntityPredicates.attributeNotNull(TestEntity.SEQUENCE).apply(entity));
        assertFalse(EntityPredicates.attributeNotNull(TestEntity.SEQUENCE.getName()).apply(entity));
    }

    @Test
    public void testAttributeEqualTo() throws Exception {
        entity.sensors().set(TestEntity.NAME, "myname");
        assertTrue(EntityPredicates.attributeEqualTo(TestEntity.NAME, "myname").apply(entity));
        assertTrue(EntityPredicates.attributeEqualTo(TestEntity.NAME.getName(), "myname").apply(entity));
        assertFalse(EntityPredicates.attributeEqualTo(TestEntity.NAME, "wrongname").apply(entity));
        assertFalse(EntityPredicates.attributeEqualTo(TestEntity.NAME.getName(), "wrongname").apply(entity));
    }

    @Test
    public void testAttributeNotEqualTo() throws Exception {
        entity.sensors().set(TestEntity.NAME, "myname");
        assertFalse(EntityPredicates.attributeNotEqualTo(TestEntity.NAME, "myname").apply(entity));
        assertFalse(EntityPredicates.attributeNotEqualTo(TestEntity.NAME.getName(), "myname").apply(entity));
        assertTrue(EntityPredicates.attributeNotEqualTo(TestEntity.NAME, "wrongname").apply(entity));
        assertTrue(EntityPredicates.attributeNotEqualTo(TestEntity.NAME.getName(), "wrongname").apply(entity));
    }

    @Test
    public void testConfigNotNull() throws Exception {
        entity.config().set(TestEntity.CONF_NAME, "myname");
        assertTrue(EntityPredicates.configNotNull(TestEntity.CONF_NAME).apply(entity));
        assertTrue(EntityPredicates.configNotNull(TestEntity.CONF_NAME.getName()).apply(entity));
        assertFalse(EntityPredicates.configNotNull(TestEntity.CONF_OBJECT).apply(entity));
        assertFalse(EntityPredicates.configNotNull(TestEntity.CONF_OBJECT.getName()).apply(entity));
    }

    @Test
    public void testConfigEqualTo() throws Exception {
        entity.config().set(TestEntity.CONF_NAME, "myname");
        assertTrue(EntityPredicates.configEqualTo(TestEntity.CONF_NAME, "myname").apply(entity));
        assertTrue(EntityPredicates.configEqualTo(TestEntity.CONF_NAME.getName(), "myname").apply(entity));
        assertFalse(EntityPredicates.configEqualTo(TestEntity.CONF_NAME, "wrongname").apply(entity));
        assertFalse(EntityPredicates.configEqualTo(TestEntity.CONF_NAME.getName(), "wrongname").apply(entity));
    }

    @Test
    public void testConfigNotEqualTo() throws Exception {
        entity.config().set(TestEntity.CONF_NAME, "myname");
        assertFalse(EntityPredicates.configNotEqualTo(TestEntity.CONF_NAME, "myname").apply(entity));
        assertFalse(EntityPredicates.configNotEqualTo(TestEntity.CONF_NAME.getName(), "myname").apply(entity));
        assertTrue(EntityPredicates.configNotEqualTo(TestEntity.CONF_NAME, "wrongname").apply(entity));
        assertTrue(EntityPredicates.configNotEqualTo(TestEntity.CONF_NAME.getName(), "wrongname").apply(entity));
    }

    @Test
    public void testDisplayNameEqualTo() throws Exception {
        assertTrue(EntityPredicates.displayNameEqualTo("mydisplayname").apply(entity));
        assertFalse(EntityPredicates.displayNameEqualTo("wrongname").apply(entity));
    }

    @Test
    public void testDisplayNameSatisfies() throws Exception {
        assertTrue(EntityPredicates.displayNameSatisfies(StringPredicates.matchesRegex("myd.*me")).apply(entity));
        assertFalse(EntityPredicates.applicationIdEqualTo("wrongname").apply(entity));
    }
    
    @Test
    public void testIsChildOf() throws Exception {
        assertTrue(EntityPredicates.isChildOf(app).apply(entity));
        assertFalse(EntityPredicates.isChildOf(entity).apply(entity));
        assertFalse(EntityPredicates.isChildOf(entity).apply(app));
    }

    @Test
    public void testIsMemberOf() throws Exception {
        group.addMember(entity);
        assertTrue(EntityPredicates.isMemberOf(group).apply(entity));
        assertFalse(EntityPredicates.isMemberOf(group).apply(app));
        assertFalse(EntityPredicates.isMemberOf(group).apply(group));
    }

    @Test
    public void testManaged() throws Exception {
        assertTrue(EntityPredicates.isManaged().apply(entity));
        Entities.unmanage(entity);
        assertFalse(EntityPredicates.isManaged().apply(entity));
    }

    @Test
    public void testWithLocation() throws Exception {
        entity.addLocations(ImmutableList.of(loc));
        assertTrue(EntityPredicates.locationsIncludes(loc).apply(entity));
        assertFalse(EntityPredicates.locationsIncludes(loc).apply(app));
    }

    @Test
    public void testHasInterfaceMatching() throws Exception {
        assertTrue(EntityPredicates.hasInterfaceMatching(".*").apply(entity));
        assertTrue(EntityPredicates.hasInterfaceMatching(".*TestEntity").apply(entity));
        assertFalse(EntityPredicates.hasInterfaceMatching(".*TestEntity").apply(group));
        assertTrue(EntityPredicates.hasInterfaceMatching(Changeable.class.getName()).apply(group));
        assertTrue(EntityPredicates.hasInterfaceMatching(".*C.*able").apply(group));
    }

}
