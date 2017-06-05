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

import static org.apache.brooklyn.test.Asserts.assertEqualsIgnoringOrder;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class OwnedChildrenTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testAddChildUsingSpec() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        
        assertEquals(e.getParent(), app);
        assertEquals(app.getChildren(), ImmutableList.of(e));
    }
    
    @Test
    public void testSpecDeclaresParent() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class).parent(app));
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
    }
    
    @Test
    public void testAddChildWhenMatchesParentSetInSpec() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class).parent(app));
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
    }
    
    @Test
    public void testAddChildWhenDifferentParentSetInSpec() {
        TestApplication app2 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        try {
            Entity e = app.addChild(EntitySpec.create(TestEntity.class).parent(app2));
            Asserts.shouldHaveFailedPreviously("entity="+e);
        } catch (Exception ex) {
            Exception cause = Exceptions.getFirstThrowableOfType(ex, IllegalArgumentException.class);
            if (cause == null || !cause.toString().contains("failed because spec has different parent")) {
                throw ex;
            }
        }
    }
    
    @Test
    public void testParentCanHaveMultipleChildren() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entity e2 = app.addChild(EntitySpec.create(TestEntity.class));
        
        assertEquals(e.getParent(), app);
        assertEquals(e2.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e,e2));
    }
    
    @Test
    public void testHierarchyOfOwners() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entity e2 = e.addChild(EntitySpec.create(TestEntity.class));
        Entity e3 = e2.addChild(EntitySpec.create(TestEntity.class));
        
        assertEquals(app.getParent(), null);
        assertEquals(e.getParent(), app);
        assertEquals(e2.getParent(), e);
        assertEquals(e3.getParent(), e2);
        
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
        assertEqualsIgnoringOrder(e.getChildren(), ImmutableList.of(e2));
        assertEqualsIgnoringOrder(e2.getChildren(), ImmutableList.of(e3));
        assertEqualsIgnoringOrder(e3.getChildren(), ImmutableList.of());
    }
    
    @Test
    public void testUnmanageEntityRemovedAsChild() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entities.unmanage(e);
        
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of());
        assertEquals(e.getParent(), null);
    }
    
    @Test
    public void testUnmanageParentRemovesChild() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entities.unmanage(app);
        
        assertFalse(Entities.isManaged(app));
        assertFalse(Entities.isManaged(e));
    }
    
    @Test
    public void testIsAncestor() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entity e2 = e.addChild(EntitySpec.create(TestEntity.class));
        
		assertTrue(Entities.isAncestor(e2, app));
		assertTrue(Entities.isAncestor(e2, e));
		assertFalse(Entities.isAncestor(e2, e2));
        assertFalse(Entities.isAncestor(e, e2));
    }
    
    @Test
    public void testIsDescendant() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        Entity e2 = e.addChild(EntitySpec.create(TestEntity.class));

		assertTrue(Entities.isDescendant(app, e));
		assertTrue(Entities.isDescendant(app, e2));
		assertFalse(Entities.isDescendant(e2, e));
        assertFalse(Entities.isDescendant(e2, e2));
    }
}
