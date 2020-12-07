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
import static org.testng.Assert.assertNull;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class OwnedChildrenDeprecatedTest extends BrooklynAppUnitTestSupport {

    // Tests that the deprecated "owner" still works
    @Test
    public void testSetOwnerInConstructorMap() {
        Entity e = new AbstractEntity(MutableMap.of("owner", app)) {};
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
        assertEquals(e.getApplication(), app);
    }
    
    // Tests that the deprecated constructor still works
    @Test
    public void testSetParentInConstructorArgument() {
        Entity e = new AbstractEntity(app) {};
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
        assertEquals(e.getApplication(), app);
    }
    
    // Tests with deprecated constructor usage
    @Test
    public void testSetParentWhenMatchesParentSetInConstructor() {
        Entity e = new AbstractEntity(app) {};
        e.setParent(app);
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
    }
    
    // Tests with deprecated constructor usage
    @Test
    public void testSetParentWhenDiffersFromParentSetInConstructor() {
        Entity e = new AbstractEntity(app) {};
        Entity e2 = new AbstractEntity() {};
        
        // since 2020 this should still work 
        e.setParent(e2);
        assertEquals(e.getParent(), e2);
        Asserts.assertSize(e2.getChildren(), 1);
        Asserts.assertEquals(e2.getChildren().iterator().next(), e);
        
        // and application ID is cleared
        Asserts.assertEquals(e.getApplicationId(), null);
        Asserts.assertEquals(e2.getApplicationId(), null);
    }
    
    // Tests deprecated creation with setParent still works - should be set through EntitySpec or addChild
    @Test
    public void testSetParentInSetterMethod() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        e.setParent(app);
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
        assertEquals(e.getApplication(), app);
    }

    @Test
    public void testSetParentWhenMatchesParentSetInSpec() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class).parent(app));
        e.setParent(app);
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
    }
    
    @Test
    public void testSetParentWhenDiffersFromParentSetInSpec() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class).parent(app));
        Entity e2 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        
        // since 2020 allowed to change parent
        e.setParent(e2);
        assertEquals(e.getParent(), e2);
        Asserts.assertSize(e2.getChildren(), 1);
        Asserts.assertEquals(e2.getChildren().iterator().next(), e);
    }
    
    // Tests deprecated addChild still works - users should instead set it through EntitySpec (or {@code addChild(EntitySpec)})
    @Test
    public void testAddChild() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        app.addChild(e);
        
        assertEquals(e.getParent(), app);
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of(e));
        assertEquals(e.getApplication(), app);
    }
    

    
    @Test(enabled = false) // FIXME fails currently
    public void testRemoveChild() {
        Entity e = app.addChild(EntitySpec.create(TestEntity.class));
        app.removeChild(e);
        
        assertEqualsIgnoringOrder(app.getChildren(), ImmutableList.of());
        assertEquals(e.getParent(), null);
    }
    
    @Test
    public void testParentalLoopForbiddenViaAddChild() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        Entity e2 = e.addChild(EntitySpec.create(TestEntity.class));
        try {
            e2.addChild(e);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception ex) {
            Exception cause = Exceptions.getFirstThrowableOfType(ex, IllegalStateException.class);
            if (cause == null || !cause.toString().contains("loop detected trying to add child")) {
                throw ex;
            }
        }
        assertEqualsIgnoringOrder(e.getChildren(), ImmutableList.of(e2));
        assertEqualsIgnoringOrder(e2.getChildren(), ImmutableList.of());
        assertEquals(e.getParent(), null);
        assertEquals(e2.getParent(), e);
    }
    
    @Test
    public void testParentalLoopForbiddenViaSetParent() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        Entity e2 = e.addChild(EntitySpec.create(TestEntity.class));
        try {
            e.setParent(e2);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception ex) {
            Exception cause = Exceptions.getFirstThrowableOfType(ex, IllegalStateException.class);
            if (cause == null || !cause.toString().contains("loop detected trying to set parent")) {
                throw ex;
            }
        }
        assertEqualsIgnoringOrder(e.getChildren(), ImmutableList.of(e2));
        assertEqualsIgnoringOrder(e2.getChildren(), ImmutableList.of());
        assertEquals(e.getParent(), null);
        assertEquals(e2.getParent(), e);
    }
    
    @Test
    public void testChildingOneselfForbidden() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        try {
            e.addChild(e);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception ex) {
            Exception cause = Exceptions.getFirstThrowableOfType(ex, IllegalStateException.class);
            if (cause == null || !cause.toString().contains("cannot own itself")) {
                throw ex;
            }
        }
        
        assertNull(e.getParent());
        assertEquals(e.getChildren(), ImmutableList.of());
    }
    
    @Test
    public void testParentingOneselfForbidden() {
        Entity e = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class));
        try {
            e.setParent(e);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception ex) {
            Exception cause = Exceptions.getFirstThrowableOfType(ex, IllegalStateException.class);
            if (cause == null || !cause.toString().contains("cannot own itself")) {
                throw ex;
            }
        }
        
        assertNull(e.getParent());
        assertEquals(e.getChildren(), ImmutableList.of());
    }
}
