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

import static org.apache.brooklyn.core.entity.EntityPredicates.applicationIdEqualTo;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;

public class EntitiesTest extends BrooklynAppUnitTestSupport {

    private static final int TIMEOUT_MS = 10*1000;
    
    private SimulatedLocation loc;
    private TestEntity entity;
    private TestEntity entity2;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = app.getManagementContext().getLocationManager().createLocation(LocationSpec.create(SimulatedLocation.class));
        entity = app.addChild(EntitySpec.create(TestEntity.class));
        entity2 = app.addChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.of(loc));
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testDescendants() throws Exception {
        Asserts.assertEqualsIgnoringOrder(Entities.descendantsAndSelf(app), ImmutableList.of(app, entity, entity2));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(app), ImmutableList.of(app, entity, entity2));
        
        Asserts.assertEqualsIgnoringOrder(Entities.descendantsAndSelf(entity), ImmutableList.of(entity));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(entity), ImmutableList.of(entity));
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testDescendantsFilteredByType() throws Exception {
        Asserts.assertEqualsIgnoringOrder(Entities.descendantsAndSelf(app, TestEntity.class), ImmutableList.of(entity, entity2));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(app, TestEntity.class), ImmutableList.of(entity, entity2));
    }
    
    @Test
    @SuppressWarnings("deprecation")
    public void testDescendantsFilteredByPredicate() throws Exception {
        Asserts.assertEqualsIgnoringOrder(Entities.descendantsAndSelf(app, Predicates.instanceOf(TestEntity.class)), ImmutableList.of(entity, entity2));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(app, Predicates.instanceOf(TestEntity.class)), ImmutableList.of(entity, entity2));
    }
    
    @Test
    public void testDescendantsWithExplicitIncludeSelf() throws Exception {
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(app, Predicates.alwaysTrue(), true), ImmutableList.of(app, entity, entity2));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(app, Predicates.alwaysTrue(), false), ImmutableList.of(entity, entity2));
        
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(entity, Predicates.alwaysTrue(), true), ImmutableList.of(entity));
        Asserts.assertEqualsIgnoringOrder(Entities.descendants(entity, Predicates.alwaysTrue(), false), ImmutableList.of());
    }
    
    @Test
    public void testAncestors() throws Exception {
        Asserts.assertEqualsIgnoringOrder(Entities.ancestorsAndSelf(app), ImmutableList.of(app));
        Asserts.assertEqualsIgnoringOrder(Entities.ancestors(app), ImmutableList.of(app));

        Asserts.assertEqualsIgnoringOrder(Entities.ancestorsAndSelf(entity), ImmutableList.of(entity, app));
        Asserts.assertEqualsIgnoringOrder(Entities.ancestors(entity), ImmutableList.of(entity, app));
    }
    
    
    @Test
    public void testAttributeSupplier() throws Exception {
        entity.sensors().set(TestEntity.NAME, "myname");
        assertEquals(Entities.attributeSupplier(entity, TestEntity.NAME).get(), "myname");
    }
    
    @Test(groups="Integration") // takes 1 second
    public void testAttributeSupplierWhenReady() throws Exception {
        final AtomicReference<String> result = new AtomicReference<String>();
        
        final Thread t = new Thread(new Runnable() {
            @Override public void run() {
                result.set(Entities.attributeSupplierWhenReady(entity, TestEntity.NAME).get());
                
            }});
        try {
            t.start();
            
            // should block, waiting for value
            Asserts.succeedsContinually(new Runnable() {
                @Override public void run() {
                    assertTrue(t.isAlive());
                }
            });
            
            entity.sensors().set(TestEntity.NAME, "myname");
            t.join(TIMEOUT_MS);
            assertFalse(t.isAlive());
            assertEquals(result.get(), "myname");
        } finally {
            t.interrupt();
        }
        
        // And now that it's set, the attribute-when-ready should return immediately
        assertEquals(Entities.attributeSupplierWhenReady(entity, TestEntity.NAME).get(), "myname");
    }
    
    @Test
    public void testCreateGetContainsAndRemoveTags() throws Exception {
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
            .tag(2)
            .addInitializer(EntityInitializers.addingTags("foo")));
        
        entity.tags().addTag(app);
        
        Assert.assertTrue(entity.tags().containsTag(app));
        Assert.assertTrue(entity.tags().containsTag("foo"));
        Assert.assertTrue(entity.tags().containsTag(2));
        Assert.assertFalse(entity.tags().containsTag("bar"));
        
        Assert.assertEquals(entity.tags().getTags(), MutableSet.of(app, "foo", 2));
        
        entity.tags().removeTag("foo");
        Assert.assertFalse(entity.tags().containsTag("foo"));
        
        Assert.assertTrue(entity.tags().containsTag(entity.getParent()));
        Assert.assertFalse(entity.tags().containsTag(entity));
        
        entity.tags().removeTag(2);
        Assert.assertEquals(entity.tags().getTags(), MutableSet.of(app));
    }
    
    @Test
    public void testWaitFor() throws Exception {
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Duration timeout = Duration.ONE_MILLISECOND;

        Entities.waitFor(entity, applicationIdEqualTo(app.getApplicationId()), timeout);

        try {
            Entities.waitFor(entity, applicationIdEqualTo(app.getApplicationId() + "-wrong"), timeout);
            Asserts.shouldHaveFailedPreviously("Entities.waitFor() should have timed out");
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Timeout waiting for ");
        }
    }

}
