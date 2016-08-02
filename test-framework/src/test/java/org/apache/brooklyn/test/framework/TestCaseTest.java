/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.FailingEntity;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class TestCaseTest extends BrooklynAppUnitTestSupport {

    private List<Location> locs = ImmutableList.of();

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }

    @Test
    public void testSucceedsWhenEmpty() throws Exception {
        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class));
        app.start(locs);

        assertTestCaseSucceeds(testCase);
    }
    
    @Test
    public void testCallsChildrenSequentially() throws Exception {
        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .child(EntitySpec.create(TestEntity.class).impl(TestEntityConcurrencyTrackerImpl.class))
                .child(EntitySpec.create(TestEntity.class).impl(TestEntityConcurrencyTrackerImpl.class)));
        TestEntity child1 = (TestEntity) Iterables.get(testCase.getChildren(), 0);
        TestEntity child2 = (TestEntity) Iterables.get(testCase.getChildren(), 1);
        app.start(locs);

        assertTestCaseSucceeds(testCase);
        assertEquals(child1.getCallHistory(), ImmutableList.of("start"));
        assertEquals(child2.getCallHistory(), ImmutableList.of("start"));
        assertEquals(TestEntityConcurrencyTrackerImpl.getMaxConcurrent(), 1);
    }
    
    @Test
    public void testDoesNotCallsOnErrorEntityIfSuccessful() throws Exception {
        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .configure(TestCase.ON_ERROR_SPEC, EntitySpec.create(TestEntity.class).displayName("onerr"))
                .child(EntitySpec.create(TestEntity.class)));
        app.start(locs);

        Optional<Entity> onErrEntity = Iterables.tryFind(testCase.getChildren(), EntityPredicates.displayNameEqualTo("onerr"));
        assertFalse(onErrEntity.isPresent(), "entity="+onErrEntity);
        
        assertTestCaseSucceeds(testCase);
    }
    
    @Test
    public void testCallsOnErrorEntity() throws Exception {
        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .configure(TestCase.ON_ERROR_SPEC, EntitySpec.create(TestEntity.class).displayName("onerr"))
                .child(EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)));
        
        try {
            app.start(locs);
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "Simulating entity start failure for test");
        }

        TestEntity onErrEntity = (TestEntity) Iterables.tryFind(testCase.getChildren(), EntityPredicates.displayNameEqualTo("onerr")).get();
        assertEquals(onErrEntity.getCallHistory(), ImmutableList.of("start"));
    }
    
    @Test
    public void testOnErrorEntityNotInherited() throws Exception {
        TestCase testCase = app.createAndManageChild(EntitySpec.create(TestCase.class)
                .configure(TestCase.ON_ERROR_SPEC, EntitySpec.create(TestEntity.class).displayName("onerr"))
                .child(EntitySpec.create(TestCase.class)
                        .child(EntitySpec.create(FailingEntity.class)
                                .configure(FailingEntity.FAIL_ON_START, true))));
        TestCase innerTestCase = (TestCase) Iterables.getOnlyElement(testCase.getChildren());
        
        try {
            app.start(locs);
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable t) {
            Asserts.expectedFailureContains(t, "Simulating entity start failure for test");
        }

        TestEntity onErrEntity = (TestEntity) Iterables.tryFind(testCase.getChildren(), EntityPredicates.displayNameEqualTo("onerr")).get();
        assertEquals(onErrEntity.getCallHistory(), ImmutableList.of("start"));
        
        Optional<Entity> innerOnErrEntity = Iterables.tryFind(innerTestCase.getChildren(), EntityPredicates.displayNameEqualTo("onerr"));
        assertFalse(innerOnErrEntity.isPresent(), "innerOnErrEntity="+innerOnErrEntity);
    }
    
    protected void assertTestCaseSucceeds(TestCase entity) {
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, true);
    }
    
    public static class TestEntityConcurrencyTrackerImpl extends TestEntityImpl {
        private static final AtomicInteger concurrentCallCounter = new AtomicInteger();
        private static final AtomicInteger maxConcurrentCalls = new AtomicInteger();
        
        public void reset() {
            maxConcurrentCalls.set(0);
        }
        
        public static int getMaxConcurrent() {
            return maxConcurrentCalls.get();
        }
        
        @Override
        public void start(Collection<? extends Location> locs) {
            int count = concurrentCallCounter.incrementAndGet();
            try {
                do {
                    int max = maxConcurrentCalls.get();
                    if (count > max) {
                        maxConcurrentCalls.compareAndSet(max, count);
                    }
                } while (count > maxConcurrentCalls.get());
                
                super.start(locs);
                
            } finally {
                concurrentCallCounter.decrementAndGet();
            }
        }
    }
}
