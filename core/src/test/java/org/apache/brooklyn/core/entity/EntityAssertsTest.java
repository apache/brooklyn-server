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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.group.DynamicGroup;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests on {@link EntityAsserts}.
 */
public class EntityAssertsTest extends BrooklynAppUnitTestSupport {

    private static final String STOOGE = "stooge";

    private TestEntity entity;
    private DynamicGroup stooges;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.<Location>of());
    }

    @Test
    public void shouldAssertAttributeEquals() {
        final String myName = "myname";
        entity.sensors().set(TestEntity.NAME, myName);
        EntityAsserts.assertAttributeEquals(entity, TestEntity.NAME, myName);
    }

    @Test(expectedExceptions = AssertionError.class)
    public void shouldFailToAssertAttributeEquals() {
        final String myName = "myname";
        entity.sensors().set(TestEntity.NAME, myName);
        EntityAsserts.assertAttributeEquals(entity, TestEntity.NAME, "bogus");
    }

    @Test
    public void shouldAssertConfigEquals() {
        EntityAsserts.assertConfigEquals(entity, TestEntity.CONF_NAME, "defaultval");
    }

    @Test(expectedExceptions = AssertionError.class)
    public void shouldFailToAssertConfigEquals() {
        EntityAsserts.assertConfigEquals(entity, TestEntity.CONF_NAME, "bogus");
    }

    @Test
    public void shouldAssertAttributeEqualsEventually() throws Exception {
        entity.sensors().set(TestEntity.NAME, "before");
        final String after = "after";

        Task<?> assertValue = entity.getExecutionContext().submit("assert attr equals", 
            () -> EntityAsserts.assertAttributeEqualsEventually(entity, TestEntity.NAME, after));
        entity.sensors().set(TestEntity.NAME, after);
        assertValue.get();
    }

    @Test(expectedExceptions = AssertionError.class)
    public void shouldFailToAssertAttributeEqualsEventually() {
        entity.sensors().set(TestEntity.NAME, "before");
        final String after = "after";
        EntityAsserts.assertAttributeEqualsEventually(ImmutableMap.of("timeout", "100ms"), entity, TestEntity.NAME, after);
    }

    @Test
    public void shouldAssertAttributeEventuallyNonNull() throws Exception {
        EntityAsserts.assertAttributeEquals(entity, TestEntity.NAME, null);
        Task<?> assertValue = entity.getExecutionContext().submit("assert attr non-null", () -> EntityAsserts.assertAttributeEventuallyNonNull(entity, TestEntity.NAME));
        entity.sensors().set(TestEntity.NAME, "something");
        assertValue.get();
    }

    @Test
    public void shouldAssertAttributeEventually() throws Exception {
        final CountDownLatch eventuallyEntered = new CountDownLatch(2);
        Task<?> assertValue = entity.getExecutionContext().submit("assert attribute", () -> EntityAsserts.assertAttributeEventually(entity, TestEntity.NAME, 
            (input) -> {
                eventuallyEntered.countDown();
                return input.matches(".*\\d+");
            }) );
        eventuallyEntered.await();
        entity.sensors().set(TestEntity.NAME, "testing testing 123");
        assertValue.get();
    }

    @Test
    public void shouldAssertAttribute() {
        final String before = "before";
        entity.sensors().set(TestEntity.NAME, before);
        EntityAsserts.assertAttribute(entity, TestEntity.NAME, Predicates.equalTo(before));
    }

    @Test
    public void shouldAssertPredicateEventuallyTrue() throws Exception {
        final int testVal = 987654321;
        final CountDownLatch eventuallyEntered = new CountDownLatch(2);
        Task<?> assertValue = entity.getExecutionContext().submit("assert predicate", () -> EntityAsserts.assertPredicateEventuallyTrue(entity, 
            (input) -> {
                eventuallyEntered.countDown();
                return testVal == input.getSequenceValue();
            }));
        eventuallyEntered.await();
        entity.setSequenceValue(testVal);
        assertValue.get();
    }

    @Test
    public void shouldAssertAttributeEqualsContinually() {
        final String myName = "myname";
        entity.sensors().set(TestEntity.NAME, myName);
        EntityAsserts.assertAttributeEqualsContinually(
                ImmutableMap.of("timeout", "100ms"), entity, TestEntity.NAME, myName);
    }

    @Test(expectedExceptions = AssertionError.class)
    public void shouldFailAssertAttributeEqualsContinually() throws Throwable {
        final String myName = "myname";
        entity.sensors().set(TestEntity.NAME, myName);
        Task<?> assertValue = entity.getExecutionContext().submit("check attr equals", () -> EntityAsserts.assertAttributeEqualsContinually(entity, TestEntity.NAME, myName));
        entity.sensors().set(TestEntity.NAME, "something");
        try {
            assertValue.get();
        } catch (ExecutionException e) {
            //strip wrapper exception
            throw e.getCause();
        }
    }

    @Test
    public void shouldAssertGroupSizeEqualsEventually() throws Exception {
        stooges = app.createAndManageChild(EntitySpec.create(DynamicGroup.class));
        final EntitySpec<TestEntity> stooge =
                EntitySpec.create(TestEntity.class).configure(TestEntity.CONF_NAME, STOOGE);
        app.createAndManageChild(stooge);
        app.createAndManageChild(stooge);
        app.createAndManageChild(stooge);

        Task<?> assertValue1 = entity.getExecutionContext().submit("assert size", () -> EntityAsserts.assertGroupSizeEqualsEventually(ImmutableMap.of("timeout", "2s"), stooges, 3));
        stooges.setEntityFilter(EntityPredicates.configEqualTo(TestEntity.CONF_NAME, STOOGE));
        assertValue1.get();
        Task<?> assertValue2 = entity.getExecutionContext().submit("assert size 0", () -> EntityAsserts.assertGroupSizeEqualsEventually(stooges, 0));
        stooges.setEntityFilter(EntityPredicates.configEqualTo(TestEntity.CONF_NAME, "Marx Brother"));
        assertValue2.get();
    }

    @Test
    public void shouldAssertAttributeChangesEventually () throws Exception{
        entity.sensors().set(TestEntity.NAME, "before");
        final Task<?> assertValue = entity.getExecutionContext().submit("check attr change", () -> EntityAsserts.assertAttributeChangesEventually(entity, TestEntity.NAME));
        Repeater.create()
            .repeat(() -> entity.sensors().set(TestEntity.NAME, "after" + System.currentTimeMillis()))
            .until(() -> assertValue.isDone())
            .every(Duration.millis(10))
            .run();
        assertValue.get();
    }

    @Test
    public void shouldAssertAttributeNever() {
        entity.sensors().set(TestEntity.NAME, "ever");
        EntityAsserts.assertAttributeContinuallyNotEqualTo(ImmutableMap.of("timeout", "100ms"), entity, TestEntity.NAME, "after");
    }

}
