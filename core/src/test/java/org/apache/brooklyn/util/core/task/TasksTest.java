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
package org.apache.brooklyn.util.core.task;

import static org.apache.brooklyn.core.sensor.DependentConfiguration.attributeWhenReady;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.EntityFunctions;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags.WrappedEntity;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;
import com.google.common.util.concurrent.Callables;

@SuppressWarnings("serial")
public class TasksTest extends BrooklynAppUnitTestSupport {

    private ExecutionContext executionContext;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executionContext = app.getExecutionContext();
    }
    
    @Test
    public void testResolveNull() throws Exception {
        assertResolvesValue(null, String.class, null);
    }
    
    @Test
    public void testResolveValueCastsToType() throws Exception {
        assertResolvesValue(123, String.class, "123");
    }
    
    @Test
    public void testResolvesAttributeWhenReady() throws Exception {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");
        assertResolvesValue(attributeWhenReady(app, TestApplication.MY_ATTRIBUTE), String.class, "myval");
    }
    
    @Test
    public void testResolvesMapWithAttributeWhenReady() throws Exception {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");
        Map<?,?> orig = ImmutableMap.of("mykey", attributeWhenReady(app, TestApplication.MY_ATTRIBUTE));
        Map<String,String> expected = ImmutableMap.of("mykey", "myval");
        assertResolvesValue(orig, new TypeToken<Map<String,String>>() {}, expected);
    }
    
    @Test
    public void testResolvesSetWithAttributeWhenReady() throws Exception {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");
        Set<?> orig = ImmutableSet.of(attributeWhenReady(app, TestApplication.MY_ATTRIBUTE));
        Set<String> expected = ImmutableSet.of("myval");
        assertResolvesValue(orig, new TypeToken<Set<String>>() {}, expected);
    }
    
    @Test
    public void testResolvesMapOfMapsWithAttributeWhenReady() throws Exception {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");
        Map<?,?> orig = ImmutableMap.of("mykey", ImmutableMap.of("mysubkey", attributeWhenReady(app, TestApplication.MY_ATTRIBUTE)));
        Map<String,Map<String,String>> expected = ImmutableMap.of("mykey", ImmutableMap.of("mysubkey", "myval"));
        assertResolvesValue(orig, new TypeToken<Map<String,Map<String,String>>>() {}, expected);
    }
    
    @Test
    public void testResolvesListOfMapsWithAttributeWhenReady() throws Exception {
        app.sensors().set(TestApplication.MY_ATTRIBUTE, "myval");
        // using Iterables.concat so that orig is of type FluentIterable rather than List etc
        List<?> orig = ImmutableList.copyOf(ImmutableList.of(ImmutableMap.of("mykey", attributeWhenReady(app, TestApplication.MY_ATTRIBUTE))));
        Iterable<Map<String,String>> expected = ImmutableList.of(ImmutableMap.of("mykey", "myval"));
        assertResolvesValue(orig, new TypeToken<Iterable<Map<String,String>>>() {}, expected);
    }
    
    private <T> void assertResolvesValue(Object actual, Class<T> type, T expected) throws Exception {
        assertResolvesValue(actual, TypeToken.of(type), expected);
    }
    private <T> void assertResolvesValue(Object actual, TypeToken<T> type, T expected) throws Exception {
        Object result = Tasks.resolveValue(actual, type, executionContext);
        assertEquals(result, expected);
    }
    
    @Test
    public void testErrorsResolvingPropagatesOrSwallowedAllCorrectly() throws Exception {
        app.config().set(TestEntity.CONF_OBJECT, ValueResolverTest.newThrowTask(Duration.ZERO));
        Task<Object> t = Tasks.builder().body(Functionals.callable(EntityFunctions.config(TestEntity.CONF_OBJECT), app)).build();
        ValueResolver<Object> v = Tasks.resolving(t).as(Object.class).context(app);
        
        ValueResolverTest.assertThrowsOnGetMaybe(v);
        ValueResolverTest.assertThrowsOnGet(v);
        
        v.swallowExceptions();
        ValueResolverTest.assertMaybeIsAbsent(v);
        ValueResolverTest.assertThrowsOnGet(v);
        
        v.defaultValue("foo");
        ValueResolverTest.assertMaybeIsAbsent(v);
        assertEquals(v.clone().get(), "foo");
        assertResolvesValue(v, Object.class, "foo");
    }

    @Test
    public void testRepeater() throws Exception {
        Task<?> t;
        
        t = Tasks.requiring(Repeater.create().until(Callables.returning(true)).every(Duration.millis(1))).build();
        app.getExecutionContext().submit(t);
        t.get(Duration.TEN_SECONDS);
        
        t = Tasks.testing(Repeater.create().until(Callables.returning(true)).every(Duration.millis(1))).build();
        app.getExecutionContext().submit(t);
        Assert.assertEquals(t.get(Duration.TEN_SECONDS), true);
        
        t = Tasks.requiring(Repeater.create().until(Callables.returning(false)).limitIterationsTo(2).every(Duration.millis(1))).build();
        app.getExecutionContext().submit(t);
        try {
            t.get(Duration.TEN_SECONDS);
            Assert.fail("Should have failed");
        } catch (Exception e) {
            // expected
        }

        t = Tasks.testing(Repeater.create().until(Callables.returning(false)).limitIterationsTo(2).every(Duration.millis(1))).build();
        app.getExecutionContext().submit(t);
        Assert.assertEquals(t.get(Duration.TEN_SECONDS), false);
    }

    @Test
    public void testRepeaterDescription() throws Exception{
        final String description = "task description";
        Repeater repeater = Repeater.create(description)
            .repeat(Callables.returning(null))
            .every(Duration.ONE_MILLISECOND)
            .limitIterationsTo(1)
            .until(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    TaskInternal<?> current = (TaskInternal<?>)Tasks.current();
                    assertEquals(current.getBlockingDetails(), description);
                    return true;
                }
            });
        Task<Boolean> t = Tasks.testing(repeater).build();
        app.getExecutionContext().submit(t);
        assertTrue(t.get(Duration.TEN_SECONDS));
    }

    @Test
    public void testSingleExecutionContextEntityWithTask() {
        // Should cause an exception to be thrown in future releases. For now will log a warning.
        // Until then make sure the task is tagged only with the context of the executor.
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Task<Void> task = Tasks.<Void>builder()
            .tag(BrooklynTaskTags.tagForContextEntity(entity))
            .body(new AssertContextRunnable(ImmutableList.of(app))).build();
        app.getExecutionContext().submit(task).getUnchecked();
    }

    @Test
    public void testSingleExecutionContextEntityWithTaskAndExternalFlags() {
        // Should cause an exception to be thrown in future releases. For now will log a warning.
        // Until then make sure the task is tagged only with the context of the executor.
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Task<Void> task = Tasks.<Void>builder()
            .body(new AssertContextRunnable(ImmutableList.of(app))).build();
        ImmutableMap<String,?> flags = ImmutableMap.of(
                "tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity)));
        app.getExecutionContext().submit(flags, task).getUnchecked();
    }

    @Test
    public void testSingleExecutionContextEntityWithCallable() {
        // Should cause an exception to be thrown in future releases. For now will log a warning.
        // Until then make sure the task is tagged only with the context of the executor.
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Callable<Void> task = new Callable<Void>() {
            @Override
            public Void call() throws Exception {
                new AssertContextRunnable(ImmutableList.of(app)).run();
                return null;
            }
        };

        ImmutableMap<String,?> flags = ImmutableMap.of(
                "tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity)));
        app.getExecutionContext().submit(flags, task).getUnchecked();
    }

    @Test
    public void testSingleExecutionContextEntityWithRunnable() {
        // Should cause an exception to be thrown in future releases. For now will log a warning.
        // Until then make sure the task is tagged only with the context of the executor.
        final TestEntity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        Runnable task = new AssertContextRunnable(ImmutableList.of(app));
        ImmutableMap<String,?> flags = ImmutableMap.of(
                "tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(entity)));
        app.getExecutionContext().submit(flags, task).getUnchecked();
    }

    private static class AssertContextRunnable implements Runnable {
        private Collection<?> expectedContext;

        public AssertContextRunnable(Collection<?> expectedContext) {
            this.expectedContext = expectedContext;
        }

        @Override
        public void run() {
            Collection<Entity> context = new ArrayList<>();
            for (Object tag : Tasks.current().getTags()) {
                if (tag instanceof WrappedEntity) {
                    WrappedEntity wrapped = (WrappedEntity)tag;
                    if (BrooklynTaskTags.CONTEXT_ENTITY.equals(wrapped.getWrappingType())) {
                        context.add(wrapped.unwrap());
                    }
                }
            }
            assertEquals(context, expectedContext, "Found " + context + ", expected " + expectedContext);
        }
        
    }

}
