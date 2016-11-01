/*
 * Copyright 2016 The Apache Software Foundation.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.camp.brooklyn.dsl;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.util.concurrent.ListenableScheduledFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class DslTest extends BrooklynAppUnitTestSupport {

    private static final int MAX_PARALLEL_RESOLVERS = 50;
    private static final int MANY_RESOLVER_ITERATIONS = 100;
    
    private ListeningScheduledExecutorService executor;
    private Random random = new Random();
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newSingleThreadScheduledExecutor());
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            if (executor != null) executor.shutdownNow();
        } finally {
            super.tearDown();
        }
    }
    
    @Test
    public void testAttributeWhenReadyEmptyDoesNotBlock() throws Exception {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestApplication.MY_ATTRIBUTE.getName());
        Maybe<?> actualValue = execDslRealRealQuick(dsl, TestApplication.MY_ATTRIBUTE.getType(), app);
        assertTrue(actualValue.isAbsent());
    }

    @Test
    public void testAttributeWhenReadyEmptyImmediatelyDoesNotBlock() throws Exception {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestApplication.MY_ATTRIBUTE.getName());
        Maybe<?> actualValue = execDslImmediately(dsl, TestApplication.MY_ATTRIBUTE.getType(), app, true);
        assertTrue(actualValue.isAbsent());
    }

    @Test
    public void testAttributeWhenReady() throws Exception {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestEntity.NAME.getName());
        new AttributeWhenReadyTestWorker(app, TestEntity.NAME, dsl).run();
    }

    @Test
    public void testAttributeWhenReadyBlocksUntilReady() throws Exception {
        // Fewer iterations, because there is a sleep each time
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestEntity.NAME.getName());
        new AttributeWhenReadyTestWorker(app, TestEntity.NAME, dsl).satisfiedAsynchronously(true).resolverIterations(2).run();
    }

    @Test(groups="Integration")
    public void testAttributeWhenReadyConcurrent() throws Exception {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestEntity.NAME.getName());
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new AttributeWhenReadyTestWorker(app, TestEntity.NAME, dsl);
            }
        });
    }

    @Test
    public void testSelf() throws Exception {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.self();
        new SelfTestWorker(app, dsl).run();
    }

    @Test(groups="Integration")
    public void testSelfConcurrent() throws Exception {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.self();
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new SelfTestWorker(app, dsl);
            }
        });
    }

    @Test
    public void testParent() throws Exception {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.parent();
        new ParentTestWorker(app, dsl).run();
    }

    @Test(groups="Integration")
    public void testParentConcurrent() throws Exception {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.parent();
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new ParentTestWorker(app, dsl);
            }
        });
    }

    protected void runConcurrentWorker(Supplier<Runnable> taskSupplier) {
        Collection<Task<?>> results = new ArrayList<>();
        for (int i = 0; i < MAX_PARALLEL_RESOLVERS; i++) {
            Task<?> result = mgmt.getExecutionManager().submit(taskSupplier.get());
            results.add(result);
        }
        for (Task<?> result : results) {
            result.getUnchecked();
        }
    }
    
    private static class DslTestWorker implements Runnable {
        protected final TestApplication parent;
        protected final BrooklynDslDeferredSupplier<?> dsl;
        protected final Class<?> type;
        protected EntitySpec<TestEntity> childSpec = EntitySpec.create(TestEntity.class);
        protected int resolverIterations = MANY_RESOLVER_ITERATIONS;
        protected boolean satisfiedAsynchronously = false;
        private boolean wrapInTaskForImmediately = true;
        
        public DslTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl, Class<?> type) {
            this.parent = checkNotNull(parent, "parent");
            this.dsl = checkNotNull(dsl, "dsl");
            this.type = checkNotNull(type, "type");
        }

        public DslTestWorker resolverIterations(int val) {
            resolverIterations = val;
            return this;
        }
        
        public DslTestWorker satisfiedAsynchronously(boolean val) {
            satisfiedAsynchronously = val;
            return this;
        }
        
        public DslTestWorker wrapInTaskForImmediately(boolean val) {
            wrapInTaskForImmediately = val;
            return this;
        }
        
        @Override
        public void run() {
            try {
                TestEntity entity = parent.addChild(childSpec);
                for (int i = 0; i < resolverIterations; i++) {
                    // Call dsl.getImmediately()
                    preResolve(entity);
                    Maybe<?> immediateValue;
                    try {
                        immediateValue = execDslImmediately(dsl, type, entity, wrapInTaskForImmediately);
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                    postResolve(entity, immediateValue, true);
                    
                    // Call dsl.get()
                    preResolve(entity);
                    Maybe<?> eventualValue = execDslEventually(dsl, type, entity, Duration.ONE_MINUTE);
                    postResolve(entity, eventualValue, false);
                }
            } catch (Exception e) {
                Exceptions.propagate(e);
            }
        }

        protected void preResolve(TestEntity entity) throws Exception {
        }

        protected void postResolve(TestEntity entity, Maybe<?> actualValue, boolean isImmediate) throws Exception {
        }
    }

    private class AttributeWhenReadyTestWorker extends DslTestWorker {
        private AttributeSensor<String> sensor;
        private String expectedValue;
        private ListenableScheduledFuture<?> future;

        public AttributeWhenReadyTestWorker(TestApplication parent, AttributeSensor<String> sensor, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, sensor.getType());
            this.sensor = sensor;
        }

        @Override
        protected void preResolve(final TestEntity entity) {
            expectedValue = Identifiers.makeRandomId(10);
            Runnable job = new Runnable() {
                public void run() {
                    entity.sensors().set(sensor, expectedValue);
                }
            };
            if (satisfiedAsynchronously) {
                future = executor.schedule(job, random.nextInt(20), TimeUnit.MILLISECONDS);
            } else {
                job.run();
            }
        }

        @Override
        protected void postResolve(TestEntity entity, Maybe<?> actualValue, boolean isImmediate) throws Exception {
            if (satisfiedAsynchronously && isImmediate) {
                // We accept a maybe.absent if we called getImmediately when satisfiedAsynchronously
                assertTrue(actualValue.isAbsent() || expectedValue.equals(actualValue.get()), "actual="+actualValue+"; expected="+expectedValue);
            } else {
                assertEquals(actualValue.get(), expectedValue);
            }
            
            if (future != null) {
                future.get(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
                future = null;
            }
            // Reset sensor - otherwise if run in a loop the old value will be picked up, before our execute sets the new value
            entity.sensors().set(sensor, null);
        }

    }

    private static class SelfTestWorker extends DslTestWorker {
        public SelfTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, Entity.class);
        }

        @Override
        protected void preResolve(TestEntity entity) {
        }

        @Override
        protected void postResolve(TestEntity entity, Maybe<?> actualValue, boolean isImmediate) {
            assertEquals(actualValue.get(), entity);
        }

    }

    private static class ParentTestWorker extends DslTestWorker {
        public ParentTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, Entity.class);
        }

        @Override
        protected void postResolve(TestEntity entity, Maybe<?> actualValue, boolean isImmediate) {
            assertEquals(actualValue.get(), parent);
        }
    }

    static Maybe<?> execDslImmediately(final BrooklynDslDeferredSupplier<?> dsl, final Class<?> type, final Entity context, boolean execInTask) throws Exception {
        // Exec'ing immediately will call DSL in current thread. It needs to find the context entity,
        // and does this using BrooklynTaskTags.getTargetOrContextEntity(Tasks.current()).
        // If we are not in a task executed by the context entity, then this lookup will fail. 
        Callable<Maybe<?>> job = new Callable<Maybe<?>>() {
            public Maybe<?> call() throws Exception {
                return Tasks.resolving(dsl).as(type)
                        .context(context)
                        .description("Computing "+dsl)
                        .immediately(true)
                        .getMaybe();
            }
        };
        if (execInTask) {
            Task<Maybe<?>> task = ((EntityInternal)context).getExecutionContext().submit(job);
            task.get(Asserts.DEFAULT_LONG_TIMEOUT);
            assertTrue(task.isDone());
            return task.get();
            
        } else {
            return job.call();
        }
    }
    
    static Maybe<?> execDslRealRealQuick(BrooklynDslDeferredSupplier<?> dsl, Class<?> type, Entity context) {
        return execDslEventually(dsl, type, context, ValueResolver.REAL_REAL_QUICK_WAIT);
    }
    
    static Maybe<?> execDslEventually(BrooklynDslDeferredSupplier<?> dsl, Class<?> type, Entity context, Duration timeout) {
        return Tasks.resolving(dsl).as(type)
                .context(context)
                .description("Computing "+dsl)
                .timeout(timeout)
                .getMaybe();
    }
}
