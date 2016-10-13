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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Collection;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;

public class DslTest extends BrooklynAppUnitTestSupport {

    private static final int MAX_PARALLEL_RESOLVERS = 50;
    private static final int RESOLVER_ITERATIONS = 1000;

    @Test
    public void testAttributeWhenReadyEmptyDoesNotBlock() {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestApplication.MY_ATTRIBUTE.getName());
        Maybe<? super String> actualValue = Tasks.resolving(dsl).as(TestEntity.NAME.getType())
                .context(app)
                .description("Computing sensor "+TestEntity.NAME+" from "+dsl)
                .timeout(ValueResolver.REAL_REAL_QUICK_WAIT)
                .getMaybe();
        assertTrue(actualValue.isAbsent());
    }

    @Test
    public void testAttributeWhenReady() {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestEntity.NAME.getName());
        new AttributeWhenReadyTestWorker(app, TestEntity.NAME, dsl).run();
    }

    @Test(groups="Integration")
    public void testAttributeWhenReadyConcurrent() {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.attributeWhenReady(TestEntity.NAME.getName());
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new AttributeWhenReadyTestWorker(app, TestEntity.NAME, dsl);
            }
        });
    }

    @Test
    public void testSelf() {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.self();
        new SelfTestWorker(app, dsl).run();
    }

    @Test(groups="Integration")
    public void testSelfConcurrent() {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.self();
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new SelfTestWorker(app, dsl);
            }
        });
    }

    @Test
    public void testParent() {
        BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.parent();
        new ParentTestWorker(app, dsl).run();
    }

    @Test(groups="Integration")
    public void testParentConcurrent() {
        final BrooklynDslDeferredSupplier<?> dsl = BrooklynDslCommon.parent();
        runConcurrentWorker(new Supplier<Runnable>() {
            @Override
            public Runnable get() {
                return new ParentTestWorker(app, dsl);
            }
        });
    }

    public void runConcurrentWorker(Supplier<Runnable> taskSupplier) {
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
        protected TestApplication parent;
        protected BrooklynDslDeferredSupplier<?> dsl;
        protected EntitySpec<TestEntity> childSpec = EntitySpec.create(TestEntity.class);
        protected Class<?> type;

        public DslTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl, Class<?> type) {
            this.parent = parent;
            this.dsl = dsl;
            this.type = type;
        }

        @Override
        public void run() {
            TestEntity entity = parent.createAndManageChild(childSpec);
            for (int i = 0; i < RESOLVER_ITERATIONS; i++) {
                preResolve(entity);
                Maybe<?> actualValue = Tasks.resolving(dsl).as(type)
                        .context(entity)
                        .description("Computing sensor "+type+" from "+dsl)
                        .timeout(Duration.ONE_MINUTE)
                        .getMaybe();
                postResolve(actualValue);
            }
        }

        protected void preResolve(TestEntity entity) {
        }

        protected void postResolve(Maybe<?> actualValue) {
        }
    }

    private static class AttributeWhenReadyTestWorker extends DslTestWorker {
        private AttributeSensor<String> sensor;
        private String expectedValue;

        public AttributeWhenReadyTestWorker(TestApplication parent, AttributeSensor<String> sensor, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, sensor.getType());
            this.sensor = sensor;
        }

        @Override
        protected void preResolve(TestEntity entity) {
            expectedValue = Identifiers.makeRandomId(10);
            entity.sensors().set(sensor, expectedValue);
        }


        @Override
        protected void postResolve(Maybe<?> actualValue) {
            assertEquals(actualValue.get(), expectedValue);
        }

    }

    private static class SelfTestWorker extends DslTestWorker {
        private TestEntity entity;

        public SelfTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, Entity.class);
        }

        @Override
        protected void preResolve(TestEntity entity) {
            this.entity = entity;
        }

        @Override
        protected void postResolve(Maybe<?> actualValue) {
            assertEquals(actualValue.get(), entity);
        }

    }

    private static class ParentTestWorker extends DslTestWorker {
        public ParentTestWorker(TestApplication parent, BrooklynDslDeferredSupplier<?> dsl) {
            super(parent, dsl, Entity.class);
        }

        @Override
        protected void postResolve(Maybe<?> actualValue) {
            assertEquals(actualValue.get(), parent);
        }
    }

}
