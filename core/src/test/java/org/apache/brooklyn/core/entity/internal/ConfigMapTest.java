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
package org.apache.brooklyn.core.entity.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigMap;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey.IntegerAttributeSensorAndConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.core.task.BasicTask;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import groovy.lang.Closure;

public class ConfigMapTest extends BrooklynAppUnitTestSupport {

    private static final int TIMEOUT_MS = 10*1000;

    private MySubEntity entity;
    private ExecutorService executor;
    private ExecutionManager executionManager;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        entity = app.addChild(EntitySpec.create(MySubEntity.class));
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        executionManager = mgmt.getExecutionManager();
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (executor != null) executor.shutdownNow();
        super.tearDown();
    }

    @Test
    public void testGetConfigKeysReturnsFromSuperAndInterfacesAndSubClass() throws Exception {
        assertEquals(entity.getEntityType().getConfigKeys(), ImmutableSet.of(
                MySubEntity.SUPER_KEY_1, MySubEntity.SUPER_KEY_2, MySubEntity.SUB_KEY_2, MySubEntity.INTERFACE_KEY_1));
    }

    @Test
    public void testConfigKeyDefaultUsesValueInSubClass() throws Exception {
        assertEquals(entity.getConfig(MyBaseEntity.SUPER_KEY_1), "overridden superKey1 default");
    }

    @Test
    public void testConfigureFromKey() throws Exception {
        MySubEntity entity2 = app.addChild(EntitySpec.create(MySubEntity.class)
                .configure(MySubEntity.SUPER_KEY_1, "changed"));
        assertEquals(entity2.getConfig(MySubEntity.SUPER_KEY_1), "changed");
    }

    @Test
    public void testConfigureFromSuperKey() throws Exception {
        MySubEntity entity2 = app.addChild(EntitySpec.create(MySubEntity.class)
                .configure(MyBaseEntity.SUPER_KEY_1, "changed"));
        assertEquals(entity2.getConfig(MySubEntity.SUPER_KEY_1), "changed");
    }

    @Test
    public void testConfigSubMap() throws Exception {
        entity.config().set(MyBaseEntity.SUPER_KEY_1, "s1");
        entity.config().set(MySubEntity.SUB_KEY_2, "s2");
        ConfigMap sub = entity.getConfigMap().submap(ConfigPredicates.nameMatchesGlob("sup*"));
        Assert.assertEquals(sub.getConfigRaw(MyBaseEntity.SUPER_KEY_1, true).get(), "s1");
        Assert.assertFalse(sub.getConfigRaw(MySubEntity.SUB_KEY_2, true).isPresent());
    }

    @Test(expectedExceptions=IllegalArgumentException.class)
    public void testFailFastOnInvalidConfigKeyCoercion() throws Exception {
        MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class));
        ConfigKey<Integer> key = MyOtherEntity.INT_KEY;
        entity2.config().set((ConfigKey)key, "thisisnotanint");
    }

    @Test
    public void testGetConfigWithDeferredSupplierReturnsSupplied() throws Exception {
        DeferredSupplier<Integer> supplier = new DeferredSupplier<Integer>() {
            volatile int next = 0;
            public Integer get() {
                return next++;
            }
        };

        MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity2.config().set((ConfigKey)MyOtherEntity.INT_KEY, supplier);

        assertEquals(entity2.getConfig(MyOtherEntity.INT_KEY), Integer.valueOf(0));
        assertEquals(entity2.getConfig(MyOtherEntity.INT_KEY), Integer.valueOf(1));
    }

    @Test
    public void testGetConfigWithFutureWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Future<String> future = executor.submit(work);

        final MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity2.config().set((ConfigKey)MyOtherEntity.STRING_KEY, future);

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            public String call() {
                return entity2.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
    }

    @Test
    public void testGetConfigWithExecutedTaskWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Task<String> task = executionManager.submit(work);

        final MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class)
                .configure(MyOtherEntity.STRING_KEY, task));

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            public String call() {
                return entity2.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
        assertEquals(work.callCount.get(), 1);
    }

    @Test
    public void testGetConfigWithUnexecutedTaskIsExecutedAndWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Task<String> task = new BasicTask<String>(work);

        final MyOtherEntity entity2 = app.addChild(EntitySpec.create(MyOtherEntity.class)
                .configure(MyOtherEntity.STRING_KEY, task));

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            public String call() {
                return entity2.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
        assertEquals(work.callCount.get(), 1);
    }

    @ImplementedBy(MyBaseEntityImpl.class)
    public interface MyBaseEntity extends EntityInternal {
        public static final ConfigKey<String> SUPER_KEY_1 = ConfigKeys.newStringConfigKey("superKey1", "superKey1 key", "superKey1 default");
        public static final ConfigKey<String> SUPER_KEY_2 = ConfigKeys.newStringConfigKey("superKey2", "superKey2 key", "superKey2 default");
    }
    
    public static class MyBaseEntityImpl extends AbstractEntity implements MyBaseEntity {
    }

    @ImplementedBy(MySubEntityImpl.class)
    public interface MySubEntity extends MyBaseEntity, MyInterface {
        public static final ConfigKey<String> SUPER_KEY_1 = ConfigKeys.newConfigKeyWithDefault(MyBaseEntity.SUPER_KEY_1, "overridden superKey1 default");
        public static final ConfigKey<String> SUB_KEY_2 = ConfigKeys.newStringConfigKey("subKey2", "subKey2 key", "subKey2 default");
    }
    
    public static class MySubEntityImpl extends MyBaseEntityImpl implements MySubEntity {
    }

    public interface MyInterface {
        public static final ConfigKey<String> INTERFACE_KEY_1 = ConfigKeys.newStringConfigKey("interfaceKey1", "interface key 1", "interfaceKey1 default");
    }

    @ImplementedBy(MyOtherEntityImpl.class)
    public interface MyOtherEntity extends Entity {
        public static final ConfigKey<Integer> INT_KEY = ConfigKeys.newIntegerConfigKey("intKey", "int key", 1);
        public static final ConfigKey<String> STRING_KEY = ConfigKeys.newStringConfigKey("stringKey", "string key", null);
        public static final ConfigKey<Object> OBJECT_KEY = ConfigKeys.newConfigKey(Object.class, "objectKey", "object key", null);
        public static final ConfigKey<Closure> CLOSURE_KEY = ConfigKeys.newConfigKey(Closure.class, "closureKey", "closure key", null);
        public static final ConfigKey<Future> FUTURE_KEY = ConfigKeys.newConfigKey(Future.class, "futureKey", "future key", null);
        public static final ConfigKey<Task> TASK_KEY = ConfigKeys.newConfigKey(Task.class, "taskKey", "task key", null);
        public static final ConfigKey<Predicate> PREDICATE_KEY = ConfigKeys.newConfigKey(Predicate.class, "predicateKey", "predicate key", null);
        public static final IntegerAttributeSensorAndConfigKey SENSOR_AND_CONFIG_KEY = new IntegerAttributeSensorAndConfigKey("sensorConfigKey", "sensor+config key", 1);
    }
    
    public static class MyOtherEntityImpl extends AbstractEntity implements MyOtherEntity {
    }

    static class LatchingCallable<T> implements Callable<T> {
        final CountDownLatch latchCalled = new CountDownLatch(1);
        final CountDownLatch latchContinued = new CountDownLatch(1);
        final AtomicInteger callCount = new AtomicInteger(0);
        final T result;
        
        public LatchingCallable(T result) {
            this.result = result;
        }
        
        @Override
        public T call() throws Exception {
            callCount.incrementAndGet();
            latchCalled.countDown();
            latchContinued.await();
            return result;
        }
    }
}
