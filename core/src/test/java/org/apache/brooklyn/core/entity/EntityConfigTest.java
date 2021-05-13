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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.policy.basic.BasicPolicyTest.MyPolicy;
import org.apache.brooklyn.core.policy.basic.BasicPolicyTest.MyPolicyWithDuration;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.DurationPredicates;
import org.apache.brooklyn.util.time.Time;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeyDeprecationTest.MyBaseEntity;
import org.apache.brooklyn.core.config.ConfigKeyDeprecationTest.MyBaseEntityImpl;
import org.apache.brooklyn.core.config.ConfigKeyDeprecationTest.MySubEntity;
import org.apache.brooklyn.core.config.ConfigKeyDeprecationTest.MySubEntityImpl;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.ConfigPredicates;
import org.apache.brooklyn.core.config.MapConfigKey;
import org.apache.brooklyn.core.sensor.BasicAttributeSensorAndConfigKey.IntegerAttributeSensorAndConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.task.BasicTask;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.InterruptingImmediateSupplier;
import org.apache.brooklyn.util.core.task.TaskBuilder;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.Tasks.ForTestingAndLegacyCompatibilityOnly.LegacyDeepResolutionMode;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.MoreExecutors;

import groovy.lang.Closure;

public class EntityConfigTest extends BrooklynAppUnitTestSupport {

    private static final Logger log = LoggerFactory.getLogger(EntityConfigTest.class);
    
    private static final int TIMEOUT_MS = 10*1000;

    private ExecutorService executor;
    private ExecutionManager executionManager;
    private LegacyDeepResolutionMode legacyDeepResolutionModeOriginal;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        executionManager = mgmt.getExecutionManager();

        legacyDeepResolutionModeOriginal = Tasks.ForTestingAndLegacyCompatibilityOnly.LEGACY_DEEP_RESOLUTION_MODE;
        Tasks.ForTestingAndLegacyCompatibilityOnly.LEGACY_DEEP_RESOLUTION_MODE = LegacyDeepResolutionMode.DISALLOW_LEGACY;
    }

    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        Tasks.ForTestingAndLegacyCompatibilityOnly.LEGACY_DEEP_RESOLUTION_MODE = legacyDeepResolutionModeOriginal;
        if (executor != null) executor.shutdownNow();
        super.tearDown();
    }

    @Test
    public void testConfigBagContainsMatchesForConfigKeyName() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("myentity.myconfig", "myval1")
                .configure("myentity.myconfigwithflagname", "myval2"));
        
        assertEquals(entity.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyEntity.MY_CONFIG, "myval1", MyEntity.MY_CONFIG_WITH_FLAGNAME, "myval2"));
        assertEquals(entity.config().getBag().getAllConfig(), ImmutableMap.of("myentity.myconfig", "myval1", "myentity.myconfigwithflagname", "myval2"));
        assertEquals(entity.config().getLocalBag().getAllConfig(), ImmutableMap.of("myentity.myconfig", "myval1", "myentity.myconfigwithflagname", "myval2"));
    }

    @Test
    public void testConfigBagContainsMatchesForFlagName() throws Exception {
        // Prefers flag-name, over config-key's name
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("myconfigflagname", "myval"));
        
        assertEquals(entity.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyEntity.MY_CONFIG_WITH_FLAGNAME, "myval"));
        assertEquals(entity.config().getBag().getAllConfig(), ImmutableMap.of("myentity.myconfigwithflagname", "myval"));
        assertEquals(entity.config().getLocalBag().getAllConfig(), ImmutableMap.of("myentity.myconfigwithflagname", "myval"));
    }

    // config key name takes priority
    @Test(enabled=false)
    public void testPrefersFlagNameOverConfigKeyName() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("myconfigflagname", "myval")
                .configure("myentity.myconfigwithflagname", "shouldIgnoreAndPreferFlagName"));
        
        assertEquals(entity.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyEntity.MY_CONFIG_WITH_FLAGNAME, "myval"));
    }

    @Test
    public void testConfigBagContainsUnmatched() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("notThere", "notThereVal"));
        
        assertEquals(entity.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(ConfigKeys.newConfigKey(Object.class, "notThere"), "notThereVal"));
        assertEquals(entity.config().getBag().getAllConfig(), ImmutableMap.of("notThere", "notThereVal"));
        assertEquals(entity.config().getLocalBag().getAllConfig(), ImmutableMap.of("notThere", "notThereVal"));
    }
    
    @Test
    public void testChildConfigBagInheritsUnmatchedAtParent() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("mychildentity.myconfig", "myval1")
                .configure("mychildconfigflagname", "myval2")
                .configure("notThere", "notThereVal"));

        EntityInternal child = mgmt.getEntityManager().createEntity(EntitySpec.create(MyChildEntity.class)
                .parent(entity));

        assertEquals(child.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyChildEntity.MY_CHILD_CONFIG, "myval1", 
            ConfigKeys.newConfigKey(Object.class, "mychildconfigflagname"), "myval2",
            ConfigKeys.newConfigKey(Object.class, "notThere"), "notThereVal"));
        assertEquals(child.config().getBag().getAllConfig(), ImmutableMap.of("mychildentity.myconfig", "myval1", "mychildconfigflagname", "myval2", "notThere", "notThereVal"));
        assertEquals(child.config().getLocalBag().getAllConfig(), ImmutableMap.of());
    }
    
    @Test
    public void testChildInheritsFromParent() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("myentity.myconfig", "myval1"));

        EntityInternal child = mgmt.getEntityManager().createEntity(EntitySpec.create(MyChildEntity.class)
                .parent(entity));

        assertEquals(child.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyEntity.MY_CONFIG, "myval1"));
        assertEquals(child.config().getBag().getAllConfig(), ImmutableMap.of("myentity.myconfig", "myval1"));
        assertEquals(child.config().getLocalBag().getAllConfig(), ImmutableMap.of());
    }
    
    @Test
    public void testChildCanOverrideConfigUsingKeyName() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure("mychildentity.myconfigwithflagname", "myval")
                .configure("notThere", "notThereVal"));

        EntityInternal child = mgmt.getEntityManager().createEntity(EntitySpec.create(MyChildEntity.class)
                .parent(entity)
                .configure("mychildentity.myconfigwithflagname", "overrideMyval")
                .configure("notThere", "overrideNotThereVal"));

        assertEquals(child.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyChildEntity.MY_CHILD_CONFIG_WITH_FLAGNAME, "overrideMyval",
            ConfigKeys.newConfigKey(Object.class, "notThere"), "overrideNotThereVal"));
        assertEquals(child.config().getBag().getAllConfig(), ImmutableMap.of("mychildentity.myconfigwithflagname", "overrideMyval", "notThere", "overrideNotThereVal"));
        assertEquals(child.config().getLocalBag().getAllConfig(), ImmutableMap.of("mychildentity.myconfigwithflagname", "overrideMyval", "notThere", "overrideNotThereVal"));
    }
    
    @Test
    public void testChildCanOverrideConfigUsingFlagName() throws Exception {
        EntityInternal entity = mgmt.getEntityManager().createEntity(EntitySpec.create(MyEntity.class)
                .configure(MyChildEntity.MY_CHILD_CONFIG_WITH_FLAGNAME, "myval"));
        assertEquals(entity.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyChildEntity.MY_CHILD_CONFIG_WITH_FLAGNAME, "myval"));

        EntityInternal child = mgmt.getEntityManager().createEntity(EntitySpec.create(MyChildEntity.class)
                .parent(entity)
                .configure("mychildconfigflagname", "overrideMyval"));

        assertEquals(child.config().getBag().getAllConfigAsConfigKeyMap(), ImmutableMap.of(MyChildEntity.MY_CHILD_CONFIG_WITH_FLAGNAME, "overrideMyval"));
        assertEquals(child.config().getBag().getAllConfig(), ImmutableMap.of("mychildentity.myconfigwithflagname", "overrideMyval"));
        assertEquals(child.config().getLocalBag().getAllConfig(), ImmutableMap.of("mychildentity.myconfigwithflagname", "overrideMyval"));
    }

    @Test
    public void testGetConfigMapWithSubKeys() throws Exception {
        TestEntity entity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.subKey("mysub"), "myval"));
        
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", "myval"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING).get(), ImmutableMap.of("mysub", "myval"));
        
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING.subKey("mysub")), "myval");
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING.subKey("mysub")).get(), "myval");
    }
    
    @Test
    public void testGetConfigMapWithExplicitMap() throws Exception {
        TestEntity entity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING, ImmutableMap.of("mysub", "myval")));
        
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", "myval"));
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING).get(), ImmutableMap.of("mysub", "myval"));
        
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING.subKey("mysub")), "myval");
        assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_THING.subKey("mysub")).get(), "myval");
    }
    
    @Test
    public void testGetConfigMapStringCoercionFromString() throws Exception {
        TestEntity entity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.getName(), "{mysub: myval}"));
        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", "myval"));
    }

    @Test
    public void testGetConfigMapStringCoercionFromStringSubtypeIgnoredForTypedConfigSet() throws Exception {
        TestEntity entity = Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY,
                () -> (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                        .configure( (MapConfigKey) TestEntity.CONF_MAP_THING, "{mysub: {sub2: 4}}")));

        assertEquals(entity.config().getLocalRaw(TestEntity.CONF_MAP_THING).get(), ImmutableMap.of("mysub", ImmutableMap.of("sub2", 4)));

        assertEquals(entity.config().getLocalRaw(TestEntity.CONF_MAP_THING.subKey("mysub")).get(), ImmutableMap.of("sub2", 4));        // legacy code
        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY, () ->
                assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", ImmutableMap.of("sub2", 4))));
        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.DISALLOW_LEGACY, ()->
                Asserts.assertFailsWith(
                        () -> entity.config().get(TestEntity.CONF_MAP_THING),
                        e -> Asserts.expectedFailureContainsIgnoreCase(e,
                                // exception used to come on coercion of map result, but now BasicConfigKey.resolve also does some coercion
                                "confMapThing",
                                "Cannot coerce", "map to java.lang.String",
                                "{sub2=4}")
                ));
    }

    @Test
    public void testGetConfigMapStringCoercionFromStringSubtypeIgnoredForAnonymousConfigSet() throws Exception {
        // behaviour no different to above
        TestEntity entity = Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY,
                () -> (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                        .configure( TestEntity.CONF_MAP_THING.getName(), "{mysub: {sub2: 4}}")));

        assertEquals(entity.config().getLocalRaw(TestEntity.CONF_MAP_THING).get(), ImmutableMap.of("mysub", ImmutableMap.of("sub2", 4)));

        assertEquals(entity.config().getLocalRaw(TestEntity.CONF_MAP_THING.subKey("mysub")).get(), ImmutableMap.of("sub2", 4));        // legacy code
        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY, () ->
                assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", ImmutableMap.of("sub2", 4))));
        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.DISALLOW_LEGACY, ()->
                Asserts.assertFailsWith(
                        () -> entity.config().get(TestEntity.CONF_MAP_THING),
                        e -> Asserts.expectedFailureContainsIgnoreCase(e,
                                "confMapThing",
                                "Cannot coerce", "map to java.lang.String",
                                "{sub2=4}")
                ));
    }

    @Test
    public void testGetConfigMapStringNoCoercionIfSubtypeMatches() throws Exception {
        String v = "{a: b}";
        TestEntity entity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING, ImmutableMap.of("mysub", v)));

        assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", v));
        
        TestEntity entity2 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.subKey("mysub"), v));
        assertEquals(entity2.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", v));
        
        TestEntity entity3 = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.getName(), ImmutableMap.of("mysub", v)));
        assertEquals(entity3.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", v));
    }

    @Test
    public void testGetConfigMapStringNoCoercionIfSubtypeDoesntMatchExceptWhenSettingSubtypeInLegacyDeepResolutionMode() throws Exception {
        ImmutableMap<String, String> v = ImmutableMap.of("a", "b");
        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY, () -> {
                    TestEntity entity = (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                            .configure((MapConfigKey) TestEntity.CONF_MAP_THING, ImmutableMap.of("mysub", v)));

                    assertEquals(entity.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", v));
                });

        // this fails even in legacy mode
        Asserts.assertFailsWith(() -> Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY,
                    () -> mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                            .configure((ConfigKey) TestEntity.CONF_MAP_THING.subKey("mysub"), v))),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "cannot coerce", "confMapThing.mysub", "String"));

        Tasks.ForTestingAndLegacyCompatibilityOnly.withLegacyDeepResolutionMode(LegacyDeepResolutionMode.ALLOW_LEGACY, () -> {
            TestEntity entity3 = (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                .configure(TestEntity.CONF_MAP_THING.getName(), ImmutableMap.of("mysub", v)));
            assertEquals(entity3.config().get(TestEntity.CONF_MAP_THING), ImmutableMap.of("mysub", v));
        });
    }

    // TODO This now fails because the task has been cancelled, in entity.config().get().
    // But it used to pass (e.g. with commit 56fcc1632ea4f5ac7f4136a7e04fabf501337540).
    // It failed after the rename of CONF_MAP_THING_OBJ to CONF_MAP_OBJ_THING, which 
    // suggests there was an underlying problem that was masked by the unfortunate naming
    // of the previous "test.confMapThing.obj".
    //
    // Presumably an earlier call to task.get() timed out, causing it to cancel the task?
    // Alex: yes, a task.cancel is performed for maps in
    // AbstractEntity$BasicConfigurationSupport(AbstractConfigurationSupportInternal).getNonBlockingResolvingStructuredKey(ConfigKey<T>)    
 
    //
    // I (Aled) question whether we want to support passing a task (rather than a 
    // DeferredSupplier or TaskFactory, for example). Our EntitySpec.configure is overloaded
    // to take a Task, but that feels wrong!?
    //
    // If starting clean I (Alex) would agree, we should use TaskFactory. However the
    // DependentConfiguration methods -- including the ubiquitous AttributeWhenReady --
    // return Task instances so they should survive a getNonBlocking or get with a short timeout 
    // access, and if a value is subsequently available it should be returned 
    // (which this test asserts, but is currently failing). If TaskFactory is used the
    // intended semantics are clear -- you create a new task on each access, and can interrupt it
    // and discard it if needed. For a Task it's less clear: probably the semantics are that the
    // first returned value is what the value is forevermore. Probably it should not be interrupted
    // on a non-blocking / short-wait access, or possibly it should simply be re-run if a previous
    // execution was interrupted (but take care if we have a simultaneous non-blocking and blocking
    // access, if the first one interrupts the second one should still get a value).
    // I tend to think ideally we should switch to using TaskFactory in DependentConfiguration.
    class ConfigNonBlockingFixture {
        final Semaphore latch = new Semaphore(0);
        final String expectedVal = "myval";
        Object blockingVal;
        List<Task<String>> tasksMadeByFactory = MutableList.of();

        protected ConfigNonBlockingFixture usingTask() {
            blockingVal = taskFactory().newTask();
            return this;
        }

        protected ConfigNonBlockingFixture usingTaskFactory() {
            blockingVal = taskFactory();
            return this;
        }

        protected ConfigNonBlockingFixture usingDeferredSupplier() {
            blockingVal = deferredSupplier();
            return this;
        }
        
        protected ConfigNonBlockingFixture usingInterruptingImmediateSupplier() {
            blockingVal = new InterruptingImmediateSupplier<String>(deferredSupplier());
            return this;
        }

        protected ConfigNonBlockingFixture usingImmediateSupplierNoSleep() {
            blockingVal = immediateSupplier(false);
            return this;
        }

        protected ConfigNonBlockingFixture usingImmediateSupplierWithSleep() {
            blockingVal = immediateSupplier(true);
            return this;
        }

        private TaskFactory<Task<String>> taskFactory() {
            final TaskBuilder<String> tb = Tasks.<String>builder().body(
                new Callable<String>() {
                    @Override
                    public String call() throws Exception {
                        if (!latch.tryAcquire()) latch.acquire();
                        latch.release();
                        return "myval";
                    }});
            return new TaskFactory<Task<String>>() {
                @Override
                public Task<String> newTask() {
                    Task<String> t = tb.build();
                    tasksMadeByFactory.add(t);
                    return t;
                }
            };
        }
        
        private DeferredSupplier<String> deferredSupplier() {
            return new DeferredSupplier<String>() {
                @Override public String get() {
                    try {
                        log.trace("acquiring");
                        if (!latch.tryAcquire()) latch.acquire();
                        latch.release();
                        log.trace("acquired and released");
                    } catch (InterruptedException e) {
                        log.trace("interrupted");
                        throw Exceptions.propagate(e);
                    }
                    return "myval";
                }
            };
        }
        
        private DeferredSupplier<String> immediateSupplier(final boolean withSleep) {
            class DeferredImmediateSupplier implements DeferredSupplier<String>, ImmediateSupplier<String> {
                @Override @JsonIgnore
                public Maybe<String> getImmediately() {
                    try {
                        sleepIfNeeded();
                        log.trace("acquiring");
                        if (latch.tryAcquire()) {
                            latch.release();
                            return Maybe.of("myval");
                        } else {
                            return Maybe.absent(new ImmediateSupplier.ImmediateValueNotAvailableException());
                        }
                    } catch (InterruptedException e) {
                        log.trace("interrupted");
                        throw Exceptions.propagate(e);
                    }
                }
                @Override public String get() {
                    try {
                        sleepIfNeeded();
                        log.trace("acquiring");
                        if (!latch.tryAcquire()) latch.acquire();
                        latch.release();
                        log.trace("acquired and released");
                    } catch (InterruptedException e) {
                        log.trace("interrupted");
                        throw Exceptions.propagate(e);
                    }
                    return "myval";
                }
                private void sleepIfNeeded() throws InterruptedException {
                    if (withSleep) {
                        try {
                            // Do a blocking operation (which would cause interrupt, if called in 
                            // InterruptingImmediateSupplier). This is to simulate use-cases like
                            // use of `$brooklyn:external("vault", "aws.secretKey")`, which is not
                            // blocked by other Brooklyn tasks, but will do IO operations.
                            Thread.sleep(1);
                        } catch (InterruptedException e) {
                            log.debug("Sleep was interrupted during eval (expected in many cases)", e);
                            throw Exceptions.propagate(e);
                        }
                    }
                }
            }
            return new DeferredImmediateSupplier();
        }
        

        protected void runGetConfigNonBlockingInKey() throws Exception {
            Preconditions.checkNotNull(blockingVal, "Fixture must set blocking val before running this");
            
            @SuppressWarnings("unchecked")
            TestEntity entity = (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                    .configure((ConfigKey<Object>)(ConfigKey<?>)TestEntity.CONF_NAME, blockingVal));
            
            log.trace("get non-blocking");
            // Will initially return absent, because task is not done
            assertTrue(entity.config().getNonBlocking(TestEntity.CONF_NAME).isAbsent());
            log.trace("got absent");
            
            latch.release();
            
            // Can now finish task, so will return expectedVal
            log.trace("get blocking");
            assertEquals(entity.config().get(TestEntity.CONF_NAME), expectedVal);
            log.trace("got blocking");
            assertEquals(entity.config().getNonBlocking(TestEntity.CONF_NAME).get(), expectedVal);
            
            latch.acquire();
            log.trace("finished");
        }
        
        protected void runGetConfigNonBlockingInMap() throws Exception {
            Preconditions.checkNotNull(blockingVal, "Fixture must set blocking val before running this");
            TestEntity entity = (TestEntity) mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class)
                    .configure(TestEntity.CONF_MAP_OBJ_THING, ImmutableMap.<String, Object>of("mysub", blockingVal)));
            
            // Will initially return absent, because task is not done
            assertTrue(entity.config().getNonBlocking(TestEntity.CONF_MAP_OBJ_THING).isAbsent());
            assertTrue(entity.config().getNonBlocking(TestEntity.CONF_MAP_OBJ_THING.subKey("mysub")).isAbsent());

            if (blockingVal instanceof TaskFactory) {
                assertAllOurConfigTasksCancelled();
            } else {
                // TaskFactory tasks are cancelled, but others are not,
                // things (ValueResolver.getMaybeInternal()) are smart enough to know to leave it running
                assertAllOurConfigTasksNotCancelled();
            }
            
            latch.release();
            
            // Can now finish task, so will return expectedVal
            assertEquals(entity.config().get(TestEntity.CONF_MAP_OBJ_THING), ImmutableMap.of("mysub", expectedVal));
            assertEquals(entity.config().get(TestEntity.CONF_MAP_OBJ_THING.subKey("mysub")), expectedVal);
            
            assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_OBJ_THING).get(), ImmutableMap.of("mysub", expectedVal));
            assertEquals(entity.config().getNonBlocking(TestEntity.CONF_MAP_OBJ_THING.subKey("mysub")).get(), expectedVal);
            
            assertAllTasksDone();
        }

        private void assertAllOurConfigTasksNotCancelled() {
            for (Task<?> t: tasksMadeByFactory) {
                Assert.assertFalse( t.isCancelled(), "Task should not have been cancelled: "+t+" - "+t.getStatusDetail(false) );
            }
        }
        
        private void assertAllOurConfigTasksCancelled() {
            // TODO added Feb 2017 - but might need an "eventually" here, if cancel is happening in a BG thread
            // (but I think it is always foreground)
            for (Task<?> t: tasksMadeByFactory) {
                Assert.assertTrue( t.isCancelled(), "Task should have been cancelled: "+t+" - "+t.getStatusDetail(false) );
            }
        }
        
        private void assertAllTasksDone() {
            for (Task<?> t: tasksMadeByFactory) {
                Assert.assertTrue( t.isDone(), "Task should have been done: "+t+" - "+t.getStatusDetail(false) );
            }
        }
    }
    
    @Test
    public void testGetTaskNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingTask().runGetConfigNonBlockingInKey(); 
    }
    @Test
    public void testGetTaskNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingTask().runGetConfigNonBlockingInMap(); 
    }
    
    @Test
    public void testGetTaskFactoryNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingTaskFactory().runGetConfigNonBlockingInKey();
    }
    @Test
    public void testGetTaskFactoryNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingTaskFactory().runGetConfigNonBlockingInMap(); 
    }
    
    @Test
    public void testGetSupplierNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingDeferredSupplier().runGetConfigNonBlockingInKey(); 
    }
    @Test
    public void testGetSupplierNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingDeferredSupplier().runGetConfigNonBlockingInMap(); 
    }
    
    @Test 
    public void testGetInterruptingImmediateSupplierNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingInterruptingImmediateSupplier().runGetConfigNonBlockingInKey(); 
    }
    @Test
    public void testGetInterruptingImmediateSupplierNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingInterruptingImmediateSupplier().runGetConfigNonBlockingInMap(); 
    }
    
    @Test 
    public void testGetImmediateSupplierNoSleepNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingImmediateSupplierNoSleep().runGetConfigNonBlockingInKey(); 
    }
    @Test
    public void testGetImmediateSupplierNoSleepNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingImmediateSupplierNoSleep().runGetConfigNonBlockingInMap(); 
    }
    @Test 
    public void testGetImmediateSupplierWithSleepNonBlockingKey() throws Exception {
        new ConfigNonBlockingFixture().usingImmediateSupplierWithSleep().runGetConfigNonBlockingInKey(); 
    }
    @Test
    public void testGetImmediateSupplierWithSleepNonBlockingMap() throws Exception {
        new ConfigNonBlockingFixture().usingImmediateSupplierWithSleep().runGetConfigNonBlockingInMap(); 
    }
    
    @Test
    public void testGetConfigKeysReturnsFromSuperAndInterfacesAndSubClass() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class));
        assertEquals(entity.getEntityType().getConfigKeys(), ImmutableSet.of(
                MySubEntity.SUPER_KEY_1, MySubEntity.SUPER_KEY_2, MySubEntity.SUB_KEY_2, MySubEntity.INTERFACE_KEY_1, AbstractEntity.DEFAULT_DISPLAY_NAME));
    }

    @Test
    public void testConfigKeyDefaultUsesValueInSubClass() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class));
        assertEquals(entity.getConfig(MyBaseEntity.SUPER_KEY_1), "overridden superKey1 default");
    }

    @Test
    public void testConfigureFromKey() throws Exception {
        MyBaseEntity entity = app.addChild(EntitySpec.create(MyBaseEntity.class)
                .configure(MyBaseEntity.SUPER_KEY_1, "changed"));
        assertEquals(entity.getConfig(MyBaseEntity.SUPER_KEY_1), "changed");
    }

    @Test
    public void testConfigureFromOverriddenKey() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class)
                .configure(MySubEntity.SUPER_KEY_1, "changed"));
        assertEquals(entity.getConfig(MySubEntity.SUPER_KEY_1), "changed");
    }

    @Test
    public void testConfigureFromSuperKey() throws Exception {
        // use parent key to set
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class)
                .configure(MyBaseEntity.SUPER_KEY_1, "changed"));
        assertEquals(entity.getConfig(MySubEntity.SUPER_KEY_1), "changed");
    }

    @Test
    public void testConfigureFromStringGetByKey() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class)
            .configure(MySubEntity.SUPER_KEY_1.getName(), "changed"));
        assertEquals(entity.getConfig(MySubEntity.SUPER_KEY_1), "changed");
    }
    
    @Test
    public void testConfigureFromStringGetByExternalKey() throws Exception {
        // config key is not present on the entity
        MyBaseEntity entity = app.addChild(EntitySpec.create(MyBaseEntity.class)
            .configure(MySubEntity.SUB_KEY_2.getName(), "changed"));
        assertEquals(entity.getConfig(MySubEntity.SUB_KEY_2), "changed");
        assertEquals(entity.getConfig(ConfigKeys.newConfigKey(Object.class, MySubEntity.SUB_KEY_2.getName())), "changed");
    }
    
    @Test
    public void testConfigureFromStringGetBySuperKey() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class)
            .configure(MySubEntity.SUPER_KEY_2.getName(), "changed"));
        assertEquals(entity.getConfig(MySubEntity.SUPER_KEY_2), "changed");
    }
    
    @Test
    public void testConfigFilterPresent() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class));
        entity.config().set(MyBaseEntity.SUPER_KEY_1, "s1");
        entity.config().set(MySubEntity.SUB_KEY_2, "s2");
        Set<ConfigKey<?>> filteredKeys = entity.config().getInternalConfigMap().findKeysPresent(ConfigPredicates.nameMatchesGlob("sup*"));
        Assert.assertTrue(filteredKeys.contains(MyBaseEntity.SUPER_KEY_1));
        Assert.assertFalse(filteredKeys.contains(MySubEntity.SUB_KEY_2));
        Assert.assertFalse(filteredKeys.contains(MyBaseEntity.SUPER_KEY_2));
        Asserts.assertSize(filteredKeys, 1);
    }

    @Test
    public void testConfigFilterDeclared() throws Exception {
        MySubEntity entity = app.addChild(EntitySpec.create(MySubEntity.class));
        Set<ConfigKey<?>> filteredKeys = entity.config().getInternalConfigMap().findKeysDeclared(ConfigPredicates.nameMatchesGlob("sup*"));
        Assert.assertTrue(filteredKeys.contains(MyBaseEntity.SUPER_KEY_1), "keys are: "+filteredKeys);
        Assert.assertTrue(filteredKeys.contains(MyBaseEntity.SUPER_KEY_2));
        Assert.assertFalse(filteredKeys.contains(MySubEntity.SUB_KEY_2));
        Asserts.assertSize(filteredKeys, 2);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testFailFastOnInvalidConfigKeyCoercion() throws Exception {
        MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class));
        ConfigKey<Integer> key = MyOtherEntity.INT_KEY;
        Asserts.assertFailsWith(() -> {
            entity.config().set((ConfigKey) key, "thisisnotanint");
        }, e -> {
            Asserts.assertStringContainsIgnoreCase(e.toString(), "thisisnotanint", "integer", "intkey");
            return true;
        });
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testGetConfigWithDeferredSupplierReturnsSupplied() throws Exception {
        DeferredSupplier<Integer> supplier = new DeferredSupplier<Integer>() {
            volatile int next = 0;
            @Override
            public Integer get() {
                return next++;
            }
        };

        MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity.config().set((ConfigKey)MyOtherEntity.INT_KEY, supplier);

        assertEquals(entity.getConfig(MyOtherEntity.INT_KEY), Integer.valueOf(0));
        assertEquals(entity.getConfig(MyOtherEntity.INT_KEY), Integer.valueOf(1));
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testGetConfigWithFutureWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Future<String> future = executor.submit(work);

        final MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class));
        entity.config().set((ConfigKey)MyOtherEntity.STRING_KEY, future);

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            @Override
            public String call() {
                return entity.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
    }

    @Test(groups="Integration")  // takes 0.5s
    public void testGetConfigWithExecutedTaskWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Task<String> task = executionManager.submit(work);

        final MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class)
                .configure(MyOtherEntity.STRING_KEY, task));

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            @Override
            public String call() {
                return entity.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
        assertEquals(work.callCount.get(), 1);
    }

    @Test(groups="Integration")  // takes 0.5s
    public void testGetConfigWithUnexecutedTaskIsExecutedAndWaitsForResult() throws Exception {
        LatchingCallable<String> work = new LatchingCallable<String>("abc");
        Task<String> task = new BasicTask<String>(work);

        final MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class)
                .configure(MyOtherEntity.STRING_KEY, task));

        Future<String> getConfigFuture = executor.submit(new Callable<String>() {
            @Override
            public String call() {
                return entity.getConfig(MyOtherEntity.STRING_KEY);
            }});

        assertTrue(work.latchCalled.await(TIMEOUT_MS, TimeUnit.MILLISECONDS));
        assertFalse(getConfigFuture.isDone());

        work.latchContinued.countDown();
        assertEquals(getConfigFuture.get(TIMEOUT_MS, TimeUnit.MILLISECONDS), "abc");
        assertEquals(work.callCount.get(), 1);
    }
    
    @Test
    public void testGetConfigRunsInSameThread() throws Exception {
        Callable<String> job = () -> ""+Thread.currentThread().getId();

        final MyOtherEntity entity = app.addChild(EntitySpec.create(MyOtherEntity.class));
        assertEquals( ((EntityInternal)entity).getExecutionContext().get(new BasicTask<>(job)), job.call() );
        
        // and config also runs in same thread (after validation)
        entity.config().set(MyOtherEntity.STRING_KEY, new BasicTask<>(job));
        assertEquals(entity.config().get(MyOtherEntity.STRING_KEY), job.call());
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
    @SuppressWarnings("rawtypes")
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

    public static class MyEntity extends AbstractEntity {
        public static final ConfigKey<String> MY_CONFIG = ConfigKeys.newStringConfigKey("myentity.myconfig");

        @SetFromFlag("myconfigflagname")
        public static final ConfigKey<String> MY_CONFIG_WITH_FLAGNAME = ConfigKeys.newStringConfigKey("myentity.myconfigwithflagname");
        
        @Override
        public void init() {
            super.init();
            
            // Just calling this to prove we can! When config() was changed to return BasicConfigurationSupport,
            // it broke because BasicConfigurationSupport was private.
            config().getLocalBag();
        }
    }
    
    public static class MyChildEntity extends AbstractEntity {
        public static final ConfigKey<String> MY_CHILD_CONFIG = ConfigKeys.newStringConfigKey("mychildentity.myconfig");

        @SetFromFlag("mychildconfigflagname")
        public static final ConfigKey<String> MY_CHILD_CONFIG_WITH_FLAGNAME = ConfigKeys.newStringConfigKey("mychildentity.myconfigwithflagname");
    }
    
    @Test
    public void testInheritedDefault() {
        final Entity e1 = app.addChild(EntitySpec.create(MyBaseEntity.class));
        final Entity e2 = e1.addChild(EntitySpec.create(BasicEntity.class));
        Assert.assertEquals(e2.config().get(MyBaseEntity.SUPER_KEY_1), MyBaseEntity.SUPER_KEY_1.getDefaultValue());
        Assert.assertEquals(e2.config().get(ConfigKeys.newStringConfigKey(MyBaseEntity.SUPER_KEY_1.getName())), MyBaseEntity.SUPER_KEY_1.getDefaultValue());
    }

    @ImplementedBy(MyEntityWithDurationImpl.class)
    public interface MyEntityWithDuration extends EntityInternal {
        public static final ConfigKey<Duration> DURATION_POSITIVE = ConfigKeys.builder(Duration.class, "duration").constraint(DurationPredicates.positive()).build();
    }

    public static class MyEntityWithDurationImpl extends AbstractEntity implements MyEntityWithDuration {
    }

    @Test
    public void testDurationConstraintValidAddSpec() throws Exception {
        MyEntityWithDuration child = app.addChild(EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE, Duration.ONE_MINUTE));

        assertEquals(child.getConfig(MyPolicyWithDuration.DURATION_POSITIVE), Duration.ONE_MINUTE);
    }

    @Test
    public void testDurationConstraintInvalidAddSpec() throws Exception {
        EntitySpec<MyEntityWithDuration> spec = EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE, Duration.minutes(-42));

        Asserts.assertFailsWith(() -> app.addChild(spec),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidCreation() throws Exception {
        EntitySpec<TestApplication> app2 = EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(MyEntityWithDuration.class)
                        .configure(MyEntityWithDuration.DURATION_POSITIVE, Duration.minutes(-42)));

        Asserts.assertFailsWith(() -> mgmt.getEntityManager().createEntity(app2),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidAddSpecByName() throws Exception {
        EntitySpec<MyEntityWithDuration> spec = EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), Duration.minutes(-42));

        Asserts.assertFailsWith(() -> app.addChild(spec),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidCreationByName() throws Exception {
        EntitySpec<TestApplication> app2 = EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(MyEntityWithDuration.class)
                        .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), Duration.minutes(-42)));

        Asserts.assertFailsWith(() -> mgmt.getEntityManager().createEntity(app2),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }


    @Test
    public void testDurationConstraintValidAddSpecByNameFromStringCoerced() throws Exception {
        MyEntityWithDuration child = app.addChild(EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), "1m"));

        assertEquals(child.getConfig(MyPolicyWithDuration.DURATION_POSITIVE), Duration.ONE_MINUTE);
    }

    @Test
    public void testDurationConstraintInvalidAddSpecByNameFromStringCoerced() throws Exception {
        EntitySpec<MyEntityWithDuration> spec = EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), "-42m");

        Asserts.assertFailsWith(() -> app.addChild(spec),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidCreationFromStringCoerced() throws Exception {
        EntitySpec<TestApplication> app2 = EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(MyEntityWithDuration.class)
                        .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), "-42m"));

        Asserts.assertFailsWith(() -> mgmt.getEntityManager().createEntity(app2),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidSetLiveTyped() throws Exception {
        EntitySpec<TestApplication> app2 = EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(MyEntityWithDuration.class)
                        .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), "1m"));
        TestApplication ent = mgmt.getEntityManager().createEntity(app2);
        Asserts.assertFailsWith(() -> ent.config().set(MyPolicyWithDuration.DURATION_POSITIVE, Duration.minutes(-42)),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidSetLiveFromStringCoerced() throws Exception {
        EntitySpec<TestApplication> app2 = EntitySpec.create(TestApplication.class)
                .child(EntitySpec.create(MyEntityWithDuration.class)
                        .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), "1m"));
        Entity ent = Iterables.getOnlyElement( mgmt.getEntityManager().createEntity(app2).getChildren() );

        ConfigKey<Object> objKey = ConfigKeys.newConfigKey(Object.class, MyEntityWithDuration.DURATION_POSITIVE.getName());

        // 2021-01 validation done even for untyped set config
        Asserts.assertFailsWith(() -> ent.config().set(objKey, "-42m"),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintDeferredSupplier() throws Exception {
        AtomicInteger invocationCount = new AtomicInteger(0);
        DeferredSupplier<Duration> deferred = new DeferredSupplier<Duration>() {
            @Override
            public Duration get() {
                Time.sleep(Duration.millis(10));
                invocationCount.incrementAndGet();
                return Duration.minutes(-42);
            }
        };
        MyEntityWithDuration child = app.addChild(EntitySpec.create(MyEntityWithDuration.class)
                .configure(MyEntityWithDuration.DURATION_POSITIVE.getName(), deferred));
        // shouldn't be accessible on setup due to get immediate semantics
        Assert.assertEquals(invocationCount.get(), 0);

        Asserts.assertFailsWith(() -> child.getConfig(MyPolicyWithDuration.DURATION_POSITIVE),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));

        ConfigKey<Object> objKey = ConfigKeys.newConfigKey(Object.class, MyEntityWithDuration.DURATION_POSITIVE.getName());
        Asserts.assertFailsWith(() -> child.config().get(objKey),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));

        Asserts.assertThat(invocationCount.get(), t -> t>0, "invocation should have happened at least once");
    }

}
