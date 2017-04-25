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
package org.apache.brooklyn.entity.group;

import static org.apache.brooklyn.entity.group.DynamicCluster.CLUSTER_MEMBER_ID;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.RecordingSensorEventListener;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.trait.Changeable;
import org.apache.brooklyn.core.entity.trait.FailingEntity;
import org.apache.brooklyn.core.entity.trait.Resizable;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.BlockingEntity;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.QuorumCheck.QuorumChecks;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Suppliers;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;


public class DynamicClusterTest extends BrooklynAppUnitTestSupport {

    private static final int TIMEOUT_MS = 2000;

    SimulatedLocation loc;
    SimulatedLocation loc2;
    Random random = new Random();

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = new SimulatedLocation();
        loc2 = new SimulatedLocation();
    }

    @Test
    public void creationOkayWithoutNewEntityFactoryArgument() throws Exception {
        app.createAndManageChild(EntitySpec.create(DynamicCluster.class));
    }

    @Test
    public void testRequiresThatMemberSpecArgumentIsAnEntitySpec() throws Exception {
        try {
            app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                    .configure("memberSpec", "error"));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailure(e);
        }
    }

    @Test
    public void startRequiresThatMemberSpecArgumentIsGiven() throws Exception {
        DynamicCluster c = app.createAndManageChild(EntitySpec.create(DynamicCluster.class));
        try {
            c.start(ImmutableList.of(loc));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, IllegalStateException.class);
        }
    }
    
    @Test
    public void startThenStopThenStartWithNewLocationFails() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        try {
            cluster.start(ImmutableList.of(loc));
            cluster.stop();
            cluster.start(ImmutableList.of(loc2));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, IllegalStateException.class);
        }
    }

    @Test
    public void startMethodFailsIfLocationsParameterHasMoreThanOneElement() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        try {
            cluster.start(ImmutableList.of(loc, loc2));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "ambiguous");
        }
    }

    @Test
    public void testClusterHasOneLocationAfterStarting() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        cluster.start(ImmutableList.of(loc));
        assertEquals(cluster.getLocations().size(), 1);
        assertEquals(ImmutableList.copyOf(cluster.getLocations()), ImmutableList.of(loc));
    }

    @Test
    public void testServiceUpAfterStartingWithNoMembers() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class))
                .configure(DynamicCluster.INITIAL_SIZE, 0));
        cluster.start(ImmutableList.of(loc));
        
        EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertTrue(cluster.getAttribute(Attributes.SERVICE_UP));
    }

    @Test
    public void usingEntitySpecResizeFromZeroToOneStartsANewEntityAndSetsItsParent() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));
        
        cluster.start(ImmutableList.of(loc));

        cluster.resize(1);
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(cluster.getMembers());
        assertEquals(entity.getCount(), 1);
        assertEquals(entity.getParent(), cluster);
        assertEquals(entity.getApplication(), app);
    }

    @Test
    public void resizeFromZeroToOneStartsANewChildEntity() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));

        cluster.resize(1);
        TestEntity entity = (TestEntity) Iterables.get(cluster.getMembers(), 0);
        assertEquals(entity.getCounter().get(), 1);
        assertEquals(entity.getParent(), cluster);
        assertEquals(entity.getApplication(), app);
    }

    @Test
    public void testResizeWhereChildThrowsNoMachineAvailableExceptionIsPropagatedAsInsufficientCapacityException() throws Exception {
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(FailingEntity.class)
                    .configure(FailingEntity.FAIL_ON_START, true)
                    .configure(FailingEntity.EXCEPTION_CLAZZ, NoMachinesAvailableException.class))
            .configure(DynamicCluster.INITIAL_SIZE, 0));
        cluster.start(ImmutableList.of(loc));
        
        try {
            cluster.resize(1);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, Resizable.InsufficientCapacityException.class);
        }
    }

    @Test
    public void testResizeWhereSubsetOfChildrenThrowsNoMachineAvailableExceptionIsPropagatedAsInsuffientCapacityException() throws Exception {
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(FailingEntity.class)
                    .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                        final AtomicInteger counter = new AtomicInteger();
                        @Override public boolean apply(FailingEntity input) {
                            // Only second and subsequent entities fail
                            int index = counter.getAndIncrement();
                            return (index >= 1);
                        }})
                    .configure(FailingEntity.EXCEPTION_CLAZZ, NoMachinesAvailableException.class))
            .configure(DynamicCluster.INITIAL_SIZE, 0));
        cluster.start(ImmutableList.of(loc));

        // Managed to partially resize, but will still throw exception.
        // The getCurrentSize will report how big we managed to get.
        // The children that failed due to NoMachinesAvailableException will have been unmanaged automatically.
        try {
            cluster.resize(2);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, Resizable.InsufficientCapacityException.class);
        }
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        Iterable<FailingEntity> children1 = Iterables.filter(cluster.getChildren(), FailingEntity.class);
        assertEquals(Iterables.size(children1), 1);
        assertEquals(Iterables.getOnlyElement(children1).sensors().get(TestEntity.SERVICE_UP), Boolean.TRUE);
        
        // This attempt will also fail, because all new children will fail
        try {
            cluster.resize(2);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, Resizable.InsufficientCapacityException.class);
        }
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        Iterable<FailingEntity> children2 = Iterables.filter(cluster.getChildren(), FailingEntity.class);
        assertEquals(Iterables.size(children2), 1);
        assertEquals(Iterables.getOnlyElement(children2), Iterables.getOnlyElement(children1));
    }

    /** This can be sensitive to order, e.g. if TestEntity set expected RUNNING before setting SERVICE_UP, 
     * there would be a point when TestEntity is ON_FIRE.
     * <p>
     * There can also be issues if a cluster is resizing from/to 0 while in a RUNNING state.
     * To correct that, use {@link ServiceStateLogic#newEnricherFromChildrenUp()}.
     */
    @Test
    public void testResizeFromZeroToOneDoesNotGoThroughFailing() throws Exception {
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class))
            .configure(DynamicCluster.INITIAL_SIZE, 1));
        
        RecordingSensorEventListener<Lifecycle> r = new RecordingSensorEventListener<>();
        app.subscriptions().subscribe(cluster, Attributes.SERVICE_STATE_ACTUAL, r);

        cluster.start(ImmutableList.of(loc));
        EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        for (SensorEvent<Lifecycle> evt: r.getEvents()) {
            if (evt.getValue()==Lifecycle.ON_FIRE)
                Assert.fail("Should not have published " + Lifecycle.ON_FIRE + " during normal start up: " + r.getEvents());
        }
    }

    @Test
    public void resizeDownByTwoAndDownByOne() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));

        cluster.resize(4);
        assertEquals(Iterables.size(Entities.descendantsAndSelf(cluster, TestEntity.class)), 4);
        
        // check delta of 2 and delta of 1, because >1 is handled differently to =1
        cluster.resize(2);
        assertEquals(Iterables.size(Entities.descendantsAndSelf(cluster, TestEntity.class)), 2);
        cluster.resize(1);
        assertEquals(Iterables.size(Entities.descendantsAndSelf(cluster, TestEntity.class)), 1);
        cluster.resize(1);
        assertEquals(Iterables.size(Entities.descendantsAndSelf(cluster, TestEntity.class)), 1);
        cluster.resize(0);
        assertEquals(Iterables.size(Entities.descendantsAndSelf(cluster, TestEntity.class)), 0);
    }

    
    @Test
    public void currentSizePropertyReflectsActualClusterSize() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        assertEquals(cluster.getCurrentSize(), (Integer)0);

        cluster.start(ImmutableList.of(loc));
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        assertEquals(cluster.getAttribute(Changeable.GROUP_SIZE), (Integer)1);

        int newSize = cluster.resize(0);
        assertEquals(newSize, 0);
        assertEquals((Integer)newSize, cluster.getCurrentSize());
        assertEquals(newSize, cluster.getMembers().size());
        assertEquals((Integer)newSize, cluster.getAttribute(Changeable.GROUP_SIZE));

        newSize = cluster.resize(4);
        assertEquals(newSize, 4);
        assertEquals((Integer)newSize, cluster.getCurrentSize());
        assertEquals(newSize, cluster.getMembers().size());
        assertEquals((Integer)newSize, cluster.getAttribute(Changeable.GROUP_SIZE));

        newSize = cluster.resize(0);
        assertEquals(newSize, 0);
        assertEquals((Integer)newSize, cluster.getCurrentSize());
        assertEquals(newSize, cluster.getMembers().size());
        assertEquals((Integer)newSize, cluster.getAttribute(Changeable.GROUP_SIZE));
    }

    @Test
    public void clusterSizeAfterStartIsInitialSize() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", 2));

        cluster.start(ImmutableList.of(loc));
        assertEquals(cluster.getCurrentSize(), (Integer)2);
        assertEquals(cluster.getMembers().size(), 2);
        assertEquals(cluster.getAttribute(Changeable.GROUP_SIZE), (Integer)2);
    }

    @Test
    public void clusterLocationIsPassedOnToEntityStart() throws Exception {
        List<SimulatedLocation> locations = ImmutableList.of(loc);
        
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", 1));

        cluster.start(locations);
        TestEntity entity = (TestEntity) Iterables.get(cluster.getMembers(), 0);
        
        assertEquals(ImmutableList.copyOf(entity.getLocations()), locations);
    }

    @Test
    public void resizeFromOneToZeroChangesClusterSize() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", 1));

        cluster.start(ImmutableList.of(loc));
        TestEntity entity = (TestEntity) Iterables.get(cluster.getMembers(), 0);
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        assertEquals(entity.getCounter().get(), 1);
        
        cluster.resize(0);
        assertEquals(cluster.getCurrentSize(), (Integer)0);
        assertEquals(entity.getCounter().get(), 0);
    }

    @Test
    public void concurrentResizesToSameNumberCreatesCorrectNumberOfNodes() throws Exception {
        final int OVERHEAD_MS = 500;
        final Duration STARTUP_TIME = Duration.millis(50);
        final AtomicInteger numCreated = new AtomicInteger(0);

        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(BlockingEntity.class)
                        .configure(BlockingEntity.STARTUP_DELAY, STARTUP_TIME)));

        cluster.subscriptions().subscribe(cluster, AbstractEntity.CHILD_ADDED, new SensorEventListener<Entity>() {
            @Override public void onEvent(SensorEvent<Entity> event) {
                if (event.getValue() instanceof BlockingEntity) {
                    numCreated.incrementAndGet();
                }
            }});

        assertEquals(cluster.getCurrentSize(), (Integer)0);
        cluster.start(ImmutableList.of(loc));

        ExecutorService executor = Executors.newCachedThreadPool();
        final List<Throwable> throwables = new CopyOnWriteArrayList<Throwable>();

        try {
            for (int i = 0; i < 10; i++) {
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            cluster.resize(2);
                        } catch (Throwable e) {
                            throwables.add(e);
                        }
                    }});
            }

            executor.shutdown();
            assertTrue(executor.awaitTermination(10*STARTUP_TIME.toMilliseconds()+OVERHEAD_MS, TimeUnit.MILLISECONDS));
            if (throwables.size() > 0) throw Exceptions.propagate(throwables.get(0));
            assertEquals(cluster.getCurrentSize(), (Integer)2);
            assertEquals(cluster.getAttribute(Changeable.GROUP_SIZE), (Integer)2);
            Asserts.succeedsEventually(new Runnable() {
                public void run() {
                    assertEquals(numCreated.get(), 2);
                }});
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    public void stoppingTheClusterStopsTheEntity() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(BlockingEntity.class))
                .configure("initialSize", 1));

        cluster.start(ImmutableList.of(loc));
        TestEntity entity = (TestEntity) Iterables.get(cluster.getMembers(), 0);
        TestEntityImpl deproxiedEntity = (TestEntityImpl) Entities.deproxy(entity);
        assertEquals(entity.getCounter().get(), 1);
        cluster.stop();
        
        // Need to use deproxiedEntity, because entity-proxy would intercept method call, and it
        // would fail because the entity has been unmanaged.
        assertEquals(deproxiedEntity.getCounter().get(), 0);
    }

    /**
     * This tests the fix for ENGR-1826.
     */
    @Test
    public void failingEntitiesDontBreakClusterActions() throws Exception {
        final int failNum = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 0)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        cluster.start(ImmutableList.of(loc));
        resizeExpectingError(cluster, 3);
        assertEquals(cluster.getCurrentSize(), (Integer)2);
        assertEquals(cluster.getMembers().size(), 2);
        for (Entity member : cluster.getMembers()) {
            assertFalse(((FailingEntity)member).getConfig(FailingEntity.FAIL_ON_START));
        }
    }

    static Exception resizeExpectingError(DynamicCluster cluster, int size) {
        try {
            cluster.resize(size);
            Assert.fail("Resize should have failed");
            // unreachable:
            return null;
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            // expect: PropagatedRuntimeException: Error invoking resize at DynamicClusterImpl{id=I9Ggxfc1}: 1 of 3 parallel child tasks failed: Simulating entity stop failure for test
            Assert.assertTrue(e.toString().contains("resize"));
            return e;
        }
    }

    @Test
    public void testInitialQuorumSizeSufficientForStartup() throws Exception {
        final int failNum = 1;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 2)
                .configure(DynamicCluster.INITIAL_QUORUM_SIZE, 1)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        cluster.start(ImmutableList.of(loc));
        
        // note that children include quarantine group; and quarantined nodes
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        assertEquals(cluster.getMembers().size(), 1);
        for (Entity member : cluster.getMembers()) {
            assertFalse(((FailingEntity)member).getConfig(FailingEntity.FAIL_ON_START));
        }
    }

    @Test
    public void testInitialQuorumSizeDefaultsToInitialSize() throws Exception {
        final int failNum = 1;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 2)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        try {
            cluster.start(ImmutableList.of(loc));
        } catch (Exception e) {
            IllegalStateException unwrapped = Exceptions.getFirstThrowableOfType(e, IllegalStateException.class);
            if (unwrapped != null && unwrapped.getMessage().contains("failed to get to initial size")) {
                // success
            } else {
                throw e; // fail
            }
        }
        
        // note that children include quarantine group; and quarantined nodes
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        assertEquals(cluster.getMembers().size(), 1);
        for (Entity member : cluster.getMembers()) {
            assertFalse(((FailingEntity)member).getConfig(FailingEntity.FAIL_ON_START));
        }
    }

    @Test
    public void testQuarantineGroupOfCorrectType() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("quarantineFailedEntities", true)
                .configure("initialSize", 0)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));
        
        QuarantineGroup quarantineGroup = cluster.getAttribute(DynamicCluster.QUARANTINE_GROUP);
        quarantineGroup.expungeMembers(true); // sanity check by calling something on it
    }

    @Test
    public void testCanQuarantineFailedEntities() throws Exception {
        final AttributeSensor<Boolean> failureMarker = Sensors.newBooleanSensor("failureMarker");
        final int failNum = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("quarantineFailedEntities", true)
                .configure("initialSize", 0)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                boolean fail = counter.incrementAndGet() == failNum;
                                input.sensors().set(failureMarker, fail);
                                return fail;
                            }})));

        cluster.start(ImmutableList.of(loc));
        resizeExpectingError(cluster, 3);
        assertEquals(cluster.getCurrentSize(), (Integer)2);
        assertEquals(cluster.getMembers().size(), 2);
        assertEquals(Iterables.size(Iterables.filter(cluster.getChildren(), Predicates.instanceOf(FailingEntity.class))), 3);
        for (Entity member : cluster.getMembers()) {
            assertFalse(((FailingEntity)member).getConfig(FailingEntity.FAIL_ON_START));
        }

        assertEquals(cluster.getAttribute(DynamicCluster.QUARANTINE_GROUP).getMembers().size(), 1);
        for (Entity member : cluster.getAttribute(DynamicCluster.QUARANTINE_GROUP).getMembers()) {
            assertTrue(member.sensors().get(failureMarker));
        }
    }

    @Test
    public void testDoNotQuarantineFailedEntities() throws Exception {
        final int failNum = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                // default is quarantineFailedEntities==true
                .configure("quarantineFailedEntities", false)
                .configure("initialSize", 0)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        cluster.start(ImmutableList.of(loc));
        
        // no quarantine group, as a child
        assertEquals(cluster.getChildren().size(), 0, "children="+cluster.getChildren());
        
        // Failed node will not be a member or child
        resizeExpectingError(cluster, 3);
        assertEquals(cluster.getCurrentSize(), (Integer)2);
        assertEquals(cluster.getMembers().size(), 2);
        assertEquals(cluster.getChildren().size(), 2, "children="+cluster.getChildren());
        
        // Failed node will not be managed either
        assertEquals(Iterables.size(Iterables.filter(cluster.getChildren(), Predicates.instanceOf(FailingEntity.class))), 2);
        for (Entity member : cluster.getMembers()) {
            assertFalse(((FailingEntity)member).getConfig(FailingEntity.FAIL_ON_START));
        }
    }

    @Test
    public void testQuarantineFailedEntitiesRespectsCustomFilter() throws Exception {
        Predicate<Throwable> filter = new Predicate<Throwable>() {
            @Override public boolean apply(Throwable input) {
                return Exceptions.getFirstThrowableOfType(input, AllowedException.class) != null;
            }
        };
        runQuarantineFailedEntitiesRespectsFilter(AllowedException.class, DisallowedException.class, filter);
    }
    @SuppressWarnings("serial")
    public static class AllowedException extends RuntimeException {
        public AllowedException(String message) {
            super(message);
        }
    }
    @SuppressWarnings("serial")
    public static class DisallowedException extends RuntimeException {
        public DisallowedException(String message) {
            super(message);
        }
    }

    @Test
    public void testQuarantineFailedEntitiesRespectsDefaultFilter() throws Exception {
        Predicate<Throwable> filter = null;
        runQuarantineFailedEntitiesRespectsFilter(AllowedException.class, NoMachinesAvailableException.class, filter);
    }
    
    protected void runQuarantineFailedEntitiesRespectsFilter(final Class<? extends Exception> allowedException, 
            final Class<? extends Exception> disallowedException, Predicate<Throwable> quarantineFilter) throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("quarantineFailedEntities", true)
                .configure("initialSize", 0)
                .configure("quarantineFilter", quarantineFilter)
                .configure(DynamicCluster.FIRST_MEMBER_SPEC, EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)
                        .configure(FailingEntity.EXCEPTION_CLAZZ, allowedException))
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START, true)
                        .configure(FailingEntity.EXCEPTION_CLAZZ, disallowedException)));

        cluster.start(ImmutableList.of(loc));
        resizeExpectingError(cluster, 2);
        Iterable<FailingEntity> children = Iterables.filter(cluster.getChildren(), FailingEntity.class);
        Collection<Entity> quarantineMembers = cluster.sensors().get(DynamicCluster.QUARANTINE_GROUP).getMembers();
        
        assertEquals(cluster.getCurrentSize(), (Integer)0);
        assertEquals(Iterables.getOnlyElement(children).config().get(FailingEntity.EXCEPTION_CLAZZ), allowedException);
        assertEquals(Iterables.getOnlyElement(quarantineMembers), Iterables.getOnlyElement(children));
    }

    @Test
    public void defaultRemovalStrategyShutsDownNewestFirstWhenResizing() throws Exception {
        final List<Entity> creationOrder = Lists.newArrayList();
        
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 0)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.subscriptions().subscribe(cluster, AbstractEntity.CHILD_ADDED, new SensorEventListener<Entity>() {
            @Override public void onEvent(SensorEvent<Entity> event) {
                if (event.getValue() instanceof TestEntity) {
                    creationOrder.add(event.getValue());
                }
            }});

        cluster.start(ImmutableList.of(loc));
        cluster.resize(1);
        
        //Prevent the two entities created in the same ms
        //so that the removal strategy can always choose the 
        //entity created next
        Thread.sleep(1);
        
        cluster.resize(2);
        Asserts.eventually(Suppliers.ofInstance(creationOrder), CollectionFunctionals.sizeEquals(2));
        assertEquals(cluster.getCurrentSize(), (Integer)2);
        assertEquals(ImmutableSet.copyOf(cluster.getMembers()), ImmutableSet.copyOf(creationOrder), "actual="+cluster.getMembers());

        // Now stop one
        cluster.resize(1);
        assertEquals(cluster.getCurrentSize(), (Integer)1);
        assertEquals(ImmutableList.copyOf(cluster.getMembers()), creationOrder.subList(0, 1));
    }

    @Test
    public void resizeLoggedAsEffectorCall() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        app.start(ImmutableList.of(loc));
        cluster.resize(1);

        Set<Task<?>> tasks = app.getManagementContext().getExecutionManager().getTasksWithAllTags(ImmutableList.of(
            BrooklynTaskTags.tagForContextEntity(cluster),"EFFECTOR"));
        assertEquals(tasks.size(), 2);
        assertTrue(Iterables.get(tasks, 0).getDescription().contains("start"));
        assertTrue(Iterables.get(tasks, 1).getDescription().contains("resize"));
    }

    @Test
    public void testUnmanagedChildIsRemoveFromGroup() throws Exception {
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));

        final TestEntity child = (TestEntity) Iterables.get(cluster.getMembers(), 0);
        Entities.unmanage(child);

        Asserts.succeedsEventually(MutableMap.of("timeout", TIMEOUT_MS), new Runnable() {
            @Override public void run() {
                assertFalse(cluster.getChildren().contains(child), "children="+cluster.getChildren());
                assertEquals(cluster.getCurrentSize(), (Integer)0);
                assertEquals(cluster.getMembers().size(), 0);
            }});
    }

    @Test
    public void testPluggableRemovalStrategyIsUsed() throws Exception {
        final List<Entity> removedEntities = Lists.newArrayList();

        Function<Collection<Entity>, Entity> removalStrategy = new Function<Collection<Entity>, Entity>() {
            @Override public Entity apply(Collection<Entity> contenders) {
                Entity choice = Iterables.get(contenders, random.nextInt(contenders.size()));
                removedEntities.add(choice);
                return choice;
            }
        };

        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", 10)
                .configure("removalStrategy", removalStrategy));

        cluster.start(ImmutableList.of(loc));
        Set<?> origMembers = ImmutableSet.copyOf(cluster.getMembers());

        for (int i = 10; i >= 0; i--) {
            cluster.resize(i);
            assertEquals(cluster.getAttribute(Changeable.GROUP_SIZE), (Integer)i);
            assertEquals(removedEntities.size(), 10-i);
            assertEquals(ImmutableSet.copyOf(Iterables.concat(cluster.getMembers(), removedEntities)), origMembers);
        }
    }

    @Test
    public void testPluggableRemovalStrategyCanBeSetAfterConstruction() throws Exception {
        final List<Entity> removedEntities = Lists.newArrayList();

        Function<Collection<Entity>, Entity> removalStrategy = new Function<Collection<Entity>, Entity>() {
            @Override public Entity apply(Collection<Entity> contenders) {
                Entity choice = Iterables.get(contenders, random.nextInt(contenders.size()));
                removedEntities.add(choice);
                return choice;
            }
        };
        
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", 10));

        cluster.start(ImmutableList.of(loc));
        Set<?> origMembers = ImmutableSet.copyOf(cluster.getMembers());

        cluster.setRemovalStrategy(removalStrategy);

        for (int i = 10; i >= 0; i--) {
            cluster.resize(i);
            assertEquals(cluster.getAttribute(Changeable.GROUP_SIZE), (Integer)i);
            assertEquals(removedEntities.size(), 10-i);
            assertEquals(ImmutableSet.copyOf(Iterables.concat(cluster.getMembers(), removedEntities)), origMembers);
        }
    }

    @Test
    public void testResizeDoesNotBlockCallsToQueryGroupMembership() throws Exception {
        final CountDownLatch executingLatch = new CountDownLatch(1);
        final CountDownLatch continuationLatch = new CountDownLatch(1);
        
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 0)
                .configure("memberSpec", EntitySpec.create(BlockingEntity.class)
                        .configure(BlockingEntity.STARTUP_LATCH, continuationLatch)
                        .configure(BlockingEntity.EXECUTING_STARTUP_NOTIFICATION_LATCH, executingLatch)));

        cluster.start(ImmutableList.of(loc));

        Thread thread = new Thread(new Runnable() {
                @Override public void run() {
                    cluster.resize(1);
                }});
        
        try {
            // wait for resize to be executing; it will be waiting for start() to complete on
            // the newly created member.
            thread.start();
            executingLatch.await(TIMEOUT_MS, TimeUnit.MILLISECONDS);

            // ensure can still call methods on group, to query/update membership
            assertEquals(cluster.getMembers().size(), 1);
            assertEquals(cluster.getCurrentSize(), (Integer)1);
            assertFalse(cluster.hasMember(cluster));
            assertTrue(cluster.addMember(cluster));
            assertTrue(cluster.removeMember(cluster));

            // allow the resize to complete
            continuationLatch.countDown();
            thread.join(TIMEOUT_MS);
            assertFalse(thread.isAlive());
        } finally {
            thread.interrupt();
        }
    }

    @Test
    public void testReplacesMember() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));
        Entity member = Iterables.get(cluster.getMembers(), 0);

        String replacementId = cluster.replaceMember(member.getId());
        Entity replacement = app.getManagementContext().getEntityManager().getEntity(replacementId);

        assertEquals(cluster.getMembers().size(), 1);
        assertFalse(cluster.getMembers().contains(member));
        assertFalse(cluster.getChildren().contains(member));
        assertNotNull(replacement, "replacementId="+replacementId);
        assertTrue(cluster.getMembers().contains(replacement), "replacement="+replacement+"; members="+cluster.getMembers());
        assertTrue(cluster.getChildren().contains(replacement), "replacement="+replacement+"; children="+cluster.getChildren());
    }

    @Test
    public void testReplaceMemberThrowsIfMemberIdDoesNotResolve() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));
        Entity member = Iterables.get(cluster.getMembers(), 0);

        try {
            cluster.replaceMember("wrong.id");
            fail();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class) == null) throw e;
            if (!Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class).getMessage().contains("entity wrong.id cannot be resolved")) throw e;
        }

        assertEquals(ImmutableSet.copyOf(cluster.getMembers()), ImmutableSet.of(member));
    }

    @Test
    public void testReplaceMemberThrowsIfNotMember() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        cluster.start(ImmutableList.of(loc));
        Entity member = Iterables.get(cluster.getMembers(), 0);

        try {
            cluster.replaceMember(app.getId());
            fail();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class) == null) throw e;
            if (!Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class).getMessage().contains("is not a member")) throw e;
        }

        assertEquals(ImmutableSet.copyOf(cluster.getMembers()), ImmutableSet.of(member));
    }

    @Test
    public void testReplaceMemberFailsIfCantProvisionReplacement() throws Exception {
        final int failNum = 2;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_START_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        cluster.start(ImmutableList.of(loc));
        Entity member = Iterables.get(cluster.getMembers(), 0);

        try {
            cluster.replaceMember(member.getId());
            fail();
        } catch (Exception e) {
            if (!e.toString().contains("failed to grow")) throw e;
            if (Exceptions.getFirstThrowableOfType(e, NoSuchElementException.class) != null) throw e;
        }
        assertEquals(ImmutableSet.copyOf(cluster.getMembers()), ImmutableSet.of(member));
    }

    @Test
    public void testReplaceMemberRemovesAndThowsIfFailToStopOld() throws Exception {
        final int failNum = 1;
        final AtomicInteger counter = new AtomicInteger(0);
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", EntitySpec.create(FailingEntity.class)
                        .configure(FailingEntity.FAIL_ON_STOP_CONDITION, new Predicate<FailingEntity>() {
                            @Override public boolean apply(FailingEntity input) {
                                return counter.incrementAndGet() == failNum;
                            }})));

        cluster.start(ImmutableList.of(loc));
        Entity member = Iterables.get(cluster.getMembers(), 0);

        try {
            cluster.replaceMember(member.getId());
            fail();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, StopFailedRuntimeException.class) == null) throw e;
            boolean found = false;
            for (Throwable t : Throwables.getCausalChain(e)) {
                if (t.toString().contains("Simulating entity stop failure")) {
                    found = true;
                    break;
                }
            }
            if (!found) throw e;
        }
        assertFalse(Entities.isManaged(member));
        assertEquals(cluster.getMembers().size(), 1);
    }

    @Test
    public void testWithNonStartableEntity() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(BasicEntity.class))
                .configure(DynamicCluster.UP_QUORUM_CHECK, QuorumChecks.alwaysTrue())
                .configure(DynamicCluster.INITIAL_SIZE, 2));
        cluster.start(ImmutableList.of(loc));
        
        EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertTrue(cluster.getAttribute(Attributes.SERVICE_UP));
    }

    @Test
    public void testDifferentFirstMemberSpec() throws Exception {
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
            .configure(DynamicCluster.FIRST_MEMBER_SPEC, 
                EntitySpec.create(BasicEntity.class).configure(TestEntity.CONF_NAME, "first"))
            .configure(DynamicCluster.MEMBER_SPEC, 
                EntitySpec.create(BasicEntity.class).configure(TestEntity.CONF_NAME, "non-first"))
            .configure(DynamicCluster.UP_QUORUM_CHECK, QuorumChecks.alwaysTrue())
            .configure(DynamicCluster.INITIAL_SIZE, 3));
        cluster.start(ImmutableList.of(loc));
        
        EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertTrue(cluster.getAttribute(Attributes.SERVICE_UP));
        
        assertEquals(cluster.getMembers().size(), 3);
        
        assertFirstAndNonFirstCounts(cluster.getMembers(), 1, 2);
        
        // and after re-size
        cluster.resize(4);
//        Entities.dumpInfo(cluster);
        assertFirstAndNonFirstCounts(cluster.getMembers(), 1, 3);
        
        // and re-size to 1
        cluster.resize(1);
        assertFirstAndNonFirstCounts(cluster.getMembers(), 1, 0);
        
        // and re-size to 0
        cluster.resize(0);
        assertFirstAndNonFirstCounts(cluster.getMembers(), 0, 0);
        
        // and back to 3
        cluster.resize(3);
        assertFirstAndNonFirstCounts(cluster.getMembers(), 1, 2);
    }

    @Test
    public void testPrefersMemberSpecLocation() throws Exception {
        @SuppressWarnings("deprecation")
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class)
                        .location(loc2))
                .configure(DynamicCluster.INITIAL_SIZE, 1));
        
        cluster.start(ImmutableList.of(loc));
        assertEquals(ImmutableList.copyOf(cluster.getLocations()), ImmutableList.of(loc));
        
        Entity member = Iterables.getOnlyElement(cluster.getMembers());
        assertEquals(ImmutableList.copyOf(member.getLocations()), ImmutableList.of(loc2));
    }

    @Test
    public void testAllClusterMemberIdsAddedInOrderOnCreation() throws Exception {
        int clusterSize = 5;

        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", clusterSize));

        cluster.start(ImmutableList.of(loc));

        assertMemberIdSensors(cluster, ImmutableList.of(0, 1, 2, 3, 4));
    }

    @Test
    public void testAllClusterMemberIdsAddedInOrderOnPositiveResize() throws Exception {
        int clusterSize = 5;

        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", clusterSize));

        cluster.start(ImmutableList.of(loc));

        int positiveResizeDelta = 3;
        cluster.resizeByDelta(positiveResizeDelta);

        assertMemberIdSensors(cluster, ImmutableList.of(0, 1, 2, 3, 4, 5, 6, 7));
    }

    @Test
    public void testAllClusterMemberIdsAddedInOrderOnNegativeThenPositiveResize() throws Exception {
        int clusterSize = 5;

        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class))
                .configure("initialSize", clusterSize));

        cluster.start(ImmutableList.of(loc));

        int negativeResizeDelta = -3;
        cluster.resizeByDelta(negativeResizeDelta);

        int positiveResizeDelta = 2;
        cluster.resizeByDelta(positiveResizeDelta);

        assertMemberIdSensors(cluster, ImmutableList.of(0, 1, 5, 6));
    }

    @Test
    public void testClustersHaveIndependentCounters() throws Exception {
        int numClusters = 2;
        int clusterInitialSize = 1;
        int clusterSizeDelta = 1;
        List<DynamicCluster> clusters = Lists.newArrayList();
        for (int i = 0; i < numClusters; i++) {
            DynamicCluster cluster = app.addChild(EntitySpec.create(DynamicCluster.class)
                    .configure("memberSpec", EntitySpec.create(TestEntity.class))
                    .configure("initialSize", clusterInitialSize));
            cluster.start(ImmutableList.of(loc));
            clusters.add(cluster);
        }

        // Each cluster has its own independent count, so should start with 0.
        for (DynamicCluster cluster : clusters) {
            List<Integer> expectedIds = ImmutableList.of(0);
            assertMemberIdSensors(cluster, expectedIds);
        }
        
        // Each cluster should continue using its own independent count when resized.
        for (DynamicCluster cluster : clusters) {
            cluster.resizeByDelta(clusterSizeDelta);
        }
        for (DynamicCluster cluster : clusters) {
            List<Integer> expectedIds = ImmutableList.of(0, 1);
            assertMemberIdSensors(cluster, expectedIds);
        }
    }

    private void assertMemberIdSensors(DynamicCluster cluster, List<Integer> expectedIds) {
        List<Entity> members = ImmutableList.copyOf(cluster.getMembers());
        assertEquals(members.size(), expectedIds.size(), "members="+members+"; expectedIds="+expectedIds);
        for (int i = 0; i < members.size(); i++) {
            assertEquals(members.get(i).config().get(CLUSTER_MEMBER_ID), expectedIds.get(i));
        }
    }
    
    @Test
    public void testResizeStrategies() throws Exception {
        int clusterSize = 5;

        ImmutableList.Builder<RemovalStrategy> sensorMatchingStrategiesBuilder = ImmutableList.builder();
        for (int i = 0; i < clusterSize; i++){
            SensorMatchingRemovalStrategy<?> sensorMatchingRemovalStrategy = new SensorMatchingRemovalStrategy<>();
            sensorMatchingRemovalStrategy.config().set(SensorMatchingRemovalStrategy.SENSOR, TestEntity.SEQUENCE);
            sensorMatchingRemovalStrategy.config().set(SensorMatchingRemovalStrategy.DESIRED_VALUE, i);
            sensorMatchingStrategiesBuilder.add(sensorMatchingRemovalStrategy);
        }

        RemovalStrategy firstFrom = new FirstFromRemovalStrategy();
        firstFrom.config().set(FirstFromRemovalStrategy.STRATEGIES, sensorMatchingStrategiesBuilder.build());

        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class))
                .configure(DynamicCluster.INITIAL_SIZE, clusterSize)
                .configure(DynamicCluster.REMOVAL_STRATEGY, firstFrom));

        cluster.start(ImmutableList.of(loc));

        assertEquals(cluster.getMembers().size(), clusterSize);
        EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_UP, true);

        // Set the sensor values of the entities in a non-linear pattern (4, 0, 3, 1, 2), then resize the cluster
        // down by 1 and see if the correct entity has been removed, then resize down by 3
        Iterator<Entity> childIterator = cluster.getMembers().iterator();
        for (int i : new int[] {4, 0, 3, 1, 2}) {
            childIterator.next().sensors().set(TestEntity.SEQUENCE, i);
        }

        assertEntityCollectionContainsSequence(cluster.getMembers(), ImmutableSet.of(0, 1, 2, 3, 4));

        cluster.resizeByDelta(-1);
        EntityAsserts.assertAttributeEqualsEventually(cluster, DynamicCluster.GROUP_SIZE, 4);
        assertEntityCollectionContainsSequence(cluster.getMembers(), ImmutableSet.of(1, 2, 3, 4));

        cluster.resizeByDelta(-3);
        EntityAsserts.assertAttributeEqualsEventually(cluster, DynamicCluster.GROUP_SIZE, 1);
        assertEntityCollectionContainsSequence(cluster.getMembers(), ImmutableSet.of(4));
    }

    private void assertEntityCollectionContainsSequence(Collection<Entity> entities, Set<Integer> expected) {
        assertEquals(entities.size(), expected.size());
        for (Entity entity : entities) {
            assertTrue(expected.contains(entity.sensors().get(TestEntity.SEQUENCE)));
        }
    }

    private void assertFirstAndNonFirstCounts(Collection<Entity> members, int expectedFirstCount, int expectedNonFirstCount) {
        Set<Entity> found = MutableSet.of();
        for (Entity e: members) {
            if ("first".equals(e.getConfig(TestEntity.CONF_NAME))) found.add(e);
        }
        assertEquals(found.size(), expectedFirstCount);
        
        found.clear();
        for (Entity e: members) {
            if ("non-first".equals(e.getConfig(TestEntity.CONF_NAME))) found.add(e);
        }
        assertEquals(found.size(), expectedNonFirstCount);
    }

    @DataProvider
    public Object[][] maxConcurrentCommandsTestProvider() {
        return new Object[][]{{1}, {2}, {3}};
    }

    @Test(dataProvider = "maxConcurrentCommandsTestProvider")
    public void testEntitiesStartAndStopSequentiallyWhenMaxConcurrentCommandsIsOne(int maxConcurrentCommands) {
        EntitySpec<ThrowOnAsyncStartEntity> memberSpec = EntitySpec.create(ThrowOnAsyncStartEntity.class)
                .configure(ThrowOnAsyncStartEntity.MAX_CONCURRENCY, maxConcurrentCommands)
                .configure(ThrowOnAsyncStartEntity.COUNTER, new AtomicInteger());
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MAX_CONCURRENT_CHILD_COMMANDS, maxConcurrentCommands)
                .configure(DynamicCluster.INITIAL_SIZE, 10)
                .configure(DynamicCluster.MEMBER_SPEC, memberSpec));
        app.start(ImmutableList.of(app.newSimulatedLocation()));
        assertEquals(cluster.sensors().get(Attributes.SERVICE_STATE_ACTUAL), Lifecycle.RUNNING);
    }

    // Tests handling of the first member of a cluster by asserting that a group, whose
    // other members wait for the first, always starts.
    @Test
    public void testFirstMemberInFirstBatchWhenMaxConcurrentCommandsSet() throws Exception {
        final AtomicInteger counter = new AtomicInteger();
        final DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.MAX_CONCURRENT_CHILD_COMMANDS, 1)
                .configure(DynamicCluster.INITIAL_SIZE, 3));

        Task<Boolean> firstMemberUp = Tasks.<Boolean>builder()
                .body(new Callable<Boolean>() {
                    @Override
                    public Boolean call() throws Exception {
                        Task<Entity> first = DependentConfiguration.attributeWhenReady(cluster, DynamicCluster.FIRST);
                        DynamicTasks.queueIfPossible(first).orSubmitAsync();
                        final Entity source = first.get();
                        final Task<Boolean> booleanTask = DependentConfiguration.attributeWhenReady(source, Attributes.SERVICE_UP);
                        DynamicTasks.queueIfPossible(booleanTask).orSubmitAsync();
                        return booleanTask.get();
                    }
                })
                .build();

        EntitySpec<ThrowOnAsyncStartEntity> firstMemberSpec = EntitySpec.create(ThrowOnAsyncStartEntity.class)
                .configure(ThrowOnAsyncStartEntity.COUNTER, counter)
                .configure(ThrowOnAsyncStartEntity.START_LATCH, true);

        EntitySpec<ThrowOnAsyncStartEntity> memberSpec = EntitySpec.create(ThrowOnAsyncStartEntity.class)
                .configure(ThrowOnAsyncStartEntity.COUNTER, counter)
                .configure(ThrowOnAsyncStartEntity.START_LATCH, firstMemberUp);

        cluster.config().set(DynamicCluster.FIRST_MEMBER_SPEC, firstMemberSpec);
        cluster.config().set(DynamicCluster.MEMBER_SPEC, memberSpec);

        // app.start blocks so in the failure case this test would block forever.
        Asserts.assertReturnsEventually(new Runnable() {
            @Override
            public void run() {
                app.start(ImmutableList.of(app.newSimulatedLocation()));
                EntityAsserts.assertAttributeEqualsEventually(cluster, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
            }
        }, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    @Test
    public void testChildCommandPermitNotReleasedWhenMemberStartTaskCancelledBeforeSubmission() {
        // Tests that permits are not released when their start task is cancelled.
        // Expected behaviour is:
        // - permit obtained for first member. cancelled task submitted. permit released.
        // - no permit obtained for second member. cancelled task submitted. no permit released.
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(CancelEffectorInvokeCluster.class)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(TestEntity.class))
                .configure(DynamicCluster.INITIAL_SIZE, 2)
                .configure(DynamicCluster.MAX_CONCURRENT_CHILD_COMMANDS, 1));
        final DynamicClusterImpl clusterImpl = DynamicClusterImpl.class.cast(Entities.deproxy(cluster));
        assertNotNull(clusterImpl.getChildTaskSemaphore());
        assertEquals(clusterImpl.getChildTaskSemaphore().availablePermits(), 1);
        try {
            app.start(ImmutableList.<Location>of(app.newSimulatedLocation()));
            Asserts.shouldHaveFailedPreviously("Cluster start should have failed because the member start was cancelled");
        } catch (Exception e) {
            // ignored.
        }
        assertEquals(clusterImpl.getChildTaskSemaphore().availablePermits(), 1);
    }

    @ImplementedBy(ThrowOnAsyncStartEntityImpl.class)
    public interface ThrowOnAsyncStartEntity extends TestEntity {
        ConfigKey<Integer> MAX_CONCURRENCY = ConfigKeys.newConfigKey(Integer.class, "concurrency", "max concurrency", 1);
        ConfigKey<AtomicInteger> COUNTER = ConfigKeys.newConfigKey(AtomicInteger.class, "counter");
        ConfigKey<Boolean> START_LATCH = ConfigKeys.newConfigKey(Boolean.class, "startlatch");
    }

    public static class ThrowOnAsyncStartEntityImpl extends TestEntityImpl implements ThrowOnAsyncStartEntity {
        private static final Logger LOG = LoggerFactory.getLogger(ThrowOnAsyncStartEntityImpl.class);
        @Override
        public void start(Collection<? extends Location> locs) {
            int count = config().get(COUNTER).incrementAndGet();
            try {
                LOG.debug("{} starting (first={})", new Object[]{this, sensors().get(AbstractGroup.FIRST_MEMBER)});
                config().get(START_LATCH);
                // Throw if more than one entity is starting at the same time as this.
                assertTrue(count <= config().get(MAX_CONCURRENCY), "expected " + count + " <= " + config().get(MAX_CONCURRENCY));
                super.start(locs);
            } finally {
                config().get(COUNTER).decrementAndGet();
            }
        }
    }

    /** Used in {@link #testChildCommandPermitNotReleasedWhenMemberStartTaskCancelledBeforeSubmission}. */
    @ImplementedBy(CancelEffectorInvokeClusterImpl.class)
    public interface CancelEffectorInvokeCluster extends DynamicCluster {}

    /** Overrides {@link DynamicClusterImpl#newThrottledEffectorTask} to cancel each task before it's submitted. */
    public static class CancelEffectorInvokeClusterImpl extends DynamicClusterImpl implements CancelEffectorInvokeCluster {
        @Override
        protected <T> Task<?> newThrottledEffectorTask(Entity target, Effector<T> effector, Map<?, ?> arguments, boolean isPrivileged) {
            Task<?> unsubmitted = super.newThrottledEffectorTask(target, effector, arguments, isPrivileged);
            unsubmitted.cancel(true);
            return unsubmitted;
        }
    }

}
