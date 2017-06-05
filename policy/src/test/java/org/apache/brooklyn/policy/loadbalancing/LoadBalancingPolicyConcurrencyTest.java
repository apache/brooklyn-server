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
package org.apache.brooklyn.policy.loadbalancing;

import java.util.Collections;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.Lists;

public class LoadBalancingPolicyConcurrencyTest extends AbstractLoadBalancingPolicyTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(LoadBalancingPolicyConcurrencyTest.class);

    private static final double WORKRATE_JITTER = 2d;
    private static final int NUM_CONTAINERS = 20;
    private static final int WORKRATE_UPDATE_PERIOD_MS = 1000;
    
    private ScheduledExecutorService scheduledExecutor;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        scheduledExecutor = Executors.newScheduledThreadPool(10);
        super.setUp();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (scheduledExecutor != null) scheduledExecutor.shutdownNow();
        super.tearDown();
    }
    
    /**
     * TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins (brooklyn-master-build #223):
     * 
     * testSimplePeriodicWorkrateUpdates(org.apache.brooklyn.policy.loadbalancing.LoadBalancingPolicyConcurrencyTest)  Time elapsed: 11.237 sec  <<< FAILURE!
     * org.apache.brooklyn.util.exceptions.PropagatedRuntimeException: failed succeeds-eventually, 29 attempts, 10002ms elapsed: AssertionError: actual=[20.0, 20.0, 21.0, 20.0, 0.0, 21.0, 21.0, 20.0, 20.0, 20.0, 19.0, 19.0, 19.0, 36.0, 21.0, 21.0, 20.0, 19.0, 20.0, 21.0]; expected=[20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0] expected [20.0] but found [0.0]
     *     at org.testng.Assert.fail(Assert.java:94)
     *     at org.testng.Assert.failNotEquals(Assert.java:494)
     *     at org.testng.Assert.assertEquals(Assert.java:207)
     *     at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkrates(AbstractLoadBalancingPolicyTest.java:122)
     *     at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest$2.run(AbstractLoadBalancingPolicyTest.java:138)
     *     at org.apache.brooklyn.test.Asserts$RunnableAdapter.call(Asserts.java:1277)
     *     at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:930)
     *     at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:854)
     *     at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkratesEventually(AbstractLoadBalancingPolicyTest.java:136)
     *     at org.apache.brooklyn.policy.loadbalancing.LoadBalancingPolicyConcurrencyTest.testSimplePeriodicWorkrateUpdates(LoadBalancingPolicyConcurrencyTest.java:79)
     */
    @Test(groups="Broken")
    public void testSimplePeriodicWorkrateUpdates() {
        List<MockItemEntity> items = Lists.newArrayList();
        List<MockContainerEntity> containers = Lists.newArrayList();
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            containers.add(newContainer(app, "container"+i, 10, 30));
        }
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            newItemWithPeriodicWorkrates(app, containers.get(0), "item"+i, 20);
        }

        assertWorkratesEventually(containers, items, Collections.nCopies(NUM_CONTAINERS, 20d), WORKRATE_JITTER);
    }
    
    // TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins:
    //    org.apache.brooklyn.util.exceptions.PropagatedRuntimeException: failed succeeds-eventually, 29 attempts, 10002ms elapsed: AssertionError: actual=[18.0, 21.0, 19.0, 0.0, 20.0, 0.0, 20.0, 36.0, 19.0, 0.0, 19.0, 21.0, 21.0, 19.0, 21.0, 36.0, 21.0, 19.0, 18.0, 36.0]; expected=[20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0] expected [20.0] but found [0.0]
    //            at org.testng.Assert.fail(Assert.java:94)
    //            at org.testng.Assert.failNotEquals(Assert.java:494)
    //            at org.testng.Assert.assertEquals(Assert.java:207)
    //            at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkrates(AbstractLoadBalancingPolicyTest.java:122)
    //            at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest$2.run(AbstractLoadBalancingPolicyTest.java:138)
    //            at org.apache.brooklyn.test.Asserts$RunnableAdapter.call(Asserts.java:1277)
    //            at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:930)
    //            at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:854)
    //            at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkratesEventually(AbstractLoadBalancingPolicyTest.java:136)
    //            at org.apache.brooklyn.policy.loadbalancing.LoadBalancingPolicyConcurrencyTest.testConcurrentlyAddContainers(LoadBalancingPolicyConcurrencyTest.java:101)
    @Test(groups={"Broken"})
    public void testConcurrentlyAddContainers() {
        final Queue<MockContainerEntity> containers = new ConcurrentLinkedQueue<MockContainerEntity>();
        final List<MockItemEntity> items = Lists.newArrayList();
        
        containers.add(newContainer(app, "container-orig", 10, 30));
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            items.add(newItemWithPeriodicWorkrates(app, containers.iterator().next(), "item"+i, 20));
        }
        for (int i = 0; i < NUM_CONTAINERS-1; i++) {
            final int index = i;
            scheduledExecutor.submit(new Callable<Void>() {
                @Override public Void call() {
                    containers.add(newContainer(app, "container"+index, 10, 30));
                    return null;
                }});
        }

        assertWorkratesEventually(containers, items, Collections.nCopies(NUM_CONTAINERS, 20d), WORKRATE_JITTER);
    }
    
    // TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins:
    //   org.apache.brooklyn.util.exceptions.PropagatedRuntimeException: failed succeeds-eventually, 29 attempts, 10001ms elapsed: AssertionError: actual=[21.0, 0.0, 20.0, 20.0, 21.0, 20.0, 20.0, 20.0, 36.0, 19.0, 20.0, 21.0, 19.0, 21.0, 20.0, 20.0, 19.0, 19.0, 21.0, 21.0]; expected=[20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0, 20.0] expected [20.0] but found [0.0]
    //        at org.testng.Assert.fail(Assert.java:94)
    //        at org.testng.Assert.failNotEquals(Assert.java:494)
    //        at org.testng.Assert.assertEquals(Assert.java:207)
    //        at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkrates(AbstractLoadBalancingPolicyTest.java:122)
    //        at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest$2.run(AbstractLoadBalancingPolicyTest.java:138)
    //        at org.apache.brooklyn.test.Asserts$RunnableAdapter.call(Asserts.java:1277)
    //        at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:930)
    //        at org.apache.brooklyn.test.Asserts.succeedsEventually(Asserts.java:854)
    //        at org.apache.brooklyn.policy.loadbalancing.AbstractLoadBalancingPolicyTest.assertWorkratesEventually(AbstractLoadBalancingPolicyTest.java:136)
    //        at org.apache.brooklyn.policy.loadbalancing.LoadBalancingPolicyConcurrencyTest.testConcurrentlyAddItems(LoadBalancingPolicyConcurrencyTest.java:132)
    @Test(groups={"Broken"})
    public void testConcurrentlyAddItems() {
        final Queue<MockItemEntity> items = new ConcurrentLinkedQueue<MockItemEntity>();
        final List<MockContainerEntity> containers = Lists.newArrayList();
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            containers.add(newContainer(app, "container"+i, 10, 30));
        }
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            final int index = i;
            scheduledExecutor.submit(new Callable<Void>() {
                @Override public Void call() {
                    items.add(newItemWithPeriodicWorkrates(app, containers.get(0), "item"+index, 20));
                    return null;
                }});
        }
        assertWorkratesEventually(containers, items, Collections.nCopies(NUM_CONTAINERS, 20d), WORKRATE_JITTER);
    }
    
    // TODO Got IndexOutOfBoundsException from containers.last().
    // Changed test group from "WIP" to "Broken".
    @Test(groups="Broken")
    public void testConcurrentlyRemoveContainers() {
        List<MockItemEntity> items = Lists.newArrayList();
        final List<MockContainerEntity> containers = Lists.newArrayList();
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            containers.add(newContainer(app, "container"+i, 15, 45));
        }
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            items.add(newItemWithPeriodicWorkrates(app, containers.get(i), "item"+i, 20));
        }
        
        final List<MockContainerEntity> containersToStop = Lists.newArrayList();
        for (int i = 0; i < NUM_CONTAINERS/2; i++) {
            containersToStop.add(containers.remove(0));
        }
        for (final MockContainerEntity containerToStop : containersToStop) {
            scheduledExecutor.submit(new Callable<Void>() {
                @Override public Void call() {
                    try {
                        containerToStop.offloadAndStop(containers.get(containers.size()-1));
                        Entities.unmanage(containerToStop);
                    } catch (Throwable t) {
                        LOG.error("Error stopping container "+containerToStop, t);
                    }
                    return null;
                }});
        }
        
        assertWorkratesEventually(containers, items, Collections.nCopies(NUM_CONTAINERS/2, 40d), WORKRATE_JITTER*2);
    }
    
    @Test(groups="WIP")
    public void testConcurrentlyRemoveItems() {
        List<MockItemEntity> items = Lists.newArrayList();
        List<MockContainerEntity> containers = Lists.newArrayList();
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            containers.add(newContainer(app, "container"+i, 15, 45));
        }
        for (int i = 0; i < NUM_CONTAINERS*2; i++) {
            items.add(newItemWithPeriodicWorkrates(app, containers.get(i%NUM_CONTAINERS), "item"+i, 20));
        }
        // should now have item0 and item{0+NUM_CONTAINERS} on container0, etc
        
        for (int i = 0; i < NUM_CONTAINERS; i++) {
            // not removing consecutive items as that would leave it balanced!
            int indexToStop = (i < NUM_CONTAINERS/2) ? NUM_CONTAINERS : 0; 
            final MockItemEntity itemToStop = items.remove(indexToStop);
            scheduledExecutor.submit(new Callable<Void>() {
                @Override public Void call() {
                    try {
                        itemToStop.stop();
                        Entities.unmanage(itemToStop);
                    } catch (Throwable t) {
                        LOG.error("Error stopping item "+itemToStop, t);
                    }
                    return null;
                }});
        }
        
        assertWorkratesEventually(containers, items, Collections.nCopies(NUM_CONTAINERS, 20d), WORKRATE_JITTER);
    }
    
    protected MockItemEntity newItemWithPeriodicWorkrates(TestApplication app, MockContainerEntity container, String name, double workrate) {
        MockItemEntity item = newItem(app, container, name, workrate);
        scheduleItemWorkrateUpdates(item, workrate, WORKRATE_JITTER);
        return item;
    }
    
    private void scheduleItemWorkrateUpdates(final MockItemEntity item, final double workrate, final double jitter) {
        final AtomicReference<Future<?>> futureRef = new AtomicReference<Future<?>>();
        Future<?> future = scheduledExecutor.scheduleAtFixedRate(
                new Runnable() {
                    @Override public void run() {
                        if (item.isStopped() && futureRef.get() != null) {
                            futureRef.get().cancel(true);
                            return;
                        }
                        double jitteredWorkrate = workrate + (random.nextDouble()*jitter*2 - jitter);
                        item.sensors().set(TEST_METRIC, (int) Math.max(0, jitteredWorkrate));
                    }
                },
                0, WORKRATE_UPDATE_PERIOD_MS, TimeUnit.MILLISECONDS);
        futureRef.set(future);
    }
}
