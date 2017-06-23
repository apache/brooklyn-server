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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.BlockingEntity;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.QuorumCheck;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

public class DynamicFabricTest extends AbstractDynamicClusterOrFabricTest {
    private static final Logger log = LoggerFactory.getLogger(DynamicFabricTest.class);

    private static final int TIMEOUT_MS = 5*1000;

    private Location loc1;
    private Location loc2;
    private Location loc3;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc1 = new SimulatedLocation();
        loc2 = new SimulatedLocation();
        loc3 = new SimulatedLocation();
    }

    @Test
    public void testDynamicFabricCreatesAndStartEntityWhenGivenSingleLocation() throws Exception {
        runStartWithLocations(ImmutableList.of(loc1));
    }

    @Test
    public void testDynamicFabricCreatesAndStartsEntityWhenGivenManyLocations() throws Exception {
        runStartWithLocations(ImmutableList.of(loc1,loc2,loc3));
    }

    private void runStartWithLocations(Collection<Location> locs) {
        Collection<Location> unclaimedLocs = Lists.newArrayList(locs);
        
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure("memberSpec", EntitySpec.create(TestEntity.class)));
        app.start(locs);

        assertEquals(fabric.getChildren().size(), locs.size(), "children="+fabric.getChildren());
        assertEquals(fabric.getMembers().size(), locs.size(), "members="+fabric.getMembers());
        assertEquals(ImmutableSet.copyOf(fabric.getMembers()), ImmutableSet.copyOf(fabric.getChildren()), "members="+fabric.getMembers()+"; children="+fabric.getChildren());

        for (Entity it : fabric.getChildren()) {
            TestEntity child = (TestEntity) it;
            assertEquals(child.getCounter().get(), 1);
            assertEquals(child.getLocations().size(), 1, "childLocs="+child.getLocations());
            assertTrue(unclaimedLocs.removeAll(child.getLocations()));
        }
        assertTrue(unclaimedLocs.isEmpty(), "unclaimedLocs="+unclaimedLocs);
    }

    @Test
    public void testSizeEnricher() throws Exception {
        Collection<Location> locs = ImmutableList.of(loc1, loc2, loc3);
        final DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
                .configure("memberSpec", EntitySpec.create(DynamicCluster.class)
                        .configure("initialSize", 0)
                        .configure("memberSpec", EntitySpec.create(TestEntity.class))));
        app.start(locs);

        final AtomicInteger i = new AtomicInteger();
        final AtomicInteger total = new AtomicInteger();

        assertEquals(fabric.getChildren().size(), locs.size(), "children="+fabric.getChildren());
        for (Entity it : fabric.getChildren()) {
            Cluster child = (Cluster) it;
            int childSize = i.incrementAndGet();
            total.addAndGet(childSize);
            child.resize(childSize);
        }

        Asserts.succeedsEventually(MutableMap.of("timeout", TIMEOUT_MS), new Runnable() {
            @Override
            public void run() {
                assertEquals(fabric.getAttribute(DynamicFabric.FABRIC_SIZE), (Integer) total.get());
                assertEquals(fabric.getFabricSize(), (Integer) total.get());
            }});
    }

    @Test
    public void testDynamicFabricStartsEntitiesInParallel() throws Exception {
        Collection<Location> locs = ImmutableList.of(loc1, loc2);
        int numLocs = locs.size();
        CountDownLatch executingStartupNotificationLatch = new CountDownLatch(numLocs);
        CountDownLatch startupLatch = new CountDownLatch(1);
        
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure("memberSpec", EntitySpec.create(BlockingEntity.class)
                    .configure(BlockingEntity.EXECUTING_STARTUP_NOTIFICATION_LATCH, executingStartupNotificationLatch)
                    .configure(BlockingEntity.STARTUP_LATCH, startupLatch)));

        final Task<?> task = fabric.invoke(Startable.START, ImmutableMap.of("locations", locs));

        boolean executing = executingStartupNotificationLatch.await(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertTrue(executing);
        assertFalse(task.isDone());

        startupLatch.countDown();
        task.get(Asserts.DEFAULT_LONG_TIMEOUT);

        assertEquals(fabric.getChildren().size(), locs.size(), "children="+fabric.getChildren());

        for (Entity it : fabric.getChildren()) {
            assertEquals(((TestEntity)it).getCounter().get(), 1);
        }
    }

    @Test(groups="Integration")
    public void testDynamicFabricStartsAndStopsEntitiesInParallelManyTimes() throws Exception {
        for (int i = 0; i < 100; i++) {
            log.info("running testDynamicFabricStartsAndStopsEntitiesInParallel iteration {}", i);
            
            testDynamicFabricStartsEntitiesInParallel();
            for (Entity child : app.getChildren()) {
                Entities.unmanage(child);
            }
            
            testDynamicFabricStopsEntitiesInParallel();
            for (Entity child : app.getChildren()) {
                Entities.unmanage(child);
            }
        }
    }

    @Test
    public void testDynamicFabricStopsEntitiesInParallel() throws Exception {
        Collection<Location> locs = ImmutableList.of(loc1, loc2);
        int numLocs = locs.size();
        final CountDownLatch shutdownLatch = new CountDownLatch(1);
        final CountDownLatch executingShutdownNotificationLatch = new CountDownLatch(numLocs);
        final DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
                .configure("memberSpec", EntitySpec.create(BlockingEntity.class)
                        .configure(BlockingEntity.SHUTDOWN_LATCH, shutdownLatch)
                        .configure(BlockingEntity.EXECUTING_SHUTDOWN_NOTIFICATION_LATCH, executingShutdownNotificationLatch)));

        // Start the fabric (and check we have the required num things to concurrently stop)
        fabric.start(locs);
        assertEquals(fabric.getChildren().size(), locs.size());

        // On stop, expect each child to get as far as blocking on its latch
        final Task<?> task = fabric.invoke(Startable.STOP, ImmutableMap.<String,Object>of());

        boolean executing = executingShutdownNotificationLatch.await(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        assertTrue(executing);
        assertFalse(task.isDone());

        // When we release the latches, expect shutdown to complete
        shutdownLatch.countDown();
        task.get(Asserts.DEFAULT_LONG_TIMEOUT);

        Asserts.succeedsEventually(new Runnable() {
            @Override
            public void run() {
                for (Entity it : fabric.getChildren()) {
                    int count = ((TestEntity)it).getCounter().get();
                    assertEquals(count, 0, it+" counter reports "+count);
                }
            }});
    }

    @Test
    public void testDynamicFabricDoesNotAcceptUnstartableChildren() throws Exception {
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
                .configure("memberSpec", EntitySpec.create(BasicEntity.class)));

        try {
            fabric.start(ImmutableList.of(loc1));
            assertEquals(fabric.getChildren().size(), 1);
        } catch (Exception e) {
            IllegalStateException unwrapped = Exceptions.getFirstThrowableOfType(e, IllegalStateException.class);
            if (unwrapped == null || !unwrapped.toString().contains("is not Startable")) {
                throw e;
            }
        }
    }

    // For follow-the-sun, a valid pattern is to associate the FollowTheSunModel as a child of the dynamic-fabric.
    // Thus we have "unstoppable" entities. Let's be relaxed about it, rather than blowing up.
    @Test
    public void testDynamicFabricIgnoresExtraUnstoppableChildrenOnStop() throws Exception {
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
                .configure("memberSpec", EntitySpec.create(TestEntity.class)));

        fabric.start(ImmutableList.of(loc1));
        
        BasicEntity extraChild = fabric.addChild(EntitySpec.create(BasicEntity.class));

        fabric.stop();
    }

    @Test
    public void testDynamicFabricPropagatesProperties() throws Exception {
        final EntitySpec<TestEntity> entitySpec = EntitySpec.create(TestEntity.class)
                .configure("b", "avail");

        final EntitySpec<DynamicCluster> clusterSpec = EntitySpec.create(DynamicCluster.class)
                .configure("initialSize", 1)
                .configure("memberSpec", entitySpec)
                .configure("customChildFlags", ImmutableMap.of("fromCluster", "passed to base entity"))
                .configure("a", "ignored");
                    // FIXME What to do about overriding DynamicCluster to do customChildFlags?
    //            new DynamicClusterImpl(clusterProperties) {
    //                protected Map getCustomChildFlags() { [fromCluster: "passed to base entity"] }
            
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure("memberSpec", clusterSpec)
            .configure("customChildFlags", ImmutableMap.of("fromFabric", "passed to cluster but not base entity"))
            .configure(Attributes.HTTP_PORT, PortRanges.fromInteger(1234))); // for inheritance by children (as a port range)

        app.start(ImmutableList.of(loc1));

        assertEquals(fabric.getChildren().size(), 1);
        DynamicCluster child = (DynamicCluster) getChild(fabric, 0);
        assertEquals(child.getMembers().size(), 1);
        assertEquals(getMember(child, 0).getConfig(Attributes.HTTP_PORT.getConfigKey()), PortRanges.fromInteger(1234));
        assertEquals(((TestEntity)getMember(child, 0)).getConfigureProperties().get("a"), null);
        assertEquals(((TestEntity)getMember(child, 0)).getConfigureProperties().get("b"), "avail");
        assertEquals(((TestEntity)getMember(child, 0)).getConfigureProperties().get("fromCluster"), "passed to base entity");
        assertEquals(((TestEntity)getMember(child, 0)).getConfigureProperties().get("fromFabric"), null);

        child.resize(2);
        assertEquals(child.getMembers().size(), 2);
        assertEquals(getGrandchild(fabric, 0, 1).getConfig(Attributes.HTTP_PORT.getConfigKey()), PortRanges.fromInteger(1234));
        assertEquals(((TestEntity)getMember(child, 1)).getConfigureProperties().get("a"), null);
        assertEquals(((TestEntity)getMember(child, 1)).getConfigureProperties().get("b"), "avail");
        assertEquals(((TestEntity)getMember(child, 1)).getConfigureProperties().get("fromCluster"), "passed to base entity");
        assertEquals(((TestEntity)getMember(child, 1)).getConfigureProperties().get("fromFabric"), null);
    }

    @Test
    public void testExistingChildrenStarted() throws Exception {
        List<Location> locs = ImmutableList.of(loc1, loc2, loc3);
        
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure(DynamicFabric.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        
        List<TestEntity> existingChildren = Lists.newArrayList();
        for (int i = 0; i < 3; i++) {
            existingChildren.add(fabric.addChild(EntitySpec.create(TestEntity.class)));
        }
        app.start(locs);

        // Expect only these existing children
        Asserts.assertEqualsIgnoringOrder(fabric.getChildren(), existingChildren);
        Asserts.assertEqualsIgnoringOrder(fabric.getMembers(), existingChildren);

        // Expect one location per existing child
        List<Location> remainingLocs = MutableList.copyOf(locs);
        for (Entity existingChild : existingChildren) {
            Collection<Location> childLocs = existingChild.getLocations();
            assertEquals(childLocs.size(), 1, "childLocs="+childLocs);
            assertTrue(remainingLocs.removeAll(childLocs));
        }
    }

    @Test
    public void testExistingChildrenStartedRoundRobiningAcrossLocations() throws Exception {
        List<Location> locs = ImmutableList.of(loc1, loc2);
        
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure(DynamicFabric.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        
        List<TestEntity> existingChildren = Lists.newArrayList();
        for (int i = 0; i < 4; i++) {
            existingChildren.add(fabric.addChild(EntitySpec.create(TestEntity.class)));
        }
        app.start(locs);

        // Expect only these existing children
        Asserts.assertEqualsIgnoringOrder(fabric.getChildren(), existingChildren);
        Asserts.assertEqualsIgnoringOrder(fabric.getMembers(), existingChildren);

        // Expect one location per existing child (round-robin)
        // Expect one location per existing child
        List<Location> remainingLocs = MutableList.<Location>builder().addAll(locs).addAll(locs).build();
        for (Entity existingChild : existingChildren) {
            Collection<Location> childLocs = existingChild.getLocations();
            assertEquals(childLocs.size(), 1, "childLocs="+childLocs);
            assertTrue(remainingLocs.remove(Iterables.get(childLocs, 0)), "childLocs="+childLocs+"; remainingLocs="+remainingLocs+"; allLocs="+locs);
        }
    }

    @Test
    public void testExistingChildrenToppedUpWhenNewMembersIfMoreLocations() throws Exception {
        List<Location> locs = ImmutableList.of(loc1, loc2, loc3);
        
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
            .configure(DynamicFabric.MEMBER_SPEC, EntitySpec.create(TestEntity.class)));
        
        TestEntity existingChild = fabric.addChild(EntitySpec.create(TestEntity.class));
        
        app.start(locs);

        // Expect three children: the existing one, and one per other location
        assertEquals(fabric.getChildren().size(), 3, "children="+fabric.getChildren());
        assertTrue(fabric.getChildren().contains(existingChild), "children="+fabric.getChildren()+"; existingChild="+existingChild);
        Asserts.assertEqualsIgnoringOrder(fabric.getMembers(), fabric.getChildren());

        List<Location> remainingLocs = MutableList.<Location>builder().addAll(locs).build();
        for (Entity child : fabric.getChildren()) {
            Collection<Location> childLocs = child.getLocations();
            assertEquals(childLocs.size(), 1, "childLocs="+childLocs);
            assertTrue(remainingLocs.remove(Iterables.get(childLocs, 0)), "childLocs="+childLocs+"; remainingLocs="+remainingLocs+"; allLocs="+locs);
        }
    }

    private Entity getGrandchild(Entity entity, int childIndex, int grandchildIndex) {
        Entity child = getChild(entity, childIndex);
        return Iterables.get(child.getChildren(), grandchildIndex);
    }

    @Test
    public void testDifferentFirstMemberSpec() throws Exception {
        DynamicFabric fabric = app.createAndManageChild(EntitySpec.create(DynamicFabric.class)
                .configure(DynamicFabric.FIRST_MEMBER_SPEC,
                        EntitySpec.create(BasicEntity.class).configure(TestEntity.CONF_NAME, "first"))
                .configure(DynamicFabric.MEMBER_SPEC,
                        EntitySpec.create(BasicEntity.class).configure(TestEntity.CONF_NAME, "non-first"))
                .configure(DynamicFabric.UP_QUORUM_CHECK, QuorumCheck.QuorumChecks.alwaysTrue()));
        List<Location> locs = ImmutableList.of(loc1, loc2, loc3);
        fabric.start(locs);

        EntityAsserts.assertAttributeEqualsEventually(fabric, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.RUNNING);
        assertTrue(fabric.getAttribute(Attributes.SERVICE_UP));

        assertEquals(fabric.getMembers().size(), 3);

        assertFirstAndNonFirstCounts(fabric.getMembers(), 1, 2);
    }

    private Entity getChild(Entity entity, int childIndex) {
        return Iterables.get(entity.getChildren(), childIndex);
    }
    
    private Entity getMember(Group entity, int memberIndex) {
        return Iterables.get(entity.getMembers(), memberIndex);
    }
}
