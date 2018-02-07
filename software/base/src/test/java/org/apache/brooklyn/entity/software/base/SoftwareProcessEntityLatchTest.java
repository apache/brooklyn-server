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
package org.apache.brooklyn.entity.software.base;

import static org.apache.brooklyn.core.mgmt.BrooklynTaskTags.getEffectorName;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.sensor.ReleaseableLatch;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.MyService;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.MyServiceImpl;
import org.apache.brooklyn.entity.software.base.SoftwareProcessEntityTest.SimulatedDriver;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskInternal;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;


public class SoftwareProcessEntityLatchTest extends BrooklynAppUnitTestSupport {


    // NB: These tests don't actually require ssh to localhost -- only that 'localhost' resolves.

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessEntityLatchTest.class);

    private static final ImmutableList<String> SOFTWARE_PROCESS_START_TASKS = ImmutableList.of("setup", "copyInstallResources", "install", "customize", "copyRuntimeResources", "launch");
    private static final ImmutableList<String> SOFTWARE_PROCESS_STOP_TASKS = ImmutableList.<String>builder().addAll(SOFTWARE_PROCESS_START_TASKS).add("stop").build();

    private SshMachineLocation machine;
    private FixedListMachineProvisioningLocation<SshMachineLocation> loc;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = getLocation();
    }

    @SuppressWarnings("unchecked")
    private FixedListMachineProvisioningLocation<SshMachineLocation> getLocation() {
        FixedListMachineProvisioningLocation<SshMachineLocation> loc = mgmt.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class));
        machine = mgmt.getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                .configure("address", "localhost"));
        loc.addMachine(machine);
        return loc;
    }
    
    @DataProvider
    public Object[][] latchAndTaskNamesProvider() {
        return new Object[][] {
            {SoftwareProcess.START_LATCH, ImmutableList.<String>of()},
            {SoftwareProcess.SETUP_LATCH, ImmutableList.<String>of()},
            {SoftwareProcess.INSTALL_RESOURCES_LATCH, ImmutableList.of("setup")},
            {SoftwareProcess.INSTALL_LATCH, ImmutableList.of("setup", "copyInstallResources")},
            {SoftwareProcess.CUSTOMIZE_LATCH, ImmutableList.of("setup", "copyInstallResources", "install")},
            {SoftwareProcess.RUNTIME_RESOURCES_LATCH, ImmutableList.of("setup", "copyInstallResources", "install", "customize")},
            {SoftwareProcess.LAUNCH_LATCH, ImmutableList.of("setup", "copyInstallResources", "install", "customize", "copyRuntimeResources")},
            {SoftwareProcess.STOP_LATCH, SOFTWARE_PROCESS_START_TASKS},
        };
    }

    @Test(dataProvider="latchAndTaskNamesProvider")
    public void testBooleanLatchBlocks(final ConfigKey<Boolean> latch, List<String> preLatchEvents) throws Exception {
        doTestLatchBlocks(latch, preLatchEvents, Boolean.TRUE, Functions.<Void>constant(null));
    }

    @Test(dataProvider="latchAndTaskNamesProvider")
    public void testReleaseableLatchBlocks(final ConfigKey<Boolean> latch, final List<String> preLatchEvents) throws Exception {
        final ReleaseableLatch latchSemaphore = ReleaseableLatch.Factory.newMaxConcurrencyLatch(0);
        doTestLatchBlocks(latch, preLatchEvents, latchSemaphore, new Function<MyService, Void>() {
            @Override
            public Void apply(MyService entity) {
                String taskName = (latch == SoftwareProcess.STOP_LATCH) ? "stop" : "start";
                assertEffectorBlockingDetailsEventually(entity, taskName, "Acquiring " + latch + " " + latchSemaphore);
                assertDriverEventsEquals(entity, preLatchEvents);
                latchSemaphore.release(entity);
                return null;
            }
        });

    }

    public void doTestLatchBlocks(ConfigKey<Boolean> latch, List<String> preLatchEvents, Object latchValue, Function<? super MyService, Void> customAssertFn) throws Exception {
        final AttributeSensor<Object> latchSensor = Sensors.newSensor(Object.class, "latch");
        final MyService entity = app.createAndManageChild(EntitySpec.create(MyService.class)
                .configure(ConfigKeys.newConfigKey(Object.class, latch.getName()), (Object)DependentConfiguration.attributeWhenReady(app, latchSensor)));

        final Task<Void> task;
        final Task<Void> startTask = Entities.invokeEffector(app, app, MyService.START, ImmutableMap.of("locations", ImmutableList.of(loc)));
        if (latch != SoftwareProcess.STOP_LATCH) {
            task = startTask;
        } else {
            startTask.get(Duration.THIRTY_SECONDS);
            task = Entities.invokeEffector(app, app, MyService.STOP);
        }

        assertEffectorBlockingDetailsEventually(entity, task.getDisplayName(), "Waiting for config " + latch.getName());
        assertDriverEventsEquals(entity, preLatchEvents);
        assertFalse(task.isDone());

        app.sensors().set(latchSensor, latchValue);

        customAssertFn.apply(entity);

        task.get(Duration.THIRTY_SECONDS);
        assertDriverEventsEquals(entity, getLatchPostTasks(latch));
    }
    
    /**
     * Deploy a cluster of 4 SoftwareProcesses, which all share the same CountingLatch instance so
     * that only two can obtain it at a time. The {@link CountingLatch} is uses the default
     * of sleeping for 100ms after acquiring the lock, so it is very likely in a non-contended 
     * machine that we'll hit max concurrency. We assert that the maximum number of things holding
     * the lock at any one time was exactly 2.
     * 
     * This is marked as an "integration" test because it is time-sensitive. If executed on a
     * low-performance machine (e.g. Apache Jenkins, where other docker containers are contending
     * for resources) then it might not get to 2 things holding the lock at the same time.
     */
    @Test(groups="Integration", dataProvider="latchAndTaskNamesProvider", timeOut=Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    public void testConcurrency(ConfigKey<Boolean> latch, List<String> _ignored) throws Exception {
        LocalhostMachineProvisioningLocation loc = app.newLocalhostProvisioningLocation(ImmutableMap.of("address", "127.0.0.1"));
        
        final int maxConcurrency = 2;
        final ReleaseableLatch latchSemaphore = ReleaseableLatch.Factory.newMaxConcurrencyLatch(maxConcurrency);
        final AttributeSensor<Object> latchSensor = Sensors.newSensor(Object.class, "latch");
        final CountingLatch countingLatch = new CountingLatch(latchSemaphore, maxConcurrency);
        @SuppressWarnings({"unused"})
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.INITIAL_SIZE, maxConcurrency*2)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(MyService.class)
                        .configure(ConfigKeys.newConfigKey(Object.class, latch.getName()), (Object)DependentConfiguration.attributeWhenReady(app, latchSensor))));
        app.sensors().set(latchSensor, countingLatch);
        final Task<Void> startTask = Entities.invokeEffector(app, app, MyService.START, ImmutableMap.of("locations", ImmutableList.of(loc)));
        startTask.get();
        final Task<Void> stopTask = Entities.invokeEffector(app, app, MyService.STOP, ImmutableMap.<String, Object>of());
        stopTask.get();
        assertEquals(countingLatch.getCounter(), 0);
        // Check we have actually used the latch
        assertNotEquals(countingLatch.getMaxCounter(), 0, "Latch not acquired at all");
        // In theory this is 0 < maxCnt <= maxConcurrency contract, but in practice
        // we should always reach the maximum due to the sleeps in CountingLatch.
        // Change if found to fail in the wild.
        assertEquals(countingLatch.getMaxCounter(), maxConcurrency);
    }

    /**
     * Deploy a cluster of 4 SoftwareProcesses, which all share the same CountingLatch instance so
     * that only two can obtain it at a time. The {@link CountingLatch} is configured with a really
     * long sleep after it acquires the lock, so we should have just 2 entities having acquired it
     * and the others blocked.
     * 
     * We assert that we got into this state, and then we tear it down by unmanaging the cluster 
     * (we unmanage because the cluster would otherwise takes ages due to the sleep in 
     * {@link CountingLatch}.
     */
    @Test(dataProvider="latchAndTaskNamesProvider", timeOut=Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    public void testConcurrencyAllowsExactlyMax(ConfigKey<Boolean> latch, List<String> _ignored) throws Exception {
        boolean isLatchOnStop = latch.getName().contains("stop");
        LocalhostMachineProvisioningLocation loc = app.newLocalhostProvisioningLocation(ImmutableMap.of("address", "127.0.0.1"));
        
        final int maxConcurrency = 2;
        final ReleaseableLatch latchSemaphore = ReleaseableLatch.Factory.newMaxConcurrencyLatch(maxConcurrency);
        final AttributeSensor<Object> latchSensor = Sensors.newSensor(Object.class, "latch");
        final CountingLatch countingLatch = new CountingLatch(latchSemaphore, maxConcurrency, Asserts.DEFAULT_LONG_TIMEOUT.multiply(2));
        
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.INITIAL_SIZE, maxConcurrency*2)
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(MyService.class)
                        .configure(ConfigKeys.newConfigKey(Object.class, latch.getName()), (Object)DependentConfiguration.attributeWhenReady(app, latchSensor))));
        app.sensors().set(latchSensor, countingLatch);
        
        try {
            if (isLatchOnStop) {
                // Start will complete; then invoke stop async (don't expect it to complete!)
                app.start(ImmutableList.of(loc));
                Entities.invokeEffector(app, app, MyService.STOP, ImmutableMap.<String, Object>of());
            } else {
                // Invoke start async (don't expect it to complete!)
                Entities.invokeEffector(app, app, MyService.START, ImmutableMap.of("locations", ImmutableList.of(loc)));
            }
            
            // Because CountingLatch waits for ages, we'll eventually have maxConcurrent having successfully 
            // acquired, but the others blocked. Wait for that, and assert it stays that way.
            countingLatch.assertMaxCounterEventually(Predicates.equalTo(maxConcurrency));
            countingLatch.assertMaxCounterContinually(Predicates.equalTo(maxConcurrency), Duration.millis(100));
        } finally {
            // Don't wait for cluster to start/stop (because of big sleep in CountingLatch) - unmanage it.
            Entities.unmanage(cluster);
        }
    }

    @Test(dataProvider="latchAndTaskNamesProvider"/*, timeOut=Asserts.THIRTY_SECONDS_TIMEOUT_MS*/)
    public void testFailedReleaseableUnblocks(final ConfigKey<Boolean> latch, List<String> _ignored) throws Exception {
        LocalhostMachineProvisioningLocation loc = app.newLocalhostProvisioningLocation(ImmutableMap.of("address", "127.0.0.1"));

        final int maxConcurrency = 1;
        final ReleaseableLatch latchSemaphore = ReleaseableLatch.Factory.newMaxConcurrencyLatch(maxConcurrency);
        final AttributeSensor<Object> latchSensor = Sensors.newSensor(Object.class, "latch");
        final CountingLatch countingLatch = new CountingLatch(latchSemaphore, maxConcurrency);
        // FIRST_MEMBER_SPEC latches are not guaranteed to be acquired before MEMBER_SPEC latches
        // so the start effector could complete, but the counting latch will catch if there are
        // any unreleased semaphores.
        @SuppressWarnings({"unused"})
        DynamicCluster cluster = app.createAndManageChild(EntitySpec.create(DynamicCluster.class)
                .configure(DynamicCluster.INITIAL_SIZE, 2)
                .configure(DynamicCluster.FIRST_MEMBER_SPEC, EntitySpec.create(FailingMyService.class)
                        .configure(ConfigKeys.newConfigKey(Object.class, latch.getName()), (Object)DependentConfiguration.attributeWhenReady(app, latchSensor)))
                .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(MyService.class)
                        .configure(ConfigKeys.newConfigKey(Object.class, latch.getName()), (Object)DependentConfiguration.attributeWhenReady(app, latchSensor))));
        app.sensors().set(latchSensor, countingLatch);
        final Task<Void> startTask = Entities.invokeEffector(app, app, MyService.START, ImmutableMap.of("locations", ImmutableList.of(loc)));
        //expected to fail but should complete quickly
        assertTrue(startTask.blockUntilEnded(Asserts.DEFAULT_LONG_TIMEOUT), "timeout waiting for start effector to complete");
        assertTrue(latch == SoftwareProcess.STOP_LATCH || startTask.isError());
        final Task<Void> stopTask = Entities.invokeEffector(app, app, MyService.STOP, ImmutableMap.<String, Object>of());
        //expected to fail but should complete quickly
        assertTrue(stopTask.blockUntilEnded(Asserts.DEFAULT_LONG_TIMEOUT), "timeout waiting for stop effector to complete");
        // stop task won't fail because the process stop failed; the error is ignored
        assertTrue(stopTask.isDone());
        assertEquals(countingLatch.getCounter(), 0);
        // Check we have actually used the latch
        assertNotEquals(countingLatch.getMaxCounter(), 0, "Latch not acquired at all");
        // In theory this is 0 < maxCnt <= maxConcurrency contract, but in practice
        // we should always reach the maximum due to the sleeps in CountingLatch.
        // Change if found to fail in the wild.
        assertEquals(countingLatch.getMaxCounter(), maxConcurrency);
    }

    protected EntityInitializer createFailingEffectorInitializer(String name) {
        return new AddEffector(AddEffector.newEffectorBuilder(Void.class,
                        ConfigBag.newInstance(ImmutableMap.of(AddEffector.EFFECTOR_NAME, name)))
                .impl(new EffectorBody<Void>() {
                    @Override
                    public Void call(ConfigBag parameters) {
                        throw new IllegalStateException("Failed to start");
                    }
                }).build());
    }

    protected List<String> getLatchPostTasks(final ConfigKey<?> latch) {
        if (latch == SoftwareProcess.STOP_LATCH) {
            return SOFTWARE_PROCESS_STOP_TASKS;
        } else {
            return SOFTWARE_PROCESS_START_TASKS;
        }
    }

    private void assertDriverEventsEquals(MyService entity, List<String> expectedEvents) {
        SimulatedDriver driver = (SimulatedDriver)entity.getDriver();
        if (driver != null) {
            List<String> events = driver.events;
            assertEquals(events, expectedEvents, "events="+events);
        } else {
            assertEquals(expectedEvents.size(), 0);
        }
    }

    private void assertEffectorBlockingDetailsEventually(final Entity entity, final String effectorName, final String blockingDetailsSnippet) {
        Asserts.succeedsEventually(new Runnable() {
            @Override public void run() {
                final Set<Task<?>> tasksWithAllTags = mgmt.getExecutionManager().getTasksWithAllTags(ImmutableList.of(BrooklynTaskTags.EFFECTOR_TAG, BrooklynTaskTags.tagForContextEntity(entity)));
                Task<?> entityTask = null;
                for (Task<?> item : tasksWithAllTags) {
                    final String itemName = getEffectorName(item);
                    entityTask = itemName.equals(effectorName) ? item : entityTask;
                }
                if (entityTask == null) {
                    Asserts.fail("Could not find task for effector " + effectorName);
                }
                String blockingDetails = getBlockingDetails(entityTask);
                assertTrue(blockingDetails.contains(blockingDetailsSnippet));
            }});
    }
    
    private String getBlockingDetails(Task<?> task) {
        List<TaskInternal<?>> taskChain = Lists.newArrayList();
        TaskInternal<?> taskI = (TaskInternal<?>) task;
        while (taskI != null) {
            taskChain.add(taskI);
            if (taskI.getBlockingDetails() != null) {
                return taskI.getBlockingDetails();
            }
            taskI = (TaskInternal<?>) taskI.getBlockingTask();
        }
        throw new IllegalStateException("No blocking details for "+task+" (walked task chain "+taskChain+")");
    }

    private static class CountingLatch implements ReleaseableLatch {
        final ReleaseableLatch delegate;
        final AtomicInteger cnt = new AtomicInteger();
        final AtomicInteger maxCnt = new AtomicInteger();
        final int maxConcurrency;
        final Duration sleepBeforeMaxCnt;

        public CountingLatch(ReleaseableLatch delegate, int maxConcurrency) {
            this(delegate, maxConcurrency, Duration.millis(100));
        }
        
        public CountingLatch(ReleaseableLatch delegate, int maxConcurrency, Duration sleepBeforeMaxCnt) {
            this.delegate = delegate;
            this.maxConcurrency = maxConcurrency;
            this.sleepBeforeMaxCnt = sleepBeforeMaxCnt;
        }

        public void acquire(Entity caller) {
            delegate.acquire(caller);
            
            int val = cnt.incrementAndGet();
            LOG.info("acquired latch by "+caller+"; cnt="+val);
            assertCount(val);
            //assertCount(cnt.incrementAndGet());
        }

        public void release(Entity caller) {
            LOG.info("releasing latch by "+caller+"; preCnt="+cnt.get());
            cnt.decrementAndGet();
            delegate.release(caller);
        }

        public int getMaxCounter() {
            return maxCnt.get();
        }
        public int getCounter() {
            return cnt.get();
        }
        private void assertCount(int newCnt) {
            synchronized(maxCnt) {
                maxCnt.set(Math.max(newCnt, maxCnt.get()));
            }
            assertTrue(newCnt <= maxConcurrency, "maxConcurrency limit failed at " + newCnt + " (max " + maxConcurrency + ")");
            if (newCnt < maxConcurrency) {
                Time.sleep(sleepBeforeMaxCnt);
            } else {
                Time.sleep(Duration.millis(20));
            }
        }

        public void assertMaxCounterEventually(Predicate<? super Integer> condition) {
            Asserts.succeedsEventually(new Runnable() {
                public void run() {
                    assertTrue(condition.apply(maxCnt.get()));
                }});
        }

        public void assertMaxCounterContinually(Predicate<? super Integer> condition, Duration duration) {
            Asserts.succeedsContinually(ImmutableMap.of("timeout", duration), new Runnable() {
                public void run() {
                    assertTrue(condition.apply(maxCnt.get()));
                }});
        }
    }

    @ImplementedBy(FailingMyServiceImpl.class)
    public static interface FailingMyService extends MyService {}
    public static class FailingMyServiceImpl extends MyServiceImpl implements FailingMyService {
        @Override
        public Class<?> getDriverInterface() {
            return FailingSimulatedDriver.class;
        }
    }
    static class FailingSimulatedDriver extends SimulatedDriver {
        public FailingSimulatedDriver(@SuppressWarnings("deprecation") org.apache.brooklyn.api.entity.EntityLocal entity, SshMachineLocation machine) {
            super(entity, machine);
        }

        @Override
        public void stop() {
            super.stop();
            failOnStep(SoftwareProcess.STOP_LATCH);
        }

        @Override
        public void install() {
            super.install();
            failOnStep(SoftwareProcess.INSTALL_LATCH);
        }

        @Override
        public void customize() {
            super.customize();
            failOnStep(SoftwareProcess.CUSTOMIZE_LATCH);
        }

        @Override
        public void launch() {
            super.launch();
            failOnStep(SoftwareProcess.START_LATCH);
            failOnStep(SoftwareProcess.LAUNCH_LATCH);
        }

        @Override
        public void setup() {
            super.setup();
            failOnStep(SoftwareProcess.SETUP_LATCH);
        }

        @Override
        public void copyInstallResources() {
            super.copyInstallResources();
            failOnStep(SoftwareProcess.INSTALL_RESOURCES_LATCH);
        }

        @Override
        public void copyRuntimeResources() {
            super.copyRuntimeResources();
            failOnStep(SoftwareProcess.RUNTIME_RESOURCES_LATCH);
        }

        @Override
        protected String getInstallLabelExtraSalt() {
            return super.getInstallLabelExtraSalt();
        }

        protected void failOnStep(ConfigKey<Boolean> latch) {
            if (((EntityInternal)entity).config().getRaw(latch).isPresent()) {
                DynamicTasks.queue("Failing task", new Runnable() {
                    @Override
                    public void run() {
                        throw new IllegalStateException("forced fail");
                    }
                });
            }
        }

}
}
