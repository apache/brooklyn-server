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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.location.MachineProvisioningLocation;
import org.apache.brooklyn.api.location.NoMachinesAvailableException;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.internal.AttributesInternal;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.Machines;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.software.base.lifecycle.MachineLifecycleEffectorTasks;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.LogWatcher;
import org.apache.brooklyn.test.LogWatcher.EventPredicates;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class SoftwareProcessStopsDuringStartTest extends BrooklynAppUnitTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(SoftwareProcessStopsDuringStartTest.class);
    
    private DelayedProvisioningLocation loc;
    private EmptySoftwareProcess entity;
    private ExecutorService executor;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loc = mgmt.getLocationManager().createLocation(LocationSpec.create(DelayedProvisioningLocation.class));
        entity = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
            .configure(EmptySoftwareProcess.START_TIMEOUT, Asserts.DEFAULT_SHORT_TIMEOUT));
        executor = Executors.newCachedThreadPool();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        if (executor != null) {
            executor.shutdownNow();
        }
        super.tearDown();
    }
    
    @Test
    public void testSequentialStartThenStop() throws Exception {
        loc.getObtainResumeLatch(0).countDown();
        
        entity.start(ImmutableList.<Location>of(loc));
        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, machine);
        
        Stopwatch stopwatch = Stopwatch.createStarted();
        entity.stop();
        Duration stopDuration = Duration.of(stopwatch);
        assertTrue(Asserts.DEFAULT_LONG_TIMEOUT.isLongerThan(stopDuration), "stop took "+stopDuration);
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, null);
        
        assertEquals(loc.getCalls(), ImmutableList.of("obtain", "release"));
    }

    @Test
    public void testSequentialStartStopStartStop() throws Exception {
        // resume-latches created with zero - they will not block
        loc.setObtainResumeLatches(ImmutableList.of(new CountDownLatch(0), new CountDownLatch(0)));
        loc.setObtainCalledLatches(ImmutableList.of(new CountDownLatch(1), new CountDownLatch(1)));
        
        entity.start(ImmutableList.<Location>of(loc));
        SshMachineLocation machine = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, machine);
        
        entity.stop();
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, null);

        entity.start(ImmutableList.<Location>of(loc));
        SshMachineLocation machine2 = Machines.findUniqueMachineLocation(entity.getLocations(), SshMachineLocation.class).get();
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, machine2);

        entity.stop();
        EntityAsserts.assertAttributeEquals(entity, AttributesInternal.INTERNAL_PROVISIONING_TASK_STATE, null);
        EntityAsserts.assertAttributeEquals(entity, MachineLifecycleEffectorTasks.INTERNAL_PROVISIONED_MACHINE, null);

        assertEquals(loc.getCalls(), ImmutableList.of("obtain", "release", "obtain", "release"));
    }

    @Test
    public void testStopDuringProvisionWaitsForCompletion() throws Exception {
        Future<?> startFuture = executor.submit(new Runnable() {
            @Override
            public void run() {
                entity.start(ImmutableList.<Location>of(loc));
            }});
        loc.getObtainCalledLatch(0).await();
        
        // Calling stop - it should block
        // TODO Nicer way of ensuring that stop is really waiting? We wait for the log message!
        Future<?> stopFuture;
        LogWatcher watcher = new LogWatcher(
                MachineLifecycleEffectorTasks.class.getName(), 
                ch.qos.logback.classic.Level.INFO,
                EventPredicates.containsMessage("for the machine to finish provisioning, before terminating it") );
        watcher.start();
        try {
            stopFuture = executor.submit(new Runnable() {
                @Override
                public void run() {
                    entity.stop();
                }});
            watcher.assertHasEventEventually();
        } finally {
            watcher.close();
        }
        assertFalse(stopFuture.isDone());

        // When the loc.obtain() call returns, that will allow stop() to complete
        loc.getObtainResumeLatch(0).countDown();
        stopFuture.get(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS); // should be successful
        try {
            // usually completes quickly, but sometimes can take a long time
            startFuture.get(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (ExecutionException e) {
            // might fail, depending how far it got before stop completed
            LOG.info("start() failed during concurrent stop; acceptable", e);
        } catch (TimeoutException e) {
            // with shorter timeout this shouldn't occur (26 Sept 2016)
            Assert.fail("start() timed out during concurrent stop; acceptable, but test should be fixed", e);
        }
        
        assertEquals(loc.getCalls(), ImmutableList.of("obtain", "release"));
    }

    @Test
    public void testStopDuringProvisionTimesOut() throws Exception {
        entity = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class)
                .configure(MachineLifecycleEffectorTasks.STOP_WAIT_PROVISIONING_TIMEOUT, Duration.millis(100)));

        executor.submit(new Runnable() {
            @Override
            public void run() {
                entity.start(ImmutableList.<Location>of(loc));
            }});
        loc.getObtainCalledLatch(0).await();
        
        LogWatcher watcher = new LogWatcher(
                MachineLifecycleEffectorTasks.class.getName(), 
                ch.qos.logback.classic.Level.WARN,
                EventPredicates.containsMessage("timed out after 100ms waiting for the machine to finish provisioning - machine may we left running") );

        watcher.start();
        try {
            Stopwatch stopwatch = Stopwatch.createStarted();
            entity.stop();
            long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            assertEquals(watcher.getEvents().size(), 1);
            assertTrue(elapsed > (100 - 10), "elapsed="+elapsed);
        } finally {
            watcher.close();
        }
        
        assertEquals(loc.getCalls(), ImmutableList.of("obtain"));
    }
    
    @Test
    public void testStopWhenProvisionFails() throws Exception {
        loc.setObtainToFail(0);
        
        executor.submit(new Runnable() {
            @Override
            public void run() {
                entity.start(ImmutableList.<Location>of(loc));
            }});
        loc.getObtainCalledLatch(0).await();
        
        // Calling stop - it should block
        // TODO Nicer way of ensuring that stop is really waiting? We wait for the log message!
        Future<?> stopFuture;
        LogWatcher watcher = new LogWatcher(
                MachineLifecycleEffectorTasks.class.getName(), 
                ch.qos.logback.classic.Level.INFO,
                EventPredicates.containsMessage("for the machine to finish provisioning, before terminating it") );
        watcher.start();
        try {
            stopFuture = executor.submit(new Runnable() {
                @Override
                public void run() {
                    entity.stop();
                }});
            watcher.assertHasEventEventually();
        } finally {
            watcher.close();
        }
        assertFalse(stopFuture.isDone());

        // When the loc.obtain() call throws exception, that will allow stop() to complete.
        // It must not wait for the full 10 minutes.
        loc.getObtainResumeLatch(0).countDown();
        stopFuture.get(Asserts.DEFAULT_LONG_TIMEOUT.toMilliseconds(), TimeUnit.MILLISECONDS); // should be successful
    }
    
    public static class DelayedProvisioningLocation extends AbstractLocation implements MachineProvisioningLocation<SshMachineLocation> {
        public List<Integer> obtainsToFail = MutableList.of();
        public List<CountDownLatch> obtainCalledLatches = MutableList.of(new CountDownLatch(1));
        public List<CountDownLatch> obtainResumeLatches = MutableList.of(new CountDownLatch(1));
        private Set<SshMachineLocation> obtainedMachines = Sets.newConcurrentHashSet();
        private final List<String> calls = Lists.newCopyOnWriteArrayList();
        private final AtomicInteger obtainCount = new AtomicInteger();
        
        public void setObtainToFail(int index) {
            this.obtainsToFail.add(index);
        }

        public void setObtainResumeLatches(List<CountDownLatch> latches) {
            this.obtainResumeLatches = latches;
        }

        public void setObtainCalledLatches(List<CountDownLatch> latches) {
            this.obtainCalledLatches = latches;
        }

        public CountDownLatch getObtainCalledLatch(int count) {
            return obtainCalledLatches.get(count);
        }

        public CountDownLatch getObtainResumeLatch(int count) {
            return obtainResumeLatches.get(count);
        }

        public List<String> getCalls() {
            return ImmutableList.copyOf(calls);
        }

        @Override
        public SshMachineLocation obtain(Map<?,?> flags) throws NoMachinesAvailableException {
            try {
                int count = obtainCount.getAndIncrement();
                calls.add("obtain");
                getObtainCalledLatch(count).countDown();
                getObtainResumeLatch(count).await();
                
                if (obtainsToFail.contains(count)) {
                    throw new RuntimeException("Simulate failure in obtain");
                }
                
                SshMachineLocation result = getManagementContext().getLocationManager().createLocation(LocationSpec.create(SshMachineLocation.class)
                        .parent(this)
                        .configure(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                        .configure("address","localhost"));
                obtainedMachines.add(result);
                
                SoftwareProcessStopsDuringStartTest.LOG.info("Simulated obtain of machine " + result);
                return result;
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
        }

        @Override
        public void release(SshMachineLocation machine) {
            calls.add("release");
            SoftwareProcessStopsDuringStartTest.LOG.info("Simulated release of machine " + machine);
            boolean removed = obtainedMachines.remove(machine);
            if (!removed) {
                throw new IllegalStateException("Unknown machine "+machine);
            }
        }

        @Override
        public Map<String,Object> getProvisioningFlags(Collection<String> tags) {
            return Collections.emptyMap();
        }

        @Override
        public MachineProvisioningLocation<SshMachineLocation> newSubLocation(Map<?, ?> newFlags) {
            throw new UnsupportedOperationException();
        }
    }
}
