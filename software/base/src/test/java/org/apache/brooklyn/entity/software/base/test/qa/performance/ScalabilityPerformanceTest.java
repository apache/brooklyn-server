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
package org.apache.brooklyn.entity.software.base.test.qa.performance;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.BrooklynGarbageCollector;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.qa.performance.AbstractPerformanceTest;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.entity.software.base.VanillaSoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.performance.PerformanceTestDescriptor;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Atomics;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * These tests are work-in-progress - they are currently more useful for investigating 
 * performance limits (and the behaviour at that limit) than for regression/automated tests.
 * 
 * For example:
 * <ol>
 *   <li>Tweak the {@link #NUM_ITERATIONS} and {@link #NUM_CONCURRENT_JOBS} values
 *   <li>Set {@code -Xms} and {@code -Xmx}
 *   <li>Run the desired test method
 *   <li>Examine the logs (as the test runs, if you want), 
 *       e.g. {@code grep -E "iteration=|CPU fraction|brooklyn gc .after" brooklyn.debug.log}
 *   <li>Examine things like the thread and memory usage, 
 *       e.g. {@code TEST_PID=ps aux | grep [t]estng | awk '{print $2}'; jmap -histo:live ${JAVA_PID}; jstack-active ${JAVA_PID}}
 * </ol>
 * 
 * Over time, we should establish a base-line for scalability and performance at scale, and use 
 * these for regression testing.
 */
public class ScalabilityPerformanceTest extends AbstractPerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ScalabilityPerformanceTest.class);

    // Adds up to 2000 apps in each test (200*10)
    private static final int NUM_ITERATIONS = 200;
    private static final int NUM_CONCURRENT_JOBS = 10;
    
    ListeningExecutorService executor;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
    }
    
    @AfterMethod(alwaysRun=true, timeOut=Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    @Override
    public void tearDown() throws Exception {
        if (executor != null) executor.shutdownNow();
        super.tearDown();
    }
    
    @Override
    protected BrooklynProperties getBrooklynProperties() {
        BrooklynProperties result = super.getBrooklynProperties();
        result.put(BrooklynGarbageCollector.GC_PERIOD, Duration.FIVE_SECONDS);
        return result;
    }

    @Test(groups={"Integration", "Acceptance"})
    public void testManyEmptyApps() {
        int numIterations = NUM_ITERATIONS;
        double minRatePerSec = 2 * PERFORMANCE_EXPECTATION;
        final AtomicInteger counter = new AtomicInteger();
        
        app.start(ImmutableList.of(loc));

        measure(PerformanceTestDescriptor.create()
                .summary("ScalabilityPerformanceTest.testManyEmptyApps")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .numConcurrentJobs(NUM_CONCURRENT_JOBS)
                .abortIfIterationLongerThan(Duration.seconds(5))
                .postWarmup(new Runnable() {
                    @Override
                    public void run() {
                        destroyApps(mgmt.getApplications());
                    }})
                .job(new Runnable() {
                    @Override
                    public void run() {
                        newEmptyApp(counter.incrementAndGet());
                    }}));
    }
    
    @Test(groups={"Integration", "Acceptance"})
    public void testManyBasicClusterApps() {
        int numIterations = NUM_ITERATIONS;
        double minRatePerSec = 1 * PERFORMANCE_EXPECTATION;
        final AtomicInteger counter = new AtomicInteger();
        
        app.start(ImmutableList.of(loc));

        measure(PerformanceTestDescriptor.create()
                .summary("ScalabilityPerformanceTest.testManyBasicClusterApps")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .numConcurrentJobs(NUM_CONCURRENT_JOBS)
                .abortIfIterationLongerThan(Duration.seconds(5))
                .postWarmup(new Runnable() {
                    @Override
                    public void run() {
                        destroyApps(mgmt.getApplications());
                    }})
                .job(new Runnable() {
                    @Override
                    public void run() {
                        newClusterApp(counter.incrementAndGet());
                    }}));
    }
    
    @Test(groups={"Integration", "Acceptance"})
    public void testManySshApps() {
        int numIterations = NUM_ITERATIONS;
        double minRatePerSec = 1 * PERFORMANCE_EXPECTATION;
        final AtomicInteger counter = new AtomicInteger();
        
        app.start(ImmutableList.of(loc));

        measure(PerformanceTestDescriptor.create()
                .summary("ScalabilityPerformanceTest.testManySshApps")
                .iterations(numIterations)
                .minAcceptablePerSecond(minRatePerSec)
                .numConcurrentJobs(NUM_CONCURRENT_JOBS)
                .abortIfIterationLongerThan(Duration.seconds(5))
                .postWarmup(new Runnable() {
                    @Override
                    public void run() {
                        destroyApps(mgmt.getApplications());
                    }})
                .job(new Runnable() {
                    @Override
                    public void run() {
                        newVanillaSoftwareProcessApp(counter.incrementAndGet());
                    }}));
    }
    
    private TestApplication newEmptyApp(int suffix) {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(EntitySpec.create(TestApplication.class)
                .displayName("app-"+suffix)));
        app.start(ImmutableList.of(app.newLocalhostProvisioningLocation()));
        return app;
    }
    
    private TestApplication newClusterApp(int suffix) {
        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(EntitySpec.create(TestApplication.class)
                .displayName("app-"+suffix)
                .child(EntitySpec.create(DynamicCluster.class)
                        .configure(DynamicCluster.INITIAL_SIZE, 1)
                        .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(BasicStartable.class)))));
        app.start(ImmutableList.of(app.newLocalhostProvisioningLocation()));
        return app;
    }
    
    private TestApplication newVanillaSoftwareProcessApp(int suffix) {
        Location loc = mgmt.getLocationManager().createLocation(LocationSpec.create(FixedListMachineProvisioningLocation.class)
                .configure(FixedListMachineProvisioningLocation.MACHINE_SPECS, ImmutableList.of(
                        LocationSpec.create(SshMachineLocation.class)
                                .configure("address", "1.2.3.4")
                                .configure("sshToolClass", RecordingSshTool.class.getName()))));

        TestApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(EntitySpec.create(TestApplication.class)
                .displayName("app-"+suffix)
                .child(EntitySpec.create(VanillaSoftwareProcess.class)
                        .configure(VanillaSoftwareProcess.INSTALL_COMMAND, "myInstall")
                        .configure(VanillaSoftwareProcess.LAUNCH_COMMAND, "myLaunch")
                        .configure(VanillaSoftwareProcess.CHECK_RUNNING_COMMAND, "myCheckRunning")
                        .configure(VanillaSoftwareProcess.STOP_COMMAND, "myStop")
                        .configure(DynamicCluster.MEMBER_SPEC, EntitySpec.create(BasicStartable.class)))));
        app.start(ImmutableList.of(loc));
        return app;
    }
    
    // TODO duplicates part of Entities.destroyAll(ManagementContext).
    // But we want to just destroy the apps rather than the management context.
    // This is useful after warm-up (before the main test) so we don't have any extra apps around.
    private void destroyApps(Iterable<? extends Application> apps) {
        final int MAX_THREADS = 100;
        
        ListeningExecutorService executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(MAX_THREADS));
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        final AtomicReference<Exception> error = Atomics.newReference();
        try {
            for (final Application app: apps) {
                futures.add(executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        ManagementContext mgmt = app.getManagementContext();
                        LOG.debug("destroying app "+app+" (managed? "+Entities.isManaged(app)+"; mgmt is "+mgmt+")");
                        try {
                            Entities.destroy(app);
                            LOG.debug("destroyed app "+app+"; mgmt now "+mgmt);
                        } catch (Exception e) {
                            LOG.warn("problems destroying app "+app+" (mgmt now "+mgmt+", will rethrow at least one exception): "+e);
                            error.compareAndSet(null, e);
                        }
                    }}));
            }
            Futures.allAsList(futures).get();
            
            if (error.get() != null) throw Exceptions.propagate(error.get());
        } catch (Exception e) {
            if (!mgmt.isRunning()) {
                LOG.debug("Destroying apps gave an error, but mgmt context was concurrently stopped so not really a problem; swallowing (unless fatal): "+e);
                Exceptions.propagateIfFatal(e);
            } else {
                throw Exceptions.propagate(e);
            }
        } finally {
            executor.shutdownNow();
        }
    }
}
