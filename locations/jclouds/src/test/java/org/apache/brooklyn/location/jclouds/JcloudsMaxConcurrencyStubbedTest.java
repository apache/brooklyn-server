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
package org.apache.brooklyn.location.jclouds;

import static org.testng.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

/**
 * Simulates the creation of a VM that has multiple IPs. Checks that we choose the right address.
 */
public class JcloudsMaxConcurrencyStubbedTest extends AbstractJcloudsStubbedUnitTest {

    private static class ConcurrencyMonitor {
        private final Object mutex = new Object();
        private final AtomicInteger concurrentCalls = new AtomicInteger();
        private final AtomicInteger maxConcurrentCalls = new AtomicInteger();
        private CountDownLatch latch;
        
        public void setLatch(CountDownLatch latch) {
            this.latch = latch;
        }

        public int getMaxConcurrentCalls() {
            return maxConcurrentCalls.get();
        }
        
        public void onStart() {
            synchronized (mutex) {
                int concurrentCallCount = concurrentCalls.incrementAndGet();
                if (concurrentCallCount > maxConcurrentCalls.get()) {
                    maxConcurrentCalls.set(concurrentCallCount);
                }
            }
            if (latch != null) {
                try {
                    latch.await();
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }
        }
        
        public void onEnd() {
            synchronized (mutex) {
                concurrentCalls.decrementAndGet();
            }
        }
    }
    
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JcloudsMaxConcurrencyStubbedTest.class);
    
    private ListeningExecutorService executor;
    private ConcurrencyMonitor creationConcurrencyMonitor;
    private ConcurrencyMonitor deletionConcurrencyMonitor;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        creationConcurrencyMonitor = new ConcurrencyMonitor();
        deletionConcurrencyMonitor = new ConcurrencyMonitor();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            if (executor != null) executor.shutdownNow();
        }
    }
    
    protected AbstractNodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                try {
                    creationConcurrencyMonitor.onStart();
                    
                    NodeMetadata result = new NodeMetadataBuilder()
                            .id("myid")
                            .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                            .loginPort(22)
                            .status(Status.RUNNING)
                            .publicAddresses(ImmutableList.of("173.194.32.123"))
                            .privateAddresses(ImmutableList.of("172.168.10.11"))
                            .build();
                    return result;
                } finally {
                    creationConcurrencyMonitor.onEnd();
                }
            }
            @Override public void destroyNode(String id) {
                try {
                    deletionConcurrencyMonitor.onStart();

                    super.destroyNode(id);
                } finally {
                    deletionConcurrencyMonitor.onEnd();
                }
            }
            @Override public Set<? extends NodeMetadata> destroyNodesMatching(Predicate<? super NodeMetadata> filter) {
                try {
                    deletionConcurrencyMonitor.onStart();

                    return super.destroyNodesMatching(filter);
                } finally {
                    deletionConcurrencyMonitor.onEnd();
                }
            }
            
        };
    }

    @Test
    public void testConcurrentCreateCalls() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        creationConcurrencyMonitor.setLatch(latch);
        
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(JcloudsLocation.MAX_CONCURRENT_MACHINE_CREATIONS, 2));
        
        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            futures.add(executor.submit(new Callable<JcloudsSshMachineLocation>() {
                public JcloudsSshMachineLocation call() throws Exception {
                    return obtainMachine();
                }}));
        }
        
        assertMaxConcurrentCallsEventually(creationConcurrencyMonitor, 2);
        assertMaxConcurrentCallsContinually(creationConcurrencyMonitor, 2);
        latch.countDown();
        Futures.allAsList(futures).get();
    }
    
    @Test
    public void testConcurrentDeletionCalls() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        deletionConcurrencyMonitor.setLatch(latch);
        
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of(JcloudsLocation.MAX_CONCURRENT_MACHINE_DELETIONS, 2));

        for (int i = 0; i < 3; i++) {
            obtainMachine();
        }
        assertEquals(machines.size(), 3, "machines="+machines);

        List<ListenableFuture<?>> futures = new ArrayList<>();
        for (final JcloudsMachineLocation machine : machines) {
            futures.add(executor.submit(new Callable<Void>() {
                public Void call() throws Exception {
                    releaseMachine(machine);
                    return null;
                }}));
        }
        
        assertMaxConcurrentCallsEventually(deletionConcurrencyMonitor, 2);
        assertMaxConcurrentCallsContinually(deletionConcurrencyMonitor, 2);
        latch.countDown();
        Futures.allAsList(futures).get();
    }
    
    void assertMaxConcurrentCallsEventually(ConcurrencyMonitor monitor, int expected) {
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                assertEquals(monitor.getMaxConcurrentCalls(), expected);
            }});
    }
    
    void assertMaxConcurrentCallsContinually(ConcurrencyMonitor monitor, int expected) {
        Asserts.succeedsContinually(MutableMap.of("timeout", Duration.millis(100)), new Runnable() {
            public void run() {
                assertEquals(monitor.getMaxConcurrentCalls(), expected);
            }});
    }
}
