/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.brooklyn.util.net;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.net.ServerSocket;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class ReachableSocketFinderTest {

    private static final Logger LOG = LoggerFactory.getLogger(ReachableSocketFinderTest.class);

    private final HostAndPort socket1 = HostAndPort.fromParts("1.1.1.1", 1111);
    private final HostAndPort socket2 = HostAndPort.fromParts("1.1.1.2", 1112);
    private final HostAndPort socket3 = HostAndPort.fromParts("1.1.1.3", 1113);
    private final Predicate<HostAndPort> socketTester = new Predicate<HostAndPort>() {
        @Override
        public boolean apply(HostAndPort input) {
            return Boolean.TRUE.equals(reachabilityResults.get(input));
        }};

    private Map<HostAndPort, Boolean> reachabilityResults;
    private ListeningExecutorService executor;
    private ReachableSocketFinder finder;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        reachabilityResults = Maps.newConcurrentMap();
        executor = MoreExecutors.listeningDecorator(Executors.newCachedThreadPool());
        finder = new ReachableSocketFinder(socketTester, executor);
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (executor != null) executor.shutdownNow();
    }
    
    @Test(expectedExceptions=IllegalStateException.class)
    public void testFindWhenNoSocketsThrowsIllegalState() throws Exception {
        finder.findOpenSocketOnNode(ImmutableList.<HostAndPort>of(), Duration.TEN_SECONDS);
    }

    @Test(expectedExceptions=IllegalStateException.class)
    public void testFindAllWhenNoSocketsThrowsIllegalState() throws Exception {
        finder.findOpenSocketsOnNode(ImmutableList.<HostAndPort>of(), Duration.TEN_SECONDS);
    }
    
    @Test
    public void testReturnsReachableSocket() throws Exception {
        reachabilityResults.put(socket1, true);
        reachabilityResults.put(socket2, false);
        assertEquals(finder.findOpenSocketOnNode(ImmutableList.of(socket1, socket2), Duration.ONE_SECOND), socket1);
        
        reachabilityResults.put(socket1, false);
        reachabilityResults.put(socket2, true);
        assertEquals(finder.findOpenSocketOnNode(ImmutableList.of(socket1, socket2), Duration.ONE_SECOND), socket2);
    }
    
    @Test
    public void testPollsUntilPortReachable() throws Exception {
        reachabilityResults.put(socket1, false);
        reachabilityResults.put(socket2, false);
        final ListenableFuture<HostAndPort> future = executor.submit(new Callable<HostAndPort>() {
                @Override public HostAndPort call() throws Exception {
                    return finder.findOpenSocketOnNode(ImmutableList.of(socket1, socket2), Duration.TEN_SECONDS);
                }});

        // Should keep trying
        Asserts.succeedsContinually(ImmutableMap.of("timeout", Duration.millis(100)), new Runnable() {
            @Override public void run() {
                assertFalse(future.isDone());
            }});

        // When port is reached, it completes
        reachabilityResults.put(socket1, true);
        assertEquals(future.get(5, TimeUnit.SECONDS), socket1);
    }

    @Test
    public void testGetReachableSockets() throws Exception {
        reachabilityResults.put(socket1, true);
        reachabilityResults.put(socket2, false);
        reachabilityResults.put(socket3, true);
        final Iterable<HostAndPort> expected = ImmutableList.of(socket1, socket3);
        final Iterable<HostAndPort> actual = finder.findOpenSocketsOnNode(
                ImmutableList.of(socket1, socket2, socket3), Duration.millis(250));
        assertEquals(actual, expected, "expected=" + expected + ", actual=" + Iterables.toString(actual));
    }

    @Test
    public void testGetAllReachableSocketsEmpty() throws Exception {
        final Iterable<HostAndPort> expected = ImmutableList.of();
        final Iterable<HostAndPort> actual = finder.findOpenSocketsOnNode(ImmutableList.of(socket2), Duration.millis(1));
        assertEquals(actual, expected, "expected=" + expected + ", actual=" + Iterables.toString(actual));
    }

    @Test
    public void testReachableSocketsIteratesInInputOrder() throws Exception {
        // i.e. not in the order that reachability was determined.
        reachabilityResults.put(socket1, false);
        reachabilityResults.put(socket2, true);
        final Iterable<HostAndPort> actual = finder.findOpenSocketsOnNode(ImmutableList.of(socket1, socket2), Duration.TEN_SECONDS);
        final Iterable<HostAndPort> expected = ImmutableList.of(socket1, socket2);
        final Future<?> future = executor.submit(new Runnable() {
            @Override
            public void run() {
                // This will block until the check for socket1 completes or times out.
                assertEquals(actual, expected, "expected=" + expected + ", actual=" + Iterables.toString(actual));
            }
        });

        // Should keep trying
        Asserts.succeedsContinually(ImmutableMap.of("timeout", Duration.ONE_SECOND), new Runnable() {
            @Override public void run() {
                assertFalse(future.isDone());
            }});

        // When port is reached, it completes. Demonstrates grace period.
        reachabilityResults.put(socket1, true);

        Asserts.succeedsEventually(ImmutableMap.of("timeout", Duration.ONE_SECOND), new Runnable() {
            @Override public void run() {
                assertTrue(future.isDone());
            }});
    }

    // Wouldn't need to be integration if grace period were configurable.
    @Test(groups = "Integration")
    public void testSocketResultIgnoredIfGracePeriodExpiresAfterFirstResultAvailable() {
        reachabilityResults.put(socket1, false);
        reachabilityResults.put(socket2, true);

        final Iterable<HostAndPort> actual = finder.findOpenSocketsOnNode(ImmutableList.of(socket1, socket2), Duration.FIVE_MINUTES);
        // Sleep through the grace period.
        Time.sleep(Duration.seconds(10));
        reachabilityResults.put(socket1, true);
        assertEquals(actual, ImmutableList.of(socket2));
    }
    
    // Mark as integration, as can't rely (in Apache infra) for a port to stay unused during test!
    @Test(groups="Integration")
    public void testReturnsRealReachableSocket() throws Exception {
        ReachableSocketFinder realFinder = new ReachableSocketFinder(executor);
        ServerSocket socket = connectToPort();
        try {
            HostAndPort addr = HostAndPort.fromParts(socket.getInetAddress().getHostAddress(), socket.getLocalPort());
            HostAndPort wrongAddr = HostAndPort.fromParts(socket.getInetAddress().getHostAddress(), findAvailablePort());
            
            assertEquals(realFinder.findOpenSocketOnNode(ImmutableList.of(addr, wrongAddr), Duration.ONE_MINUTE), addr);
        } finally {
            if (socket != null) {
                socket.close();
            }
        }
    }

    // Mark as integration, as can't rely (in Apache infra) for a port to stay unused during test!
    // And slow test - takes 5 seconds.
    @Test(groups="Integration")
    public void testFailsIfRealSocketUnreachable() throws Exception {
        ReachableSocketFinder realFinder = new ReachableSocketFinder(executor);
        HostAndPort wrongAddr = HostAndPort.fromParts(Networking.getLocalHost().getHostAddress(), findAvailablePort());
        
        try {
            HostAndPort result = realFinder.findOpenSocketOnNode(ImmutableList.of(wrongAddr), Duration.FIVE_SECONDS);
            fail("Expected failure, but got "+result);
        } catch (NoSuchElementException e) {
            // success
        }
    }

    private ServerSocket connectToPort() throws Exception {
        ServerSocket result = new ServerSocket(0);
        LOG.info("Acquired port "+result+" for test "+JavaClassNames.niceClassAndMethod());
        return result;
    }
    
    private int findAvailablePort() throws Exception {
        final int startPort = 58767;
        final int endPort = 60000;
        int port = startPort;
        do {
            if (Networking.isPortAvailable(port)) {
                return port;
            }
            port++;
            // repeat until we can get a port
        } while (port <= endPort);
        throw new Exception("could not get a port in range "+startPort+"-"+endPort);
    }
}
