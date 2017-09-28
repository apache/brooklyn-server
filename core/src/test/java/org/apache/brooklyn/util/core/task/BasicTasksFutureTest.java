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
package org.apache.brooklyn.util.core.task;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Stopwatch;

public class BasicTasksFutureTest {

    private static final Logger log = LoggerFactory.getLogger(BasicTasksFutureTest.class);
    
    private BasicExecutionManager em;
    private BasicExecutionContext ec;
    private Map<Object,Object> data;
    private ExecutorService ex;
    private Semaphore started;
    private Semaphore waitInTask;
    private Semaphore cancelledWhileSleeping;

    @BeforeMethod(alwaysRun=true)
    public void setUp() {
        em = new BasicExecutionManager("mycontext");
        ec = new BasicExecutionContext(em);
        ex = Executors.newCachedThreadPool();
        data = Collections.synchronizedMap(new LinkedHashMap<Object,Object>());
        started = new Semaphore(0);
        waitInTask = new Semaphore(0);
        cancelledWhileSleeping = new Semaphore(0);
    }
    
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        if (em != null) em.shutdownNow();
        if (ex != null) ex.shutdownNow();
    }

    @Test
    public void testBlockAndGetWithTimeoutsAndListenableFuture() throws InterruptedException {
        Task<String> t = waitForSemaphore(Duration.FIVE_SECONDS, true, "x", true);
        
        Assert.assertFalse(t.blockUntilEnded(Duration.millis(1)));
        Assert.assertFalse(t.blockUntilEnded(Duration.ZERO));
        boolean didNotThrow = false;
        
        try { t.getUnchecked(Duration.millis(1)); didNotThrow = true; }
        catch (Exception e) { /* expected */ }
        Assert.assertFalse(didNotThrow);
        
        try { t.getUnchecked(Duration.ZERO); didNotThrow = true; }
        catch (Exception e) { /* expected */ }
        Assert.assertFalse(didNotThrow);

        addFutureListener(t, "before");
        ec.submit(t);
        
        Assert.assertFalse(t.blockUntilEnded(Duration.millis(1)));
        Assert.assertFalse(t.blockUntilEnded(Duration.ZERO));
        
        try { t.getUnchecked(Duration.millis(1)); didNotThrow = true; }
        catch (Exception e) { /* expected */ }
        Assert.assertFalse(didNotThrow);
        
        try { t.getUnchecked(Duration.ZERO); didNotThrow = true; }
        catch (Exception e) { /* expected */ }
        Assert.assertFalse(didNotThrow);

        addFutureListener(t, "during");
            
        synchronized (data) {
            // now let it finish
            waitInTask.release();
            Assert.assertTrue(t.blockUntilEnded(Duration.TEN_SECONDS));

            Assert.assertEquals(t.getUnchecked(Duration.millis(1)), "x");
            Assert.assertEquals(t.getUnchecked(Duration.ZERO), "x");
            
            Assert.assertNull(data.get("before"));
            Assert.assertNull(data.get("during"));
            // can't set the data(above) until we release the lock (in assert call below)
            assertSoonGetsData("before");
            assertSoonGetsData("during");
        }

        // and see that a listener added late also runs
        synchronized (data) {
            addFutureListener(t, "after");
            Assert.assertNull(data.get("after"));
            assertSoonGetsData("after");
        }
    }

    private void addFutureListener(Task<String> t, final String key) {
        t.addListener(new Runnable() { @Override public void run() {
            synchronized (data) {
                log.info("notifying for "+key);
                data.notifyAll();
                data.put(key, true);
            }
        }}, ex);
    }

    private void assertSoonGetsData(String key) throws InterruptedException {
        for (int i=0; i<10; i++) {
            if (Boolean.TRUE.equals(data.get(key))) {
                log.info("got data for "+key);
                return;
            }
            data.wait(Duration.ONE_SECOND.toMilliseconds());
        }
        Assert.fail("did not get data for '"+key+"' in time");
    }

    private <T> Task<T> waitForSemaphore(final Duration time, final boolean requireSemaphore, final T result, boolean dynamic) {
        return Tasks.<T>builder().dynamic(dynamic).body(new Callable<T>() {
            @Override
            public T call() { 
                try {
                    log.info("about to release semaphore");
                    started.release();
                    log.info("waiting up to "+time+" to acquire before returning "+result);
                    if (!waitInTask.tryAcquire(time.toMilliseconds(), TimeUnit.MILLISECONDS)) {
                        log.info("did not get semaphore");
                        if (requireSemaphore) Assert.fail("task did not get semaphore");
                    } else {
                        log.info("got semaphore");
                    }
                } catch (Exception e) {
                    log.info("cancelled before returning "+result);
                    cancelledWhileSleeping.release();
                    throw Exceptions.propagate(e);
                }
                log.info("task returning "+result);
                return result; 
            }
        }).build();
    }

    @Test
    public void testCancelAfterStartTriggersListenableFutureDynamic() throws Exception {
        doTestCancelTriggersListenableFuture(null, Duration.millis(50), true);
    }
    @Test
    public void testCancelImmediateTriggersListenableFutureDynamic() throws Exception {
        // if cancel fires after submit but before it passes to the executor,
        // that needs handling separately as it falls into an edge where the future is set and cancelled
        // so the executor won't run it, and our wrapper logic doesn't apply; 
        // this test doesn't guarantee this code path, but makes it likely enough it happens once in a while.
        doTestCancelTriggersListenableFuture(null, Duration.ZERO, true);
    }
    @Test
    public void testCancelBeforeTriggersListenableFutureDynamic() throws Exception {
        doTestCancelTriggersListenableFuture(Duration.millis(50), null, true);
    }
    @Test
    public void testCancelAfterStartTriggersListenableFutureSimple() throws Exception {
        doTestCancelTriggersListenableFuture(null, Duration.millis(50), true);
    }
    @Test
    public void testCancelImmediateTriggersListenableFutureSimple() throws Exception {
        doTestCancelTriggersListenableFuture(null, Duration.ZERO, false);
    }
    @Test
    public void testCancelBeforeTriggersListenableFutureSimple() throws Exception {
        doTestCancelTriggersListenableFuture(Duration.millis(50), null, false);
    }
    public void doTestCancelTriggersListenableFuture(Duration delayBeforeSubmit, Duration delayBeforeCancel, boolean dynamic) throws Exception {
        Task<String> t = waitForSemaphore(Duration.TEN_SECONDS, true, "x", dynamic);
        addFutureListener(t, "before");

        Stopwatch watch = Stopwatch.createStarted();
        if (delayBeforeSubmit!=null) {
            new Thread(() -> {
                Time.sleep(delayBeforeSubmit); 
                ec.submit(t); 
            }).start(); 
        } else {
            ec.submit(t);
        }
        
        addFutureListener(t, "during");

        log.info("test cancelling "+t+" ("+t.getClass()+") after "+delayBeforeCancel+
            ", submit delay "+delayBeforeSubmit);
        // NB: three different code paths (callers to this method) for notifying futures 
        // depending whether task is started before, at, or after the cancel 
        if (delayBeforeCancel!=null) Time.sleep(delayBeforeCancel);

        synchronized (data) {
            t.cancel(true);
            
            assertSoonGetsData("before");
            assertSoonGetsData("during");

            addFutureListener(t, "after");
            Assert.assertNull(data.get("after"));
            assertSoonGetsData("after");
        }
        
        Assert.assertTrue(t.isDone());
        Assert.assertTrue(t.isCancelled());
        try {
            t.get();
            Assert.fail("should have thrown CancellationException");
        } catch (CancellationException e) { /* expected */ }
        
        Assert.assertTrue(watch.elapsed(TimeUnit.MILLISECONDS) < Duration.FIVE_SECONDS.toMilliseconds(), 
            Time.makeTimeStringRounded(watch.elapsed(TimeUnit.MILLISECONDS))+" is too long; should have cancelled very quickly");

        if (started.tryAcquire())
            // if the task is begun, this should get released
            Assert.assertTrue(cancelledWhileSleeping.tryAcquire(5, TimeUnit.SECONDS));
    }

}
