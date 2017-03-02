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

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;
import java.util.concurrent.Callable;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.Runnables;

public class InterruptingImmediateSupplierTest {

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public void testOfInvalidType() throws Exception {
        InterruptingImmediateSupplier.of("myval");
    }
    
    @Test
    public void testRunnable() throws Exception {
        assertImmediatelyPresent(Runnables.doNothing(), null);
        assertImmediatelyAbsent(new SleepingRunnable());
        assertImmediatelyFails(new FailingRunnable(), MarkerException.class);
    }
    
    @Test
    public void testCallable() throws Exception {
        assertImmediatelyPresent(Callables.returning("myval"), "myval");
        assertImmediatelyAbsent(new SleepingCallable());
        assertImmediatelyFails(new FailingCallable(), MarkerException.class);
    }
    
    @Test
    public void testSupplier() throws Exception {
        assertImmediatelyPresent(Suppliers.ofInstance("myval"), "myval");
        assertImmediatelyAbsent(new SleepingSupplier());
        assertImmediatelyFails(new FailingSupplier(), MarkerException.class);
    }
    
    private void assertImmediatelyPresent(Object orig, Object expected) {
        Maybe<Object> result = getImmediately(orig);
        assertEquals(result.get(), expected);
        assertFalse(Thread.currentThread().isInterrupted());
    }
    
    private void assertImmediatelyAbsent(Object orig) {
        Maybe<Object> result = getImmediately(orig);
        assertTrue(result.isAbsent(), "result="+result);
        assertFalse(Thread.currentThread().isInterrupted());
    }
    
    private void assertImmediatelyFails(Object orig, Class<? extends Exception> expected) {
        try {
            Maybe<Object> result = getImmediately(orig);
            Asserts.shouldHaveFailedPreviously("result="+result);
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, expected);
        }
        assertFalse(Thread.currentThread().isInterrupted());
    }
    
    private Maybe<Object> getImmediately(Object val) {
        InterruptingImmediateSupplier<Object> supplier = InterruptingImmediateSupplier.of(val);
        return supplier.getImmediately();
    }
    
    public static class SleepingRunnable implements Runnable {
        @Override public void run() {
            Time.sleep(Duration.ONE_MINUTE);
        }
    }
    
    public static class SleepingCallable implements Callable<Void> {
        @Override public Void call() {
            Time.sleep(Duration.ONE_MINUTE);
            return null;
        }
    }
    
    public static class SleepingSupplier implements Supplier<Void> {
        @Override public Void get() {
            Time.sleep(Duration.ONE_MINUTE);
            return null;
        }
    }
    
    public static class FailingRunnable implements Runnable {
        @Override public void run() {
            throw new MarkerException();
        }
    }
    
    public static class FailingCallable implements Callable<Void> {
        @Override public Void call() {
            throw new MarkerException();
        }
    }
    
    public static class FailingSupplier implements Supplier<Void> {
        @Override public Void get() {
            throw new MarkerException();
        }
    }
    
    public static class MarkerException extends RuntimeException {
        private static final long serialVersionUID = -3395361406478634652L;
    }
}