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
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * see also {@link TasksTest} for more tests
 */
@Test
public class ValueResolverTest extends BrooklynAppUnitTestSupport {

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
    }
    
    public void testTimeoutZero() {
        Maybe<String> result = Tasks.resolving(newSleepTask(Duration.TEN_SECONDS, "foo")).as(String.class).context(app).timeout(Duration.ZERO).getMaybe();
        Assert.assertFalse(result.isPresent());
    }
    
    public void testTimeoutBig() {
        Maybe<String> result = Tasks.resolving(newSleepTask(Duration.ZERO, "foo")).as(String.class).context(app).timeout(Duration.TEN_SECONDS).getMaybe();
        Assert.assertEquals(result.get(), "foo");
    }

    public void testCompletedTaskReturnsResultImmediately() {
        // Call ValueResolver.getMaybe() from this thread, which has no execution context.
        // However, the task has already been submitted and we have waited for it to complete.
        // Therefore the ValueResolver can simply check for task.isDone() and return its result immediately.
        Task<String> t = newSleepTask(Duration.ZERO, "foo");
        app.getExecutionContext().submit(t).getUnchecked();
        Maybe<String> result = Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe();
        Assert.assertEquals(result.get(), "foo");
    }

    public void testUnsubmittedTaskWhenNoExecutionContextFails() {
        // ValueResolver.getMaybe() is called with no execution context. Therefore it will not execute the task.
        Task<String> t = newSleepTask(Duration.ZERO, "foo");
        Maybe<String> result = Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe();
        
        Assert.assertTrue(result.isAbsent(), "result="+result);
        Exception exception = ((Maybe.Absent<?>)result).getException();
        Assert.assertTrue(exception.toString().contains("no execution context available"), "exception="+exception);
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndReturns() {
        // ValueResolver.getMaybe() is called in app's execution context. Therefore it will execute the task.
        final Task<String> t = newSleepTask(Duration.ZERO, "foo");
        
        Maybe<String>  result = app.getExecutionContext()
                .submit(new Callable<Maybe<String> >() {
                    public Maybe<String>  call() throws Exception {
                        return Tasks.resolving(t).as(String.class).timeout(Asserts.DEFAULT_LONG_TIMEOUT).getMaybe();
                    }})
                .getUnchecked();
        
        Assert.assertEquals(result.get(), "foo");
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndTimesOut() {
        // ValueResolver.getMaybe() is called in app's execution context. Therefore it will execute the task.
        final Task<String> t = newSleepTask(Duration.ONE_MINUTE, "foo");
        
        Maybe<String>  result = app.getExecutionContext()
                .submit(new Callable<Maybe<String> >() {
                    public Maybe<String>  call() throws Exception {
                        return Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe();
                    }})
                .getUnchecked();
        
        Assert.assertTrue(result.isAbsent(), "result="+result);
        Exception exception = ((Maybe.Absent<?>)result).getException();
        Assert.assertTrue(exception.toString().contains("not completed when immediate completion requested"), "exception="+exception);
    }

    public void testSwallowError() {
        ValueResolver<String> result = Tasks.resolving(newThrowTask(Duration.ZERO)).as(String.class).context(app).swallowExceptions();
        assertMaybeIsAbsent(result);
        assertThrowsOnGet(result);
    }

    public void testDontSwallowError() {
        ValueResolver<String> result = Tasks.resolving(newThrowTask(Duration.ZERO)).as(String.class).context(app);
        assertThrowsOnGetMaybe(result);
        assertThrowsOnGet(result);
    }

    public void testDefaultWhenSwallowError() {
        ValueResolver<String> result = Tasks.resolving(newThrowTask(Duration.ZERO)).as(String.class).context(app).swallowExceptions().defaultValue("foo");
        assertMaybeIsAbsent(result);
        Assert.assertEquals(result.get(), "foo");
    }

    public void testDefaultBeforeDelayAndError() {
        ValueResolver<String> result = Tasks.resolving(newThrowTask(Duration.TEN_SECONDS)).as(String.class).context(app).timeout(Duration.ZERO).defaultValue("foo");
        assertMaybeIsAbsent(result);
        Assert.assertEquals(result.get(), "foo");
    }

    public void testGetImmediately() {
        MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier();
        CallInfo callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).immediately(true).get();
        assertNull(callInfo.task);
        assertContainsCallingMethod(callInfo.stackTrace, "testGetImmediately");
    }
    
    public void testGetImmediateSupplierWithTimeoutUsesBlocking() {
        MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier();
        CallInfo callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).timeout(Asserts.DEFAULT_LONG_TIMEOUT).get();
        assertNotNull(callInfo.task);
        assertNotContainsCallingMethod(callInfo.stackTrace, "testGetImmediately");
    }
    
    public void testGetImmediatelyInTask() throws Exception {
        final MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier();
        Task<CallInfo> task = app.getExecutionContext().submit(new Callable<CallInfo>() {
            public CallInfo call() {
                return myUniquelyNamedMethod();
            }
            private CallInfo myUniquelyNamedMethod() {
                return Tasks.resolving(supplier).as(CallInfo.class).immediately(true).get();
            }
        });
        CallInfo callInfo = task.get();
        assertEquals(callInfo.task, task);
        assertContainsCallingMethod(callInfo.stackTrace, "myUniquelyNamedMethod");
    }
    
    public void testGetImmediatelyFallsBackToDeferredCallInTask() throws Exception {
        final MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier(true);
        CallInfo callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).immediately(true).get();
        assertNotNull(callInfo.task);
        assertEquals(BrooklynTaskTags.getContextEntity(callInfo.task), app);
        assertNotContainsCallingMethod(callInfo.stackTrace, "testGetImmediately");
    }
    
    private static class MyImmediateAndDeferredSupplier implements ImmediateSupplier<CallInfo>, DeferredSupplier<CallInfo> {
        private final boolean failImmediately;
        
        public MyImmediateAndDeferredSupplier() {
            this(false);
        }
        
        public MyImmediateAndDeferredSupplier(boolean simulateImmediateUnsupported) {
            this.failImmediately = simulateImmediateUnsupported;
        }
        
        @Override
        public Maybe<CallInfo> getImmediately() {
            if (failImmediately) {
                throw new ImmediateSupplier.ImmediateUnsupportedException("Simulate immediate unsupported");
            } else {
                return Maybe.of(CallInfo.newInstance());
            }
        }
        @Override
        public CallInfo get() {
            return CallInfo.newInstance();
        }
    }
    
    private static class CallInfo {
        final StackTraceElement[] stackTrace;
        final Task<?> task;

        public static CallInfo newInstance() {
            Exception e = new Exception("for stacktrace");
            e.fillInStackTrace();
            return new CallInfo(e.getStackTrace(), (Task<?>) Tasks.current());
        }
        
        CallInfo(StackTraceElement[] stackTrace, Task<?> task) {
            this.stackTrace = stackTrace;
            this.task = task;
        }
    }
    
    public static final Task<String> newSleepTask(final Duration timeout, final String result) {
        return Tasks.<String>builder().body(new Callable<String>() { 
            public String call() { 
                Time.sleep(timeout); 
                return result; 
            }}
        ).build();
    }
    
    public static final Task<String> newThrowTask(final Duration timeout) {
        return Tasks.<String>builder().body(new Callable<String>() { 
            public String call() {
                Time.sleep(timeout); 
                throw new IllegalStateException("intended, during tests");
            }}
        ).build();
    }
    
    public static Exception assertThrowsOnGetMaybe(ValueResolver<?> result) {
        try {
            result = result.clone();
            result.getMaybe();
            Assert.fail("should have thrown");
            return null;
        } catch (Exception e) { return e; }
    }
    
    public static Exception assertThrowsOnGet(ValueResolver<?> result) {
        result = result.clone();
        try {
            result.get();
            Assert.fail("should have thrown");
            return null;
        } catch (Exception e) { return e; }
    }
    
    public static <T> Maybe<T> assertMaybeIsAbsent(ValueResolver<T> result) {
        result = result.clone();
        Maybe<T> maybe = result.getMaybe();
        Assert.assertFalse(maybe.isPresent());
        return maybe;
    }
    
    private void assertContainsCallingMethod(StackTraceElement[] stackTrace, String expectedMethod) {
        for (StackTraceElement element : stackTrace) {
            if (expectedMethod.equals(element.getMethodName())) {
                return;
            }
        }
        fail("Method "+expectedMethod+" not found: "+Arrays.toString(stackTrace));
    }
    
    private void assertNotContainsCallingMethod(StackTraceElement[] stackTrace, String notExpectedMethod) {
        for (StackTraceElement element : stackTrace) {
            if (notExpectedMethod.equals(element.getMethodName())) {
                fail("Method "+notExpectedMethod+" not expected: "+Arrays.toString(stackTrace));
            }
        }
    }
}
