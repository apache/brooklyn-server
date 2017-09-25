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
import static org.testng.Assert.fail;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateValueNotAvailableException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Callables;

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
        Task<String> t = newSleepTask(Duration.ZERO, "foo");
        app.getExecutionContext().submit(t).getUnchecked();
        
        // Below, we call ValueResolver.getMaybe() from this thread, which has no execution context.
        // However, the task has already been submitted and we have waited for it to complete.
        // Therefore the ValueResolver can simply check for task.isDone() and return its result immediately.
        Maybe<String> result = Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe();
        Assert.assertEquals(result.get(), "foo");
    }

    public void testUnsubmittedTaskWhenNoExecutionContextFails() {
        Task<String> t = newSleepTask(Duration.ZERO, "foo");
        
        // Below, we call ValueResolver.getMaybe() with no execution context. Therefore it will not execute the task.
        Maybe<String> result = Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe();
        
        Assert.assertTrue(result.isAbsent(), "result="+result);
        Exception exception = Maybe.getException(result);
        Asserts.assertStringContains(exception.toString(), "no execution context available");
        
        Asserts.assertThat(t, (tt) -> !tt.isBegun());
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndReturns() {
        final Task<String> t = newSleepTask(Duration.ZERO, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task.
        Maybe<String> result = app.getExecutionContext()
                .submit("resolving sleep task", () -> Tasks.resolving(t).as(String.class).timeout(Asserts.DEFAULT_LONG_TIMEOUT).getMaybe())
                .getUnchecked();
        
        Assert.assertEquals(result.get(), "foo");
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndTimesOut() {
        final Task<String> t = newSleepTask(Duration.ONE_MINUTE, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task.
        // However, it will quickly timeout as the task will not have completed.
        Maybe<String> result = app.getExecutionContext()
                .submit("resolving sleep task", () -> Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe())
                .getUnchecked();
        
        Assert.assertTrue(result.isAbsent(), "result="+result);
        Exception exception = Maybe.getException(result);
        Assert.assertTrue(exception.toString().contains("not completed when immediate completion requested"), "exception="+exception);
        
        Asserts.eventually(() -> t, (tt) -> tt.isBegun(), Duration.TEN_SECONDS);
        Asserts.assertThat(t, (tt) -> !tt.isDone());
    }
    
    public void testUnsubmittedTaskWithExecutionContextExecutesAndReturnsForeground() {
        final Task<String> t = newSleepTask(Duration.ZERO, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task.
        Maybe<String> result = app.getExecutionContext()
                .get(new BasicTask<>( () -> Tasks.resolving(t).as(String.class).timeout(Asserts.DEFAULT_LONG_TIMEOUT).getMaybe() ));
        
        Assert.assertEquals(result.get(), "foo");
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndTimesOutForeground() {
        final Task<String> t = newSleepTask(Duration.ONE_MINUTE, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task.
        // However, it will quickly timeout as the task will not have completed.
        Maybe<String> result = app.getExecutionContext()
            .get(new BasicTask<>( () -> Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe() ));
        
        Assert.assertTrue(result.isAbsent(), "result="+result);
        Exception exception = Maybe.getException(result);
        Assert.assertTrue(exception.toString().contains("not completed when immediate completion requested"), "exception="+exception);
        
        Asserts.eventually(() -> t, (tt) -> tt.isBegun(), Duration.TEN_SECONDS);
        Asserts.assertThat(t, (tt) -> !tt.isDone());
    }

    public void testUnsubmittedTaskWithExecutionContextTimesOutWhenImmediate() {
        final Task<String> t = newSleepTask(Duration.ZERO, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task
        Maybe<Maybe<String>> result = app.getExecutionContext()
                .getImmediately(new BasicTask<>( () -> Tasks.resolving(t).as(String.class).timeout(Asserts.DEFAULT_LONG_TIMEOUT).getMaybe() ));
        
        // However, the resubmission will not be waited upon
        Assert.assertTrue(result.isPresent(), "result="+result);
        Assert.assertTrue(result.get().isAbsent(), "result="+result);
        Exception exception = Maybe.getException(result.get());
        
        Asserts.assertStringContainsIgnoreCase(exception.toString(), "immediate", "not", "available");

        // But the underlying task is running
        Asserts.eventually(() -> t, (tt) -> tt.isBegun(), Duration.TEN_SECONDS);
        Asserts.eventually(() -> t, (tt) -> tt.isDone(), Duration.TEN_SECONDS);
        Asserts.assertThat(t, (tt) -> Objects.equals(tt.getUnchecked(), "foo"));
        
        // And subsequent get _is_ immediate
        result = app.getExecutionContext()
            .getImmediately(new BasicTask<>( () -> Tasks.resolving(t).as(String.class).timeout(Asserts.DEFAULT_LONG_TIMEOUT).getMaybe() ));
        Assert.assertEquals(result.get().get(), "foo");
    }

    public void testUnsubmittedTaskWithExecutionContextExecutesAndTimesOutImmediate() {
        final Task<String> t = newSleepTask(Duration.ONE_MINUTE, "foo");
        
        // Below, we call ValueResolver.getMaybe() in app's execution context. Therefore it will execute the task.
        // However, it will quickly timeout as the task will not have completed.
        Maybe<Maybe<String>> result = app.getExecutionContext()
            .getImmediately(new BasicTask<>( () -> Tasks.resolving(t).as(String.class).timeout(Duration.ZERO).getMaybe() ));
        
        Assert.assertTrue(result.isPresent(), "result="+result);
        Assert.assertTrue(result.get().isAbsent(), "result="+result);
        Exception exception = Maybe.getException(result.get());
        Asserts.assertStringContainsIgnoreCase(exception.toString(), "immediate", "not", "available");
        Asserts.eventually(() -> t, (tt) -> tt.isBegun(), Duration.TEN_SECONDS);
        Asserts.assertThat(t, (tt) -> !tt.isDone());
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
        assertImmediateFakeTaskFromMethod(callInfo, "testGetImmediately");
    }
    
    public void testImmediateSupplierWithTimeoutUsesBlocking() {
        MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier();
        CallInfo callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).timeout(Asserts.DEFAULT_LONG_TIMEOUT).get();
        assertRealTaskNotFromMethod(callInfo, "testImmediateSupplierWithTimeoutUsesBlocking");
    }
    
    public void testGetImmediatelyInTask() throws Exception {
        final MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier();
        Task<CallInfo> task = app.getExecutionContext().submit("test task for call stack", new Callable<CallInfo>() {
            @Override
            public CallInfo call() {
                return myUniquelyNamedMethod();
            }
            private CallInfo myUniquelyNamedMethod() {
                return Tasks.resolving(supplier).as(CallInfo.class).immediately(true).get();
            }
        });
        CallInfo callInfo = task.get();
        assertImmediateFakeTaskFromMethod(callInfo, "myUniquelyNamedMethod");
    }
    
    public void testGetImmediatelyFallsBackToDeferredCallInTaskOnUnsupported() throws Exception {
        final MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier(new ImmediateSupplier.ImmediateUnsupportedException("Simulate immediate unsupported"));
        CallInfo callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).immediately(true).get();
        assertNotNull(callInfo.task);
        assertEquals(BrooklynTaskTags.getContextEntity(callInfo.task), app);
        assertNotContainsCallingMethod(callInfo.stackTrace, "testGetImmediatelyFallsBackToDeferredCallInTask");
    }

    public void testGetImmediatelyDoesntFallBackToDeferredCallOnNotAvailable() throws Exception {
        final MyImmediateAndDeferredSupplier supplier = new MyImmediateAndDeferredSupplier(new ImmediateSupplier.ImmediateValueNotAvailableException());
        Maybe<CallInfo> callInfo = Tasks.resolving(supplier).as(CallInfo.class).context(app).immediately(true).getMaybe();
        Asserts.assertNotPresent(callInfo);
        
        try {
            callInfo.get();
            Asserts.shouldHaveFailedPreviously("resolution should have failed now the ImmediateSupplier is not expected to fallback to other evaluation forms; instead got "+callInfo);
            
        } catch (Exception e) {
            Asserts.expectedFailureOfType(e, ImmediateValueNotAvailableException.class);
        }
    }

    public void testNonRecursiveBlockingFailsOnNonObjectType() throws Exception {
        try {
            Tasks.resolving(new WrappingImmediateAndDeferredSupplier(new FailingImmediateAndDeferredSupplier()))
                .as(FailingImmediateAndDeferredSupplier.class)
                .context(app)
                .immediately(false)
                .recursive(false)
                .get();
            Asserts.shouldHaveFailedPreviously("recursive(true) accepts only as(Object.class)");
        } catch (IllegalStateException e) {
            Asserts.expectedFailureContains(e, "must be Object");
        }
    }

    public void testNonRecursiveBlocking() throws Exception {
        Object result = Tasks.resolving(new WrappingImmediateAndDeferredSupplier(new FailingImmediateAndDeferredSupplier()))
            .as(Object.class)
            .context(app)
            .immediately(false)
            .recursive(false)
            .get();
        assertEquals(result.getClass(), FailingImmediateAndDeferredSupplier.class);
    }

    public void testNonRecursiveImmediateFailsOnNonObjectType() throws Exception {
        try {
            Tasks.resolving(new WrappingImmediateAndDeferredSupplier(new FailingImmediateAndDeferredSupplier()))
                .as(FailingImmediateAndDeferredSupplier.class)
                .context(app)
                .immediately(true)
                .recursive(false)
                .get();
            Asserts.shouldHaveFailedPreviously("recursive(true) accepts only as(Object.class)");
        } catch (IllegalStateException e) {
            Asserts.expectedFailureContains(e, "must be Object");
        }
    }

    public void testNonRecursiveImmediately() throws Exception {
        Object result = Tasks.resolving(new WrappingImmediateAndDeferredSupplier(new FailingImmediateAndDeferredSupplier()))
                .as(Object.class)
                .context(app)
                .immediately(true)
                .recursive(false)
                .get();
            assertEquals(result.getClass(), FailingImmediateAndDeferredSupplier.class);
    }

    public void testTaskFactoryGet() {
        TaskFactory<TaskAdaptable<String>> taskFactory = new TaskFactory<TaskAdaptable<String>>() {
            @Override public TaskAdaptable<String> newTask() {
                return new BasicTask<>(Callables.returning("myval"));
            }
        };
        String result = Tasks.resolving(taskFactory).as(String.class).context(app).get();
        assertEquals(result, "myval");
    }
    
    public void testTaskFactoryGetImmediately() {
        TaskFactory<TaskAdaptable<String>> taskFactory = new TaskFactory<TaskAdaptable<String>>() {
            @Override public TaskAdaptable<String> newTask() {
                return new BasicTask<>(Callables.returning("myval"));
            }
        };
        String result = Tasks.resolving(taskFactory).as(String.class).context(app).immediately(true).get();
        assertEquals(result, "myval");
    }
    
    public void testTaskFactoryGetImmediatelyDoesNotBlock() {
        final AtomicBoolean executing = new AtomicBoolean();
        TaskFactory<TaskAdaptable<String>> taskFactory = new TaskFactory<TaskAdaptable<String>>() {
            @Override public TaskAdaptable<String> newTask() {
                return new BasicTask<>(new Callable<String>() {
                    public String call() {
                        executing.set(true);
                        try {
                            Time.sleep(Duration.ONE_MINUTE);
                            return "myval";
                        } finally {
                            executing.set(false);
                        }
                    }});
            }
        };
        Maybe<String> result = Tasks.resolving(taskFactory).as(String.class).context(app).immediately(true).getMaybe();
        Asserts.assertTrue(result.isAbsent(), "result="+result);
        // the call below default times out after 30s while the task above is still running
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                Asserts.assertFalse(executing.get());
            }
        });
    }
    
    public void testTaskFactoryGetImmediatelyDoesNotBlockWithNestedTasks() {
        final int NUM_CALLS = 3;
        final AtomicInteger executingCount = new AtomicInteger();
        final List<SequentialTask<?>> outerTasks = Lists.newArrayList();
        
        TaskFactory<Task<?>> taskFactory = new TaskFactory<Task<?>>() {
            @Override public Task<?> newTask() {
                SequentialTask<?> result = new SequentialTask<>(ImmutableList.of(new Callable<String>() {
                    public String call() {
                        executingCount.incrementAndGet();
                        try {
                            Time.sleep(Duration.ONE_MINUTE);
                            return "myval";
                        } finally {
                            executingCount.decrementAndGet();
                        }
                    }}));
                outerTasks.add(result);
                return result;
            }
        };
        for (int i = 0; i < NUM_CALLS; i++) {
            Maybe<String> result = Tasks.resolving(taskFactory).as(String.class).context(app).immediately(true).getMaybe();
            Asserts.assertTrue(result.isAbsent(), "result="+result);
        }
        // the call below default times out after 30s while the task above is still running
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                Asserts.assertEquals(outerTasks.size(), NUM_CALLS);
                for (Task<?> task : outerTasks) {
                    Asserts.assertTrue(task.isDone());
                    Asserts.assertTrue(task.isCancelled());
                }
            }
        });
        Asserts.succeedsEventually(new Runnable() {
            public void run() {
                Asserts.assertEquals(executingCount.get(), 0);
            }
        });
    }
    
    private static class MyImmediateAndDeferredSupplier implements ImmediateSupplier<CallInfo>, DeferredSupplier<CallInfo> {
        private final RuntimeException failImmediately;
        
        public MyImmediateAndDeferredSupplier() {
            this(null);
        }
        
        public MyImmediateAndDeferredSupplier(RuntimeException failImmediately) {
            this.failImmediately = failImmediately;
        }
        
        @Override
        public Maybe<CallInfo> getImmediately() {
            if (failImmediately!=null) {
                throw failImmediately;
            } else {
                return Maybe.of(CallInfo.newInstance());
            }
        }
        @Override
        public CallInfo get() {
            return CallInfo.newInstance();
        }
    }
    
    static class WrappingImmediateAndDeferredSupplier implements ImmediateSupplier<Object>, DeferredSupplier<Object> {
        private Object value;

        public WrappingImmediateAndDeferredSupplier(Object value) {
            this.value = value;
        }

        @Override
        public Object get() {
            return getImmediately().get();
        }

        @Override
        public Maybe<Object> getImmediately() {
            return Maybe.of(value);
        }
        
    }

    static class FailingImmediateAndDeferredSupplier implements ImmediateSupplier<Object>, DeferredSupplier<Object> {

        @Override
        public Object get() {
            throw new IllegalStateException("Not to be called");
        }

        @Override
        public Maybe<Object> getImmediately() {
            throw new IllegalStateException("Not to be called");
        }
        
    }

    private static class CallInfo {
        final StackTraceElement[] stackTrace;
        final Task<?> task;

        public static CallInfo newInstance() {
            Exception e = new Exception("for stacktrace");
            e.fillInStackTrace();
            return new CallInfo(e.getStackTrace(), Tasks.current());
        }
        
        CallInfo(StackTraceElement[] stackTrace, Task<?> task) {
            this.stackTrace = stackTrace;
            this.task = task;
        }
    }
    
    public static final Task<String> newSleepTask(final Duration timeout, final String result) {
        return Tasks.<String>builder().body(new Callable<String>() { 
            @Override
            public String call() { 
                Time.sleep(timeout); 
                return result; 
            }}
        ).build();
    }
    
    public static final Task<String> newThrowTask(final Duration timeout) {
        return Tasks.<String>builder().body(new Callable<String>() { 
            @Override
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
    
    private void assertImmediateFakeTaskFromMethod(CallInfo callInfo, String method) {
        // previously task was null, but now there is a "fake task"
        assertNotNull(callInfo.task);
        // it is now submitted in same thread (2017-09)
        Assert.assertTrue(callInfo.task.isSubmitted());       
        assertContainsCallingMethod(callInfo.stackTrace, method);
    }
    
    private void assertRealTaskNotFromMethod(CallInfo callInfo, String method) {
        assertNotNull(callInfo.task);
        Assert.assertTrue(callInfo.task.isSubmitted());   
        assertNotContainsCallingMethod(callInfo.stackTrace, method); 
    }

}
