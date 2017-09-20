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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.HasTaskChildren;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.TaskInternal.TaskCancellationMode;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.CaseFormat;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Callables;
import com.google.common.util.concurrent.ExecutionList;
import com.google.common.util.concurrent.ForwardingFuture.SimpleForwardingFuture;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

import groovy.lang.Closure;

/**
 * Manages the execution of atomic tasks and scheduled (recurring) tasks,
 * including setting tags and invoking callbacks.
 */
public class BasicExecutionManager implements ExecutionManager {
    private static final Logger log = LoggerFactory.getLogger(BasicExecutionManager.class);

    private static final boolean RENAME_THREADS = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_RENAME_THREADS);
    private static final String JITTER_THREADS_MAX_DELAY_PROPERTY = BrooklynFeatureEnablement.FEATURE_JITTER_THREADS + ".maxDelay";

    private boolean jitterThreads = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_JITTER_THREADS);
    private int jitterThreadsMaxDelay = Integer.getInteger(JITTER_THREADS_MAX_DELAY_PROPERTY, 200);

    private static class PerThreadCurrentTaskHolder {
        public static final ThreadLocal<Task<?>> perThreadCurrentTask = new ThreadLocal<Task<?>>();
    }

    public static ThreadLocal<Task<?>> getPerThreadCurrentTask() {
        return PerThreadCurrentTaskHolder.perThreadCurrentTask;
    }

    private final ThreadFactory threadFactory;
    
    private final ThreadFactory daemonThreadFactory;
    
    private final ExecutorService runner;
        
    private final ScheduledExecutorService delayedRunner;

    // inefficient having so many records, and also doing searches through ...
    // many things in here could be more efficient however (different types of lookup etc),
    // do that when we need to.
    
    //access to this field AND to members in this field is synchronized, 
    //to allow us to preserve order while guaranteeing thread-safe
    //(but more testing is needed before we are completely sure it is thread-safe!)
    //synch blocks are as finely grained as possible for efficiency;
    //NB CopyOnWriteArraySet is a perf bottleneck, and the simple map makes it easier to remove when a tag is empty
    private Map<Object,Set<Task<?>>> tasksByTag = new HashMap<Object,Set<Task<?>>>();
    
    private ConcurrentMap<String,Task<?>> tasksById = new ConcurrentHashMap<String,Task<?>>();

    private ConcurrentMap<Object, TaskScheduler> schedulerByTag = new ConcurrentHashMap<Object, TaskScheduler>();

    /** count of all tasks submitted, including finished */
    private final AtomicLong totalTaskCount = new AtomicLong();
    
    /** tasks submitted but not yet done (or in cases of interruption/cancelled not yet GC'd) */
    private Set<String> incompleteTaskIds = Sets.newConcurrentHashSet();
    
    /** tasks started but not yet finished */
    private final AtomicInteger activeTaskCount = new AtomicInteger();
    
    private final List<ExecutionListener> listeners = new CopyOnWriteArrayList<ExecutionListener>();
    
    private final static ThreadLocal<String> threadOriginalName = new ThreadLocal<String>() {
        @Override
        protected String initialValue() {
            // should not happen, as only access is in _afterEnd with a check that _beforeStart was invoked 
            log.warn("No original name recorded for thread "+Thread.currentThread().getName()+"; task "+Tasks.current());
            return "brooklyn-thread-pool-"+Identifiers.makeRandomId(8);
        }
    };
    
    public BasicExecutionManager(String contextid) {
        threadFactory = newThreadFactory(contextid);
        daemonThreadFactory = new ThreadFactoryBuilder()
                .setThreadFactory(threadFactory)
                .setDaemon(true)
                .build();
                
        // use Executors.newCachedThreadPool(daemonThreadFactory), but timeout of 1s rather than 60s for better shutdown!
        runner = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), 
                daemonThreadFactory);
            
        delayedRunner = new ScheduledThreadPoolExecutor(1, daemonThreadFactory);

        if (jitterThreads) {
            log.info("Task startup jittering enabled with a maximum of " + jitterThreadsMaxDelay + " delay.");
        }
    }
    
    private final static class UncaughtExceptionHandlerImplementation implements Thread.UncaughtExceptionHandler {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            log.error("Uncaught exception in thread "+t.getName(), e);
        }
    }
    
    /** 
     * For use by overriders to use custom thread factory.
     * But be extremely careful: called by constructor, so before sub-class' constructor will
     * have been invoked!
     */
    protected ThreadFactory newThreadFactory(String contextid) {
        return new ThreadFactoryBuilder()
                .setNameFormat("brooklyn-execmanager-"+contextid+"-%d")
                .setUncaughtExceptionHandler(new UncaughtExceptionHandlerImplementation())
                .build();
    }
    
    public void shutdownNow() {
        shutdownNow(null);
    }
    
    /** shuts down the executor, and if a duration is supplied awaits termination for that long.
     * @return whether everything is terminated
     */
    @Beta
    public boolean shutdownNow(Duration howLongToWaitForTermination) {
        runner.shutdownNow();
        delayedRunner.shutdownNow();
        if (howLongToWaitForTermination!=null) {
            CountdownTimer timer = howLongToWaitForTermination.countdownTimer();
            try {
                runner.awaitTermination(timer.getDurationRemaining().toMilliseconds(), TimeUnit.MILLISECONDS);
                if (timer.isLive()) delayedRunner.awaitTermination(timer.getDurationRemaining().toMilliseconds(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            }
        }
        return runner.isTerminated() && delayedRunner.isTerminated();
    }
    
    public void addListener(ExecutionListener listener) {
        listeners.add(listener);
    }
    
    public void removeListener(ExecutionListener listener) {
        listeners.remove(listener);
    }
    
    /**
     * Deletes the given tag, including all tasks using this tag.
     * 
     * Useful, for example, if an entity is being expunged so that we don't keep holding
     * a reference to it as a tag.
     */
    public void deleteTag(Object tag) {
        Set<Task<?>> tasks;
        synchronized (tasksByTag) {
            tasks = tasksByTag.remove(tag);
        }
        if (tasks != null) {
            for (Task<?> task : tasks) {
                deleteTask(task);
            }
        }
    }

    public void deleteTask(Task<?> task) {
        boolean removed = deleteTaskNonRecursive(task);
        if (!removed) return;
        
        if (task instanceof HasTaskChildren) {
            List<Task<?>> children = ImmutableList.copyOf(((HasTaskChildren)task).getChildren());
            for (Task<?> child : children) {
                deleteTask(child);
            }
        }
    }

    protected boolean deleteTaskNonRecursive(Task<?> task) {
        Set<?> tags = TaskTags.getTagsFast(checkNotNull(task, "task"));
        for (Object tag : tags) {
            synchronized (tasksByTag) {
                Set<Task<?>> tasks = tasksWithTagLiveOrNull(tag);
                if (tasks != null) {
                    tasks.remove(task);
                    if (tasks.isEmpty()) {
                        tasksByTag.remove(tag);
                    }
                }
            }
        }
        Task<?> removed = tasksById.remove(task.getId());
        incompleteTaskIds.remove(task.getId());
        if (removed!=null && removed.isSubmitted() && !removed.isDone(true)) {
            Entity context = BrooklynTaskTags.getContextEntity(removed);
            if (!Entities.isManaged(context)) {
                log.debug("Forgetting about active task on unmanagement of "+context+": "+removed);
            } else {
                log.warn("Deleting submitted task before completion: "+removed+"; this task will continue to run in the background outwith "+this+", but perhaps it should have been cancelled?");
            }
        }
        return removed != null;
    }

    @Override
    public boolean isShutdown() {
        return runner.isShutdown();
    }
    
    /** count of all tasks submitted */
    public long getTotalTasksSubmitted() {
        return totalTaskCount.get();
    }
    
    /** count of tasks submitted but not ended */
    public long getNumIncompleteTasks() {
        return incompleteTaskIds.size();
    }
    
    /** count of tasks started but not ended */
    public long getNumActiveTasks() {
        return activeTaskCount.get();
    }

    /** count of tasks kept in memory, often including ended tasks */
    public long getNumInMemoryTasks() {
        return tasksById.size();
    }

    private Set<Task<?>> tasksWithTagCreating(Object tag) {
        Preconditions.checkNotNull(tag);
        synchronized (tasksByTag) {
            Set<Task<?>> result = tasksWithTagLiveOrNull(tag);
            if (result==null) {
                result = Collections.synchronizedSet(new LinkedHashSet<Task<?>>());
                tasksByTag.put(tag, result);
            }
            return result;
        }
    }

    /** exposes live view, for internal use only */
    @Beta
    public Set<Task<?>> tasksWithTagLiveOrNull(Object tag) {
        synchronized (tasksByTag) {
            return tasksByTag.get(tag);
        }
    }

    @Override
    public Task<?> getTask(String id) {
        return tasksById.get(id);
    }
    
    /** not on interface because potentially expensive */
    public List<Task<?>> getAllTasks() {
        // not sure if synching makes any difference; have not observed CME's yet
        // (and so far this is only called when a CME was caught on a previous operation)
        synchronized (tasksById) {
            return MutableList.copyOf(tasksById.values());
        }
    }
    
    @Override
    public Set<Task<?>> getTasksWithTag(Object tag) {
        Set<Task<?>> result = tasksWithTagLiveOrNull(tag);
        if (result==null) return Collections.emptySet();
        synchronized (result) {
            return Collections.unmodifiableSet(new LinkedHashSet<Task<?>>(result));
        }
    }
    
    @Override
    public Set<Task<?>> getTasksWithAnyTag(Iterable<?> tags) {
        Set<Task<?>> result = new LinkedHashSet<Task<?>>();
        Iterator<?> ti = tags.iterator();
        while (ti.hasNext()) {
            Set<Task<?>> tasksForTag = tasksWithTagLiveOrNull(ti.next());
            if (tasksForTag!=null) {
                synchronized (tasksForTag) {
                    result.addAll(tasksForTag);
                }
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /** only works with at least one tag; returns empty if no tags */
    @Override
    public Set<Task<?>> getTasksWithAllTags(Iterable<?> tags) {
        //NB: for this method retrieval for multiple tags could be made (much) more efficient (if/when it is used with multiple tags!)
        //by first looking for the least-used tag, getting those tasks, and then for each of those tasks
        //checking whether it contains the other tags (looking for second-least used, then third-least used, etc)
        Set<Task<?>> result = new LinkedHashSet<Task<?>>();
        boolean first = true;
        Iterator<?> ti = tags.iterator();
        while (ti.hasNext()) {
            Object tag = ti.next();
            if (first) { 
                first = false;
                result.addAll(getTasksWithTag(tag));
            } else {
                result.retainAll(getTasksWithTag(tag));
            }
        }
        return Collections.unmodifiableSet(result);
    }

    /** live view of all tasks, for internal use only */
    @Beta
    public Collection<Task<?>> allTasksLive() { return tasksById.values(); }
    
    @Override
    public Set<Object> getTaskTags() { 
        synchronized (tasksByTag) {
            return Collections.unmodifiableSet(Sets.newLinkedHashSet(tasksByTag.keySet())); 
        }
    }

    @Override public Task<?> submit(Runnable r) { return submit(new LinkedHashMap<Object,Object>(1), r); }
    @Override public Task<?> submit(Map<?,?> flags, Runnable r) { return submit(flags, new BasicTask<Void>(flags, r)); }

    @Override public <T> Task<T> submit(Callable<T> c) { return submit(new LinkedHashMap<Object,Object>(1), c); }
    @Override public <T> Task<T> submit(Map<?,?> flags, Callable<T> c) { return submit(flags, new BasicTask<T>(flags, c)); }

    @Override public <T> Task<T> submit(TaskAdaptable<T> t) { return submit(new LinkedHashMap<Object,Object>(1), t); }

    @Override
    public <T> Task<T> submit(Map<?,?> flags, TaskAdaptable<T> task) {
        if (!(task instanceof Task))
            task = task.asTask();
        synchronized (task) {
            if (((TaskInternal<?>)task).getInternalFuture()!=null) return (Task<T>)task;
            return submitNewTask(flags, (Task<T>) task);
        }
    }

    public <T> Task<T> scheduleWith(Task<T> task) { return scheduleWith(Collections.emptyMap(), task); }
    public <T> Task<T> scheduleWith(Map<?,?> flags, Task<T> task) {
        synchronized (task) {
            if (((TaskInternal<?>)task).getInternalFuture()!=null) return task;
            return submitNewTask(flags, task);
        }
    }

    protected Task<?> submitNewScheduledTask(final Map<?,?> flags, final ScheduledTask task) {
        beforeSubmitScheduledTaskAllIterations(flags, task);
        
        if (!submitSubsequentScheduledTask(flags, task)) {
            afterEndScheduledTaskAllIterations(flags, task, null);
        }
        return task;
    }
    
    private boolean submitSubsequentScheduledTask(final Map<?,?> flags, final ScheduledTask task) {
        if (!task.isDone()) {
            task.internalFuture = delayedRunner.schedule(new ScheduledTaskCallable(task, flags),
                task.delay.toNanoseconds(), TimeUnit.NANOSECONDS);
            return true;
        } else {
            return false;
        }
    }

    protected class ScheduledTaskCallable implements Callable<Object> {
        public ScheduledTask task;
        public Map<?,?> flags;

        public ScheduledTaskCallable(ScheduledTask task, Map<?, ?> flags) {
            this.task = task;
            this.flags = flags;
        }

        @Override
        @SuppressWarnings({ "rawtypes", "unchecked" })
        public Object call() {
            if (task.startTimeUtc==-1) {
                // this is overwritten on each run; not sure if that's best or not
                task.startTimeUtc = System.currentTimeMillis();
            }
            TaskInternal<?> taskScheduled = null;
            Throwable error = null;
            try {
                taskScheduled = (TaskInternal<?>) task.newTask();
                taskScheduled.setSubmittedByTask(task);
                beforeStartScheduledTaskSubmissionIteration(flags, task, taskScheduled);
                final Callable<?> oldJob = taskScheduled.getJob();
                final TaskInternal<?> taskScheduledF = taskScheduled;
                taskScheduled.setJob(new Callable() { @Override public Object call() {
                    if (task.isCancelled()) {
                        afterEndScheduledTaskAllIterations(flags, task, new CancellationException("cancel detected"));
                        throw new CancellationException("cancel detected");  // above throws, but for good measure
                    }
                    Throwable lastError = null;
                    boolean shouldResubmit = true;
                    task.recentRun = taskScheduledF;
                    try {
                        synchronized (task) {
                            task.notifyAll();
                        }
                        Object result;
                        try {
                            lastError = null;
                            result = oldJob.call();
                            task.lastThrownType = null;
                        } catch (Exception e) {
                            lastError = e;
                            shouldResubmit = shouldResubmitOnException(oldJob, e);
                            throw Exceptions.propagate(e);
                        }
                        return result;
                    } finally {
                        // do in finally block in case we were interrupted
                        if (shouldResubmit && resubmit()) {
                            // resubmitted fine, no-op
                        } else {
                            // not resubmitted, note ending
                            afterEndScheduledTaskAllIterations(flags, task, lastError);
                        }
                    }
                }});
                task.nextRun = taskScheduled;
                BasicExecutionContext ec = BasicExecutionContext.getCurrentExecutionContext();
                if (ec!=null) return ec.submit(taskScheduled);
                else return submit(taskScheduled);

            } catch (Exception e) {
                error = e;
                throw Exceptions.propagate(e);
                
            } finally {
                afterEndScheduledTaskSubmissionIteration(flags, task, taskScheduled, error);
            }
        }

        private boolean resubmit() {
            task.runCount++;
            if (task.period!=null && !task.isCancelled()) {
                task.delay = task.period;
                return submitSubsequentScheduledTask(flags, task);
            } else {
                return false;
            }
        }

        private boolean shouldResubmitOnException(Callable<?> oldJob, Exception e) {
            String message = "Error executing " + oldJob + " (scheduled job of " + task + " - " + task.getDescription() + ")";
            if (Tasks.isInterrupted()) {
                log.debug(message + "; cancelling scheduled execution: " + e);
                return false;
            } else if (task.cancelOnException) {
                log.warn(message + "; cancelling scheduled execution.", e);
                return false;
            } else {
                message += "; resubmitting task and throwing: " + e;
                if (!e.getClass().equals(task.lastThrownType)) {
                    task.lastThrownType = e.getClass();
                    message += " (logging subsequent exceptions at trace)";
                    log.debug(message);
                } else {
                    message += " (repeat exception)";
                    log.trace(message);
                }
                return true;
            }
        }

        @Override
        public String toString() {
            return "ScheduledTaskCallable["+task+","+flags+"]";
        }
    }

    private final class SubmissionCallable<T> implements Callable<T> {
        private final Map<?, ?> flags;
        private final Task<T> task;

        private SubmissionCallable(Map<?, ?> flags, Task<T> task) {
            this.flags = flags;
            this.task = task;
        }

        @Override
        public T call() {
            T result = null;
            Throwable error = null;
            try {
                beforeStartAtomicTask(flags, task);
                if (task.isCancelled()) {
                    afterEndForCancelBeforeStart(flags, task, false);
                    throw new CancellationException();
                }
                result = ((TaskInternal<T>)task).getJob().call();
            } catch(Throwable e) {
                error = e;
            } finally {
                afterEndAtomicTask(flags, task, error);
            }
            return result;
        }

        @Override
        public String toString() {
            return "BEM.call("+task+","+flags+")";
        }
    }

    final static class CancellingListenableForwardingFutureForTask<T> extends SimpleForwardingFuture<T> implements ListenableFuture<T> {
        private final Task<T> task;
        private BasicExecutionManager execMgmt;
        private final ExecutionList listeners;
        
        private CancellingListenableForwardingFutureForTask(BasicExecutionManager execMgmt, Future<T> delegate, ExecutionList list, Task<T> task) {
            super(delegate);
            this.listeners = list;
            this.execMgmt = execMgmt;
            this.task = task;
        }

        @Override
        public final boolean cancel(boolean mayInterrupt) {
            return cancel(TaskCancellationMode.INTERRUPT_TASK_AND_DEPENDENT_SUBMITTED_TASKS);
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            listeners.add(listener, executor);
        }

        public ExecutionList getListeners() {
            return listeners;
        }
        
        boolean cancel(TaskCancellationMode mode) {
            boolean result = false;
            if (log.isTraceEnabled()) {
                log.trace("CLFFT cancelling "+task+" mode "+mode);
            }
            if (!task.isCancelled()) result |= ((TaskInternal<T>)task).cancel(mode);
            result |= delegate().cancel(mode.isAllowedToInterruptTask());
            
            if (mode.isAllowedToInterruptDependentSubmittedTasks()) {
                int subtasksFound=0;
                int subtasksReallyCancelled=0;
                
                if (task instanceof HasTaskChildren) {
                    // cancel tasks in reverse order --
                    // it should be the case that if child1 is cancelled,
                    // a parentTask should NOT call a subsequent child2,
                    // but just in case, we cancel child2 first
                    // NB: DST and others may apply their own recursive cancel behaviour
                    MutableList<Task<?>> childrenReversed = MutableList.copyOf( ((HasTaskChildren)task).getChildren() );
                    Collections.reverse(childrenReversed);
                    
                    for (Task<?> child: childrenReversed) {
                        if (log.isTraceEnabled()) {
                            log.trace("Cancelling "+child+" on recursive cancellation of "+task);
                        }
                        subtasksFound++;
                        if (((TaskInternal<?>)child).cancel(mode)) {
                            result = true;
                            subtasksReallyCancelled++;
                        }
                    }
                }
                for (Task<?> t: execMgmt.getAllTasks()) {
                    if (task.equals(t.getSubmittedByTask())) {
                        if (mode.isAllowedToInterruptAllSubmittedTasks() || BrooklynTaskTags.isTransient(t)) {
                            if (log.isTraceEnabled()) {
                                log.trace("Cancelling "+t+" on recursive cancellation of "+task);
                            }
                            subtasksFound++;
                            if (((TaskInternal<?>)t).cancel(mode)) {
                                result = true;
                                subtasksReallyCancelled++;
                            }
                        }
                    }
                }
                if (log.isTraceEnabled()) {
                    log.trace("On cancel of "+task+", applicable subtask count "+subtasksFound+", of which "+subtasksReallyCancelled+" were actively cancelled");
                }
            }
  
            execMgmt.afterEndForCancelBeforeStart(null, task, true);
            return result;
        }
    }

    // NB: intended to be run by task.runListeners, used to run any listeners the manager wants 
    private final class SubmissionListenerToCallManagerListeners<T> implements Runnable {
        private final Task<T> task;
        private final CancellingListenableForwardingFutureForTask<T> listenerSource;

        public SubmissionListenerToCallManagerListeners(Task<T> task, CancellingListenableForwardingFutureForTask<T> listenableFuture) {
            this.task = task;
            listenerSource = listenableFuture;
        }

        @Override
        public void run() {
            for (ExecutionListener listener : listeners) {
                try {
                    listener.onTaskDone(task);
                } catch (Exception e) {
                    log.warn("Error running execution listener "+listener+" of task "+task+" done", e);
                }
            }
            // run any listeners the task owner has added to its future
            listenerSource.getListeners().execute();
        }
    }

    @SuppressWarnings("unchecked")
    protected <T> Task<T> submitNewTask(final Map<?,?> flags, final Task<T> task) {
        if (log.isTraceEnabled()) {
            log.trace("Submitting task {} ({}), with flags {}, and tags {}, job {}; caller {}", 
                new Object[] {task.getId(), task, Sanitizer.sanitize(flags), BrooklynTaskTags.getTagsFast(task), 
                (task instanceof TaskInternal ? ((TaskInternal<T>)task).getJob() : "<unavailable>"),
                Tasks.current() });
            if (Tasks.current()==null && BrooklynTaskTags.isTransient(task)) {
                log.trace("Stack trace for unparented submission of transient "+task, new Throwable("trace only (not an error)"));
            }
        }
        
        if (task instanceof ScheduledTask)
            return (Task<T>) submitNewScheduledTask(flags, (ScheduledTask)task);
        
        beforeSubmitAtomicTask(flags, task);
        
        if (((TaskInternal<T>)task).getJob() == null) 
            throw new NullPointerException("Task "+task+" submitted with with null job: job must be supplied.");
        
        Callable<T> job = new SubmissionCallable<T>(flags, task);
        
        // If there's a scheduler then use that; otherwise execute it directly
        Set<TaskScheduler> schedulers = null;
        for (Object tago: BrooklynTaskTags.getTagsFast(task)) {
            TaskScheduler scheduler = getTaskSchedulerForTag(tago);
            if (scheduler!=null) {
                if (schedulers==null) schedulers = new LinkedHashSet<TaskScheduler>(2);
                schedulers.add(scheduler);
            }
        }
        Future<T> future;
        if (schedulers!=null && !schedulers.isEmpty()) {
            if (schedulers.size()>1) log.warn("multiple schedulers detected, using only the first, for "+task+": "+schedulers);
            future = schedulers.iterator().next().submit(job);
        } else {
            future = runner.submit(job);
        }
        afterSubmitRecordFuture(task, future);
        
        return task;
    }

    protected <T> void afterSubmitRecordFuture(final Task<T> task, Future<T> future) {
        // SubmissionCallable (above) invokes the listeners on completion;
        // this future allows a caller to add custom listeners
        // (it does not notify the listeners; that's our job);
        // except on cancel we want to listen
        CancellingListenableForwardingFutureForTask<T> listenableFuture = new CancellingListenableForwardingFutureForTask<T>(this, future, ((TaskInternal<T>)task).getListeners(), task);
        // and we want to make sure *our* (manager) listeners are given suitable callback 
        ((TaskInternal<T>)task).addListener(new SubmissionListenerToCallManagerListeners<T>(task, listenableFuture), runner);
        // NB: can the above mean multiple callbacks to TaskInternal#runListeners?
        
        // finally expose the future to callers
        ((TaskInternal<T>)task).initInternalFuture(listenableFuture);
    }
    
    protected void beforeSubmitScheduledTaskAllIterations(Map<?,?> flags, Task<?> task) {
        internalBeforeSubmit(flags, task);
    }
    protected void beforeSubmitAtomicTask(Map<?,?> flags, Task<?> task) {
        internalBeforeSubmit(flags, task);
    }
    protected void beforeSubmitInSameThreadTask(Map<?,?> flags, Task<?> task) {
        internalBeforeSubmit(flags, task);
    }
    /** invoked when a task is submitted */
    protected void internalBeforeSubmit(Map<?,?> flags, Task<?> task) {
        incompleteTaskIds.add(task.getId());
        
        if (task.getSubmittedByTaskId()==null) {
            Task<?> currentTask = Tasks.current();
            if (currentTask!=null) ((TaskInternal<?>)task).setSubmittedByTask(
                    // do this instead of soft reference (2017-09) as soft refs impact GC 
                    Maybe.of(new TaskLookup(this, currentTask)) );
        }
        ((TaskInternal<?>)task).setSubmitTimeUtc(System.currentTimeMillis());
        
        if (flags!=null && flags.get("tag")!=null) ((TaskInternal<?>)task).getMutableTags().add(flags.remove("tag"));
        if (flags!=null && flags.get("tags")!=null) ((TaskInternal<?>)task).getMutableTags().addAll((Collection<?>)flags.remove("tags"));

        for (Object tag: BrooklynTaskTags.getTagsFast(task)) {
            tasksWithTagCreating(tag).add(task);
        }
        
        tasksById.put(task.getId(), task);
        totalTaskCount.incrementAndGet();
    }
    
    private static class TaskLookup implements Supplier<Task<?>> {
        // this class is not meant to be serialized, but if it is, make sure exec mgr doesn't sneak in
        transient BasicExecutionManager mgr;
        
        String id;
        String displayName;
        public TaskLookup(BasicExecutionManager mgr, Task<?> t) {
            this.mgr = mgr;
            id = t.getId();
            if (mgr.getTask(id)==null) {
                log.warn("Created task lookup for task which is not registered: "+t);
            }
            displayName = t.getDisplayName();
        }
        @Override
        public Task<?> get() {
            if (mgr==null) return gone();
            Task<?> result = mgr.getTask(id);
            if (result!=null) return result;
            return gone();
        }
        private <T> Task<T> gone() {
            Task<T> t = Tasks.<T>builder().dynamic(false).displayName(displayName+" (placeholder for "+id+")")
                .description("Details of the original task have been forgotten.")
                .body(Callables.returning((T)null)).build();
            ((BasicTask<T>)t).ignoreIfNotRun();
            ((BasicTask<T>)t).cancelled = true;
            return t;
        }
    }

    protected void beforeStartScheduledTaskSubmissionIteration(Map<?,?> flags, Task<?> taskRepeatedlyScheduling, Task<?> taskIteration) {
        internalBeforeStart(flags, taskRepeatedlyScheduling, true);
    }
    protected void beforeStartAtomicTask(Map<?,?> flags, Task<?> task) {
        internalBeforeStart(flags, task, true);
    }
    protected void beforeStartInSameThreadTask(Map<?,?> flags, Task<?> task) {
        internalBeforeStart(flags, task, false);
    }
    
    /** invoked in a task's thread when a task is starting to run (may be some time after submitted), 
     * but before doing any of the task's work, so that we can update bookkeeping and notify callbacks */
    protected void internalBeforeStart(Map<?,?> flags, Task<?> task, boolean allowJitter) {
        int count = activeTaskCount.incrementAndGet();
        if (count % 1000==0) {
            log.warn("High number of active tasks: task #"+count+" is "+task);
        }
        
        //set thread _before_ start time, so we won't get a null thread when there is a start-time
        if (log.isTraceEnabled()) log.trace(""+this+" beforeStart, task: "+task + " running on thread " + Thread.currentThread().getName());
        if (!task.isCancelled()) {
            Thread thread = Thread.currentThread();
            ((TaskInternal<?>)task).setThread(thread);
            if (RENAME_THREADS) {
                threadOriginalName.set(thread.getName());
                String newThreadName = "brooklyn-" + CaseFormat.LOWER_HYPHEN.to(CaseFormat.LOWER_CAMEL, task.getDisplayName().replace(" ", "")) + "-" + task.getId().substring(0, 8);
                thread.setName(newThreadName);
            }
            PerThreadCurrentTaskHolder.perThreadCurrentTask.set(task);
            ((TaskInternal<?>)task).setStartTimeUtc(System.currentTimeMillis());
        }

        if (allowJitter) {
            jitterThreadStart(task);
        }
        if (flags!=null) {
            invokeCallback(flags.get("newTaskStartCallback"), task);
        }
    }

    private void jitterThreadStart(Task<?> task) {
        if (jitterThreads) {
            try {
                Thread.sleep(ThreadLocalRandom.current().nextInt(jitterThreadsMaxDelay));
            } catch (InterruptedException e) {
                log.warn("Task " + task + " got cancelled before starting because of jitter.");
                throw Exceptions.propagate(e);
            }
        }
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    // not ideal, such loose typing on the callback -- should prefer Function<Task,Object>
    // but at least it's package-private
    static Object invokeCallback(Object callable, Task<?> task) {
        if (callable instanceof Closure) {
            if (!loggedClosureDeprecatedInInvokeCallback) {
                log.warn("Use of groovy.lang.Closure is deprecated, in ExecutionManager.invokeCallback");
                loggedClosureDeprecatedInInvokeCallback = true;
            }
            return ((Closure<?>)callable).call(task);
        }
        if (callable instanceof Callable) {
            try {
                return ((Callable<?>)callable).call();
            } catch (Throwable t) {
                throw Exceptions.propagate(t);
            }
        }
        if (callable instanceof Runnable) { ((Runnable)callable).run(); return null; }
        if (callable instanceof Function) { return ((Function)callable).apply(task); }
        if (callable==null) return null;
        throw new IllegalArgumentException("Cannot invoke unexpected callback object "+callable+" of type "+callable.getClass()+" on "+task);
    }
    private static boolean loggedClosureDeprecatedInInvokeCallback;
    
    /** normally (if not interrupted) called once for each call to {@link #beforeSubmitScheduledTaskAllIterations(Map, Task)} */
    protected void afterEndScheduledTaskAllIterations(Map<?,?> flags, Task<?> taskRepeatedlyScheduling, Throwable error) {
        internalAfterEnd(flags, taskRepeatedlyScheduling, false, true, error);
    }
    /** called once for each call to {@link #beforeStartScheduledTaskSubmissionIteration(Map, Task)},
     * with a per-iteration task generated by the surrounding scheduled task */
    protected void afterEndScheduledTaskSubmissionIteration(Map<?,?> flags, Task<?> taskRepeatedlyScheduling, Task<?> taskIteration, Throwable error) {
        internalAfterEnd(flags, taskRepeatedlyScheduling, true, false, error);
    }
    /** called once for each task on which {@link #beforeStartAtomicTask(Map, Task)} is invoked,
     * and normally (if not interrupted prior to start) 
     * called once for each task on which {@link #beforeSubmitAtomicTask(Map, Task)} */
    protected void afterEndAtomicTask(Map<?,?> flags, Task<?> task, Throwable error) {
        internalAfterEnd(flags, task, true, true, error);
    }
    protected void afterEndInSameThreadTask(Map<?,?> flags, Task<?> task, Throwable error) {
        internalAfterEnd(flags, task, true, true, error);
    }
    protected void afterEndForCancelBeforeStart(Map<?,?> flags, Task<?> task, boolean calledFromCanceller) {
        if (calledFromCanceller) {
            if (task.isBegun()) {
                // do nothing from canceller thread if task has begin;
                // we don't want to set end time or listeners prematurely.
                return ;
            } else {
                // normally task won't be submitted by executor, so do some of the end operations.
                // there is a chance task has begun but not set start time,
                // in which case _both_ canceller thread and task thread will run this,
                // but they will happen very close in time so end time is set sensibly by either,
                // and dedupe will be done based on presence in "incompleteTaskIds"
                // to ensure listeners and callback only invoked once
            }
        }
        internalAfterEnd(flags, task, !calledFromCanceller, true, null);
    }
    
    /** normally (if not interrupted) called once for each call to {@link #internalBeforeSubmit(Map, Task)},
     * and, for atomic tasks and scheduled-task submission iterations where 
     * always called once if {@link #internalBeforeStart(Map, Task)} is invoked and in the same thread as that method */
    protected void internalAfterEnd(Map<?,?> flags, Task<?> task, boolean startedInThisThread, boolean isEndingAllIterations, Throwable error) {
        boolean taskWasSubmittedAndNotYetEnded = true;
        try {
            if (log.isTraceEnabled()) log.trace(this+" afterEnd, task: "+task);
            if (startedInThisThread) {
                activeTaskCount.decrementAndGet();
            }
            if (isEndingAllIterations) {
                taskWasSubmittedAndNotYetEnded = incompleteTaskIds.remove(task.getId());
                if (flags!=null && taskWasSubmittedAndNotYetEnded) {
                    invokeCallback(flags.get("newTaskEndCallback"), task);
                }
                ((TaskInternal<?>)task).setEndTimeUtc(System.currentTimeMillis());
            }
    
            if (startedInThisThread) {
                PerThreadCurrentTaskHolder.perThreadCurrentTask.remove();
                //clear thread _after_ endTime set, so we won't get a null thread when there is no end-time
                if (RENAME_THREADS) {
                    Thread thread = task.getThread();
                    if (thread==null) {
                        log.warn("BasicTask.afterEnd invoked without corresponding beforeStart");
                    } else {
                        thread.setName(threadOriginalName.get());
                        threadOriginalName.remove();
                    }
                }
                ((TaskInternal<?>)task).setThread(null);
            }
        } finally {
            try {
                if (error!=null) {
                    /* we throw, after logging debug.
                     * the throw means the error is available for task submitters to monitor.
                     * however it is possible no one is monitoring it, in which case we will have debug logging only for errors.
                     * (the alternative, of warn-level logging in lots of places where we don't want it, seems worse!) 
                     */
                    if (log.isDebugEnabled()) {
                        // debug only here, because most submitters will handle failures
                        if (error instanceof InterruptedException || error instanceof RuntimeInterruptedException) {
                            log.debug("Detected interruption on task "+task+" (rethrowing)" +
                                    (Strings.isNonBlank(error.getMessage()) ? ": "+error.getMessage() : ""));
                        } else if (error instanceof NullPointerException ||
                                error instanceof IndexOutOfBoundsException ||
                                error instanceof ClassCastException) {
                            log.debug("Exception running task "+task+" (rethrowing): "+error, error);
                        } else {
                            log.debug("Exception running task "+task+" (rethrowing): "+error);
                        }
                        if (log.isTraceEnabled()) {
                            log.trace("Trace for exception running task "+task+" (rethrowing): "+error, error);
                        }
                    }
                    throw Exceptions.propagate(error);
                }
            } finally {
                synchronized (task) { task.notifyAll(); }
                if (isEndingAllIterations && taskWasSubmittedAndNotYetEnded) {
                    // prevent from running twice on cancellation after start
                    ((TaskInternal<?>)task).runListeners();
                }
            }
        }
    }

    public TaskScheduler getTaskSchedulerForTag(Object tag) {
        return schedulerByTag.get(tag);
    }
    
    public void setTaskSchedulerForTag(Object tag, Class<? extends TaskScheduler> scheduler) {
        synchronized (schedulerByTag) {
            TaskScheduler old = getTaskSchedulerForTag(tag);
            if (old!=null) {
                if (scheduler.isAssignableFrom(old.getClass())) {
                    /* already have such an instance */
                    return;
                }
                //might support multiple in future...
                throw new IllegalStateException("Not allowed to set multiple TaskSchedulers on ExecutionManager tag (tag "+tag+", has "+old+", setting new "+scheduler+")");
            }
            try {
                TaskScheduler schedulerI = scheduler.newInstance();
                // allow scheduler to have a nice name, for logging etc
                if (schedulerI instanceof CanSetName) ((CanSetName)schedulerI).setName(""+tag);
                setTaskSchedulerForTag(tag, schedulerI);
            } catch (InstantiationException e) {
                throw Exceptions.propagate(e);
            } catch (IllegalAccessException e) {
                throw Exceptions.propagate(e);
            }
        }
    }
    
    /**
     * Defines a {@link TaskScheduler} to run on all subsequently submitted jobs with the given tag.
     *
     * Maximum of one allowed currently. Resubmissions of the same scheduler (or scheduler class)
     * allowed. If changing, you must call {@link #clearTaskSchedulerForTag(Object)} between the two.
     *
     * @see #setTaskSchedulerForTag(Object, Class)
     */
    public void setTaskSchedulerForTag(Object tag, TaskScheduler scheduler) {
        synchronized (schedulerByTag) {
            scheduler.injectExecutor(runner);

            Object old = schedulerByTag.put(tag, scheduler);
            if (old!=null && old!=scheduler) {
                //might support multiple in future...
                throw new IllegalStateException("Not allowed to set multiple TaskSchedulers on ExecutionManager tag (tag "+tag+")");
            }
        }
    }

    /**
     * Forgets that any scheduler was associated with a tag.
     *
     * @see #setTaskSchedulerForTag(Object, TaskScheduler)
     * @see #setTaskSchedulerForTag(Object, Class)
     */
    public boolean clearTaskSchedulerForTag(Object tag) {
        synchronized (schedulerByTag) {
            Object old = schedulerByTag.remove(tag);
            return (old!=null);
        }
    }
    
    @VisibleForTesting
    public ConcurrentMap<Object, TaskScheduler> getSchedulerByTag() {
        return schedulerByTag;
    }

    public void setJitterThreads(boolean jitterThreads) {
        this.jitterThreads = jitterThreads;
        if (jitterThreads) {
            log.info("Task startup jittering enabled with a maximum of " + jitterThreadsMaxDelay + " delay.");
        } else {
            log.info("Disabled task startup jittering");
        }
    }

    public void setJitterThreadsMaxDelay(int jitterThreadsMaxDelay) {
        this.jitterThreadsMaxDelay = jitterThreadsMaxDelay;
        log.info("Setting task startup jittering maximum delay to " + jitterThreadsMaxDelay);
    }

}
