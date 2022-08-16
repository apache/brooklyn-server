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
package org.apache.brooklyn.core.feed;

import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.DynamicSequentialTask;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.MoreObjects;


/** 
 * For executing periodic polls.
 * Jobs are added to the schedule, and then the poller is started.
 * The jobs will then be executed periodically, and the handler called for the result/failure.
 * 
 * Assumes the schedule+start will be done single threaded, and that stop will not be done concurrently.
 */
public class Poller<V> {
    public static final Logger log = LoggerFactory.getLogger(Poller.class);

    private final Entity entity;
    private final AbstractFeed feed;
    private final boolean onlyIfServiceUp;
    private final Set<Callable<?>> oneOffJobs = new LinkedHashSet<Callable<?>>();
    private final Set<PollJob<V>> pollJobs = new LinkedHashSet<PollJob<V>>();
    private final Set<Task<?>> oneOffTasks = new LinkedHashSet<Task<?>>();
    private final Set<ScheduledTask> tasks = new LinkedHashSet<ScheduledTask>();
    private volatile boolean started = false;
    
    private static class PollJob<V> {
        final PollHandler<? super V> handler;
        final Duration pollPeriod;
        final Runnable wrappedJob;
        final Entity sensorSource;
        final Sensor<?> sensor;
        SubscriptionHandle subscription;
        private boolean loggedPreviousException = false;

        PollJob(final Callable<V> job, final PollHandler<? super V> handler, Duration period) {
            this(job, handler, period, null, null);
        }

        PollJob(final Callable<V> job, final PollHandler<? super V> handler, Duration period, Entity sensorSource, Sensor<?> sensor) {
            this.handler = handler;
            this.pollPeriod = period;
            this.sensorSource = sensorSource;
            this.sensor = sensor;
            
            wrappedJob = new Runnable() {
                @Override
                public void run() {
                    try {
                        V val = job.call();
                        if (handler.checkSuccess(val)) {
                            handler.onSuccess(val);
                        } else {
                            handler.onFailure(val);
                        }
                        loggedPreviousException = false;
                    } catch (Exception e) {
                        if (loggedPreviousException) {
                            if (log.isTraceEnabled()) log.trace("PollJob for {}, repeated consecutive failures, handling {} using {}", job, e, handler);
                        } else {
                            if (log.isDebugEnabled()) log.debug("PollJob for {}, repeated consecutive failures, handling {} using {}", job, e, handler);
                            loggedPreviousException = true;
                        }
                        handler.onException(e);
                    }
                }
            };
        }
    }

    /** @deprecated since 0.12.0 pass in feed */
    @Deprecated
    public Poller(Entity entity, boolean onlyIfServiceUp) {
        this(entity, null, onlyIfServiceUp);
    }
    public Poller(Entity entity, AbstractFeed feed, boolean onlyIfServiceUp) {
        this.entity = entity;
        this.feed = feed;
        this.onlyIfServiceUp = onlyIfServiceUp;
    }
    
    /** Submits a one-off poll job; recommended that callers supply to-String so that task has a decent description */
    public void submit(Callable<?> job) {
        if (started) {
            throw new IllegalStateException("Cannot submit additional tasks after poller has started");
        }
        oneOffJobs.add(job);
    }

    public void scheduleAtFixedRate(Callable<V> job, PollHandler<? super V> handler, long period) {
        scheduleAtFixedRate(job, handler, Duration.millis(period));
    }
    public void scheduleAtFixedRate(Callable<V> job, PollHandler<? super V> handler, Duration period) {
        if (started) {
            throw new IllegalStateException("Cannot schedule additional tasks after poller has started");
        }
        PollJob<V> foo = new PollJob<V>(job, handler, period);
        pollJobs.add(foo);
    }

    public void subscribe(Callable<V> job, PollHandler<? super V> handler, Entity sensorSource, Sensor<?> sensor) {
        pollJobs.add(new PollJob<V>(job, handler, null, sensorSource, sensor));
    }

    @SuppressWarnings({ "unchecked" })
    public void start() {
        if (log.isDebugEnabled()) log.debug("Starting poll for {} (using {})", new Object[] {entity, this});
        if (started) { 
            throw new IllegalStateException(String.format("Attempt to start poller %s of entity %s when already running", 
                    this, entity));
        }
        
        started = true;
        
        for (final Callable<?> oneOffJob : oneOffJobs) {
            Task<?> task = Tasks.builder().dynamic(false).body((Callable<Object>) oneOffJob).displayName("Poll").description("One-time poll job "+oneOffJob).build();
            oneOffTasks.add(feed.getExecutionContext().submit(task));
        }
        
        Duration minPeriod = null;
        Set<String> sensors = MutableSet.of();
        for (final PollJob<V> pollJob : pollJobs) {
            final String scheduleName = (feed!=null ? feed.getDisplayName()+", " : "") +pollJob.handler.getDescription();
            boolean added = false;

            Callable<Task<?>> tf = () -> {
                DynamicSequentialTask<Void> task = new DynamicSequentialTask<Void>(MutableMap.of("displayName", scheduleName, "entity", entity),
                        new Callable<Void>() { @Override public Void call() {
                            if (!Entities.isManagedActive(entity)) {
                                return null;
                            }
                            if (onlyIfServiceUp && !Boolean.TRUE.equals(entity.getAttribute(Attributes.SERVICE_UP))) {
                                return null;
                            }
                            pollJob.wrappedJob.run();
                            return null;
                        } } );
                // explicitly make non-transient -- we want to see its execution, even if parent is transient
                BrooklynTaskTags.addTagDynamically(task, BrooklynTaskTags.NON_TRANSIENT_TASK_TAG);
                return task;
            };

            if (pollJob.pollPeriod!=null && pollJob.pollPeriod.compareTo(Duration.ZERO) > 0) {
                added =true;
                ScheduledTask t = ScheduledTask.builder(tf)
                        .displayName("Periodic: " + scheduleName)
                        .period(pollJob.pollPeriod)
                        .cancelOnException(false)
                        .tag(feed!=null ? BrooklynTaskTags.tagForContextAdjunct(feed) : null)
                        .build();
                tasks.add(Entities.submit(entity, t));
                if (minPeriod==null || (pollJob.pollPeriod.isShorterThan(minPeriod))) {
                    minPeriod = pollJob.pollPeriod;
                }
            }

            if (pollJob.sensor!=null) {
                added = true;
                if (pollJob.subscription!=null) {
                    throw new IllegalStateException(String.format("Attempt to start poller %s of entity %s when already has subscription %s",
                            this, entity, pollJob.subscription));
                }
                sensors.add(pollJob.sensor.getName());
                pollJob.subscription = feed.subscriptions().subscribe(pollJob.sensorSource!=null ? pollJob.sensorSource : feed.getEntity(), pollJob.sensor, event -> {
                    // submit this on every event
                    try {
                        feed.getExecutionContext().submit(tf.call());
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                });
            }

            if (!added) {
                if (log.isDebugEnabled()) log.debug("Activating poll (but leaving off, as period {} and no subscriptions) for {} (using {})", new Object[] {pollJob.pollPeriod, entity, this});
            }
        }
        
        if (feed!=null) {
            if (sensors.isEmpty()) {
                if (minPeriod==null) {
                    feed.highlightTriggers("Not configured with a period or triggers");
                } else {
                    feed.highlightTriggerPeriod(minPeriod);
                }
            } else if (minPeriod==null) {
                feed.highlightTriggers("Triggered by: "+sensors);
            } else {
                // both
                feed.highlightTriggers("Running every "+minPeriod+" and on triggers: "+sensors);
            }
        }
    }
    
    public void stop() {
        if (log.isDebugEnabled()) log.debug("Stopping poll for {} (using {})", new Object[] {entity, this});
        if (!started) { 
            throw new IllegalStateException(String.format("Attempt to stop poller %s of entity %s when not running", 
                    this, entity));
        }
        
        started = false;
        for (Task<?> task : oneOffTasks) {
            if (task != null) task.cancel(true);
        }
        for (ScheduledTask task : tasks) {
            if (task != null) task.cancel();
        }
        for (PollJob<?> j: pollJobs) {
            if (j.subscription!=null) {
                feed.subscriptions().unsubscribe(j.subscription);
                j.subscription = null;
            }
        }
        oneOffTasks.clear();
        tasks.clear();
    }

    public boolean isRunning() {
        boolean hasActiveTasks = false;
        for (Task<?> task: tasks) {
            if (task.isBegun() && !task.isDone()) {
                hasActiveTasks = true;
                break;
            }
        }
        boolean hasSubscriptions = pollJobs.stream().anyMatch(j -> j.subscription!=null);
        if (!started && hasActiveTasks) {
            log.warn("Poller should not be running, but has active tasks, tasks: "+tasks);
        }
        if (!started && hasSubscriptions) {
            log.warn("Poller should not be running, but has subscriptions on jobs: "+pollJobs);
        }
        return started && (hasActiveTasks || hasSubscriptions);
    }
    
    protected boolean isEmpty() {
        return pollJobs.isEmpty();
    }
    
    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("entity", entity).toString();
    }
}
