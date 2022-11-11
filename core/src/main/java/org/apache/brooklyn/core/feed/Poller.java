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
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import com.google.common.collect.*;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.sensor.AbstractAddTriggerableSensor;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.predicates.DslPredicates;
import org.apache.brooklyn.util.core.task.DynamicSequentialTask;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;
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
    private final AbstractEntityAdjunct adjunct;
    private final boolean onlyIfServiceUp;
    private final Set<Callable<?>> oneOffJobs = new LinkedHashSet<Callable<?>>();
    private final Set<PollJob<V>> pollJobs = new LinkedHashSet<PollJob<V>>();
    private final Set<Task<?>> oneOffTasks = new LinkedHashSet<Task<?>>();
    private final Set<ScheduledTask> scheduledTasks = new LinkedHashSet<>();
    private volatile boolean started = false;

    public <PI,PC extends PollConfig> void scheduleFeed(AbstractFeed feed, SetMultimap<PI,PC> polls, Function<PI,Callable<?>> jobFactory) {
        for (final PI identifer : polls.keySet()) {
            Set<PC> pollConfigs = polls.get(identifer);
            Set<AttributePollHandler<?>> handlers = Sets.newLinkedHashSet();

            for (PC config : pollConfigs) {
                handlers.add(new AttributePollHandler(config, entity, feed));
            }

            Callable pollCallable = jobFactory.apply(identifer);
            DelegatingPollHandler handlerDelegate = new DelegatingPollHandler(handlers);
            schedulePoll(feed, pollConfigs, pollCallable, handlerDelegate);
        }
    }

    public void schedulePoll(AbstractEntityAdjunct feed, Set<? extends PollConfig> pollConfigs, Callable pollCallable, PollHandler pollHandler) {
        boolean subscribed = false;
        long minPeriodMillis = Long.MAX_VALUE;
        Set<Supplier<DslPredicates.DslPredicate>> conditions = MutableSet.of();

        for (PollConfig pc: pollConfigs) {
            conditions.add(pc.getCondition());
            if (pc.getPeriod() > 0) minPeriodMillis = Math.min(minPeriodMillis, pc.getPeriod());

            Set<Pair<Entity, Sensor>> triggersResolved = MutableSet.of();
            if (pc.getOtherTriggers()!=null) {
                triggersResolved.addAll(AbstractAddTriggerableSensor.resolveTriggers(feed.getEntity(), pc.getOtherTriggers()));
            }
            if (onlyIfServiceUp) {
                // if 'onlyIfServiceUp' is set then automatically subscribe to that sensor.
                // this is the default for ssh and other sensors which need a target machine. for others it defaults false.
                triggersResolved.add(Pair.of(feed.getEntity(), Attributes.SERVICE_UP));
            }

            for (Pair<Entity, Sensor> pair : triggersResolved) {
                subscribe(pollCallable, pollHandler, pair.getLeft(), pair.getRight(), pc.getCondition());
                subscribed = true;
            }
        }

        if (minPeriodMillis >0 && (minPeriodMillis < Duration.PRACTICALLY_FOREVER.toMilliseconds() || !subscribed)) {
            Supplier<DslPredicates.DslPredicate> condition = null;
            if (!conditions.isEmpty()) {
                if (conditions.size()==1) condition = Iterables.getOnlyElement(conditions);
                else if (conditions.contains(null)) /* if any are unset then ignored */ condition = null;
                else condition = () -> {
                    DslPredicates.DslPredicateDefault aggregate = new DslPredicates.DslPredicateDefault();
                    aggregate.any = conditions.stream().collect(Collectors.toList());
                    return aggregate;
                };
            }
            scheduleAtFixedRate(pollCallable, pollHandler, Duration.millis(minPeriodMillis), condition);
        }
    }

    private static class PollJob<V> {
        final PollHandler<? super V> handler;
        final Duration pollPeriod;
        final Callable<?> job;
        final Runnable wrappedJob;
        final Entity pollTriggerEntity;
        final Sensor<?> pollTriggerSensor;
        final Supplier<DslPredicates.DslPredicate> pollCondition;
        SubscriptionHandle subscription;
        private boolean loggedPreviousException = false;

        PollJob(final Callable<V> job, final PollHandler<? super V> handler, Duration period) {
            this(job, handler, period, null, null, null);
        }

        PollJob(final Callable<V> job, final PollHandler<? super V> handler, Duration period, Entity sensorSource, Sensor<?> sensor, Supplier<DslPredicates.DslPredicate> pollCondition) {
            this.handler = handler;
            this.pollPeriod = period;
            this.pollTriggerEntity = sensorSource;
            this.pollTriggerSensor = sensor;
            this.pollCondition = pollCondition;
            this.job = job;
            wrappedJob = new Runnable() {
                @Override
                public void run() {
                    try {
                        if (pollCondition!=null) {
                            DslPredicates.DslPredicate pc = pollCondition.get();
                            if (pc!=null) {
                                if (!pc.apply(BrooklynTaskTags.getContextEntity(Tasks.current()))) {
                                    if (log.isTraceEnabled()) log.trace("Skipping execution for PollJob {} because condition does not apply", job);
                                    log.debug("Skipping poll/feed execution because condition does not apply");  // log so we can see in log viewer
                                    return;
                                }
                            }
                        }
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

    public Poller(Entity entity, AbstractEntityAdjunct adjunct, boolean onlyIfServiceUp) {
        this.entity = entity;
        this.adjunct = adjunct;
        this.onlyIfServiceUp = onlyIfServiceUp;
    }
    
    /** Submits a one-off poll job; recommended that callers supply to-String so that task has a decent description */
    public void submit(Callable<?> job) {
        if (started) {
            throw new IllegalStateException("Cannot submit additional tasks after poller has started");
        }
        oneOffJobs.add(job);
    }

    public void scheduleAtFixedRate(Callable<V> job, PollHandler<? super V> handler, long periodMillis) {
        scheduleAtFixedRate(job, handler, Duration.millis(periodMillis), null);
    }
    public void scheduleAtFixedRate(Callable<V> job, PollHandler<? super V> handler, Duration period) {
        scheduleAtFixedRate(job, handler, period, null);
    }
    public void scheduleAtFixedRate(Callable<V> job, PollHandler<? super V> handler, Duration period, Supplier<DslPredicates.DslPredicate> pollCondition) {
        if (started) {
            throw new IllegalStateException("Cannot schedule additional tasks after poller has started");
        }
        PollJob<V> foo = new PollJob<V>(job, handler, period, null, null, pollCondition);
        pollJobs.add(foo);
    }

    public void subscribe(Callable<V> job, PollHandler<? super V> handler, Entity sensorSource, Sensor<?> sensor, Supplier<DslPredicates.DslPredicate> condition) {
        pollJobs.add(new PollJob<V>(job, handler, null, sensorSource, sensor, condition));
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
            oneOffTasks.add(adjunct.getExecutionContext().submit(task));
        }
        
        Duration minPeriod = null;
        Set<String> sensorSummaries = MutableSet.of();

        final Function<PollJob,String> scheduleNameFn = pollJob -> MutableList.of(adjunct !=null ? adjunct.getDisplayName() : null, pollJob.handler.getDescription())
                .stream().filter(Strings::isNonBlank).collect(Collectors.joining("; "));

        BiFunction<Runnable,String,Task<?>> tf = (job, scheduleName) -> {
            DynamicSequentialTask<Void> task = new DynamicSequentialTask<Void>(MutableMap.of("displayName", scheduleName, "entity", entity),
                    () -> {
                        if (!Entities.isManagedActive(entity)) {
                            return null;
                        }
                        if (onlyIfServiceUp && !Boolean.TRUE.equals(entity.getAttribute(Attributes.SERVICE_UP))) {
                            return null;
                        }
                        job.run();
                        return null;
                    });
            // explicitly make non-transient -- we want to see its execution, even if parent is transient
            BrooklynTaskTags.addTagDynamically(task, BrooklynTaskTags.NON_TRANSIENT_TASK_TAG);
            return task;
        };
        Multimap<Callable,PollJob> nonScheduledJobs = Multimaps.newSetMultimap(MutableMap.of(), MutableSet::of);
        pollJobs.forEach(pollJob -> nonScheduledJobs.put(pollJob.job, pollJob));

        // 'runInitially' could be an option on the job; currently we always do
        // if it's a scheduled task, that happens automatically; if it's a triggered task
        // we collect the distinct runnables and run each of those
        // (the poll job model doesn't work perfectly since usually all schedules/triggers are for the same job)

        for (final PollJob<V> pollJob : pollJobs) {
            String scheduleName = scheduleNameFn.apply(pollJob);
            boolean added = false;

            if (pollJob.pollPeriod!=null && pollJob.pollPeriod.compareTo(Duration.ZERO) > 0) {
                ScheduledTask.Builder tb = ScheduledTask.builder(() -> tf.apply(pollJob.wrappedJob, scheduleName))
                        .cancelOnException(false)
                        .tag(adjunct != null ? BrooklynTaskTags.tagForContextAdjunct(adjunct) : null);
                added = true;
                tb.displayName("Periodic: " + scheduleName);
                tb.period(pollJob.pollPeriod);

                if (minPeriod==null || (pollJob.pollPeriod.isShorterThan(minPeriod))) {
                    minPeriod = pollJob.pollPeriod;
                }
                ScheduledTask st = tb.build();
                scheduledTasks.add(st);
                log.debug("Submitting scheduled task "+st+" for poll/feed "+this+", job "+pollJob);
                Entities.submit(entity, st);
                nonScheduledJobs.removeAll(pollJob.job);
            }

            if (pollJob.pollTriggerSensor !=null) {
                added = true;
                if (pollJob.subscription!=null) {
                    throw new IllegalStateException(String.format("Attempt to start poller %s of entity %s when already has subscription %s",
                            this, entity, pollJob.subscription));
                }
                String summary = pollJob.pollTriggerSensor.getName();
                if (pollJob.pollTriggerEntity!=null && !pollJob.pollTriggerEntity.equals(entity)) summary += " on "+pollJob.pollTriggerEntity;
                log.debug("Adding subscription to "+summary+" for poll/feed "+this+", job "+pollJob);
                sensorSummaries.add(summary);
                pollJob.subscription = adjunct.subscriptions().subscribe(pollJob.pollTriggerEntity !=null ? pollJob.pollTriggerEntity : adjunct.getEntity(), pollJob.pollTriggerSensor, event -> {
                    // submit this on every event
                    try {
                        adjunct.getExecutionContext().submit(tf.apply(pollJob.wrappedJob, scheduleName));
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                });
            }

            if (!added) {
                if (log.isDebugEnabled()) log.debug("Empty poll job "+pollJob+" in "+this+" for "+entity+"; if all jobs are empty (or trigger only), will add a trivial one-time initial task");
            }
        }

        // no period for these, but we do need to run them initially, but combine if the Callable is the same (e.g. multiple triggers)
        // not the PollJob is one per trigger, and the wrappedJob is specific to the poll job, but doesn't depend on the trigger, so we can just take the first
        nonScheduledJobs.asMap().forEach( (jobC,jobP) -> {
            Runnable job = jobP.iterator().next().wrappedJob;
            String jobSummaries = jobP.stream().map(j -> j.handler.getDescription()).filter(Strings::isNonBlank).collect(Collectors.joining(", "));
            String name =  (adjunct !=null ? adjunct.getDisplayName() : "anonymous")+(Strings.isNonBlank(jobSummaries) ? "; "+jobSummaries : "");
            Task<Object> t = Tasks.builder().dynamic(true).displayName("Initial: " +name)
                    .body(
                            () -> DynamicTasks.queue(tf.apply(job, name)).getUnchecked())
                    .tag(adjunct != null ? BrooklynTaskTags.tagForContextAdjunct(adjunct) : null)
                    .build();
            log.debug("Submitting initial task "+t+" for poll/feed "+this+", job "+job+" (because otherwise is trigger-only)");
            Entities.submit(entity, t);
        });
        
        if (adjunct !=null) {
            if (sensorSummaries.isEmpty()) {
                if (minPeriod==null || minPeriod.equals(Duration.PRACTICALLY_FOREVER) || !minPeriod.isPositive()) {
                    adjunct.highlightTriggers("Not configured with a period or triggers");
                } else {
                    highlightTriggerPeriod(minPeriod);
                }
            } else if (minPeriod==null) {
                adjunct.highlightTriggers("Triggered by: "+ Strings.join(sensorSummaries, "; "));
            } else {
                // both
                adjunct.highlightTriggers("Running every "+minPeriod+" and on triggers: "+Strings.join(sensorSummaries, "; "));
            }
        }
    }

    void highlightTriggerPeriod(Duration minPeriod) {
        adjunct.highlightTriggers("Running every "+minPeriod);
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
        for (ScheduledTask task : scheduledTasks) {
            if (task != null) task.cancel();
        }
        for (PollJob<?> j: pollJobs) {
            if (j.subscription!=null) {
                adjunct.subscriptions().unsubscribe(j.subscription);
                j.subscription = null;
            }
        }
        oneOffTasks.clear();
        scheduledTasks.clear();
    }

    public boolean isRunning() {
        boolean hasActiveTasks = false;
        for (Task<?> task: scheduledTasks) {
            if (task.isBegun() && !task.isDone()) {
                hasActiveTasks = true;
                break;
            }
        }
        boolean hasSubscriptions = pollJobs.stream().anyMatch(j -> j.subscription!=null);
        if (!started && hasActiveTasks) {
            log.warn("Poller should not be running, but has active tasks, tasks: "+ scheduledTasks);
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
