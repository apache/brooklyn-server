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
package org.apache.brooklyn.core.sensor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.JavaGroovyEquivalents;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicTask;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.ParallelTask;
import org.apache.brooklyn.util.core.task.TaskInternal;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.core.task.ValueResolver;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.NotManagedException;
import org.apache.brooklyn.util.exceptions.RuntimeTimeoutException;
import org.apache.brooklyn.util.groovy.GroovyJavaMethods;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.Maybe.Absent;
import org.apache.brooklyn.util.net.Urls;
import org.apache.brooklyn.util.text.StringFunctions;
import org.apache.brooklyn.util.text.StringFunctions.RegexReplacer;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import groovy.lang.Closure;

/** Conveniences for making tasks which run in entity {@link ExecutionContext}s, blocking on attributes from other entities, possibly transforming those;
 * these {@link Task} instances are typically passed in {@link Entity#setConfig(ConfigKey, Object)}.
 * <p>
 * If using a lot it may be useful to:
 * <pre>
 * {@code
 *   import static org.apache.brooklyn.core.sensor.DependentConfiguration.*;
 * }
 * </pre>
 * <p>
 * Note that these methods return one-time tasks. The DslComponent methods return long-lasting pointers
 * and should now normally be used instead. If using these methods that return {@link Task} instances
 * you should either ensure it is submitted or that validation and any other attempts to 
 * {@link ExecutionContext#getImmediately(TaskAdaptable)} on this task is blocked;
 * it is all to easy otherwise for an innocuous immediate-get to render this task interrupted
 */
// possibly even the REST API just looking at config could cancel?
// TODO should we deprecate these and provide variants that return TaskFactory instances?
// maybe even ones that are nicely persistable? i (alex) think so, 2017-10.
// see https://github.com/apache/brooklyn-server/pull/816#issuecomment-333858098
public class DependentConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(DependentConfiguration.class);
    
    //not instantiable, only a static helper
    private DependentConfiguration() {}

    /**
     * Default readiness is Groovy truth.
     * 
     * @see #attributeWhenReady(Entity, AttributeSensor, Predicate)
     */
    public static <T> Task<T> attributeWhenReady(Entity source, AttributeSensor<T> sensor) {
        return attributeWhenReady(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate());
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T> Task<T> attributeWhenReady(Entity source, AttributeSensor<T> sensor, Closure<Boolean> ready) {
        Predicate<Object> readyPredicate = (ready != null) ? GroovyJavaMethods.<Object>predicateFromClosure(ready) : JavaGroovyEquivalents.groovyTruthPredicate();
        return attributeWhenReady(source, sensor, readyPredicate);
    }
    
    /** returns an unsubmitted {@link Task} which blocks until the given sensor on the given source entity gives a value that satisfies ready, then returns that value;
     * particular useful in Entity configuration where config will block until Tasks have a value
     */
    public static <T> Task<T> attributeWhenReady(final Entity source, final AttributeSensor<T> sensor, final Predicate<? super T> ready) {
        Builder<T, T> builder = builder().attributeWhenReady(source, sensor);
        if (ready != null) builder.readiness(ready);
        return builder.build();

    }

    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T,V> Task<V> attributePostProcessedWhenReady(Entity source, AttributeSensor<T> sensor, Closure<Boolean> ready, Closure<V> postProcess) {
        Predicate<? super T> readyPredicate = (ready != null) ? GroovyJavaMethods.predicateFromClosure(ready) : JavaGroovyEquivalents.groovyTruthPredicate();
        Function<? super T, V> postProcessFunction = GroovyJavaMethods.<T,V>functionFromClosure(postProcess);
        return attributePostProcessedWhenReady(source, sensor, readyPredicate, postProcessFunction);
    }

    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T,V> Task<V> attributePostProcessedWhenReady(Entity source, AttributeSensor<T> sensor, Closure<V> postProcess) {
        return attributePostProcessedWhenReady(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate(), GroovyJavaMethods.<T,V>functionFromClosure(postProcess));
    }

    public static <T> Task<T> valueWhenAttributeReady(Entity source, AttributeSensor<T> sensor, T value) {
        return DependentConfiguration.<T,T>attributePostProcessedWhenReady(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate(), Functions.constant(value));
    }

    public static <T,V> Task<V> valueWhenAttributeReady(Entity source, AttributeSensor<T> sensor, Function<? super T,V> valueProvider) {
        return attributePostProcessedWhenReady(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate(), valueProvider);
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T,V> Task<V> valueWhenAttributeReady(Entity source, AttributeSensor<T> sensor, Closure<V> valueProvider) {
        return attributePostProcessedWhenReady(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate(), valueProvider);
    }
    
    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T,V> Task<V> attributePostProcessedWhenReady(final Entity source, final AttributeSensor<T> sensor, final Predicate<? super T> ready, final Closure<V> postProcess) {
        return attributePostProcessedWhenReady(source, sensor, ready, GroovyJavaMethods.<T,V>functionFromClosure(postProcess));
    }
    
    @SuppressWarnings("unchecked")
    public static <T,V> Task<V> attributePostProcessedWhenReady(final Entity source, final AttributeSensor<T> sensor, final Predicate<? super T> ready, final Function<? super T,V> postProcess) {
        Builder<T,T> builder1 = DependentConfiguration.builder().attributeWhenReady(source, sensor);
        // messy generics here to support null postProcess; would be nice to disallow that here
        Builder<T,V> builder;
        if (postProcess != null) {
            builder = builder1.postProcess(postProcess);
        } else {
            builder = (Builder<T,V>)builder1;
        }
        if (ready != null) builder.readiness(ready);
        
        return builder.build();
    }

    public static <T> T waitInTaskForAttributeReady(Entity source, AttributeSensor<T> sensor, Predicate<? super T> ready) {
        return waitInTaskForAttributeReady(source, sensor, ready, ImmutableList.<AttributeAndSensorCondition<?>>of());
    }

    public static <T> T waitInTaskForAttributeReady(final Entity source, final AttributeSensor<T> sensor, Predicate<? super T> ready, List<AttributeAndSensorCondition<?>> abortConditions) {
        String blockingDetails = "Waiting for ready from "+source+" "+sensor+" (subscription)";
        return waitInTaskForAttributeReady(source, sensor, ready, abortConditions, blockingDetails);
    }
    
    // TODO would be nice to have an easy semantics for whenServiceUp (cf DynamicWebAppClusterImpl.whenServiceUp)
    
    public static <T> T waitInTaskForAttributeReady(final Entity source, final AttributeSensor<T> sensor, Predicate<? super T> ready, List<AttributeAndSensorCondition<?>> abortConditions, String blockingDetails) {
        return new WaitInTaskForAttributeReady<T,T>(source, sensor, ready, abortConditions, blockingDetails).call();
    }

    protected static class WaitInTaskForAttributeReady<T,V> implements Callable<V> {

        /* This is a change since before Oct 2014. Previously it would continue to poll,
         * (maybe finding a different error) if the target entity becomes unmanaged. 
         * Now it actively checks unmanaged by default, and still throws although it might 
         * now find a different problem. */
        private final static boolean DEFAULT_IGNORE_UNMANAGED = false;
        
        protected final Entity source;
        protected final AttributeSensor<T> sensor;
        protected final Predicate<? super T> ready;
        protected final List<AttributeAndSensorCondition<?>> abortSensorConditions;
        protected final String blockingDetails;
        protected final Function<? super T,? extends V> postProcess;
        protected final Duration timeout;
        protected final Maybe<V> onTimeout;
        protected final boolean ignoreUnmanaged;
        protected final Maybe<V> onUnmanaged;
        // TODO onError Continue / Throw / Return(V)
        
        protected WaitInTaskForAttributeReady(Builder<T, V> builder) {
            this.source = builder.source;
            this.sensor = builder.sensor;
            this.ready = builder.readiness;
            this.abortSensorConditions = builder.abortSensorConditions;
            this.blockingDetails = builder.blockingDetails;
            this.postProcess = builder.postProcess;
            this.timeout = builder.timeout;
            this.onTimeout = builder.onTimeout;
            this.ignoreUnmanaged = builder.ignoreUnmanaged;
            this.onUnmanaged = builder.onUnmanaged;
        }
        
        private WaitInTaskForAttributeReady(Entity source, AttributeSensor<T> sensor, Predicate<? super T> ready,
                List<AttributeAndSensorCondition<?>> abortConditions, String blockingDetails) {
            this.source = source;
            this.sensor = sensor;
            this.ready = ready;
            this.abortSensorConditions = abortConditions;
            this.blockingDetails = blockingDetails;
            
            this.timeout = Duration.PRACTICALLY_FOREVER;
            this.onTimeout = Maybe.absent();
            this.ignoreUnmanaged = DEFAULT_IGNORE_UNMANAGED;
            this.onUnmanaged = Maybe.absent();
            this.postProcess = null;
        }

        @SuppressWarnings("unchecked")
        protected V postProcess(T value) {
            if (this.postProcess!=null) return postProcess.apply(value);
            // if no post-processing assume the types are correct
            return (V) value;
        }
        
        protected boolean ready(T value) {
            if (ready!=null) return ready.apply(value);
            return JavaGroovyEquivalents.groovyTruth(value);
        }
        
        @SuppressWarnings({ "rawtypes", "unchecked" })
        @Override
        public V call() {
            T value = source.getAttribute(sensor);

            // return immediately if either the ready predicate or the abort conditions hold
            if (ready(value)) return postProcess(value);
            
            final List<Exception> abortionExceptions = Lists.newCopyOnWriteArrayList();
            long start = System.currentTimeMillis();
            
            for (AttributeAndSensorCondition abortCondition : abortSensorConditions) {
                Object abortValue = abortCondition.source.getAttribute(abortCondition.sensor);
                if (abortCondition.predicate.apply(abortValue)) {
                    abortionExceptions.add(new Exception("Abort due to "+abortCondition.source+" -> "+abortCondition.sensor));
                }
            }
            if (abortionExceptions.size() > 0) {
                throw new CompoundRuntimeException("Aborted waiting for ready from "+source+" "+sensor, abortionExceptions);
            }

            TaskInternal<?> current = (TaskInternal<?>) Tasks.current();
            if (current == null) throw new IllegalStateException("Should only be invoked in a running task");
            Entity entity = BrooklynTaskTags.getTargetOrContextEntity(current);
            if (entity == null) throw new IllegalStateException("Should only be invoked in a running task with an entity tag; "+
                current+" has no entity tag ("+current.getStatusDetail(false)+")");
            
            final LinkedList<T> publishedValues = new LinkedList<T>();
            final Semaphore semaphore = new Semaphore(0); // could use Exchanger
            SubscriptionHandle subscription = null;
            List<SubscriptionHandle> abortSubscriptions = Lists.newArrayList();
            
            try {
                subscription = entity.subscriptions().subscribe(source, sensor, new SensorEventListener<T>() {
                    @Override public void onEvent(SensorEvent<T> event) {
                        synchronized (publishedValues) { publishedValues.add(event.getValue()); }
                        semaphore.release();
                    }});
                for (final AttributeAndSensorCondition abortCondition : abortSensorConditions) {
                    abortSubscriptions.add(entity.subscriptions().subscribe(abortCondition.source, abortCondition.sensor, new SensorEventListener<Object>() {
                        @Override public void onEvent(SensorEvent<Object> event) {
                            if (abortCondition.predicate.apply(event.getValue())) {
                                abortionExceptions.add(new Exception("Abort due to "+abortCondition.source+" -> "+abortCondition.sensor));
                                semaphore.release();
                            }
                        }}));
                    Object abortValue = abortCondition.source.getAttribute(abortCondition.sensor);
                    if (abortCondition.predicate.apply(abortValue)) {
                        abortionExceptions.add(new Exception("Abort due to "+abortCondition.source+" -> "+abortCondition.sensor));
                    }
                }
                if (abortionExceptions.size() > 0) {
                    throw new CompoundRuntimeException("Aborted waiting for ready from "+source+" "+sensor, abortionExceptions);
                }

                CountdownTimer timer = timeout!=null ? timeout.countdownTimer() : null;
                Duration maxPeriod = ValueResolver.PRETTY_QUICK_WAIT;
                Duration nextPeriod = ValueResolver.REAL_QUICK_PERIOD;
                while (true) {
                    // check the source on initial run (could be done outside the loop) 
                    // and also (optionally) on each iteration in case it is more recent 
                    value = source.getAttribute(sensor);
                    if (ready(value)) break;

                    if (timer!=null) {
                        if (timer.getDurationRemaining().isShorterThan(nextPeriod)) {
                            nextPeriod = timer.getDurationRemaining();
                        }
                        if (timer.isExpired()) {
                            if (onTimeout.isPresent()) return onTimeout.get();
                            throw new RuntimeTimeoutException("Unsatisfied after "+Duration.sinceUtc(start));
                        }
                    }

                    String prevBlockingDetails = current.setBlockingDetails(blockingDetails);
                    try {
                        if (semaphore.tryAcquire(nextPeriod.toMilliseconds(), TimeUnit.MILLISECONDS)) {
                            // immediately release so we are available for the next check
                            semaphore.release();
                            // if other permits have been made available (e.g. multiple notifications) drain them all as no point running multiple times
                            semaphore.drainPermits();
                        }
                    } finally {
                        current.setBlockingDetails(prevBlockingDetails);
                    }

                    // check any subscribed values which have come in first
                    while (true) {
                        synchronized (publishedValues) {
                            if (publishedValues.isEmpty()) break;
                            value = publishedValues.pop(); 
                        }
                        if (ready(value)) break;
                    }

                    // if unmanaged then ignore the other abort conditions
                    if (!ignoreUnmanaged && Entities.isNoLongerManaged(entity)) {
                        if (onUnmanaged.isPresent()) return onUnmanaged.get();
                        throw new NotManagedException(entity);                        
                    }
                    
                    if (abortionExceptions.size() > 0) {
                        throw new CompoundRuntimeException("Aborted waiting for ready from "+source+" "+sensor, abortionExceptions);
                    }

                    nextPeriod = nextPeriod.multiply(2).upperBound(maxPeriod);
                }
                if (LOG.isDebugEnabled()) LOG.debug("Attribute-ready for {} in entity {}", sensor, source);
                return postProcess(value);
            } catch (InterruptedException e) {
                throw Exceptions.propagate(e);
            } finally {
                if (subscription != null) {
                    entity.subscriptions().unsubscribe(subscription);
                }
                for (SubscriptionHandle handle : abortSubscriptions) {
                    entity.subscriptions().unsubscribe(handle);
                }
            }
        }
    }
    
    /**
     * Returns a {@link Task} which blocks until the given job returns, then returns the value of that job.
     * 
     * @deprecated since 0.7; code will be moved into test utilities
     */
    @Deprecated
    public static <T> Task<T> whenDone(Callable<T> job) {
        return new BasicTask<T>(MutableMap.of("tag", "whenDone", "displayName", "waiting for job"), job);
    }

    /**
     * Returns a {@link Task} which waits for the result of first parameter, then applies the function in the second
     * parameter to it, returning that result.
     *
     * Particular useful in Entity configuration where config will block until Tasks have completed,
     * allowing for example an {@link #attributeWhenReady(Entity, AttributeSensor, Predicate)} expression to be
     * passed in the first argument then transformed by the function in the second argument to generate
     * the value that is used for the configuration
     */
    public static <U,T> Task<T> transform(final Task<U> task, final Function<U,T> transformer) {
        return transform(MutableMap.of("displayName", "transforming "+task), task, transformer);
    }
 
    /** 
     * @see #transform(Task, Function)
     * 
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <U,T> Task<T> transform(Task<U> task, Closure transformer) {
        return transform(task, GroovyJavaMethods.functionFromClosure(transformer));
    }
    
    /**
     * @see #transform(Task, Function)
     */
    @SuppressWarnings({ "rawtypes" })
    public static <U,T> Task<T> transform(final Map flags, final TaskAdaptable<U> task, final Function<U,T> transformer) {
        return new BasicTask<T>(flags, new Callable<T>() {
            @Override
            public T call() throws Exception {
                if (!task.asTask().isSubmitted()) {
                    BasicExecutionContext.getCurrentExecutionContext().submit(task);
                } 
                return transformer.apply(task.asTask().get());
            }});        
    }
     
    /** Returns a task which waits for multiple other tasks (submitting if necessary)
     * and performs arbitrary computation over the List of results.
     * @see #transform(Task, Function) but note argument order is reversed (counterintuitive) to allow for varargs */
    public static <U,T> Task<T> transformMultiple(Function<List<U>,T> transformer, @SuppressWarnings("unchecked") TaskAdaptable<U> ...tasks) {
        return transformMultiple(MutableMap.of("displayName", "transforming multiple"), transformer, tasks);
    }

    /**
     * @see #transformMultiple(Function, TaskAdaptable...)
     * 
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <U,T> Task<T> transformMultiple(Closure transformer, TaskAdaptable<U> ...tasks) {
        return transformMultiple(GroovyJavaMethods.functionFromClosure(transformer), tasks);
    }

    /**
     * @see #transformMultiple(Function, TaskAdaptable...)
     * 
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public static <U,T> Task<T> transformMultiple(Map flags, Closure transformer, TaskAdaptable<U> ...tasks) {
        return transformMultiple(flags, GroovyJavaMethods.functionFromClosure(transformer), tasks);
    }
    
    /** @see #transformMultiple(Function, TaskAdaptable...) */
    @SuppressWarnings({ "rawtypes" })
    public static <U,T> Task<T> transformMultiple(Map flags, final Function<List<U>,T> transformer, @SuppressWarnings("unchecked") TaskAdaptable<U> ...tasks) {
        return transformMultiple(flags, transformer, Arrays.asList(tasks));
    }
    @SuppressWarnings({ "rawtypes" })
    public static <U,T> Task<T> transformMultiple(Map flags, final Function<List<U>,T> transformer, Collection<? extends TaskAdaptable<U>> tasks) {
        if (tasks.size()==1) {
            return transform(flags, Iterables.getOnlyElement(tasks), new Function<U,T>() {
                @Override
                @Nullable
                public T apply(@Nullable U input) {
                    return transformer.apply(ImmutableList.of(input));
                }
            });
        }
        return transform(flags, new ParallelTask<U>(tasks), transformer);
    }


    /** Method which returns a Future containing a string formatted using String.format,
     * where the arguments can be normal objects or tasks;
     * tasks will be waited on (submitted if necessary) and their results substituted in the call
     * to String.format.
     * <p>
     * Example:
     * <pre>
     * {@code
     *   setConfig(URL, DependentConfiguration.formatString("%s:%s", 
     *           DependentConfiguration.attributeWhenReady(target, Target.HOSTNAME),
     *           DependentConfiguration.attributeWhenReady(target, Target.PORT) ) );
     * }
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static Task<String> formatString(final String spec, final Object ...args) {
        List<TaskAdaptable<Object>> taskArgs = Lists.newArrayList();
        for (Object arg: args) {
            if (arg instanceof TaskAdaptable) taskArgs.add((TaskAdaptable<Object>)arg);
            else if (arg instanceof TaskFactory) taskArgs.add( ((TaskFactory<TaskAdaptable<Object>>)arg).newTask() );
        }
        
        return transformMultiple(
            MutableMap.<String,String>of("displayName", "formatting '"+spec+"' with "+taskArgs.size()+" task"+(taskArgs.size()!=1?"s":"")), 
            new Function<List<Object>, String>() {
                @Override public String apply(List<Object> input) {
                    Iterator<?> tri = input.iterator();
                    Object[] vv = new Object[args.length];
                    int i=0;
                    for (Object arg : args) {
                        if (arg instanceof TaskAdaptable || arg instanceof TaskFactory) vv[i] = tri.next();
                        else if (arg instanceof DeferredSupplier) vv[i] = ((DeferredSupplier<?>) arg).get();
                        else vv[i] = arg;
                        i++;
                    }
                    return String.format(spec, vv);
                }},
            taskArgs);
    }

    /**
     * @throws ImmediateSupplier.ImmediateUnsupportedException if cannot evaluate this in a timely manner
     */
    public static Maybe<String> formatStringImmediately(final String spec, final Object ...args) {
        List<Object> resolvedArgs = Lists.newArrayList();
        for (Object arg : args) {
            Maybe<?> argVal = resolveImmediately(arg);
            if (argVal.isAbsent()) return  Maybe.Absent.castAbsent(argVal);
            resolvedArgs.add(argVal.get());
        }

        return Maybe.of(String.format(spec, resolvedArgs.toArray()));
    }

    /**
     * @throws ImmediateSupplier.ImmediateUnsupportedException if cannot evaluate this in a timely manner
     */
    public static Maybe<String> urlEncodeImmediately(Object arg) {
        Maybe<?> resolvedArg = resolveImmediately(arg);
        if (resolvedArg.isAbsent()) return Absent.castAbsent(resolvedArg);
        if (resolvedArg.isNull()) return Maybe.<String>of((String)null);
        
        String resolvedString = resolvedArg.get().toString();
        return Maybe.of(Urls.encode(resolvedString));
    }

    /** 
     * Method which returns a Future containing an escaped URL string (see {@link Urls#encode(String)}).
     * The arguments can be normal objects, tasks or {@link DeferredSupplier}s.
     * tasks will be waited on (submitted if necessary) and their results substituted.
     */
    @SuppressWarnings("unchecked")
    public static Task<String> urlEncode(final Object arg) {
        List<TaskAdaptable<Object>> taskArgs = Lists.newArrayList();
        if (arg instanceof TaskAdaptable) taskArgs.add((TaskAdaptable<Object>)arg);
        else if (arg instanceof TaskFactory) taskArgs.add( ((TaskFactory<TaskAdaptable<Object>>)arg).newTask() );
        
        return transformMultiple(
                MutableMap.<String,String>of("displayName", "url-escaping '"+arg), 
                new Function<List<Object>, String>() {
                    @Override
                    @Nullable
                    public String apply(@Nullable List<Object> input) {
                        Object resolvedArg;
                        if (arg instanceof TaskAdaptable || arg instanceof TaskFactory) resolvedArg = Iterables.getOnlyElement(input);
                        else if (arg instanceof DeferredSupplier) resolvedArg = ((DeferredSupplier<?>) arg).get();
                        else resolvedArg = arg;
                        
                        if (resolvedArg == null) return null;
                        String resolvedString = resolvedArg.toString();
                        return Urls.encode(resolvedString);
                    }
                },
                taskArgs);
    }

    protected static <T> Maybe<?> resolveImmediately(Object val) {
        if (val instanceof ImmediateSupplier<?>) {
            return ((ImmediateSupplier<?>)val).getImmediately();
        } else if (val instanceof TaskAdaptable) {
            throw new ImmediateSupplier.ImmediateUnsupportedException("Cannot immediately resolve value "+val);
        } else if (val instanceof TaskFactory) {
            throw new ImmediateSupplier.ImmediateUnsupportedException("Cannot immediately resolve value "+val);
        } else if (val instanceof DeferredSupplier<?>) {
            throw new ImmediateSupplier.ImmediateUnsupportedException("Cannot immediately resolve value "+val);
        } else {
            return Maybe.of(val);
        }
    }
    
    public static Maybe<String> regexReplacementImmediately(Object source, Object pattern, Object replacement) {
        Maybe<?> resolvedSource = resolveImmediately(source);
        if (resolvedSource.isAbsent()) return Absent.castAbsent(resolvedSource);
        String resolvedSourceStr = String.valueOf(resolvedSource.get());
        
        Maybe<?> resolvedPattern = resolveImmediately(pattern);
        if (resolvedPattern.isAbsent()) return Absent.castAbsent(resolvedPattern);
        String resolvedPatternStr = String.valueOf(resolvedPattern.get());
        
        Maybe<?> resolvedReplacement = resolveImmediately(replacement);
        if (resolvedReplacement.isAbsent()) return Absent.castAbsent(resolvedReplacement);
        String resolvedReplacementStr = String.valueOf(resolvedReplacement.get());

        String result = new StringFunctions.RegexReplacer(resolvedPatternStr, resolvedReplacementStr).apply(resolvedSourceStr);
        return Maybe.of(result);
    }

    public static Task<String> regexReplacement(Object source, Object pattern, Object replacement) {
        List<TaskAdaptable<Object>> taskArgs = getTaskAdaptable(source, pattern, replacement);
        Function<List<Object>, String> transformer = new RegexTransformerString(source, pattern, replacement);
        return transformMultiple(
                MutableMap.of("displayName", String.format("creating regex replacement function (%s:%s)", pattern, replacement)),
                transformer,
                taskArgs
        );
    }

    public static Maybe<Function<String, String>> regexReplacementImmediately(Object pattern, Object replacement) {
        Maybe<?> resolvedPattern = resolveImmediately(pattern);
        if (resolvedPattern.isAbsent()) return Absent.castAbsent(resolvedPattern);
        String resolvedPatternStr = String.valueOf(resolvedPattern.get());
        
        Maybe<?> resolvedReplacement = resolveImmediately(replacement);
        if (resolvedReplacement.isAbsent()) return Absent.castAbsent(resolvedReplacement);
        String resolvedReplacementStr = String.valueOf(resolvedReplacement.get());

        RegexReplacer result = new StringFunctions.RegexReplacer(resolvedPatternStr, resolvedReplacementStr);
        return Maybe.<Function<String, String>>of(result);
    }

    public static Task<Function<String, String>> regexReplacement(Object pattern, Object replacement) {
        List<TaskAdaptable<Object>> taskArgs = getTaskAdaptable(pattern, replacement);
        Function<List<Object>, Function<String, String>> transformer = new RegexTransformerFunction(pattern, replacement);
        return transformMultiple(
                MutableMap.of("displayName", String.format("creating regex replacement function (%s:%s)", pattern, replacement)),
                transformer,
                taskArgs
        );
    }

    public static Maybe<ReleaseableLatch> maxConcurrencyImmediately(Object maxThreads) {
        Maybe<?> resolvedMaxThreads = resolveImmediately(maxThreads);
        if (resolvedMaxThreads.isAbsent()) return Maybe.absent();
        Integer resolvedMaxThreadsInt = TypeCoercions.coerce(resolvedMaxThreads, Integer.class);

        ReleaseableLatch result = ReleaseableLatch.Factory.newMaxConcurrencyLatch(resolvedMaxThreadsInt);
        return Maybe.<ReleaseableLatch>of(result);
    }

    public static Task<ReleaseableLatch> maxConcurrency(Object maxThreads) {
        List<TaskAdaptable<Object>> taskArgs = getTaskAdaptable(maxThreads);
        Function<List<Object>, ReleaseableLatch> transformer = new MaxThreadsTransformerFunction(maxThreads);
        return transformMultiple(
                MutableMap.of("displayName", String.format("creating max concurrency semaphore(%s)", maxThreads)),
                transformer,
                taskArgs
        );
    }

    @SuppressWarnings("unchecked")
    private static List<TaskAdaptable<Object>> getTaskAdaptable(Object... args){
        List<TaskAdaptable<Object>> taskArgs = Lists.newArrayList();
        for (Object arg: args) {
            if (arg instanceof TaskAdaptable) {
                taskArgs.add((TaskAdaptable<Object>)arg);
            } else if (arg instanceof TaskFactory) {
                taskArgs.add(((TaskFactory<TaskAdaptable<Object>>)arg).newTask());
            }
        }
        return taskArgs;
    }

    public static class RegexTransformerString implements Function<List<Object>, String> {

        private final Object source;
        private final Object pattern;
        private final Object replacement;

        public RegexTransformerString(Object source, Object pattern, Object replacement){
            this.source = source;
            this.pattern = pattern;
            this.replacement = replacement;
        }

        @Nullable
        @Override
        public String apply(@Nullable List<Object> input) {
            Iterator<?> taskArgsIterator = input.iterator();
            String resolvedSource = resolveArgument(source, taskArgsIterator);
            String resolvedPattern = resolveArgument(pattern, taskArgsIterator);
            String resolvedReplacement = resolveArgument(replacement, taskArgsIterator);
            return new StringFunctions.RegexReplacer(resolvedPattern, resolvedReplacement).apply(resolvedSource);
        }
    }

    @Beta
    public static class RegexTransformerFunction implements Function<List<Object>, Function<String, String>> {

        private final Object pattern;
        private final Object replacement;

        public RegexTransformerFunction(Object pattern, Object replacement){
            this.pattern = pattern;
            this.replacement = replacement;
        }

        @Override
        public Function<String, String> apply(List<Object> input) {
            Iterator<?> taskArgsIterator = input.iterator();
            return new StringFunctions.RegexReplacer(resolveArgument(pattern, taskArgsIterator), resolveArgument(replacement, taskArgsIterator));
        }

    }

    public static class MaxThreadsTransformerFunction implements Function<List<Object>, ReleaseableLatch> {
        private final Object maxThreads;

        public MaxThreadsTransformerFunction(Object maxThreads) {
            this.maxThreads = maxThreads;
        }

        @Override
        public ReleaseableLatch apply(List<Object> input) {
            Iterator<?> taskArgsIterator = input.iterator();
            Integer maxThreadsNum = resolveArgument(maxThreads, taskArgsIterator, Integer.class);
            return ReleaseableLatch.Factory.newMaxConcurrencyLatch(maxThreadsNum);
        }

    }

    /**
     * Same as {@link #resolveArgument(Object, Iterator, Class) with type of String
     */
    private static String resolveArgument(Object argument, Iterator<?> taskArgsIterator) {
        return resolveArgument(argument, taskArgsIterator, String.class);
    }

    /**
     * Resolves the argument as follows:
     *
     * If the argument is a DeferredSupplier, we will block and wait for it to resolve. If the argument is TaskAdaptable or TaskFactory,
     * we will assume that the resolved task has been queued on the {@code taskArgsIterator}, otherwise the argument has already been resolved.
     * 
     * @param type coerces the return value to the requested type
     */
    private static <T> T resolveArgument(Object argument, Iterator<?> taskArgsIterator, Class<T> type) {
        Object resolvedArgument;
        if (argument instanceof TaskAdaptable) {
            resolvedArgument = taskArgsIterator.next();
        } else if (argument instanceof DeferredSupplier) {
            resolvedArgument = ((DeferredSupplier<?>) argument).get();
        } else {
            resolvedArgument = argument;
        }
        return TypeCoercions.coerce(resolvedArgument, type);
    }


    /** returns a task for parallel execution returning a list of values for the given sensor for the given entity list,
     * optionally when the values satisfy a given readiness predicate (defaulting to groovy truth if not supplied) */
    public static <T> Task<List<T>> listAttributesWhenReady(AttributeSensor<T> sensor, Iterable<Entity> entities) {
        return listAttributesWhenReady(sensor, entities, JavaGroovyEquivalents.groovyTruthPredicate());
    }

    /**
     * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
     */
    @Deprecated
    public static <T> Task<List<T>> listAttributesWhenReady(AttributeSensor<T> sensor, Iterable<Entity> entities, Closure<Boolean> readiness) {
        Predicate<Object> readinessPredicate = (readiness != null) ? GroovyJavaMethods.<Object>predicateFromClosure(readiness) : JavaGroovyEquivalents.groovyTruthPredicate();
        return listAttributesWhenReady(sensor, entities, readinessPredicate);
    }
    
    /** returns a task for parallel execution returning a list of values of the given sensor list on the given entity, 
     * optionally when the values satisfy a given readiness predicate (defaulting to groovy truth if not supplied) */    
    public static <T> Task<List<T>> listAttributesWhenReady(final AttributeSensor<T> sensor, Iterable<Entity> entities, Predicate<? super T> readiness) {
        if (readiness == null) readiness = JavaGroovyEquivalents.groovyTruthPredicate();
        return builder().attributeWhenReadyFromMultiple(entities, sensor, readiness).build();
    }

    /** @see #waitForTask(Task, Entity, String) */
    public static <T> T waitForTask(Task<T> t, Entity context) throws InterruptedException {
        return waitForTask(t, context, null);
    }
    
    /** blocks until the given task completes, submitting if necessary, returning the result of that task;
     * optional contextMessage is available in status if this is running in a task
     */
    @SuppressWarnings("unchecked")
    public static <T> T waitForTask(Task<T> t, Entity context, String contextMessage) throws InterruptedException {
        try {
            return (T) Tasks.resolveValue(t, Object.class, ((EntityInternal)context).getExecutionContext(), contextMessage);
        } catch (ExecutionException e) {
            throw Throwables.propagate(e);
        }
    }
    
    public static class AttributeAndSensorCondition<T> {
        protected final Entity source;
        protected final AttributeSensor<T> sensor;
        protected final Predicate<? super T> predicate;
        
        public AttributeAndSensorCondition(Entity source, AttributeSensor<T> sensor, Predicate<? super T> predicate) {
            this.source = checkNotNull(source, "source");
            this.sensor = checkNotNull(sensor, "sensor");
            this.predicate = checkNotNull(predicate, "predicate");
        }
    }
    
    public static ProtoBuilder builder() {
        return new ProtoBuilder();
    }
    
    /**
     * Builder for producing variants of attributeWhenReady.
     */
    @Beta
    public static class ProtoBuilder {
        /**
         * Will wait for the attribute on the given entity, with default behaviour:
         * If that entity reports {@link Lifecycle#ON_FIRE} for its {@link Attributes#SERVICE_STATE_ACTUAL} then it will abort;
         * If that entity is stopping or destroyed (see {@link Builder#timeoutIfStoppingOrDestroyed(Duration)}),
         * then it will timeout after 1 minute.
         */
        public <T2> Builder<T2,T2> attributeWhenReady(Entity source, AttributeSensor<T2> sensor) {
            return new Builder<T2,T2>(source, sensor).abortIfOnFire().timeoutIfStoppingOrDestroyed(Duration.ONE_MINUTE);
        }

        /**
         * Will wait for the attribute on the given entity, not aborting when it goes {@link Lifecycle#ON_FIRE}.
         */
        public <T2> Builder<T2,T2> attributeWhenReadyAllowingOnFire(Entity source, AttributeSensor<T2> sensor) {
            return new Builder<T2,T2>(source, sensor);
        }

        /** Constructs a builder for task for parallel execution returning a list of values of the given sensor list on the given entity, 
         * optionally when the values satisfy a given readiness predicate (defaulting to groovy truth if not supplied) */ 
        @Beta
        public <T> MultiBuilder<T, T, List<T>> attributeWhenReadyFromMultiple(Iterable<? extends Entity> sources, AttributeSensor<T> sensor) {
            return attributeWhenReadyFromMultiple(sources, sensor, JavaGroovyEquivalents.groovyTruthPredicate());
        }
        /** As {@link #attributeWhenReadyFromMultiple(Iterable, AttributeSensor)} with an explicit readiness test. */
        @Beta
        public <T> MultiBuilder<T, T, List<T>> attributeWhenReadyFromMultiple(Iterable<? extends Entity> sources, AttributeSensor<T> sensor, Predicate<? super T> readiness) {
            return new MultiBuilder<T, T, List<T>>(sources, sensor, readiness);
        }
    }

    /**
     * Builder for producing variants of attributeWhenReady.
     */
    public static class Builder<T,V> {
        protected Entity source;
        protected AttributeSensor<T> sensor;
        protected Predicate<? super T> readiness;
        protected Function<? super T, ? extends V> postProcess;
        protected List<AttributeAndSensorCondition<?>> abortSensorConditions = Lists.newArrayList();
        protected String blockingDetails;
        protected Duration timeout;
        protected Maybe<V> onTimeout = Maybe.absent();
        protected  boolean ignoreUnmanaged = WaitInTaskForAttributeReady.DEFAULT_IGNORE_UNMANAGED;
        protected Maybe<V> onUnmanaged = Maybe.absent();

        protected Builder(Entity source, AttributeSensor<T> sensor) {
            this.source = source;
            this.sensor = sensor;
        }
        
        /**
         * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
         */
        @Deprecated
        public Builder<T,V> readiness(Closure<Boolean> val) {
            this.readiness = GroovyJavaMethods.predicateFromClosure(checkNotNull(val, "val"));
            return this;
        }
        public Builder<T,V> readiness(Predicate<? super T> val) {
            this.readiness = checkNotNull(val, "ready");
            return this;
        }
        /**
         * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
         */
        @Deprecated
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <V2> Builder<T,V2> postProcess(Closure<V2> val) {
            this.postProcess = (Function) GroovyJavaMethods.<T,V2>functionFromClosure(checkNotNull(val, "postProcess"));
            return (Builder<T,V2>) this;
        }
        @SuppressWarnings({ "unchecked", "rawtypes" })
        public <V2> Builder<T,V2> postProcess(final Function<? super T, V2>  val) {
            this.postProcess = (Function) checkNotNull(val, "postProcess");
            return (Builder<T,V2>) this;
        }
        public <T2> Builder<T,V> abortIf(Entity source, AttributeSensor<T2> sensor) {
            return abortIf(source, sensor, JavaGroovyEquivalents.groovyTruthPredicate());
        }
        public <T2> Builder<T,V> abortIf(Entity source, AttributeSensor<T2> sensor, Predicate<? super T2> predicate) {
            abortSensorConditions.add(new AttributeAndSensorCondition<T2>(source, sensor, predicate));
            return this;
        }
        /** Causes the depender to abort immediately if {@link Attributes#SERVICE_STATE_ACTUAL}
         * is {@link Lifecycle#ON_FIRE}. */
        public Builder<T,V> abortIfOnFire() {
            abortIf(source, Attributes.SERVICE_STATE_ACTUAL, Predicates.equalTo(Lifecycle.ON_FIRE));
            return this;
        }
        /** Causes the depender to timeout after the given time if {@link Attributes#SERVICE_STATE_ACTUAL}
         * is one of {@link Lifecycle#STOPPING}, {@link Lifecycle#STOPPED}, or {@link Lifecycle#DESTROYED}. */
        public Builder<T,V> timeoutIfStoppingOrDestroyed(Duration time) {
            timeoutIf(source, Attributes.SERVICE_STATE_ACTUAL, Predicates.equalTo(Lifecycle.STOPPING), time);
            timeoutIf(source, Attributes.SERVICE_STATE_ACTUAL, Predicates.equalTo(Lifecycle.STOPPED), time);
            timeoutIf(source, Attributes.SERVICE_STATE_ACTUAL, Predicates.equalTo(Lifecycle.DESTROYED), time);
            return this;
        }
        public Builder<T,V> blockingDetails(String val) {
            blockingDetails = val;
            return this;
        }
        /** specifies an optional timeout; by default it waits forever, or until unmanaged or other abort condition */
        public Builder<T,V> timeout(Duration val) {
            timeout = val;
            return this;
        }
        /** specifies the supplied timeout if the condition is met */
        public <T2> Builder<T,V> timeoutIf(Entity source, AttributeSensor<T2> sensor, Predicate<? super T2> predicate, Duration val) {
            if (predicate.apply(source.sensors().get(sensor))) timeout(val);
            return this;
        }
        public Builder<T,V> onTimeoutReturn(V val) {
            onTimeout = Maybe.of(val);
            return this;
        }
        public Builder<T,V> onTimeoutThrow() {
            onTimeout = Maybe.<V>absent();
            return this;
        }
        public Builder<T,V> onUnmanagedReturn(V val) {
            onUnmanaged = Maybe.of(val);
            return this;
        }
        public Builder<T,V> onUnmanagedThrow() {
            onUnmanaged = Maybe.<V>absent();
            return this;
        }
        /** @since 0.7.0 included in case old behaviour of not checking whether the entity is managed is required
         * (I can't see why it is; polling will likely give errors, once it is unmanaged this will never completed,
         * and before management the current code will continue, so long as there are no other errors) */ @Deprecated
        public Builder<T,V> onUnmanagedContinue() {
            ignoreUnmanaged = true;
            return this;
        }
        /** take advantage of the fact that this builder can build multiple times, allowing subclasses 
         * to change the source along the way */
        protected Builder<T,V> source(Entity source) {
            this.source = source;
            return this;
        }
        /** as {@link #source(Entity)} */
        @SuppressWarnings({ "unchecked", "rawtypes" })
        protected Builder<T,V> sensor(AttributeSensor<? extends T> sensor) {
            this.sensor = (AttributeSensor) sensor;
            return this;
        }
        public Task<V> build() {
            validate();
            
            return Tasks.<V>builder().dynamic(false)
                .displayName("waiting on "+sensor.getName())
                .description("Waiting on sensor "+sensor.getName()+" from "+source)
                .tag("attributeWhenReady")
                .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
                .body(new WaitInTaskForAttributeReady<T,V>(this))
                .build();
        }
        
        public V runNow() {
            validate();
            return new WaitInTaskForAttributeReady<T,V>(this).call();
        }
        @SuppressWarnings({ "unchecked", "rawtypes" })
        private void validate() {
            checkNotNull(source, "Entity source");
            checkNotNull(sensor, "Sensor");
            if (readiness == null) readiness = JavaGroovyEquivalents.groovyTruthPredicate();
            if (postProcess == null) postProcess = (Function) Functions.identity();
        }
    }

    /**
     * Builder for producing variants of attributeWhenReady.
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Beta
    public static class MultiBuilder<T, V, V2> {
        protected final String name;
        protected final String descriptionBase;
        protected final Builder<T,V> builder;
        // if desired, the use of this multiSource could allow different conditions; 
        // but probably an easier API just for the caller to build the parallel task  
        protected final List<AttributeAndSensorCondition<?>> multiSource = Lists.newArrayList();
        protected Function<? super List<V>, V2> postProcessFromMultiple;
        
        /** returns a task for parallel execution returning a list of values of the given sensor list on the given entity, 
         * optionally when the values satisfy a given readiness predicate (defaulting to groovy truth if not supplied) */ 
        @Beta
        protected MultiBuilder(Iterable<? extends Entity> sources, AttributeSensor<T> sensor) {
            this(sources, sensor, JavaGroovyEquivalents.groovyTruthPredicate());
        }
        @Beta
        protected MultiBuilder(Iterable<? extends Entity> sources, AttributeSensor<T> sensor, Predicate<? super T> readiness) {
            builder = new Builder<T,V>(null, sensor);
            builder.readiness(readiness);
            
            for (Entity s : checkNotNull(sources, "sources")) {
                multiSource.add(new AttributeAndSensorCondition<T>(s, sensor, readiness));
            }
            this.name = "waiting on "+sensor.getName();
            this.descriptionBase = "waiting on "+sensor.getName()+" "+readiness
                +" from "+Iterables.size(sources)+" entit"+Strings.ies(sources);
        }
        
        /** Apply post-processing to the entire list of results */
        public <V2b> MultiBuilder<T, V, V2b> postProcessFromMultiple(final Function<? super List<V>, V2b> val) {
            this.postProcessFromMultiple = (Function) checkNotNull(val, "postProcessFromMulitple");
            return (MultiBuilder<T,V, V2b>) this;
        }
        /** Apply post-processing to the entire list of results 
         * See {@link CollectionFunctionals#all(Predicate)} and {@link CollectionFunctionals#quorum(org.apache.brooklyn.util.collections.QuorumCheck, Predicate)
         * which allow useful arguments. */
        public MultiBuilder<T, V, Boolean> postProcessFromMultiple(final Predicate<? super List<V>> val) {
            return postProcessFromMultiple(Functions.forPredicate(val));
        }
        
        /**
         * @deprecated since 0.11.0; explicit groovy utilities/support will be deleted.
         */
        @Deprecated
        public <V1> MultiBuilder<T, V1, V2> postProcess(Closure<V1> val) {
            builder.postProcess(val);
            return (MultiBuilder<T, V1, V2>) this;
        }
        public <V1> MultiBuilder<T, V1, V2> postProcess(final Function<? super T, V1>  val) {
            builder.postProcess(val);
            return (MultiBuilder<T, V1, V2>) this;
        }
        public <T2> MultiBuilder<T, V, V2> abortIf(Entity source, AttributeSensor<T2> sensor) {
            builder.abortIf(source, sensor);
            return this;
        }
        public <T2> MultiBuilder<T, V, V2> abortIf(Entity source, AttributeSensor<T2> sensor, Predicate<? super T2> predicate) {
            builder.abortIf(source, sensor, predicate);
            return this;
        }
        public MultiBuilder<T, V, V2> abortIfOnFire() {
            builder.abortIfOnFire();
            return this;
        }
        public MultiBuilder<T, V, V2> blockingDetails(String val) {
            builder.blockingDetails(val);
            return this;
        }
        public MultiBuilder<T, V, V2> timeout(Duration val) {
            builder.timeout(val);
            return this;
        }
        public MultiBuilder<T, V, V2> onTimeoutReturn(V val) {
            builder.onTimeoutReturn(val);
            return this;
        }
        public MultiBuilder<T, V, V2> onTimeoutThrow() {
            builder.onTimeoutThrow();
            return this;
        }
        public MultiBuilder<T, V, V2> onUnmanagedReturn(V val) {
            builder.onUnmanagedReturn(val);
            return this;
        }
        public MultiBuilder<T, V, V2> onUnmanagedThrow() {
            builder.onUnmanagedThrow();
            return this;
        }
        
        public Task<V2> build() {
            List<Task<V>> tasks = MutableList.of();
            for (AttributeAndSensorCondition<?> source: multiSource) {
                builder.source(source.source);
                builder.sensor((AttributeSensor)source.sensor);
                builder.readiness((Predicate)source.predicate);
                tasks.add(builder.build());
            }
            final Task<List<V>> parallelTask = Tasks.<List<V>>builder().parallel(true).addAll(tasks)
                .displayName(name)
                .description(descriptionBase+
                    (builder.timeout!=null ? ", timeout "+builder.timeout : ""))
                .build();
            
            if (postProcessFromMultiple == null) {
                // V2 should be the right type in normal operations
                return (Task<V2>) parallelTask;
            } else {
                return Tasks.<V2>builder().displayName(name).description(descriptionBase)
                    .tag("attributeWhenReady")
                    .body(new Callable<V2>() {
                        @Override public V2 call() throws Exception {
                            List<V> prePostProgress = DynamicTasks.queue(parallelTask).get();
                            return DynamicTasks.queue(
                                Tasks.<V2>builder().displayName("post-processing").description("Applying "+postProcessFromMultiple)
                                    .body(Functionals.callable(postProcessFromMultiple, prePostProgress))
                                    .build()).get();
                        }
                    })
                    .build();
            }
        }
    }
    
}
