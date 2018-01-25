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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateUnsupportedException;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateValueNotAvailableException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.reflect.TypeToken;

/** 
 * Resolves a given object, as follows:
 * <li> If it is a {@link Tasks} or a {@link DeferredSupplier} then get its contents
 * <li> If it's a map and {@link #deep(boolean)} is requested, it applies resolution to contents
 * <li> It applies coercion
 * <p>
 * Fluent-style API exposes a number of other options.
 */
public class ValueResolver<T> implements DeferredSupplier<T>, Iterable<Maybe<Object>> {

    // TODO most of these usages should be removed when we have
    // an ability to run resolution in a non-blocking mode
    // (i.e. running resolution tasks in the same thread,
    // or in a context where they can only wait on subtasks
    // which are guaranteed to have the same constraint)
    /** 
     * Period to wait if we're expected to return real quick 
     * but we want fast things to have time to finish.
     * <p>
     * Timings are always somewhat arbitrary but this at least
     * allows some intention to be captured in code rather than arbitrary values. */
    @Beta
    public static Duration REAL_QUICK_WAIT = Duration.millis(50);
    /** 
     * Like {@link #REAL_QUICK_WAIT} but even smaller, for use when potentially
     * resolving multiple items in sequence. */
    @Beta
    public static Duration REAL_REAL_QUICK_WAIT = Duration.millis(5);
    /** 
     * Period to wait if we're expected to return quickly 
     * but we want to be a bit more generous for things to finish,
     * without letting a caller get annoyed. 
     * <p>
     * See {@link #REAL_QUICK_WAIT}. */
    public static Duration PRETTY_QUICK_WAIT = Duration.millis(200);

    /** 
     * Period to wait if we're expecting the operation to be non-blocking, so want to abort if the 
     * invoked task/supplier is taking too long (likely because its value is not yet ready, and 
     * the invoked task is blocked waiting for it). 
     * <p>
     * See {@link #REAL_QUICK_WAIT} and <a href="https://issues.apache.org/jira/browse/BROOKLYN-356">BROOKLYN-356</a>.
     */
    @Beta
    public static final Duration NON_BLOCKING_WAIT = Duration.millis(500);

    /** Period to wait when we have to poll but want to give the illusion of no wait.
     * See {@link Repeater#DEFAULT_REAL_QUICK_PERIOD} */ 
    public static Duration REAL_QUICK_PERIOD = Repeater.DEFAULT_REAL_QUICK_PERIOD;
    
    private static final Logger log = LoggerFactory.getLogger(ValueResolver.class);
    
    final Object value;
    final Class<T> type;
    ExecutionContext exec;
    String description;
    boolean forceDeep;
    /** null means do it if you can; true means always, false means never */
    Boolean embedResolutionInTask;
    /** timeout on execution, if possible, or if embedResolutionInTask is true */
    Duration timeout;
    boolean immediately;
    boolean recursive = true;
    boolean isTransientTask = true;
    
    T defaultValue = null;
    boolean returnDefaultOnGet = false;
    boolean swallowExceptions = false;
    
    // internal fields
    final Object parentOriginalValue;
    final CountdownTimer parentTimer;
    AtomicBoolean started = new AtomicBoolean(false);
    boolean expired;
    
    ValueResolver(Object v, Class<T> type) {
        this.value = v;
        this.type = type;
        checkTypeNotNull();
        parentOriginalValue = null;
        parentTimer = null;
    }
    
    ValueResolver(Object v, Class<T> type, ValueResolver<?> parent) {
        this.value = v;
        this.type = type;
        checkTypeNotNull();
        
        exec = parent.exec;
        description = parent.description;
        forceDeep = parent.forceDeep;
        embedResolutionInTask = parent.embedResolutionInTask;

        parentOriginalValue = parent.getOriginalValue();

        timeout = parent.timeout;
        immediately = parent.immediately;
        // not copying recursive as we want deep resolving to be recursive, only top-level values should be non-recursive
        parentTimer = parent.parentTimer;
        if (parentTimer!=null && parentTimer.isExpired())
            expired = true;
        
        // default value and swallow exceptions do not need to be nested
    }

    public static class ResolverBuilderPretype {
        final Object v;
        public ResolverBuilderPretype(Object v) {
            this.v = v;
        }
        public <T> ValueResolver<T> as(Class<T> type) {
            return new ValueResolver<T>(v, type);
        }
    }

    /** returns a copy of this resolver which can be queried, even if the original (single-use instance) has already been copied */
    @Override
    public ValueResolver<T> clone() {
        return cloneReplacingValueAndType(value, type);
    }
    
    <S> ValueResolver<S> cloneReplacingValueAndType(Object newValue, Class<S> superType) {
        // superType expected to be either type or Object.class
        if (!superType.isAssignableFrom(type)) {
            throw new IllegalStateException("superType must be assignable from " + type);
        }
        ValueResolver<S> result = new ValueResolver<S>(newValue, superType)
            .context(exec).description(description)
            .embedResolutionInTask(embedResolutionInTask)
            .deep(forceDeep)
            .timeout(timeout)
            .immediately(immediately)
            .recursive(recursive);
        if (returnDefaultOnGet) {
            if (!superType.isInstance(defaultValue)) {
                throw new IllegalStateException("Existing default value " + defaultValue + " not compatible with new type " + superType);
            }
            @SuppressWarnings("unchecked")
            S typedDefaultValue = (S)defaultValue;
            result.defaultValue(typedDefaultValue);
        }
        if (swallowExceptions) result.swallowExceptions();
        return result;
    }
    
    /** execution context to use when resolving; required if resolving unsubmitted tasks or running with a time limit */
    public ValueResolver<T> context(ExecutionContext exec) {
        this.exec = exec;
        return this;
    }
    /** as {@link #context(ExecutionContext)} for use from an entity */
    public ValueResolver<T> context(Entity entity) {
        return context(entity!=null ? ((EntityInternal)entity).getExecutionContext() : null);
    }
    
    /** sets a message which will be displayed in status reports while it waits (e.g. the name of the config key being looked up) */
    public ValueResolver<T> description(String description) {
        this.description = description;
        return this;
    }
    
    /** sets a default value which will be returned on a call to {@link #get()} if the task does not complete
     * or completes with an error
     * <p>
     * note that {@link #getMaybe()} returns an absent object even in the presence of
     * a default, so that any error can still be accessed */
    public ValueResolver<T> defaultValue(T defaultValue) {
        this.defaultValue = defaultValue;
        this.returnDefaultOnGet = true;
        return this;
    }

    /** indicates that no default value should be returned on a call to {@link #get()}, and instead it should throw
     * (this is the default; this method is provided to undo a call to {@link #defaultValue(Object)}) */
    public ValueResolver<T> noDefaultValue() {
        this.returnDefaultOnGet = false;
        this.defaultValue = null;
        return this;
    }
    
    /** indicates that exceptions in resolution should not be thrown on a call to {@link #getMaybe()}, 
     * but rather used as part of the {@link Maybe#get()} if it's absent, 
     * and swallowed altogether on a call to {@link #get()} in the presence of a {@link #defaultValue(Object)} */
    public ValueResolver<T> swallowExceptions() {
        this.swallowExceptions = true;
        return this;
    }
    
    /** whether the task should be marked as transient; defaults true */
    public ValueResolver<T> transientTask(boolean isTransientTask) {
        this.isTransientTask = isTransientTask;
        return this;
    }
    
    public Maybe<T> getDefault() {
        if (returnDefaultOnGet) return Maybe.of(defaultValue);
        else return Maybe.absent("No default value set");
    }
    
    /** causes nested structures (maps, lists) to be descended and nested unresolved values resolved */
    public ValueResolver<T> deep(boolean forceDeep) {
        this.forceDeep = forceDeep;
        return this;
    }

    /** if true, forces execution of a deferred supplier to be run in a task;
     * if false, it prevents it (meaning time limits may not be applied);
     * if null, the default, it runs in a task if a time limit is applied.
     * <p>
     * running inside a task is required for some {@link DeferredSupplier}
     * instances which look up a task {@link ExecutionContext}. */
    public ValueResolver<T> embedResolutionInTask(Boolean embedResolutionInTask) {
        this.embedResolutionInTask = embedResolutionInTask;
        return this;
    }
    
    /** sets a time limit on executions
     * <p>
     * used for {@link Task} and {@link DeferredSupplier} instances.
     * may require an execution context at runtime. */
    public ValueResolver<T> timeout(Duration timeout) {
        this.timeout = timeout;
        return this;
    }

    /**
     * Whether the value should be resolved immediately
     * (following {@link ImmediateSupplier#getImmediately()} semantics with regards to when errors may be thrown,
     * except some cases where {@link ImmediateUnsupportedException} is thrown may be re-run with a {@link #NON_BLOCKING_WAIT}
     * after which the more definitive {@link ImmediateValueNotAvailableException} will be thrown/wrapped.
     */
    @Beta
    public ValueResolver<T> immediately(boolean val) {
        this.immediately = val;
        if (val && timeout == null) timeout = ValueResolver.NON_BLOCKING_WAIT;
        return this;
    }

    /**
     * Whether the value should be resolved recursively. When true the result of
     * the resolving will be resolved again recursively until the value is an immediate object.
     * When false will try to resolve the value a single time and return the result even if it
     * can be resolved further (e.x. it is DeferredSupplier).
     */
    @Beta
    public ValueResolver<T> recursive(boolean val) {
        this.recursive = val;
        return this;
    }

    protected void checkTypeNotNull() {
        if (type==null) 
            throw new NullPointerException("type must be set to resolve, for '"+value+"'"+(description!=null ? ", "+description : ""));
    }

    @Override
    public ValueResolverIterator<T> iterator() {
        return new ValueResolverIterator<T>(this);
    }

    @Override
    public T get() {
        Maybe<T> m = getMaybe();
        if (m.isPresent()) return m.get();
        if (returnDefaultOnGet) return defaultValue;
        return m.get();
    }

    public Maybe<T> getMaybe() {
        Maybe<T> result = getMaybeInternal();
        if (log.isTraceEnabled()) {
            log.trace(this+" evaluated as "+result);
        }
        return result;
    }
    
    protected boolean isEvaluatingImmediately() {
        return immediately || BrooklynTaskTags.hasTag(Tasks.current(), BrooklynTaskTags.IMMEDIATE_TASK_TAG);
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    protected Maybe<T> getMaybeInternal() {
        if (started.getAndSet(true))
            throw new IllegalStateException("ValueResolver can only be used once");
        
        if (expired) return Maybe.absent("Nested resolution of "+getOriginalValue()+" did not complete within "+timeout);
        
        ExecutionContext exec = this.exec;
        if (exec==null) {
            // if execution context not specified, take it from the current task if present
            exec = BasicExecutionContext.getCurrentExecutionContext();
        }
        
        if (!recursive && type != Object.class) {
            throw new IllegalStateException("When non-recursive resolver requested the return type must be Object " +
                    "as the immediately resolved value could be a number of (deferred) types.");
        }
        
        CountdownTimer timerU = parentTimer;
        if (timerU==null && timeout!=null)
            timerU = timeout.countdownTimer();
        final CountdownTimer timer = timerU;
        if (timer!=null && !timer.isNotPaused())
            timer.start();
        
        checkTypeNotNull();
        Object v = this.value;
        
        //if the expected type is a closure or map and that's what we have, we're done (or if it's null);
        //but not allowed to return a future or DeferredSupplier as the resolved value
        if (v==null || (!forceDeep && type.isInstance(v) && !Future.class.isInstance(v) && !DeferredSupplier.class.isInstance(v) && !TaskFactory.class.isInstance(v)))
            return Maybe.of((T) v);
        
        try {
            boolean allowImmediateExecution = false;
            boolean bailOutAfterImmediateExecution = false;
            
            if (v instanceof ImmediateSupplier || v instanceof DeferredSupplier) {
                allowImmediateExecution = true;
                
            } else {
                if (v instanceof TaskFactory<?>) {
                    v = ((TaskFactory<?>)v).newTask();
                    allowImmediateExecution = true;
                    bailOutAfterImmediateExecution = true;
                    BrooklynTaskTags.setTransient(((TaskAdaptable<?>)v).asTask());
                }
                
                //if it's a task or a future, we wait for the task to complete
                if (v instanceof TaskAdaptable<?>) {
                    v = ((TaskAdaptable<?>) v).asTask();
                }
            }

            if (allowImmediateExecution && isEvaluatingImmediately()) {
                // Feb 2017 - many things now we try to run immediate; notable exceptions are:
                // * where the target isn't safe to run again (such as a Task which someone else might need), 
                // * or where he can't be run in an "interrupting" mode even if non-blocking (eg Future.get(), some other tasks)
                // (the latter could be tried here, with bailOut false, but in most cases it will just throw so we still need to
                // have the timings as in SHORT_WAIT etc as a fallack)
                
                // TODO svet suggested at https://github.com/apache/brooklyn-server/pull/565#pullrequestreview-27124074
                // that we might flip the immediately bit if interrupted -- or maybe instead (alex's idea)
                // enter this block
                // if (allowImmediateExecution && (isEvaluatingImmediately() || Thread.isInterrupted())
                // -- feels right, and would help with some recursive immediate values but no compelling 
                // use case yet and needs some deep thought which we're deferring for now
                
                Maybe<T> result = null;
                try {
                    result = exec.getImmediately(v);
                    
                    return (result.isPresent())
                        ? recursive
                            ? new ValueResolver<T>(result.get(), type, this).getMaybe()
                                : result
                                : result;
                } catch (ImmediateSupplier.ImmediateUnsupportedException e) {
                    if (bailOutAfterImmediateExecution) {
                        throw new ImmediateSupplier.ImmediateUnsupportedException("Cannot get immediately: "+v);
                    }
                    // ignore, continue below
                    if (log.isTraceEnabled()) {
                        log.trace("Unable to resolve-immediately for "+description+" ("+v+", unsupported, type "+v.getClass()+"); falling back to executing with timeout: "+e);
                    }
                } catch (ImmediateSupplier.ImmediateValueNotAvailableException e) {
                    // definitively not available
                    return ImmediateSupplier.ImmediateValueNotAvailableException.newAbsentWithExceptionSupplier();
                }
            }
            
            if (v instanceof Task) {
                //if it's a task, we make sure it is submitted
                Task<?> task = (Task<?>) v;
                if (!task.isSubmitted()) {
                    if (exec==null) {
                        return Maybe.absent("Value for unsubmitted task '"+getDescription()+"' requested but no execution context available");
                    }
                    if (!task.getTags().contains(BrooklynTaskTags.TRANSIENT_TASK_TAG)) {
                        // mark this non-transient, because this value is usually something set e.g. in config
                        // (should discourage this in favour of task factories which can be transiently interrupted?)
                        BrooklynTaskTags.addTagDynamically(task, BrooklynTaskTags.NON_TRANSIENT_TASK_TAG);
                    }
                    if (timer==null && !Thread.currentThread().isInterrupted() && !isEvaluatingImmediately()) {
                        // if all conditions above:  no timeout, not immediate, and not interrupted,
                        // then we can run in this thread
                        exec.get(task);
                    } else {
                        exec.submit(task);
                    }
                }
            }

            if (v instanceof Future) {
                final Future<?> vfuture = (Future<?>) v;

                //including tasks, above
                if (!vfuture.isDone()) {
                    if (isEvaluatingImmediately()) {
                        return ImmediateSupplier.ImmediateValueNotAvailableException.newAbsentWithExceptionSupplier();
                    }
                    
                    Callable<Maybe> callable = new Callable<Maybe>() {
                        @Override
                        public Maybe call() throws Exception {
                            return Durations.get(vfuture, timer);
                        } };

                    String description = getDescription();
                    Maybe vm = Tasks.withBlockingDetails("Waiting for "+description, callable);
                    if (vm.isAbsent()) return vm;
                    v = vm.get();

                } else {
                    v = vfuture.get();
                    
                }

            } else if (v instanceof DeferredSupplier<?>) {
                final DeferredSupplier<?> ds = (DeferredSupplier<?>) v;

                if ((!Boolean.FALSE.equals(embedResolutionInTask) && (exec!=null || timeout!=null)) || Boolean.TRUE.equals(embedResolutionInTask)) {
                    if (exec==null)
                        return Maybe.absent("Embedding in task needed for '"+getDescription()+"' but no execution context available");
                        
                    Callable<Object> callable = new Callable<Object>() {
                        @Override
                        public Object call() throws Exception {
                            try {
                                Tasks.setBlockingDetails("Retrieving "+ds);
                                return ds.get();
                            } finally {
                                Tasks.resetBlockingDetails();
                            }
                        } };
                    String description = getDescription();
                    TaskBuilder<Object> tb = Tasks.<Object>builder()
                            .body(callable)
                            .displayName("Resolving dependent value of deferred supplier")
                            .description(description);
                    if (isTransientTask) tb.tag(BrooklynTaskTags.TRANSIENT_TASK_TAG);
                    
                    // immediate resolution is handled above
                    Task<Object> vt = exec.submit(tb.build());
                    Maybe<Object> vm = Durations.get(vt, timer);
                    vt.cancel(true);
                    if (vm.isAbsent()) return (Maybe<T>)vm;
                    v = vm.get();
                    
                } else {
                    try {
                        Tasks.setBlockingDetails("Retrieving (non-task) "+ds);
                        v = ((DeferredSupplier<?>) ds).get();
                    } finally {
                        Tasks.resetBlockingDetails();
                    }
                }

            } else {
                if (supportsDeepResolution(v)) {
                    // restrict deep resolution to the same set of types as calling code;
                    // in particular need to avoid for "interesting iterables" such as PortRange
                    
                    if (v instanceof Map) {
                        //and if a map or list we look inside
                        Map result = Maps.newLinkedHashMap();
                        for (Map.Entry<?,?> entry : ((Map<?,?>)v).entrySet()) {
                            Maybe<?> kk = new ValueResolver(entry.getKey(), type, this)
                                .description( (description!=null ? description+", " : "") + "map key "+entry.getKey() )
                                .getMaybe();
                            if (kk.isAbsent()) return (Maybe<T>)kk;
                            Maybe<?> vv = new ValueResolver(entry.getValue(), type, this)
                                .description( (description!=null ? description+", " : "") + "map value for key "+kk.get() )
                                .getMaybe();
                            if (vv.isAbsent()) return (Maybe<T>)vv;
                            result.put(kk.get(), vv.get());
                        }
                        return Maybe.of((T) result);
        
                    } else if (v instanceof Set) {
                        Set result = Sets.newLinkedHashSet();
                        int count = 0;
                        for (Object it : (Set)v) {
                            Maybe<?> vv = new ValueResolver(it, type, this)
                                .description( (description!=null ? description+", " : "") + "entry "+count )
                                .getMaybe();
                            if (vv.isAbsent()) return (Maybe<T>)vv;
                            result.add(vv.get());
                            count++;
                        }
                        return Maybe.of((T) result);
        
                    } else if (v instanceof Iterable) {
                        List result = Lists.newArrayList();
                        int count = 0;
                        for (Object it : (Iterable)v) {
                            Maybe<?> vv = new ValueResolver(it, type, this)
                                .description( (description!=null ? description+", " : "") + "entry "+count )
                                .getMaybe();
                            if (vv.isAbsent()) return (Maybe<T>)vv;
                            result.add(vv.get());
                            count++;
                        }
                        return Maybe.of((T) result);
                    }
                }
                
                return TypeCoercions.tryCoerce(v, TypeToken.of(type));
            }

        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            
            String msg = "Error resolving "+(description!=null ? description+", " : "")+v+", in "+exec;
            String eTxt = Exceptions.collapseText(e);
            IllegalArgumentException problem = eTxt.startsWith(msg) ? new IllegalArgumentException(e) : new IllegalArgumentException(msg+": "+eTxt, e);
            if (swallowExceptions) {
                if (log.isDebugEnabled())
                    log.debug("Resolution of "+this+" failed, swallowing and returning: "+e);
                return Maybe.absent(problem);
            }
            if (log.isDebugEnabled())
                log.debug("Resolution of "+this+" failed, throwing: "+e);
            throw problem;
        }
        
        if (recursive) {
            return new ValueResolver(v, type, this).getMaybe();
        } else {
            return (Maybe<T>) Maybe.of(v);
        }
    }

    // whether value resolution supports deep resolution
    @Beta
    public static boolean supportsDeepResolution(Object v) {
        return (v instanceof Map || v instanceof Collection);
    }
    
    protected String getDescription() {
        return description!=null ? description : ""+value;
    }
    protected Object getOriginalValue() {
        if (parentOriginalValue!=null) return parentOriginalValue;
        return value;
    }
    protected Class<T> getType() {
        return type;
    }

    @Override
    public String toString() {
        return JavaClassNames.cleanSimpleClassName(this)+"["+JavaClassNames.cleanSimpleClassName(type)+" "+value+"]";
    }
    
    /** Returns a quick resolving type coercer. May allow more underlying {@link ValueResolver} customization in the future. */
    public static class ResolvingTypeCoercer implements TypeCoercer {
        @Override
        public <T> T coerce(Object input, Class<T> type) {
            return tryCoerce(input, type).get();
        }

        @Override
        public <T> Maybe<T> tryCoerce(Object input, Class<T> type) {
            return new ValueResolver<T>(input, type).timeout(REAL_QUICK_WAIT).getMaybe();
        }

        @SuppressWarnings({ "unchecked", "rawtypes" })
        @Override
        public <T> Maybe<T> tryCoerce(Object input, TypeToken<T> type) {
            return (Maybe) tryCoerce(input, type.getRawType());
        }
    }
    
}
