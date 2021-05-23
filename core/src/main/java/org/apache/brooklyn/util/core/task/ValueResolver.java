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
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateUnsupportedException;
import org.apache.brooklyn.util.core.task.ImmediateSupplier.ImmediateValueNotAvailableException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.guava.TypeTokens;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Durations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.reflect.TypeToken;

import javax.annotation.Nullable;

/** 
 * Resolves a given object, as follows:
 * <li> If it is a {@link Tasks} or a {@link DeferredSupplier} then get its contents
 * <li> If it's a map/iterable depending on {@link #deep()}, it can apply resolution to contents
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
    final TypeToken<T> typeT;
    ExecutionContext exec;
    String description;

    boolean allowDeepResolution = true;
    /** whether to traverse into the object coercing even if the types appear to match at this level;
     * useful if resolving a map to a map but we want to look inside the map */
    boolean forceDeep;
    /** if true, the type specified here is re-used when coercing contents (eg map keys, values);
     * if null (the default), then do that only if the type here has no generics;
     * if false then rely on generics or if no generics then don't coerce.
     * if the {@link #typeT} is Object this has no effect. */
    // TODO i don't think we ever rely this -Alex
    Boolean ignoreGenericsAndApplyThisTypeToContents;

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
    
    ValueResolver(Object v, TypeToken<T> type) {
        this.value = v;
        this.typeT = type;
        checkTypeNotNull();
        parentOriginalValue = null;
        parentTimer = null;
    }
    
    ValueResolver(Object v, TypeToken<T> type, ValueResolver<?> parent) {
        this.value = v;
        this.typeT = type;
        checkTypeNotNull();

        parentOriginalValue = parent.getOriginalValue();
        parentTimer = parent.parentTimer;

        copyNonFinalFields(parent);

        // these should need to be nested/copied (this constructor is for setting up a nested resolver during deep resolution)
        returnDefaultOnGet = false;
        defaultValue = null;
        swallowExceptions = false;
        recursive = true;
        ignoreGenericsAndApplyThisTypeToContents = null;
    }

    private void copyNonFinalFields(ValueResolver<?> parent) {
        exec = parent.exec;
        description = parent.description;
        embedResolutionInTask = parent.embedResolutionInTask;
        swallowExceptions = parent.swallowExceptions;

        recursive = parent.recursive;
        forceDeep = parent.forceDeep;
        allowDeepResolution = parent.allowDeepResolution;
        ignoreGenericsAndApplyThisTypeToContents = parent.ignoreGenericsAndApplyThisTypeToContents;

        timeout = parent.timeout;
        immediately = parent.immediately;
        if (parentTimer!=null && parentTimer.isExpired())
            expired = true;

        defaultValue = (T)parent.defaultValue;
        returnDefaultOnGet = parent.returnDefaultOnGet;
    }

    public static class ResolverBuilderPretype {
        final Object v;
        public ResolverBuilderPretype(Object v) {
            this.v = v;
        }
        public <T> ValueResolver<T> as(Class<T> type) {
            return as(TypeToken.of(type));
        }
        public <T> ValueResolver<T> as(TypeToken<T> type) {
            return new ValueResolver<T>(v, type);
        }
    }

    /** returns a copy of this resolver which can be queried, even if the original (single-use instance) has already been copied */
    @Override
    public ValueResolver<T> clone() {
        return cloneReplacingValueAndType(value, typeT);
    }
    
    <S> ValueResolver<S> cloneReplacingValueAndType(Object newValue, TypeToken<S> superType) {
        // superType expected to be either type or Object.class
        if (!superType.isAssignableFrom(typeT)) {
            throw new IllegalStateException("superType must be assignable from " + typeT);
        }
        ValueResolver<S> result = new ValueResolver<S>(newValue, superType);
        result.copyNonFinalFields(this);

        if (returnDefaultOnGet) {
            if (!TypeTokens.isInstanceRaw(superType, defaultValue)) {
                throw new IllegalStateException("Existing default value " + defaultValue + " not compatible with new type " + superType);
            }
        }

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

    /** causes nested structures (maps, lists) to be descended, resolved, and (if the target type is not Object) coerced.
     * as {@link #deep(boolean, boolean, Boolean)} with true, true, false. */
    public ValueResolver<T> deep() { return deep(true, true, false); }
    /** causes nested structures (maps, lists) to be descended and nested unresolved values resolved.
     * for legacy reasons this sets deepTraversalUsesRootType.
     * @deprecated use {@link #deep(boolean, boolean, Boolean)} */
    @Deprecated public ValueResolver<T> deep(boolean forceDeep) { return deep(true, true, true); }
    /* @deprecated use {@link #deep(boolean, boolean, Boolean)} */
    @Deprecated public ValueResolver<T> deep(boolean forceDeep, Boolean deepTraversalUsesRootType) { return deep(true,true,true); }
    /** causes nested structures (maps, lists) to be descended and nested unresolved values resolved.
     * if the second argument is true, the type specified here is used against non-map/iterable items
     * inside maps and iterables encountered. if false, any generic signature on the supplied type
     * is traversed to match contained items. if null (default), it is inferred from the type,
     * those with generics imply false, and those without imply true.
     *
     * see {@link #allowDeepResolution}, {@link Tasks#resolveDeepValue(Object, Class, ExecutionContext, String)} and
     * {@link Tasks#resolveDeepValueCoerced(Object, TypeToken, ExecutionContext, String)}. */
    public ValueResolver<T> deep(boolean allowDeepResolution, boolean forceDeep, Boolean deepTraversalUsesRootType) {
        this.allowDeepResolution = allowDeepResolution;
        this.forceDeep = forceDeep;
        this.ignoreGenericsAndApplyThisTypeToContents = deepTraversalUsesRootType;
        return this;
    }

    /** if true, forces execution of a deferred supplier to be run in a task;
     * if false, it prevents it (meaning time limits may not be applied);
     * if null, the default, it runs in a task if a time limit is applied.
     * <p>
     * running inside a task is required for some {@link DeferredSupplier}
     * instances which look up a task {@link ExecutionContext},
     * and for coercions that need to look up registered types in the mgmt context,
     * and such tasks might be automatically applied if so. */
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

    /**
     * Whether the value should be resolved deeply, if it's a map or collection, by looking inside it.
     * The default here is true (mainly for legacy reasons).
     * If used, and {@link #ignoreGenericsAndApplyThisTypeToContents} is set (which it often is),
     * the type supplied is used to convert keys _and_ values of the map or collection,
     * rather than the overall return type (so set false if trying to convert a map to another type).
     * There is not currently any support for entry schemas on the map,
     * not even by supplying a generically typed map (eg Map<String,Integer>).
     */
    @Beta
    public ValueResolver<T> allowDeepResolution(boolean val) {
        this.allowDeepResolution = val;
        return this;
    }

    protected void checkTypeNotNull() {
        if (typeT==null)
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

    public static boolean isDeferredOrTaskInternal(Object o) {
        if (o instanceof TaskAdaptable || o instanceof DeferredSupplier || o instanceof Future) {
            return true;
        }
        return false;
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

        if (!recursive && !TypeTokens.equalsRaw(Object.class, typeT)) {
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

        //if the expected type is what we have, we're done (or if it's null);
        //but not allowed to return a future or DeferredSupplier as the resolved value,
        //and not if generics signatures might be different
        if (v==null || (!forceDeep && TypeTokens.isRaw(typeT) && TypeTokens.isInstanceRaw(typeT, v) && !Future.class.isInstance(v) && !DeferredSupplier.class.isInstance(v) && !TaskFactory.class.isInstance(v)))
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
                    if (exec==null) {
                        return Maybe.absent("Immediate resolution requested for '"+getDescription()+"' but no execution context available");
                    }
                    result = exec.getImmediately(v);

                    return (result.isPresent())
                        ? recursive
                            ? new ValueResolver<T>(result.get(), typeT, this).getMaybe()
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
                if (allowDeepResolution && supportsDeepResolution(v, typeT)) {

                    // allows user to resolveValue(map, String) with the effect
                    // that things _in_ the collection would be resolved as string.
                    // alternatively use generics.
                    boolean applyThisTypeToContents = Boolean.TRUE.equals(ignoreGenericsAndApplyThisTypeToContents);

                    // restrict deep resolution to the same set of types as calling code;
                    // in particular need to avoid for "interesting iterables" such as PortRange

                    if (v instanceof Map) {
                        TypeToken<?> keyT;
                        TypeToken<?> valT;
                        if (applyThisTypeToContents) {
                            keyT = typeT;
                            valT = typeT;
                        } else {
                            TypeToken<?>[] innerTypes = TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(typeT, Map.class); // innerTypes = Reflections.getGenericParameterTypeTokens( typeT );
                            if (innerTypes.length==2) {
                                keyT = innerTypes[0];
                                valT = innerTypes[1];
                            } else {
                                keyT = valT = TypeToken.of(Object.class);
                            }
                        }
                        //and if a map or list we look inside
                        Map result = Maps.newLinkedHashMap();
                        for (Map.Entry<?,?> entry : ((Map<?,?>)v).entrySet()) {
                            Maybe<?> kk = new ValueResolver(entry.getKey(), keyT, this)
                                .description( (description!=null ? description+", " : "") + "map key "+entry.getKey() )
                                .getMaybe();
                            if (kk.isAbsent()) return (Maybe<T>)kk;
                            Maybe<?> vv = new ValueResolver(entry.getValue(), valT, this)
                                .description( (description!=null ? description+", " : "") + "map value for key "+kk.get() )
                                .getMaybe();
                            if (vv.isAbsent()) return (Maybe<T>)vv;
                            result.put(kk.get(), vv.get());
                        }
                        v = result;

                    } else if (v instanceof Iterable) {
                        TypeToken<?> entryT;
                        if (applyThisTypeToContents) {
                            entryT = typeT;
                        } else {
                            TypeToken<?>[] innerTypes = TypeTokens.getGenericParameterTypeTokensWhenUpcastToClassRaw(typeT, Iterable.class);
                            if (innerTypes.length==1) {
                                entryT = innerTypes[0];
                            } else {
                                entryT = TypeToken.of(Object.class);
                            }
                        }

                        Collection<Object> result = v instanceof Set ? MutableSet.of() : Lists.newArrayList();
                        int count = 0;
                        for (Object it : (Iterable)v) {
                            Maybe<?> vv = new ValueResolver(it, entryT, this)
                                .description( (description!=null ? description+", " : "") + "entry "+count )
                                .getMaybe();
                            if (vv.isAbsent()) return (Maybe<T>)vv;
                            result.add(vv.get());
                            count++;
                        }
                        v = result;
                    }
                }

                if (exec!=null && (
                        ((v instanceof Map) && !((Map)v).isEmpty())
                                ||
                        ((v instanceof List) && !((List)v).isEmpty())) ) {
                    // do type coercion in a task to allow registered types
                    Object vf = v;
                    Task<Maybe<T>> task = Tasks.create("type coercion", () -> TypeCoercions.tryCoerce(vf, typeT));
                    BrooklynTaskTags.setTransient(task);
                    return exec.get(task);
                } else {
                    return TypeCoercions.tryCoerce(v, typeT);
                }
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
            return new ValueResolver(v, typeT, this).getMaybe();
        } else {
            return (Maybe<T>) Maybe.of(v);
        }
    }

    // whether value resolution supports deep resolution
    /** @deprecated since 1.0.0 use {@link #supportsDeepResolution(Object, TypeToken)} */
    @Beta @Deprecated
    public static boolean supportsDeepResolution(Object v) {
        return supportsDeepResolution(v, null);
    }
    @Beta
    public static boolean supportsDeepResolution(Object v, @Nullable TypeToken<?> type) {
        return (v instanceof Map || v instanceof Collection) && (type==null || typeAllowsDeepResolution(type));
    }
    @Beta
    public static boolean typeAllowsDeepResolution(@Nullable TypeToken<?> type) {
        if (type==null) return true;
        return TypeTokens.isAssignableFromRaw(Map.class, type) || TypeTokens.isAssignableFromRaw(Iterable.class, type) || TypeTokens.isAssignableFromRaw(Object.class, type);
    }

    protected String getDescription() {
        return description!=null ? description : ""+value;
    }
    protected Object getOriginalValue() {
        if (parentOriginalValue!=null) return parentOriginalValue;
        return value;
    }
    protected TypeToken<T> getTypeToken() {
        return typeT;
    }

    @Override
    public String toString() {
        return JavaClassNames.cleanSimpleClassName(this)+"["+typeT+" "+value+"]";
    }
}
