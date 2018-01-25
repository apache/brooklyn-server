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
package org.apache.brooklyn.api.mgmt;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.util.guava.Maybe;

import com.google.common.annotations.Beta;
import com.google.common.base.Supplier;

/**
 * This is a Brooklyn extension to the Java {@link Executor}.
 * 
 * The "context" could, for example, be an {@link Entity} so that tasks executed 
 * can be annotated as executing in that context.
 */
public interface ExecutionContext extends Executor {

    /**
     * Get the tasks executed through this context (returning an immutable set).
     */
    Set<Task<?>> getTasks();

    /**
     * See {@link ExecutionManager#submit(Map, TaskAdaptable)} for properties that can be passed in.
     */
    Task<?> submit(Map<?,?> properties, Runnable runnable);

    /**
     * See {@link ExecutionManager#submit(Map, TaskAdaptable)} for properties that can be passed in.
     */
    <T> Task<T> submit(Map<?,?> properties, Callable<T> callable);

    /** {@link ExecutionManager#submit(Runnable) 
     * @deprecated since 1.0.0 pass a display name or a more detailed map */
    @Deprecated
    Task<?> submit(Runnable runnable);
 
    /** {@link ExecutionManager#submit(Callable)
     * @deprecated since 1.0.0 pass a display name or a more detailed map */
    @Deprecated
    <T> Task<T> submit(Callable<T> callable);

    /** {@link ExecutionManager#submit(String, Runnable) */
    Task<?> submit(String displayName, Runnable runnable);
 
    /** {@link ExecutionManager#submit(String, Callable) */
    <T> Task<T> submit(String displayName, Callable<T> callable);

    /** See {@link ExecutionManager#submit(Map, TaskAdaptable)}. */
    <T> Task<T> submit(TaskAdaptable<T> task);
    
    /**
     * See {@link ExecutionManager#submit(Map, TaskAdaptable)} for properties that can be passed in.
     */
    <T> Task<T> submit(Map<?,?> properties, TaskAdaptable<T> task);

    boolean isShutdown();

    /**
     * Gets the value promptly, or returns {@link Maybe#absent()} if the value is not yet available.
     * It may throw an error if it cannot be determined whether a value is available immediately or not.
     * <p>
     * Implementations will typically act like {@link #get(TaskAdaptable)} with additional
     * tricks to attempt to be non-blocking, such as recognizing some "immediate" markers.  
     * <p>
     * Supports {@link Callable}, {@link Runnable}, {@link Supplier}, and {@link TaskFactory} argument types.
     * <p>
     * Passing in {@link Task} is deprecated and discouraged - see {@link #getImmediately(Task)}.
     */
    // TODO reference ImmediateSupplier when that class is moved to utils project
    @Beta
    <T> Maybe<T> getImmediately(Object callableOrSupplierOrTaskFactory);
    
    /**
     * As {@link #getImmediately(Object)} but strongly typed for a task.
     * 
     * @deprecated since 1.0.0; this can cause the task to be interrupted/cancelled, such that subsequent  
     *             use of {@code task.get()} will fail (if the value was not resolved immediately previously).
     *             It is only safe to call this if the the given task is for a one-off usage (not expected
     *             to be used again later). Consider supplying a {@link TaskFactory} if this tasks's 
     *             {@link Task#cancel(boolean)} is problematic, or consider other utilities 
     *             (such as ValueResolver with immediate(true) in the brooklyn-core project).
     */
    @Beta
    <T> Maybe<T> getImmediately(Task<T> task);

    /**
     * Efficient implementation of common case when {@link #submit(TaskAdaptable)} is followed by an immediate {@link Task#get()}.
     * <p>
     * This is efficient in that it may typically attempt to execute in the current thread, 
     * with appropriate configuration to make it look like it is in a sub-thread, 
     * ie registering this as a task and allowing context methods on tasks to see the given sub-task.
     * However it will normally be non-blocking which reduces overhead and 
     * is permissible within a {@link #getImmediately(Object)} task
     * <p>
     * If the argument has already been submitted it simply blocks on it 
     * (i.e. no additional execution, and in that case would fail within a {@link #getImmediately(Object)}).
     * 
     * @param task the task whose result is being sought
     * @return result of the task execution
     */
    @Beta
    <T> T get(TaskAdaptable<T> task);
    
}