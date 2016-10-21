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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.camp.spi.Assembly;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationNode;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * Provide an object suitable to resolve chained invocations in a parsed YAML / Deployment Plan DSL,
 * which also implements {@link DeferredSupplier} so that they can be resolved when needed
 * (e.g. when entity-lookup and execution contexts are available).
 * <p>
 * Implementations of this abstract class are expected to be immutable and thread safe,
 * as instances must support usage in multiple {@link Assembly} instances
 * created from a single {@link AssemblyTemplate}. The object can be used in parallel
 * from multiple threads and no locking is done as all extending objects are assumed to be stateless.
 * <p>
 * Subclasses which return a deferred value are typically only
 * resolvable in the context of a {@link Task} on an {@link Entity}; 
 * these should be only used as the value of a {@link ConfigKey} set in the YAML,
 * and should not accessed until after the components / entities are created
 * and are being started.
 * (TODO the precise semantics of this are under development.)
 * <p>
 * The threading model is that only one thread can call {@link #get()} at a time. An interruptible
 * lock is obtained using {@link #lock} for the duration of that method. It is important to not
 * use {@code synchronized} because that is not interruptible - if someone tries to get the value
 * and interrupts after a short wait, then we must release the lock immediately and return.
 **/
public abstract class BrooklynDslDeferredSupplier<T> implements DeferredSupplier<T>, TaskFactory<Task<T>>, Serializable {

    private static final long serialVersionUID = -8789624905412198233L;

    private static final Logger log = LoggerFactory.getLogger(BrooklynDslDeferredSupplier.class);

    // TODO json of this object should *be* this, not wrapped this ($brooklyn:literal is a bit of a hack, though it might work!)
    @JsonInclude
    @JsonProperty(value="$brooklyn:literal")
    // currently marked transient because it's only needed for logging
    private transient Object dsl = "(gone)";

    public BrooklynDslDeferredSupplier() {
        PlanInterpretationNode sourceNode = BrooklynDslInterpreter.currentNode();
        dsl = sourceNode!=null ? sourceNode.getOriginalValue() : null;
    }
    
    /** returns the current entity; for use in implementations of {@link #get()} */
    protected final static EntityInternal entity() {
        return (EntityInternal) BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
    }

    /**
     * Returns the current management context; for use in implementations of {@link #get()} that are not associated
     * with an entity.
     */
    protected final static ManagementContextInternal managementContext() {
        return (ManagementContextInternal) BrooklynTaskTags.getManagementContext(Tasks.current());
    }

    @Override
    public final T get() {
        if (log.isDebugEnabled())
            log.debug("Queuing task to resolve "+dsl+", called by "+Tasks.current());

        EntityInternal entity = (EntityInternal) BrooklynTaskTags.getTargetOrContextEntity(Tasks.current());
        ExecutionContext exec =
                (entity != null) ? entity.getExecutionContext()
                                 : BasicExecutionContext.getCurrentExecutionContext();
        if (exec == null) {
            throw new IllegalStateException("No execution context available to resolve " + dsl);
        }

        Task<T> task = newTask();
        T result;
        try {
            result = exec.submit(task).get();
        } catch (InterruptedException | ExecutionException e) {
            Task<?> currentTask = Tasks.current();
            if (currentTask != null && currentTask.isCancelled()) {
                task.cancel(true);
            }
            throw Exceptions.propagate(e);
        }

        if (log.isDebugEnabled())
            log.debug("Resolved "+result+" from "+dsl);
        return result;
    }

    @Override
    public abstract Task<T> newTask();

}
