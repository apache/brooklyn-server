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

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskFactory;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslToStringHelpers;
import org.apache.brooklyn.camp.spi.Assembly;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.camp.spi.resolve.interpret.PlanInterpretationNode;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.core.task.ImmediateSupplier;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.commons.lang3.tuple.Pair;
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
 * <p>
 * The {@link #get()} method may (and often does) rely on thread locals from the task.
 * In some cases, eg when using {@link #getImmediately()} the thread is temporarily interrupted
 * to prevent any blocking activities.  {@link #get()} must support interruption.
 **/
public abstract class BrooklynDslDeferredSupplier<T> implements DeferredSupplier<T>, ImmediateSupplier<T>, TaskFactory<Task<T>>, Serializable {

    private static final long serialVersionUID = -8789624905412198233L;

    private static final Logger log = LoggerFactory.getLogger(BrooklynDslDeferredSupplier.class);

    @JsonInclude(Include.NON_NULL)
    @JsonProperty(value="$brooklyn:literal")
    /** The original DSL which generated the expression, if available.
     * Note the {@link #toString()} should create an equivalent expression.
     */
    // now included in xstream persistence, so that jackson reconstruction (for steps) has access to it;
    // but cleaned up so it is only applied to the outermost DSL object (last one created for given parse node);
    // jackson deserialization includes this, and relies on it if reading an Object,
    // but if reading to a Supplier it will correctly instantiate based on the type field.
    protected Object dsl = null;
    protected static ThreadLocal<Pair<Integer,BrooklynDslDeferredSupplier>> lastSourceNode = new ThreadLocal<>();

    public BrooklynDslDeferredSupplier() {
        PlanInterpretationNode sourceNode = BrooklynDslInterpreter.currentNode();
        if (sourceNode!=null && sourceNode.getOriginalValue()!=null) {
            Pair<Integer, BrooklynDslDeferredSupplier> last = lastSourceNode.get();
            if (last!=null && last.getLeft().equals(sourceNode.hashCode())) {
                // set DSL on the last object created, and clear on previous ones;
                // a bit of a hack, might be better to do on callers, ideally ensure all calls come from same CAMP parse caller
                // (which i think they probably do)
                last.getRight().dsl = null;
            }
            dsl = sourceNode.getOriginalValue();
            lastSourceNode.set(Pair.of(sourceNode.hashCode(), this));
        }
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
        if (log.isTraceEnabled())
            log.trace("Queuing task to resolve {}, called by {}", dsl, Tasks.current());

        ExecutionContext exec = BrooklynTaskTags.getCurrentExecutionContext();
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

        if (log.isTraceEnabled()) {
            // https://issues.apache.org/jira/browse/BROOKLYN-269
            // We must not log sensitve data, such as from $brooklyn:external, or if the value
            // is to be used as a password etc. Unfortunately we don't know the context, so can't
            // use Sanitizer.sanitize.
            log.trace("Resolved "+dsl);
        }
        return result;
    }

    @Override
    public abstract Task<T> newTask();
    
    protected void checkAndTagForRecursiveReference(Entity targetEntity, String tag) {
        Task<?> ancestor = Tasks.current();
        if (ancestor!=null) {
            ancestor = ancestor.getSubmittedByTask();
        }
        while (ancestor!=null) {
            if (TaskTags.hasTag(ancestor, tag)) {
                throw new IllegalStateException("Recursive reference "+tag); 
            }
            ancestor = ancestor.getSubmittedByTask();
        }
        
        Tasks.addTagDynamically(tag);
    }
}
