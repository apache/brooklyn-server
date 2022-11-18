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
package org.apache.brooklyn.core.workflow.store;

import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowStatePersistenceViaSensors {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStatePersistenceViaSensors.class);

    public static final AttributeSensor<Map<String,WorkflowExecutionContext>> INTERNAL_WORKFLOWS = Sensors.newSensor(new TypeToken<Map<String, WorkflowExecutionContext>>() {}, "internals.brooklyn.workflow");


    private final ManagementContext mgmt;

    public WorkflowStatePersistenceViaSensors(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

   enum PersistenceWithQueuedTasks { ALLOW, WARN, FAIL }

    public void checkpoint(WorkflowExecutionContext context) {
        checkpoint(context, PersistenceWithQueuedTasks.WARN);
    }
    public void checkpoint(WorkflowExecutionContext context, PersistenceWithQueuedTasks expectQueuedTasks) {

        Entity entity = context.getEntity();
        if (Entities.isUnmanagingOrNoLongerManaged(entity)) {
            log.debug("Skipping persistence of "+context+" as entity is no longer active here");
            return;
        }

        if (expectQueuedTasks!= PersistenceWithQueuedTasks.ALLOW && DynamicTasks.getTaskQueuingContext()!=null) {
            List<Task<?>> tasks = DynamicTasks.getTaskQueuingContext().getQueue().stream().filter(t -> !t.isDone()).collect(Collectors.toList());
            if (!tasks.isEmpty()) {
                String msg = "Persisting " + context + " when there are still queued tasks (probably an error): " + tasks;
                if (expectQueuedTasks==PersistenceWithQueuedTasks.FAIL) throw new IllegalStateException(msg);
                log.warn(msg);
            }
        }

        // clear interrupt status so we can persist e.g. if we are interrupted or shutdown
        boolean interrupted = Thread.interrupted();
        boolean doExpiry = WorkflowRetentionAndExpiration.isExpirationCheckNeeded(entity);
        try {
            updateMap(entity, doExpiry, true, v -> v.put(context.getWorkflowId(), context));

        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    void updateMap(Entity entity, boolean doExpiry, boolean persist, Consumer<Map<String,WorkflowExecutionContext>> action) {
        entity.sensors().modify(INTERNAL_WORKFLOWS, v -> {
            if (v == null) v = MutableMap.of();
            if (action!=null) action.accept(v);
            if (doExpiry) v = WorkflowRetentionAndExpiration.recomputeExpiration(v, null);
            return Maybe.of(v);
        });
        if (persist) mgmt.getRebindManager().forcePersistNow(false, null);
    }

    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        Map<String, WorkflowExecutionContext> result = entity.sensors().get(INTERNAL_WORKFLOWS);
        if (result==null) result = ImmutableMap.of();
        return ImmutableMap.copyOf(result);
    }

    public void updateWithoutPersist(Entity entity, List<WorkflowExecutionContext> workflows) {
        if (workflows!=null && !workflows.isEmpty()) entity.sensors().modify(INTERNAL_WORKFLOWS, v -> {
            if (v == null) {
                throw new IllegalStateException("Update workflows requested for "+workflows+" when none recorded against "+entity);
            }
            workflows.forEach(w -> v.put(w.getWorkflowId(), w));
            return Maybe.of(v);
        });
    }

    public Maybe<WorkflowExecutionContext> getFromTag(BrooklynTaskTags.WorkflowTaskTag nestedWorkflowTag) {
        Entity targetEntity = mgmt.lookup(nestedWorkflowTag.getEntityId(), Entity.class);
        if (targetEntity==null) {
            return Maybe.absent("Entity "+nestedWorkflowTag.getWorkflowId()+" not found");
        } else {
            WorkflowExecutionContext nestedWorkflowToReplay = new WorkflowStatePersistenceViaSensors(mgmt).getWorkflows(targetEntity).get(nestedWorkflowTag.getWorkflowId());
            if (nestedWorkflowToReplay == null) {
                // shouldn't happen unless workflow was expired, as workflow will be saved before resumption
                return Maybe.absent("Workflow "+nestedWorkflowTag.getWorkflowId()+" not found on entity "+targetEntity+"; possibly expired?");
            } else {
                return Maybe.of(nestedWorkflowToReplay);
            }
        }
    }
}
