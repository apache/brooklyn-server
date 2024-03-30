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

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowStatePersistenceViaSensors {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStatePersistenceViaSensors.class);

    public static final ConfigKey<WorkflowStatePersistenceViaSensors> SENSOR_WORKFLOW_PERSISTER = ConfigKeys.newConfigKey(WorkflowStatePersistenceViaSensors.class, "internals.brooklyn.workflow.sensor_persister");

    public static final AttributeSensor<Map<String,WorkflowExecutionContext>> INTERNAL_WORKFLOWS = Sensors.newSensor(new TypeToken<Map<String, WorkflowExecutionContext>>() {}, "internals.brooklyn.workflow");

    private static final long GLOBAL_UPDATE_FREQUENCY = 5*60*1000;  // every 5m wipe out old workflows

    public static WorkflowStatePersistenceViaSensors get(ManagementContext mgmt) {
        WorkflowStatePersistenceViaSensors sharedInstance = mgmt.getScratchpad().get(SENSOR_WORKFLOW_PERSISTER);
        if (sharedInstance==null) {
            synchronized (SENSOR_WORKFLOW_PERSISTER) {
                sharedInstance = mgmt.getScratchpad().get(SENSOR_WORKFLOW_PERSISTER);
                if (sharedInstance==null) {
                    sharedInstance = new WorkflowStatePersistenceViaSensors(mgmt);
                    mgmt.getScratchpad().put(SENSOR_WORKFLOW_PERSISTER, sharedInstance);
                }
            }
        }
        return sharedInstance;
    }

    private final ManagementContext mgmt;

    public WorkflowStatePersistenceViaSensors(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    enum PersistenceWithQueuedTasks { ALLOW, WARN, FAIL }

    long lastInMemClear = System.currentTimeMillis();

    public void checkpoint(WorkflowExecutionContext context) {
        checkpoint(context, PersistenceWithQueuedTasks.WARN);
    }
    public void checkpoint(WorkflowExecutionContext context, PersistenceWithQueuedTasks expectQueuedTasks) {
        doGlobalUpdateIfNeeded();

        Entity entity = context.getEntity();
        if (Entities.isUnmanagingOrNoLongerManaged(entity)) {
            log.debug("Skipping persistence of "+context+" as entity is no longer active here");
            return;
        }

        // keep active workflows in memory, even if disabled
        WorkflowStateActiveInMemory.get(context.getManagementContext()).checkpoint(context);

        if (Boolean.TRUE.equals(context.getRetentionSettings().disabled)) {
            if (getFromTag(BrooklynTaskTags.tagForWorkflow(context), false, false)!=null) {
                // need to clear
                updateMap(entity, false, true, v -> v.remove(context.getWorkflowId(), context));
            }
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

        expireOldWorkflows(entity, context);
    }

    private void doGlobalUpdateIfNeeded() {
        if (lastInMemClear + GLOBAL_UPDATE_FREQUENCY > System.currentTimeMillis()) return;
        lastInMemClear = System.currentTimeMillis();
        AtomicInteger total = new AtomicInteger(0);
        Collection<Entity> entities = mgmt.getEntityManager().getEntities();
        entities.forEach(entity -> {
            int change = expireOldWorkflows(entity, null);
            if (change!=0) log.debug("Global entity workflow persistence update, removed "+(-change)+" workflows from "+entity);
            total.addAndGet(change);
        });
        if (total.get()!=0) log.debug("Global entity workflow persistence update, removed "+(-total.get())+" workflows across all "+entities.size()+" entities");
    }

    public int expireOldWorkflows(Entity entity, @Nullable WorkflowExecutionContext context) {
        // clear interrupt status so we can persist e.g. if we are interrupted or shutdown
        boolean interrupted = Thread.interrupted();
        boolean doExpiry = WorkflowRetentionAndExpiration.isExpirationCheckNeeded(entity);
        try {
            return updateMaps(entity, doExpiry, true, context==null ? null : v -> v.put(context.getWorkflowId(), context), null);

        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public boolean deleteWorkflow(WorkflowExecutionContext w) {
        if (w.getStatus()==null || w.getStatus().expirable || w.getStatus()== WorkflowExecutionContext.WorkflowStatus.STAGED) {
            log.debug("Explicit request to delete workflow "+w);
            AtomicBoolean result = new AtomicBoolean(false);
            updateMaps(w.getEntity(), false, true, map -> {
                boolean removed = WorkflowRetentionAndExpiration.deleteWorkflowFromMap(map, w, true, true);
                if (removed) result.set(true);
            }, w);
            return result.get();
        } else {
            log.warn("Explicit request to delete non-expirable workflow "+w+"; ignoring");
            return false;
        }
    }

    int updateMaps(Entity entity, boolean doExpiry, boolean persist, Consumer<Map<String,WorkflowExecutionContext>> action, WorkflowExecutionContext contextToRemoveFromSoftMemory) {
        int result = updateMap(entity, doExpiry, persist, action);

        // and update softly kept
        WorkflowStateActiveInMemory activeInMemory = WorkflowStateActiveInMemory.get(mgmt);
        if (contextToRemoveFromSoftMemory!=null) {
            activeInMemory.deleteWorkflow(contextToRemoveFromSoftMemory);
        }
        if (doExpiry) activeInMemory.recomputeExpiration(entity);

        return result;
    }

    int updateMap(Entity entity, boolean doExpiry, boolean persist, Consumer<Map<String,WorkflowExecutionContext>> action) {
        AtomicInteger delta = new AtomicInteger(0);
        entity.sensors().modify(INTERNAL_WORKFLOWS, vo -> {
            Map<String, WorkflowExecutionContext> v = MutableMap.copyOf(vo);
            delta.set(-v.size());
            if (action!=null) action.accept(v);
            if (doExpiry) v = WorkflowRetentionAndExpiration.recomputeExpiration(v, null, false);
            delta.getAndAdd(v.size());
            return Maybe.of(v);
        });
        if (persist) mgmt.getRebindManager().forcePersistNow(false, null);
        return delta.get();
    }

    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        return getWorkflows(entity, true);
    }
    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity, boolean includeSoftlyKeptCompleted) {
        MutableMap<String, WorkflowExecutionContext> result = WorkflowStateActiveInMemory.get(mgmt).getWorkflowsCopy(entity, includeSoftlyKeptCompleted);
        result.add(entity.sensors().get(INTERNAL_WORKFLOWS));
        return result;
    }

    public void updateWithoutPersist(Entity entity, List<WorkflowExecutionContext> workflows) {
        if (workflows!=null && !workflows.isEmpty()) entity.sensors().modify(INTERNAL_WORKFLOWS, vo -> {
            if (vo == null) {
                throw new IllegalStateException("Update workflows requested for "+workflows+" when none recorded against "+entity);
            }
            Map<String, WorkflowExecutionContext> v = MutableMap.copyOf(vo);
            workflows.forEach(w -> v.put(w.getWorkflowId(), w));
            return Maybe.of(v);
        });
    }

    public Maybe<WorkflowExecutionContext> getFromTag(BrooklynTaskTags.WorkflowTaskTag tag) {
        return getFromTag(tag, true, true);
    }
    public Maybe<WorkflowExecutionContext> getFromTag(BrooklynTaskTags.WorkflowTaskTag tag, boolean includeSoftlyKeptInMemory) {
        return getFromTag(tag, true, includeSoftlyKeptInMemory);
    }

    private Maybe<WorkflowExecutionContext> getFromTag(BrooklynTaskTags.WorkflowTaskTag tag, boolean allowActiveInMemory, boolean allowActiveAndSoftlyKeptInMemory) {
        Entity targetEntity = mgmt.lookup(tag.getEntityId(), Entity.class);
        if (targetEntity==null) {
            return Maybe.absent("Entity "+tag.getWorkflowId()+" not found");
        } else {
            WorkflowExecutionContext w = null;

            if (allowActiveInMemory || allowActiveAndSoftlyKeptInMemory) w = WorkflowStateActiveInMemory.get(mgmt).getFromTag(tag, allowActiveAndSoftlyKeptInMemory);

            if (w==null) {
                w = new WorkflowStatePersistenceViaSensors(mgmt).getWorkflows(targetEntity).get(tag.getWorkflowId());
            }
            if (w == null) {
                // shouldn't happen unless workflow was expired, as workflow will be saved before resumption
                return Maybe.absent("Workflow "+tag.getWorkflowId()+" not found on entity "+targetEntity+"; possibly expired?");
            } else {
                return Maybe.of(w);
            }
        }
    }
}
