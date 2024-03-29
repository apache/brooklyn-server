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

import java.util.Map;
import java.util.Set;
import java.util.function.Function;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowStateActiveInMemory {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStateActiveInMemory.class);

    public static final ConfigKey<WorkflowStateActiveInMemory> IN_MEMORY_WORKFLOWS = ConfigKeys.newConfigKey(WorkflowStateActiveInMemory.class, "internals.brooklyn.workflow.in_memory");

    private static final long GLOBAL_UPDATE_FREQUENCY = 5*60*1000;  // every 5m wipe out workflows from old entities

    public static WorkflowStateActiveInMemory get(ManagementContext mgmt) {
        WorkflowStateActiveInMemory localActiveWorkflows = mgmt.getScratchpad().get(IN_MEMORY_WORKFLOWS);
        if (localActiveWorkflows==null) {
            synchronized (IN_MEMORY_WORKFLOWS) {
                localActiveWorkflows = mgmt.getScratchpad().get(IN_MEMORY_WORKFLOWS);
                if (localActiveWorkflows==null) {
                    localActiveWorkflows = new WorkflowStateActiveInMemory(mgmt);
                    mgmt.getScratchpad().put(IN_MEMORY_WORKFLOWS, localActiveWorkflows);
                }
            }
        }
        return localActiveWorkflows;
    }

    private final ManagementContext mgmt;
    // active workflows by entity then workflow id
    final Map<String,Map<String,WorkflowExecutionContext>> active = MutableMap.of();
    // cached remembered workflows by entity then workflow id
    final Map<String, Cache<String, WorkflowExecutionContext>> completedSoftlyKept = MutableMap.of();

    // created and managed by mgmt context scratchpad
    protected WorkflowStateActiveInMemory(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    long lastInMemClear = System.currentTimeMillis();

    public void expireAbsentEntities() {
        lastInMemClear = System.currentTimeMillis();
        Set<String> copy;
        synchronized (active) { copy = MutableSet.copyOf(active.keySet()); }
        synchronized (completedSoftlyKept) { copy.addAll(completedSoftlyKept.keySet()); }

        copy.forEach(entityId -> {
            if (mgmt.getEntityManager().getEntity(entityId) == null) {
                synchronized (active) { active.remove(entityId); }
                synchronized (completedSoftlyKept) { completedSoftlyKept.remove(entityId); }
            }
        });
    }

    public void checkpoint(WorkflowExecutionContext context) {
        if (context.getStatus().expirable) {
            withActiveForEntity(context.getEntity().getId(), false, wfm -> wfm.remove(context.getWorkflowId()));
            withSoftlyKeptForEntity(context.getEntity().getId(), true, wfm -> { wfm.put(context.getWorkflowId(), context); return null; });
        } else {
            // keep active workflows in memory, even if disabled
            withActiveForEntity(context.getEntity().getId(), true, wfm -> wfm.put(context.getWorkflowId(), context));
        }
        if (lastInMemClear + GLOBAL_UPDATE_FREQUENCY < System.currentTimeMillis()) {
            // poor man's cleanup, every minute, but good enough
            expireAbsentEntities();
        }
    }

    /** @deprecated since 1.1 returns a _copy_; use the method which makes that explicit */
    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        return getWorkflowsCopy(entity);
    }
    public MutableMap<String,WorkflowExecutionContext> getWorkflowsCopy(Entity entity) {
        return getWorkflowsCopy(entity, true);
    }
    public MutableMap<String,WorkflowExecutionContext> getWorkflowsCopy(Entity entity, boolean includeSoftlyKeptCompleted) {
        MutableMap<String,WorkflowExecutionContext> result = MutableMap.of();
        withActiveForEntity(entity.getId(), false, wfm -> { result.putAll(wfm); return null; });
        if (includeSoftlyKeptCompleted) withSoftlyKeptForEntity(entity.getId(), false, wfm -> { result.putAll(wfm.asMap()); return null; });
        return result;
    }

    boolean deleteWorkflow(WorkflowExecutionContext context) {
        boolean result = false;
        result = Boolean.TRUE.equals(withActiveForEntity(context.getEntity().getId(), false, wfm -> wfm.remove(context.getWorkflowId())!=null)) || result;
        result = Boolean.TRUE.equals(withSoftlyKeptForEntity(context.getEntity().getId(), false, wfm -> {
            WorkflowExecutionContext soft = wfm.getIfPresent(context.getWorkflowId());
            wfm.invalidate(context.getWorkflowId());
            return (soft!=null);
        })) || result;

        return result;
    }

    private <T> T withActiveForEntity(String entityId, boolean upsert, Function<Map<String, WorkflowExecutionContext>,T> fn) {
        if (entityId==null) return null;
        Map<String, WorkflowExecutionContext> result;
        synchronized (active) {
            result = active.computeIfAbsent(entityId, _key -> upsert ? MutableMap.of() : null);
        }
        if (result==null) return null;
        synchronized (result) {
            return fn.apply(result);
        }
    }

    private <T> T withSoftlyKeptForEntity(String entityId, boolean upsert, Function<Cache<String, WorkflowExecutionContext>,T> fn) {
        if (entityId==null) return null;
        Cache<String, WorkflowExecutionContext> result;
        synchronized (completedSoftlyKept) {
            result = completedSoftlyKept.computeIfAbsent(entityId, _key -> upsert ? CacheBuilder.newBuilder().softValues().build() : null);
        }
        if (result==null) return null;
        synchronized (result) {
            return fn.apply(result);
        }
    }

    public WorkflowExecutionContext getFromTag(BrooklynTaskTags.WorkflowTaskTag tag) {
        return getFromTag(tag, true);
    }
    public WorkflowExecutionContext getFromTag(BrooklynTaskTags.WorkflowTaskTag tag, boolean includeCompletedSoftlyKept) {
        WorkflowExecutionContext result = withActiveForEntity(tag.getEntityId(), false, wfm -> wfm.get(tag.getWorkflowId()));
        if (includeCompletedSoftlyKept && result==null) {
            result = withSoftlyKeptForEntity(tag.getEntityId(), false, wfm -> wfm.getIfPresent(tag.getWorkflowId()));
        }
        return result;
    }
}
