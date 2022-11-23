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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class WorkflowStateActiveInMemory {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStateActiveInMemory.class);

    public static final ConfigKey<WorkflowStateActiveInMemory> IN_MEMORY_WORKFLOWS = ConfigKeys.newConfigKey(WorkflowStateActiveInMemory.class, "internals.brooklyn.workflow.in_memory");

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
    final Map<String,Map<String,WorkflowExecutionContext>> data = MutableMap.of();

    public WorkflowStateActiveInMemory(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    long lastInMemClear = System.currentTimeMillis();

    public void expireAbsentEntities() {
        lastInMemClear = System.currentTimeMillis();
        MutableMap.copyOf(data).forEach( (entityId, mapByWorkflowId) -> {
            if (mgmt.getEntityManager().getEntity(entityId)==null) data.remove(entityId);
        });
    }

    public void checkpoint(WorkflowExecutionContext context) {
        // keep active workflows in memory, even if disabled
        Map<String, WorkflowExecutionContext> entityActiveWorkflows = data.get(context.getEntity().getId());
        if (context.getStatus().expirable) {
            if (entityActiveWorkflows!=null) entityActiveWorkflows.remove(context.getWorkflowId());
        } else {
            if (entityActiveWorkflows==null) {
                synchronized (data) {
                    entityActiveWorkflows = data.get(context.getEntity().getId());
                    if (entityActiveWorkflows==null) {
                        entityActiveWorkflows = MutableMap.of();
                        data.put(context.getEntity().getId(), entityActiveWorkflows);
                    }
                }
            }
            entityActiveWorkflows.put(context.getWorkflowId(), context);
        }

        if (lastInMemClear + 60*1000 < System.currentTimeMillis()) {
            // poor man's cleanup, every minute, but good enough
            expireAbsentEntities();
        }
    }

    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        return MutableMap.copyOf(data.get(entity.getId()));
    }

    public WorkflowExecutionContext getFromTag(BrooklynTaskTags.WorkflowTaskTag tag) {
        Map<String, WorkflowExecutionContext> activeForEntity = data.get(tag.getEntityId());
        if (activeForEntity!=null) {
            return activeForEntity.get(tag.getWorkflowId());
        }
        return null;
    }
}
