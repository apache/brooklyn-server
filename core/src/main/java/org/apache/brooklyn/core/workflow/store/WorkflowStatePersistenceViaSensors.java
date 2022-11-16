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
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowErrorHandling;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class WorkflowStatePersistenceViaSensors {

    private static final Logger log = LoggerFactory.getLogger(WorkflowStatePersistenceViaSensors.class);

    public static final AttributeSensor<Map<String,WorkflowExecutionContext>> INTERNAL_WORKFLOWS = Sensors.newSensor(new TypeToken<Map<String, WorkflowExecutionContext>>() {}, "internals.brooklyn.workflow");


    private final ManagementContext mgmt;

    public WorkflowStatePersistenceViaSensors(ManagementContext mgmt) {
        this.mgmt = mgmt;
    }

    static int MAX_TO_KEEP_PER_KEY = 20;  // 3 would be fine, apart from tests; when it waits on parent being finished we can get rid of

    public void checkpoint(WorkflowExecutionContext context) {
        if (DynamicTasks.getTaskQueuingContext()!=null) {
            if (DynamicTasks.getTaskQueuingContext().getQueue().stream().filter(t -> !t.isDone()).findAny().isPresent()) {
                log.warn("Persisting when there are still queued tasks");
            }
        }
        // clear interrupt status so we can persist e.g. if we are interrupted or shutdown
        boolean interrupted = Thread.interrupted();
        try {
            Entity entity = context.getEntity();
            entity.sensors().modify(INTERNAL_WORKFLOWS, v -> {
                if (v == null) v = MutableMap.of();
                v.put(context.getWorkflowId(), context);

                boolean doExpiry = true;
                if (doExpiry && Tasks.isAncestor(Tasks.current(), t -> BrooklynTaskTags.getTagsFast(t).contains(BrooklynTaskTags.ENTITY_INITIALIZATION))) {
                    // skip expiry during initialization
                    doExpiry = false;
                }
                if (doExpiry && Entities.isUnmanagingOrNoLongerManaged(entity)) {
                    // skip expiry during shutdown
                    doExpiry = false;
                }

                if (doExpiry) {
                    String k = Strings.firstNonBlank(context.getExpiryKey(), "empty-expiry-key");  //should always be set
                    // TODO follow expiry instructions; for now, just keep N latest, apart from this one
                    List<WorkflowExecutionContext> finishedTwins = v.values().stream()
                            .filter(c -> k.equals(c.getExpiryKey()))
                            .filter(c -> c.getStatus() != null && c.getStatus().ended)
                            // TODO don't expire if parentTag points to workflow which is known and active
                            .filter(c -> !c.equals(context))
                            .collect(Collectors.toList());
                    if (finishedTwins.size() > MAX_TO_KEEP_PER_KEY) {
                        finishedTwins = MutableList.copyOf(finishedTwins);
                        Collections.sort(finishedTwins, (t1, t2) -> Long.compare(t2.getMostRecentActivityTime(), t1.getMostRecentActivityTime()));
                        Iterator<WorkflowExecutionContext> ti = finishedTwins.iterator();
                        for (int i = 0; i < MAX_TO_KEEP_PER_KEY; i++) ti.next();
                        while (ti.hasNext()) {
                            WorkflowExecutionContext w = ti.next();
                            log.debug("Expiring old workflow " + w + " because it is finished and there are newer ones");
                            v.remove(w.getWorkflowId());
                        }
                    }
                }
                return Maybe.of(v);
            });
            mgmt.getRebindManager().forcePersistNow(false, null);

        } finally {
            if (interrupted) Thread.currentThread().interrupt();
        }
    }

    public Map<String,WorkflowExecutionContext> getWorkflows(Entity entity) {
        Map<String, WorkflowExecutionContext> result = entity.sensors().get(INTERNAL_WORKFLOWS);
        if (result==null) result = ImmutableMap.of();
        return ImmutableMap.copyOf(result);
    }

    public void expireOldWorkflows(Entity entity) {
        // TODO
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
