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
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class WorkflowRetentionAndExpiration {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRetentionAndExpiration.class);

    public static class RetentionInstruction {
        public String hash;
    }

    // TODO temporary for testing; note, some tests might require 20, until parents being finished is checked
    static int MAX_TO_KEEP_PER_HASH = 20;  // 3 would be fine, apart from tests; when it waits on parent being finished we can get rid of

    static Map<String, WorkflowExecutionContext> recomputeExpiration(Map<String, WorkflowExecutionContext> v, WorkflowExecutionContext optionalContext) {
        Set<String> workflowHashesToUpdate = optionalContext!=null ? MutableSet.of(Strings.firstNonBlank(optionalContext.getRetentionHash(), "empty-expiry-hash"))  //should always be set
            : v.values().stream().map(WorkflowExecutionContext::getRetentionHash).collect(Collectors.toSet());

        workflowHashesToUpdate.forEach(k -> {
            List<WorkflowExecutionContext> finishedTwins = v.values().stream()
                    .filter(c -> k.equals(c.getRetentionHash()))
                    .filter(c -> c.getStatus() != null && c.getStatus().expirable)
                    // TODO don't expire if parentTag points to workflow which is known and active
                    .filter(c -> !c.equals(optionalContext))
                    .collect(Collectors.toList());
            // TODO follow expiry instructions; for now, just keep N latest, apart from this one
            if (finishedTwins.size() > MAX_TO_KEEP_PER_HASH) {
                finishedTwins = MutableList.copyOf(finishedTwins);
                Collections.sort(finishedTwins, (t1, t2) -> Long.compare(t2.getMostRecentActivityTime(), t1.getMostRecentActivityTime()));
                Iterator<WorkflowExecutionContext> ti = finishedTwins.iterator();
                for (int i = 0; i < MAX_TO_KEEP_PER_HASH; i++) ti.next();
                while (ti.hasNext()) {
                    WorkflowExecutionContext w = ti.next();
                    log.debug("Expiring old workflow " + w + " because it is finished and there are newer ones");
                    v.remove(w.getWorkflowId());
                }
            }
        });

        return v;
    }

    static boolean isExpirationCheckNeeded(Entity entity) {
        if (Tasks.isAncestor(Tasks.current(), t -> BrooklynTaskTags.getTagsFast(t).contains(BrooklynTaskTags.ENTITY_INITIALIZATION))) {
            // skip expiry during initialization
            return false;
        }
        if (Entities.isUnmanagingOrNoLongerManaged(entity)) {
            // skip expiry during shutdown
            return false;
        }
        return true;
    }


    public static void expireOldWorkflows(Entity entity) {
        new WorkflowStatePersistenceViaSensors(((EntityInternal)entity).getManagementContext()).updateMap(entity, true, true, null);
    }
}
