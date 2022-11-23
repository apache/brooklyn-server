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

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.utils.WorkflowRetentionParser;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class WorkflowRetentionAndExpiration {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRetentionAndExpiration.class);

    public static final ConfigKey<String> WORKFLOW_RETENTION_DEFAULT = ConfigKeys.newStringConfigKey("workflow.retention.default",
            "Default retention for workflows", "3");

    public static class WorkflowRetentionSettings {
        public Boolean disabled;
        public String hash;
        public String expiry;
        public String expiryResolved;

        @JsonIgnore
        private transient WorkflowRetentionParser.WorkflowRetentionFilter expiryFn;

        public WorkflowRetentionParser.WorkflowRetentionFilter getExpiryFn(WorkflowExecutionContext w) {
            return init(w).expiryFn;
        }
        public WorkflowRetentionSettings init(WorkflowExecutionContext w) {
            if (w.getParent()!=null && Boolean.TRUE.equals(w.getParent().getRetentionSettings().disabled)) {
                disabled = true;

            } else if (expiryFn==null) {
                expiryResolved = expiryResolved!=null ? expiryResolved : expiry;
                expiryFn = new WorkflowRetentionParser(expiryResolved).parse();
                if (w != null) {
                    Set<String> set = INIT_REENTRANT.get();
                    if (set==null) {
                        set = MutableSet.of();
                        INIT_REENTRANT.set(set);
                    }
                    if (!set.add(w.getWorkflowId()+":"+expiryResolved)) {
                        // double-check we don't cause endless loops; see KeepContext notes
                        throw new IllegalStateException("Invalid workflow retention '"+expiryResolved+"' as it refers to itself");
                    }
                    try {
                        expiryFn = expiryFn.init(w);
                    } finally {
                        set.remove(w.getWorkflowId()+":"+expiryResolved);
                        if (set.isEmpty()) INIT_REENTRANT.remove();
                    }
                }
                expiryResolved = expiryFn.toString();  // remove any references to `context` that might trigger an infinite loop
            }
            return this;
        }

        public void updateFrom(WorkflowRetentionSettings r2) {
            if (Strings.isNonBlank(r2.hash)) this.hash = r2.hash;
            this.disabled = Boolean.TRUE.equals(r2.disabled) ? true : null;
            if (Strings.isNonEmpty(r2.expiry)) {
                this.expiry = r2.expiry;
                this.expiryFn = r2.expiryFn;
                this.expiryResolved = r2.expiryResolved;
            }
        }
    }

    static ThreadLocal<Set<String>> INIT_REENTRANT = new ThreadLocal<Set<String>>();

    static Map<String, WorkflowExecutionContext> recomputeExpiration(Map<String, WorkflowExecutionContext> v, WorkflowExecutionContext optionalContext) {
        Set<String> workflowHashesToUpdate = optionalContext!=null ? MutableSet.of(Strings.firstNonBlank(optionalContext.getRetentionHash(), "empty-expiry-hash"))  //should always be set
            : v.values().stream().map(WorkflowExecutionContext::getRetentionHash).collect(Collectors.toSet());

        workflowHashesToUpdate.forEach(k -> {
            List<WorkflowExecutionContext> finishedTwins = v.values().stream()
                    .filter(c -> k.equals(c.getRetentionHash()))
                    .filter(c -> isExpirable(c))
                    .filter(c -> !c.equals(optionalContext))
                    .collect(Collectors.toList());

            if (finishedTwins.isEmpty()) return;

            Optional<WorkflowExecutionContext> existingRetentionExpiry = finishedTwins.stream().filter(w -> w.getRetentionSettings().expiry != null).findAny();
            WorkflowRetentionParser.WorkflowRetentionFilter expiry;
            if (existingRetentionExpiry.isPresent()) {
                // log if expiry fn differs for the same hash
                // (but note if it refers to parents, invocations from different places could result in different expiry functions)
                if (optionalContext!=null && optionalContext.getRetentionHash().equals(k)) {
                    if (optionalContext.getRetentionSettings().expiry != null) {
                        if (!optionalContext.getRetentionSettings().expiry.equals(existingRetentionExpiry.get().getRetentionSettings().expiry)) {
                            log.warn("Retention specification for " + optionalContext + " '" + optionalContext.getRetentionSettings().expiry + "' is different for same hash. " +
                                    "Expiry should be constant within a hash but " + existingRetentionExpiry.get() + " has '" + existingRetentionExpiry.get().getRetentionSettings().expiry + "'");
                        }
                    }
                }
                expiry = existingRetentionExpiry.get().getRetentionSettings().getExpiryFn(existingRetentionExpiry.get());
            } else {
                expiry = WorkflowRetentionParser.newDefaultFilter().init(finishedTwins.iterator().next());
            }

            Collection<WorkflowExecutionContext> retainedFinishedTwins = expiry.apply(finishedTwins);

            if (retainedFinishedTwins.size() < finishedTwins.size()) {
                MutableSet<WorkflowExecutionContext> toRemove = MutableSet.copyOf(finishedTwins);
                toRemove.removeAll(retainedFinishedTwins);
                toRemove.forEach(w -> {
                    log.debug("Expiring old workflow " + w + " as there are "+retainedFinishedTwins.size()+" more recent ones also completed");
                    v.remove(w.getWorkflowId());
                });
            }
        });

        return v;
    }

    private static boolean isExpirable(WorkflowExecutionContext c) {
        if (c.getStatus() == null || !c.getStatus().expirable) return false;
        if (c.getParent()!=null) {
            // fow now, don't expire children workflows unless parents are also expirable
            if (!isExpirable(c.getParent())) return false;

            // we could weaken this if we have lots of children workflows, but that is more work; left as an enhancement
        }
        return true;
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
