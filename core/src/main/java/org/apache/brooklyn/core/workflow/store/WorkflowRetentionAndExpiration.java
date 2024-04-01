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

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors.PersistenceWithQueuedTasks;
import org.apache.brooklyn.core.workflow.utils.WorkflowRetentionParser;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WorkflowRetentionAndExpiration {

    private static final Logger log = LoggerFactory.getLogger(WorkflowRetentionAndExpiration.class);

    public static final ConfigKey<String> WORKFLOW_RETENTION_DEFAULT = ConfigKeys.newStringConfigKey("workflow.retention.default",
            "Default retention for workflows (persisted)", "3");
    public static final ConfigKey<String> WORKFLOW_RETENTION_DEFAULT_SOFT = ConfigKeys.newStringConfigKey("workflow.retention.default.soft",
            "Default soft retention for workflows (in-memory)", "3");

    public static void checkpoint(ManagementContext mgmt, WorkflowExecutionContext context) {
        Entity entity = context.getEntity();
        if (Entities.isUnmanagingOrNoLongerManaged(entity)) {
            log.debug("Skipping persistence of "+context+" as entity is no longer active here");
            return;
        }

        doGlobalUpdateIfNeededOnDiskAndInMemory(mgmt);

        new WorkflowStatePersistenceViaSensors(mgmt).checkpoint(context, PersistenceWithQueuedTasks.WARN);

        // keep active workflows in memory, even if disabled
        WorkflowStateActiveInMemory.get(context.getManagementContext()).checkpoint(context);
    }

    static final long GLOBAL_UPDATE_FREQUENCY = 5*60*1000;  // every 5m wipe out old workflows

    static void doGlobalUpdateIfNeededOnDiskAndInMemory(ManagementContext mgmt) {
        WorkflowStateActiveInMemory inMem = WorkflowStateActiveInMemory.get(mgmt);

        if (inMem.lastGlobalClear + GLOBAL_UPDATE_FREQUENCY > System.currentTimeMillis()) return;
        inMem.lastGlobalClear = System.currentTimeMillis();

        AtomicInteger total = new AtomicInteger(0);
        Collection<Entity> entities = mgmt.getEntityManager().getEntities();
        entities.forEach(entity -> {
            // on disk
            int change = new WorkflowStatePersistenceViaSensors(mgmt).expireOldWorkflowsOnDisk(entity, null);
            if (change!=0) log.debug("Global entity workflow persistence update, removed "+(-change)+" workflows from "+entity);
            total.addAndGet(change);

            // in memory
            inMem.recomputeExpiration(entity, null);
        });
        if (total.get()!=0) log.debug("Global entity workflow persistence update, removed "+(-total.get())+" workflows across all "+entities.size()+" entities");
    }

    @JsonInclude(JsonInclude.Include.NON_EMPTY)
    public static class WorkflowRetentionSettings {
        public Boolean disabled;
        public String hash;

        public String expiry;
        public String expiryResolved;

        // soft expiry refers to what is kept in memory
        public String softExpiry;
        public String softExpiryResolved;

        @JsonIgnore
        private transient WorkflowRetentionParser.WorkflowRetentionFilter expiryFn;
        @JsonIgnore
        private transient WorkflowRetentionParser.WorkflowRetentionFilter softExpiryFn;

        public WorkflowRetentionParser.WorkflowRetentionFilter getExpiryFn(WorkflowExecutionContext w) {
            return init(w).expiryFn;
        }
        public WorkflowRetentionParser.WorkflowRetentionFilter getSoftExpiryFn(WorkflowExecutionContext w) {
            return init(w).softExpiryFn;
        }
        public WorkflowRetentionSettings init(WorkflowExecutionContext w) {
            if (w.getParent()!=null && Boolean.TRUE.equals(w.getParent().getRetentionSettings().disabled)) {
                disabled = true;

            } else {
                if (expiryFn == null) {
                    expiryResolved = expiryResolved != null ? expiryResolved : expiry;
                    expiryFn = new WorkflowRetentionParser(expiryResolved).parse();
                    if (w != null) {
                        Set<String> set = INIT_REENTRANT.get();
                        if (set == null) {
                            set = MutableSet.of();
                            INIT_REENTRANT.set(set);
                        }
                        if (!set.add(w.getWorkflowId() + ":" + expiryResolved)) {
                            // double-check we don't cause endless loops; see KeepContext notes
                            throw new IllegalStateException("Invalid workflow retention '" + expiryResolved + "' as it refers to itself");
                        }
                        try {
                            expiryFn = expiryFn.init(w);
                        } finally {
                            set.remove(w.getWorkflowId() + ":" + expiryResolved);
                            if (set.isEmpty()) INIT_REENTRANT.remove();
                        }
                    }
                    expiryResolved = expiryFn.toString();  // remove any references to `context` that might trigger an infinite loop
                }
                if (softExpiryFn == null) {
                    softExpiryResolved = softExpiryResolved != null ? softExpiryResolved : softExpiry;
                    softExpiryFn = new WorkflowRetentionParser(softExpiryResolved).soft().parse();
                    if (w != null) {
                        Set<String> set = INIT_REENTRANT.get();
                        if (set == null) {
                            set = MutableSet.of();
                            INIT_REENTRANT.set(set);
                        }
                        if (!set.add(w.getWorkflowId() + ":" + softExpiryResolved)) {
                            // double-check we don't cause endless loops; see KeepContext notes
                            throw new IllegalStateException("Invalid workflow retention '" + softExpiryResolved + "' as it refers to itself");
                        }
                        try {
                            softExpiryFn = softExpiryFn.init(w);
                        } finally {
                            set.remove(w.getWorkflowId() + ":" + softExpiryResolved);
                            if (set.isEmpty()) INIT_REENTRANT.remove();
                        }
                    }
                    softExpiryResolved = softExpiryFn.toString();  // remove any references to `context` that might trigger an infinite loop
                }
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
            if (Strings.isNonEmpty(r2.softExpiry)) {
                this.softExpiry = r2.softExpiry;
                this.softExpiryFn = r2.softExpiryFn;
                this.softExpiryResolved = r2.softExpiryResolved;
            }
        }
    }

    static ThreadLocal<Set<String>> INIT_REENTRANT = new ThreadLocal<Set<String>>();

    static Map<String, WorkflowExecutionContext> recomputeExpiration(Map<String, WorkflowExecutionContext> v, @Nullable WorkflowExecutionContext optionalContext, boolean useSoftlyKeptExpiry) {
        Set<String> workflowHashesToUpdate = optionalContext!=null ? MutableSet.of(Strings.firstNonBlank(optionalContext.getRetentionHash(), "empty-expiry-hash"))  //should always be set
            : v.values().stream().map(WorkflowExecutionContext::getRetentionHash).collect(Collectors.toSet());

        workflowHashesToUpdate.forEach(k -> {
            // if optional context supplied, perhaps only recompute for that hash
            if (optionalContext!=null && !k.equals(optionalContext.getRetentionHash())) {
                if (!isExpirable(optionalContext)) {
                    return;
                } else {
                    // no-op -- if it is expirable, do a full recompute for the entity, to ensure sub-workflows are no longer retained
                    // (cross-entity subworkflows will not be cleaned up; they will get collected when another workflow runs there,
                    // or when there is a global cleanup event)
                }
            }

            List<WorkflowExecutionContext> finishedTwins = v.values().stream()
                    .filter(c -> k.equals(c.getRetentionHash()))
                    .filter(c -> isExpirable(c))
                    .collect(Collectors.toList());

            if (finishedTwins.isEmpty()) return;

            Function<WorkflowRetentionSettings,String> expiryAccessor = useSoftlyKeptExpiry ? wrs -> wrs.softExpiry : wrs -> wrs.expiry;
            Optional<WorkflowExecutionContext> existingRetentionExpiry;
            if (optionalContext!=null && k.equals(optionalContext.getRetentionHash()) && expiryAccessor.apply(optionalContext.getRetentionSettings())!=null)
                existingRetentionExpiry = Optional.of(optionalContext);
            else
                existingRetentionExpiry = finishedTwins.stream().filter(w -> expiryAccessor.apply(w.getRetentionSettings()) != null).findAny();

            WorkflowRetentionParser.WorkflowRetentionFilter expiry;

            if (existingRetentionExpiry.isPresent()) {
                // log if expiry fn differs for the same hash
                // (but note if it refers to parents, invocations from different places could result in different expiry functions)
                // (no such warning for the soft side of it)
                if (useSoftlyKeptExpiry) {
                    expiry = existingRetentionExpiry.get().getRetentionSettings().getSoftExpiryFn(existingRetentionExpiry.get());
                } else {
                    if (optionalContext != null && optionalContext.getRetentionHash().equals(k)) {
                        if (optionalContext.getRetentionSettings().expiry != null) {
                            if (!optionalContext.getRetentionSettings().expiry.equals(existingRetentionExpiry.get().getRetentionSettings().expiry)) {
                                log.warn("Retention specification for " + optionalContext + " '" + optionalContext.getRetentionSettings().expiry + "' is different for same hash. " +
                                        "Expiry should be constant within a hash but " + existingRetentionExpiry.get() + " has '" + existingRetentionExpiry.get().getRetentionSettings().expiry + "'");
                            }
                        }
                    }
                    expiry = existingRetentionExpiry.get().getRetentionSettings().getExpiryFn(existingRetentionExpiry.get());
                }
            } else {
                expiry = WorkflowRetentionParser.newDefaultFilter(useSoftlyKeptExpiry).init(finishedTwins.iterator().next());
            }

            Collection<WorkflowExecutionContext> retainedFinishedTwins = expiry.apply(finishedTwins);

            if (retainedFinishedTwins.size() < finishedTwins.size()) {
                MutableSet<WorkflowExecutionContext> toRemove = MutableSet.copyOf(finishedTwins);
                toRemove.removeAll(retainedFinishedTwins);
                toRemove.forEach(w -> {
                    log.debug("Expiring old workflow " + w + " as there are "+retainedFinishedTwins.size()+" more recent ones also completed");
                    deleteWorkflowFromMap(v, w, true, false);
                });
            }
        });

        return v;
    }

    static boolean deleteWorkflowFromMap(Map<String, WorkflowExecutionContext> v, WorkflowExecutionContext w, boolean andAllReplayTasks, boolean andSoftlyKept) {
        boolean removed = v.remove(w.getWorkflowId()) != null;
        if (andSoftlyKept) removed = WorkflowStateActiveInMemory.get(w.getManagementContext()).deleteWorkflow(w) || removed;
        if (andAllReplayTasks) {
            BasicExecutionManager em = ((BasicExecutionManager) w.getManagementContext().getExecutionManager());
            w.getReplays().forEach(wr -> {
                Task<?> wrt = em.getTask(wr.getTaskId());
                if (wrt != null) em.deleteTask(wrt, false, true);
            });
        }
        return removed;
    }

    private static boolean isExpirable(WorkflowExecutionContext c) {
        if (c.getStatus() == null || !c.getStatus().expirable) return false;

        // should we expire of completed children workflows even if an ancestor workflow is not expirable?
        // this would prevent silly retention of workflows whose parents are about to end, where the retention check runs on the child just before the parent finishes;
        // however it would also limit the ability to inspect children workflows e.g. in a foreach block, as only e.g. 3 of the children would be kept ever.
        // on balance, do NOT expire those; wait for another event to trigger their clean-up.
        // (if the child workflow is marked disabled, it is not persisted, but takes effect in all other cases.)
        if (c.getParent()!=null) {
            if (!isExpirable(c.getParent())) return false;
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
        new WorkflowStatePersistenceViaSensors(((EntityInternal)entity).getManagementContext()).updateMaps(entity, null, true, true, true, null, null);
    }
}
