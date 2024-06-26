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
package org.apache.brooklyn.core.mgmt.internal;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionContext;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementManager;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.AttributeSensor.SensorPersistenceMode;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.EntityAndItem;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements.StringAndArgument;
import org.apache.brooklyn.core.mgmt.internal.NonDeploymentManagementContext.NonDeploymentManagementContextMode;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.workflow.DanglingWorkflowException;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;

/**
 * Encapsulates management activities at an entity.
 * <p>
 * On entity deployment, ManagementContext.manage(entity) causes
 * <p>
 * * onManagementStarting(ManagementContext)
 * * onManagementStartingSubscriptions()
 * * onManagementStartingSensorEmissions()
 * * onManagementStartingExecutions()
 * * onManagementStarted() - when all the above is said and done
 * * onManagementStartingHere();
 * <p>
 * on unmanage it hits onManagementStoppingHere() then onManagementStopping().
 * <p>
 * When an entity's management migrates, it invokes onManagementStoppingHere() at the old location,
 * then onManagementStartingHere() at the new location.
 */
public class EntityManagementSupport {

    private static final Logger log = LoggerFactory.getLogger(EntityManagementSupport.class);
    
    public EntityManagementSupport(AbstractEntity entity) {
        this.entity = entity;
        nonDeploymentManagementContext = new NonDeploymentManagementContext(entity, NonDeploymentManagementContextMode.PRE_MANAGEMENT);
    }

    @VisibleForTesting
    public static boolean AUTO_FAIL_AND_RESUME_WORKFLOWS = true;

    protected transient AbstractEntity entity;
    NonDeploymentManagementContext nonDeploymentManagementContext;
    
    protected transient ManagementContext initialManagementContext;
    protected transient ManagementContext managementContext;
    protected transient volatile SubscriptionContext subscriptionContext;
    protected transient volatile ExecutionContext executionContext;
    
    protected final AtomicBoolean managementContextUsable = new AtomicBoolean(false);
    protected final AtomicBoolean currentlyStopping = new AtomicBoolean(false);
    protected final AtomicBoolean currentlyDeployed = new AtomicBoolean(false);
    protected final AtomicBoolean everDeployed = new AtomicBoolean(false);
    protected Boolean readOnly = null;
    protected final AtomicBoolean managementFailed = new AtomicBoolean(false);
    
    private volatile EntityChangeListener entityChangeListener = EntityChangeListener.NOOP;

    /**
     * Whether this entity is managed (i.e. "onManagementStarting" has been called, so the framework knows about it,
     * and it has not been unmanaged).
     */
    public boolean isDeployed() {
        return currentlyDeployed.get();
    }

    /**
     * Use this instead of negating {@link Entities#isManaged(Entity)} to avoid skipping publishing of values that to be published before the entity is deployed;
     * or (better) see {@link Entities#isManagedActiveOrComingUp(Entity)}
     */
    public boolean isNoLongerManaged() {
        return wasDeployed() && !isDeployed();
    }

    public boolean isActive() { return !isUnmanaging() && isDeployed(); }

    public boolean isUnmanaging() { return currentlyStopping.get(); }

    /** whether entity has ever been deployed (managed) */
    public boolean wasDeployed() {
        return everDeployed.get();
    }
    
    @Beta
    public void setReadOnly(boolean isReadOnly) {
        if (isDeployed())
            throw new IllegalStateException("Cannot set read only after deployment");
        this.readOnly = isReadOnly;
    }

    /** Whether the entity and its adjuncts should be treated as read-only;
     * may be null briefly when initializing if RO status is unknown. */
    @Beta
    public Boolean isReadOnlyRaw() {
        return readOnly;
    }

    /** Whether the entity and its adjuncts should be treated as read-only;
     * error if initializing and RO status is unknown. */
    @Beta
    public boolean isReadOnly() {
        Preconditions.checkNotNull(readOnly, "Read-only status of %s not yet known", entity);
        return readOnly;
    }

    /**
     * Whether the entity's management lifecycle is complete (i.e. both "onManagementStarting" and "onManagementStarted" have
     * been called, and it is has not been unmanaged). 
     */
    public boolean isFullyManaged() {
        return (nonDeploymentManagementContext == null) && currentlyDeployed.get();
    }

    public synchronized void setManagementContext(ManagementContextInternal val) {
        if (initialManagementContext != null) {
            throw new IllegalStateException("Initial management context is already set for "+entity+"; cannot change");
        }
        if (managementContext != null && !managementContext.equals(val)) {
            throw new IllegalStateException("Management context is already set for "+entity+"; cannot change");
        }
        
        this.initialManagementContext = checkNotNull(val, "managementContext");
        if (nonDeploymentManagementContext != null) {
            nonDeploymentManagementContext.setManagementContext(val);
        }
    }
    
    public void onRebind(ManagementTransitionInfo info) {
        nonDeploymentManagementContext.setMode(NonDeploymentManagementContextMode.MANAGEMENT_REBINDING);
    }
    
    public void onManagementStarting(ManagementTransitionInfo info) {
        info.getManagementContext().getExecutionContext(entity).get( Tasks.builder().displayName("Management starting")
            .dynamic(false)
            .tag(BrooklynTaskTags.ENTITY_INITIALIZATION)
//            .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
            .body(() -> {
                try {
                    synchronized (this) {
                        boolean alreadyManaging = isDeployed();

                        if (alreadyManaging) {
                            log.warn("Already managed: "+entity+" ("+nonDeploymentManagementContext+"); onManagementStarting is no-op");
                        } else if (nonDeploymentManagementContext == null || !nonDeploymentManagementContext.getMode().isPreManaged()) {
                            throw new IllegalStateException("Not in expected pre-managed state: "+entity+" ("+nonDeploymentManagementContext+")");
                        }
                        if (managementContext != null && !managementContext.equals(info.getManagementContext())) {
                            throw new IllegalStateException("Already has management context: "+managementContext+"; can't set "+info.getManagementContext());
                        }
                        if (initialManagementContext != null && !initialManagementContext.equals(info.getManagementContext())) {
                            throw new IllegalStateException("Already has different initial management context: "+initialManagementContext+"; can't set "+info.getManagementContext());
                        }
                        if (alreadyManaging) {
                            return;
                        }

                        if (AUTO_FAIL_AND_RESUME_WORKFLOWS) {
                            if (!info.getMode().isCreating()) {
                                // mark interrupted workflows as failed; will compensate shortly (below)
                                // only done if not creating to avoid cancelling workflows launched by EntityInitializer (there won't be any to mark as interrupted if we are creating a new entity)
                                WorkflowStatePersistenceViaSensors persister = new WorkflowStatePersistenceViaSensors(info.getManagementContext());
                                Map<String, WorkflowExecutionContext> workflows = persister.getWorkflows(entity);
                                List<WorkflowExecutionContext> wasRunningWorkflows = workflows.values().stream().filter(w -> !w.getStatus().ended).collect(Collectors.toList());
                                if (!wasRunningWorkflows.isEmpty()) {
                                    log.debug("Discovered workflows noted as 'running' on startup at "+entity+", will mark as interrupted: "+wasRunningWorkflows);
                                    entity.getExecutionContext().submit("Marking " + wasRunningWorkflows.size() + " interrupted workflow" + (wasRunningWorkflows.size() != 1 ? "s" : "") + " as shutdown", () -> {
                                        wasRunningWorkflows.forEach(WorkflowExecutionContext::markShutdown);
                                        // not necessary as in memory, but good practice
                                        persister.updateWithoutPersist(entity, wasRunningWorkflows);
                                    }).get();
                                } else {
                                    log.debug("No workflows identified as interrupted");
                                }
                            }
                        }

                        this.managementContext = info.getManagementContext();
                        nonDeploymentManagementContext.setMode(NonDeploymentManagementContextMode.MANAGEMENT_STARTING);

                        // defer this until mgmt context started, so all other entities will be known, in case they are accessed in the tasks
//                        if (!isReadOnly()) {
//                            nonDeploymentManagementContext.getSubscriptionManager().setDelegate((AbstractSubscriptionManager) managementContext.getSubscriptionManager());
//                            nonDeploymentManagementContext.getSubscriptionManager().startDelegatingForSubscribing();
//                        }

                        managementContextUsable.set(true);
                        currentlyDeployed.set(true);
                        everDeployed.set(true);

                        entityChangeListener = new EntityChangeListenerImpl();
                    }

                    /*
                     * framework starting events:
                     *
                     * "creation" comes first, in InternalEntityFactory and optionally rebind, where the following is done:
                     *  - establish hierarchy (child, groups, etc; construction if necessary on rebind)
                     *  - set location
                     *  - set local config values
                     *  - set saved sensor values
                     *  - add adjuncts
                     *  - register subscriptions
                     *  - run EntityInitializer apply (creation only)
                     *
                     * we try to minimize assumptions about order but it is unavoidable for some things.
                     * tasks _can_ be run against the entity context almost immediately, with the execution context routed through this class,
                     * using the "initialManagementContext" if necessary, if it is before it has become the "managementContext".
                     * this is so that startup management activities (such as this task) are recorded and trackable.
                     * however for simplicity (as much as possible) this should be minimized, restricted to necessary startup tasks,
                     * and using other mechanisms -- eg subscriptions (which are queued), AutoStartEntityAdjunct#start elements,
                     * and Entity.onManagement{Starting,Started} -- which happen after the above, on "management",
                     * from LocalEntityManager.manage calling in to this class:
                     *
                     * - mark failed any previously active workflows
                     * - set the management support managementContext to the initial / real one (above);
                     *   this is when the entity is "managed" from the perspective of external viewers (ManagementContext.isManaged(entity) returns true)
                     * - "startDelegatingForSubscribing" (above) which triggers sensors etc,
                     * - call entity.onManagementStarting
                     * - start the AutoStart adjuncts (below; mainly for timer-based items, but possibly some subscriptions;
                     *   most of those have initial runs and conditions so not so important if they are run later.)
                     * - resume any failed workflows
                     *
                     * then when the above is finished recursively, entity.onManagementStarted runs.
                     */

                    if (!isReadOnly()) {
                        entity.onManagementStarting();

                        // start those policies etc which are labelled as auto-start
                        BiConsumer<String,Runnable> queueTask = (name, r) -> entity.getExecutionContext().submit(name, r);
                        entity.policies().forEach(adj -> { if (adj instanceof EntityAdjunct.AutoStartEntityAdjunct)
                            queueTask.accept("Start policy "+adj, ((EntityAdjunct.AutoStartEntityAdjunct)adj)::start); });
                        entity.enrichers().forEach(adj -> { if (adj instanceof EntityAdjunct.AutoStartEntityAdjunct)
                            queueTask.accept("Start enricher "+adj, ((EntityAdjunct.AutoStartEntityAdjunct)adj)::start); });
                        entity.feeds().forEach(f -> { if (!f.isActivated())
                            queueTask.accept("Start feed "+f, f::start); });

                        if (AUTO_FAIL_AND_RESUME_WORKFLOWS) {
                            // resume any workflows that were dangling due to shutdown
                            WorkflowStatePersistenceViaSensors persister = new WorkflowStatePersistenceViaSensors(info.getManagementContext());
                            Map<String, WorkflowExecutionContext> workflows = persister.getWorkflows(entity);
                            List<WorkflowExecutionContext> shutdownInterruptedWorkflows = workflows.values().stream().filter(w ->
                                            w.getStatus() == WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN && w.getParentTag() == null)
                                    .collect(Collectors.toList());
                            if (!shutdownInterruptedWorkflows.isEmpty()) {
                                log.debug("Discovered workflows noted as 'interrupted' on startup at "+entity+", will resume as dangling: "+shutdownInterruptedWorkflows);
                                getManagementContext().getExecutionContext(entity).submit(DynamicTasks.of("Resuming with failure " + shutdownInterruptedWorkflows.size() + " interrupted workflow" + (shutdownInterruptedWorkflows.size() != 1 ? "s" : ""), () -> {
                                    shutdownInterruptedWorkflows.forEach(w -> {
                                        // these are backgrounded because they are expected to fail
                                        // we also have to wait until mgmt is complete
                                        Entities.submit(entity, w.factory(true).createTaskReplaying(
                                                () -> entity.getManagementContext().waitForManagementStartupComplete(Duration.minutes(15)),
                                                w.factory(true).makeInstructionsForReplayResumingForcedWithCustom("Resumed dangling on server restart", () -> {
                                                    throw new DanglingWorkflowException();
                                                })));

                                        // could do this, but instead it is handled specially in the UI
                                        //TaskTags.addTagDynamically(task, BrooklynTaskTags.TOP_LEVEL_TASK);
                                    });
                                })).get();  // not backgrounded because we want the new task to be recorded against all the workflows
                            }
                        }
                    }
                } catch (Throwable t) {
                    managementFailed.set(true);
                    throw Exceptions.propagate(t);
                }
            }
        ).build() );
    }

    @SuppressWarnings("deprecation")
    public void onManagementStarted(ManagementTransitionInfo info) {
        info.getManagementContext().getExecutionContext(entity).get( Tasks.builder().displayName("Management started")
            .dynamic(false)
            .tag(BrooklynTaskTags.ENTITY_INITIALIZATION)
//            .tag(BrooklynTaskTags.TRANSIENT_TASK_TAG)
            .body(() -> { try { synchronized (this) {
                boolean alreadyManaged = isFullyManaged();
                
                if (alreadyManaged) {
                    log.warn("Already managed: "+entity+" ("+nonDeploymentManagementContext+"); onManagementStarted is no-op");
                } else if (nonDeploymentManagementContext == null || nonDeploymentManagementContext.getMode() != NonDeploymentManagementContextMode.MANAGEMENT_STARTING) {
                    throw new IllegalStateException("Not in expected \"management starting\" state: "+entity+" ("+nonDeploymentManagementContext+")");
                }
                if (managementContext != info.getManagementContext()) {
                    throw new IllegalStateException("Already has management context: "+managementContext+"; can't set "+info.getManagementContext());
                }
                if (alreadyManaged) {
                    return;
                }
                
                nonDeploymentManagementContext.setMode(NonDeploymentManagementContextMode.MANAGEMENT_STARTED);
                
            }
            
            /* on start, we want to:
             * - set derived/inherited config values (not needed, the specs should have taken care of that?)
             * - publish all queued sensors (done below)
             * - start all queued executions (unpause entity's execution context, subscription delivery)
             * [in exactly this order, at each entity]
             * then subsequent sensor events and executions occur directly (no queueing)
             * 
             * NOTE: should happen out of synch block in case something is potentially long running;
             * should happen quickly tough, state might get messy and errors occur if stopped while starting!
             */                
            
            if (!isReadOnly()) {
                nonDeploymentManagementContext.getSubscriptionManager().setDelegate((AbstractSubscriptionManager) managementContext.getSubscriptionManager());
                nonDeploymentManagementContext.getSubscriptionManager().startDelegatingForPublishing();
                nonDeploymentManagementContext.getSubscriptionManager().startDelegatingForSubscribing();
                ((BasicExecutionContext)getExecutionContext()).unpause();
            }
            
            if (!isReadOnly()) {
                entity.onManagementBecomingMaster();
                if (info.getMode().isCreating()) entity.onManagementCreated();
                entity.onManagementStarted();
            }
            
            synchronized (this) {
                if (nonDeploymentManagementContext!=null && nonDeploymentManagementContext.getMode()!=null) {
                    switch (nonDeploymentManagementContext.getMode()) {
                        case MANAGEMENT_STARTED:
                            // normal
                            break;
                        case PRE_MANAGEMENT:
                        case MANAGEMENT_REBINDING:
                        case MANAGEMENT_STARTING:
                            // odd, but not a problem
                            log.warn("Management started invoked on "+entity+" when in unexpected state "+nonDeploymentManagementContext.getMode());
                            break;
                        case MANAGEMENT_STOPPING:
                        case MANAGEMENT_STOPPED:
                            // problematic
                            throw new IllegalStateException("Cannot start management of "+entity+" at this time; its management has been told to be "+
                                    nonDeploymentManagementContext.getMode());
                    }
                } else {
                    // odd - already started
                    log.warn("Management started invoked on "+entity+" when non-deployment context already cleared");
                }

                nonDeploymentManagementContext = null;
            }
        } catch (Throwable t) {
            managementFailed.set(true);
            throw Exceptions.propagate(t);
        }}).build() );
    }
    
    @SuppressWarnings("deprecation")
    public void onManagementStopping(ManagementTransitionInfo info, boolean wasDryRun) {
        synchronized (this) {
            currentlyStopping.set(true);
            if (!wasDryRun) {
                if (managementContext != info.getManagementContext()) {
                    throw new IllegalStateException("onManagementStopping encountered different management context for " + entity +
                            (!wasDeployed() ? " (wasn't deployed)" : !isDeployed() ? " (no longer deployed)" : "") +
                            ": " + managementContext + "; expected " + info.getManagementContext() + " (may be a pre-registered entity which was never properly managed)");
                }
                Stopwatch startTime = Stopwatch.createStarted();
                while (!managementFailed.get() && nonDeploymentManagementContext != null &&
                        nonDeploymentManagementContext.getMode() == NonDeploymentManagementContextMode.MANAGEMENT_STARTING) {
                    // still becoming managed
                    try {
                        if (startTime.elapsed(TimeUnit.SECONDS) > 30) {
                            // emergency fix, 30s timeout for management starting
                            log.error("Management stopping event " + info + " in " + this + " timed out waiting for start; proceeding to stopping");
                            break;
                        }
                        wait(100);
                    } catch (InterruptedException e) {
                        Exceptions.propagate(e);
                    }
                }
            }
            if (nonDeploymentManagementContext==null) {
                nonDeploymentManagementContext = new NonDeploymentManagementContext(entity, NonDeploymentManagementContextMode.MANAGEMENT_STOPPING);
            } else {
                // already stopped? or not started?
                nonDeploymentManagementContext.setMode(NonDeploymentManagementContextMode.MANAGEMENT_STOPPING);
            }
        }

        if (!wasDryRun && !isReadOnly()) {
            entity.onManagementStopping();
            if (info.getMode().isDestroying()) entity.onManagementDestroying();
        }

        if (wasDryRun || (!isReadOnly() && info.getMode().isDestroying())) {

            // ensure adjuncts get a destroy callback
            // note they don't get any alert if the entity is being locally unmanaged to run somewhere else.
            // framework should introduce a call for that ideally, but in interim if needed they
            // can listen to the entity becoming locally unmanaged
            entity.feeds().forEach(this::destroyAdjunct);
            entity.enrichers().forEach(this::destroyAdjunct);
            entity.policies().forEach(this::destroyAdjunct);
                
            // if we support remote parent of local child, the following call will need to be properly remoted
            if (entity.getParent()!=null) entity.getParent().removeChild(entity.getProxyIfAvailable());
        }
        // new subscriptions will be queued / not allowed
        nonDeploymentManagementContext.getSubscriptionManager().stopDelegatingForSubscribing();
        // new publications will be queued / not allowed
        nonDeploymentManagementContext.getSubscriptionManager().stopDelegatingForPublishing();
        
        if (!wasDryRun && !isReadOnly()) {
            entity.onManagementNoLongerMaster();
            entity.onManagementStopped();
        }
    }
    
    protected void destroyAdjunct(EntityAdjunct adjunct) {
        if (adjunct instanceof AbstractEntityAdjunct) {
            try {
                ((AbstractEntityAdjunct)adjunct).destroy();
            } catch (Exception e) {
                Exceptions.propagateIfFatal(e);
                log.error("Error destroying "+adjunct+" (ignoring): "+e);
                log.trace("Trace for error", e);
            }
        }
    }
    
    public void onManagementStopped(ManagementTransitionInfo info, boolean wasDryRun) {
        synchronized (this) {
            if (!wasDryRun) {
                if (managementContext == null && nonDeploymentManagementContext.getMode() == NonDeploymentManagementContextMode.MANAGEMENT_STOPPED) {
                    return;
                }
                if (managementContext != info.getManagementContext()) {
                    if (managementContext == null && nonDeploymentManagementContext.getMode() == NonDeploymentManagementContextMode.PRE_MANAGEMENT) {
                        log.info("Stopping management of "+entity+" during pre-management phase; likely concurrent creation/deletion");
                        // proceed to below, without error
                    } else {
                        throw new IllegalStateException(entity + " has different management context: " + managementContext + "; expected " + info.getManagementContext());
                    }
                }
            }
            getSubscriptionContext().unsubscribeAll();
            entityChangeListener = EntityChangeListener.NOOP;
            managementContextUsable.set(false);
            currentlyDeployed.set(false);
            currentlyStopping.set(false);
            executionContext = null;
            subscriptionContext = null;
        }
        
        // TODO framework stopped activities, e.g. serialize state ?
        entity.invalidateReferences();
        
        synchronized (this) {
            managementContext = null;
            nonDeploymentManagementContext.setMode(NonDeploymentManagementContextMode.MANAGEMENT_STOPPED);
        }
    }

    @VisibleForTesting
    @Beta
    public boolean isManagementContextReal() {
        return managementContextUsable.get();
    }
    
    public synchronized ManagementContext getManagementContext() {
        return (managementContextUsable.get()) ? managementContext : nonDeploymentManagementContext;
    }    
    
    public ExecutionContext getExecutionContext() {
        if (executionContext!=null) return executionContext;
        if (managementContextUsable.get()) {
            synchronized (this) {
                if (executionContext!=null) return executionContext;
                ExecutionContext newExecutionContext = managementContext.getExecutionContext(entity);
                ((BasicExecutionContext)newExecutionContext).pause(); // start paused, so things don't run until mgmt is started, and all entities known
                executionContext = newExecutionContext;
                return executionContext;
            }
        }
        return nonDeploymentManagementContext.getExecutionContext(entity);
    }
    public SubscriptionContext getSubscriptionContext() {
        if (subscriptionContext!=null) return subscriptionContext;
        if (managementContextUsable.get()) {
            synchronized (this) {
                if (subscriptionContext!=null) return subscriptionContext;
                subscriptionContext = managementContext.getSubscriptionContext(entity);
                return subscriptionContext;
            }
        }
        return nonDeploymentManagementContext.getSubscriptionContext(entity);
    }
    public synchronized EntitlementManager getEntitlementManager() {
        return getManagementContext().getEntitlementManager();
    }

    public void attemptLegacyAutodeployment(String effectorName) {
        synchronized (this) {
            if (managementContext != null) {
                log.warn("Autodeployment suggested but not required for " + entity + "." + effectorName);
                return;
            }
            if (entity instanceof Application) {
                log.warn("Autodeployment with new management context triggered for " + entity + "." + effectorName + " -- will not be supported in future. Explicit manage call required.");
                if (initialManagementContext != null) {
                    initialManagementContext.getEntityManager().manage(entity);
                } else {
                    Entities.startManagement(entity);
                }
                return;
            }
        }
        if ("start".equals(effectorName)) {
            Entity e=entity;
            if (e.getParent()!=null && ((EntityInternal)e.getParent()).getManagementSupport().isDeployed()) { 
                log.warn("Autodeployment in parent's management context triggered for "+entity+"."+effectorName+" -- will not be supported in future. Explicit manage call required.");
                ((EntityInternal)e.getParent()).getManagementContext().getEntityManager().manage(entity);
                return;
            }
        }
        log.warn("Autodeployment not available for "+entity+"."+effectorName);
    }
    
    public EntityChangeListener getEntityChangeListener() {
        return entityChangeListener;
    }
    
    private class EntityChangeListenerImpl implements EntityChangeListener {
        @Override
        public void onChanged() {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onChildrenChanged() {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onLocationsChanged() {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onTagsChanged() {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onMembersChanged() {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onPolicyAdded(Policy policy) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onManaged(policy);
        }
        @Override
        public void onEnricherAdded(Enricher enricher) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onManaged(enricher);
        }
        @Override
        public void onFeedAdded(Feed feed) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onManaged(feed);
        }
        @Override
        public void onPolicyRemoved(Policy policy) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onUnmanaged(policy);
        }
        @Override
        public void onEnricherRemoved(Enricher enricher) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onUnmanaged(enricher);
        }
        @Override
        public void onFeedRemoved(Feed feed) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            getManagementContext().getRebindManager().getChangeListener().onUnmanaged(feed);
        }
        @Override
        public void onAttributeChanged(AttributeSensor<?> attribute) {
            if (attribute.getPersistenceMode() != SensorPersistenceMode.NONE) {
                getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
            }
        }
        @Override
        public void onConfigChanged(ConfigKey<?> key) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
        @Override
        public void onEffectorStarting(Effector<?> effector, Object parameters) {
            Entitlements.checkEntitled(getEntitlementManager(), Entitlements.INVOKE_EFFECTOR, EntityAndItem.of(entity, StringAndArgument.of(effector.getName(), parameters)));
        }
        @Override
        public void onEffectorCompleted(Effector<?> effector) {
            getManagementContext().getRebindManager().getChangeListener().onChanged(entity);
        }
    }

    @Override
    public String toString() {
        return super.toString()+"["+(entity==null ? "null" : entity.getId())+"]";
    }
}
