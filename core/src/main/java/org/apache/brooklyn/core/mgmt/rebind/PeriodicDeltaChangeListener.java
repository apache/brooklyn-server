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
package org.apache.brooklyn.core.mgmt.rebind;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.brooklyn.api.catalog.CatalogItem;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.rebind.ChangeListener;
import org.apache.brooklyn.api.mgmt.rebind.PersistenceExceptionHandler;
import org.apache.brooklyn.api.mgmt.rebind.mementos.BrooklynMementoPersister;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.BrooklynObjectType;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.typereg.ManagedBundle;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.persist.BrooklynMementoPersisterToObjectStore;
import org.apache.brooklyn.core.mgmt.persist.BrooklynPersistenceUtils;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.persist.PersistenceActivityMetrics;
import org.apache.brooklyn.core.objs.BrooklynObjectInternal;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.task.ScheduledTask;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

/**
 * A "simple" implementation that periodically persists all entities/locations/policies that have changed
 * since the last periodic persistence.
 * 
 * TODO A better implementation would look at a per-entity basis. When the entity was modified, then  
 * schedule a write for that entity in X milliseconds time (if not already scheduled). That would
 * prevent hammering the persister when a bunch of entity attributes change (e.g. when the entity
 * has just polled over JMX/http/etc). Such a scheduled-write approach would be similar to the 
 * Nagle buffering algorithm in TCP (see tcp_nodelay).
 * 
 * @author aled
 *
 */
public class PeriodicDeltaChangeListener implements ChangeListener {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicDeltaChangeListener.class);

    protected final AtomicLong checkpointLogCount = new AtomicLong();
    private static final int INITIAL_LOG_WRITES = 5;
    private static final Duration PERSIST_PLANE_ID_PERIOD = Duration.ONE_HOUR;

    private static class DeltaCollector {
        private String planeId;

        private Set<Location> locations = Sets.newLinkedHashSet();
        private Set<Entity> entities = Sets.newLinkedHashSet();
        private Set<Policy> policies = Sets.newLinkedHashSet();
        private Set<Enricher> enrichers = Sets.newLinkedHashSet();
        private Set<Feed> feeds = Sets.newLinkedHashSet();
        private Set<CatalogItem<?, ?>> catalogItems = Sets.newLinkedHashSet();
        private Set<ManagedBundle> bundles = Sets.newLinkedHashSet();
        
        private Set<String> removedLocationIds = Sets.newLinkedHashSet();
        private Set<String> removedEntityIds = Sets.newLinkedHashSet();
        private Set<String> removedPolicyIds = Sets.newLinkedHashSet();
        private Set<String> removedEnricherIds = Sets.newLinkedHashSet();
        private Set<String> removedFeedIds = Sets.newLinkedHashSet();
        private Set<String> removedCatalogItemIds = Sets.newLinkedHashSet();
        private Set<String> removedBundleIds = Sets.newLinkedHashSet();

        public boolean isEmpty() {
            return planeId == null &&
                    locations.isEmpty() && entities.isEmpty() && policies.isEmpty() && 
                    enrichers.isEmpty() && feeds.isEmpty() &&
                    catalogItems.isEmpty() && bundles.isEmpty() &&
                    removedEntityIds.isEmpty() && removedLocationIds.isEmpty() && removedPolicyIds.isEmpty() && 
                    removedEnricherIds.isEmpty() && removedFeedIds.isEmpty() &&
                    removedCatalogItemIds.isEmpty() && removedBundleIds.isEmpty();
        }
        
        public void setPlaneId(String planeId) {
            this.planeId = planeId;
        }

        public void add(BrooklynObject instance) {
            BrooklynObjectType type = BrooklynObjectType.of(instance);
            getUnsafeCollectionOfType(type).add(instance);
            if (type==BrooklynObjectType.CATALOG_ITEM) {
                removedCatalogItemIds.remove(instance.getId());
            }
        }
        
        public void addIfNotRemoved(BrooklynObject instance) {
            BrooklynObjectType type = BrooklynObjectType.of(instance);
            if (!getRemovedIdsOfType(type).contains(instance.getId())) {
                getUnsafeCollectionOfType(type).add(instance);
            }
        }

        public void remove(BrooklynObject instance) {
            BrooklynObjectType type = BrooklynObjectType.of(instance);
            getUnsafeCollectionOfType(type).remove(instance);
            getRemovedIdsOfType(type).add(instance.getId());
        }

        @SuppressWarnings("unchecked")
        private Set<BrooklynObject> getUnsafeCollectionOfType(BrooklynObjectType type) {
            return (Set<BrooklynObject>)getCollectionOfType(type);
        }

        private Set<? extends BrooklynObject> getCollectionOfType(BrooklynObjectType type) {
            switch (type) {
            case ENTITY: return entities;
            case LOCATION: return locations;
            case ENRICHER: return enrichers;
            case FEED: return feeds;
            case POLICY: return policies;
            case CATALOG_ITEM: return catalogItems;
            case MANAGED_BUNDLE: return bundles;
            
            case UNKNOWN: // below
            }
            throw new IllegalStateException("No collection for type "+type);
        }
        
        private Set<String> getRemovedIdsOfType(BrooklynObjectType type) {
            switch (type) {
            case ENTITY: return removedEntityIds;
            case LOCATION: return removedLocationIds;
            case ENRICHER: return removedEnricherIds;
            case FEED: return removedFeedIds;
            case POLICY: return removedPolicyIds;
            case CATALOG_ITEM: return removedCatalogItemIds;
            case MANAGED_BUNDLE: return removedBundleIds;
            
            case UNKNOWN: // below
            }
            throw new IllegalStateException("No removed collection for type "+type);
        }

    }
    
    private final ExecutionContext executionContext;
    
    private final BrooklynMementoPersister persister;

    private final PersistenceExceptionHandler exceptionHandler;
    
    private final Duration period;
        
    private DeltaCollector deltaCollector = new DeltaCollector();

    private enum ListenerState { INIT, RUNNING, STOPPING, STOPPED } 
    private volatile ListenerState state = ListenerState.INIT;

    private volatile ScheduledTask scheduledTask;

    private final boolean persistPoliciesEnabled;
    private final boolean persistEnrichersEnabled;
    private final boolean persistFeedsEnabled;
    private final boolean rePersistReferencedObjectsEnabled;
    
    private final Semaphore persistingMutex = new Semaphore(1);
    private final Object startStopMutex = new Object();
    private final AtomicInteger writeCount = new AtomicInteger(0);

    private PersistenceActivityMetrics metrics;

    private CountdownTimer planeIdPersistTimer = CountdownTimer.newInstanceStarted(Duration.ZERO);
    private Supplier<String> planeIdSupplier;

    public PeriodicDeltaChangeListener(
            Supplier<String> planeIdSupplier,
            ExecutionContext executionContext,
            BrooklynMementoPersister persister,
            PersistenceExceptionHandler exceptionHandler,
            PersistenceActivityMetrics metrics,
            Duration period) {
        this.planeIdSupplier = planeIdSupplier;
        this.executionContext = executionContext;
        this.persister = persister;
        this.exceptionHandler = exceptionHandler;
        this.metrics = metrics;
        this.period = period;
        
        this.persistPoliciesEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_POLICY_PERSISTENCE_PROPERTY);
        this.persistEnrichersEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_ENRICHER_PERSISTENCE_PROPERTY);
        this.persistFeedsEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_FEED_PERSISTENCE_PROPERTY);
        this.rePersistReferencedObjectsEnabled = BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_REFERENCED_OBJECTS_REPERSISTENCE_PROPERTY);
    }
    
    public void start() {
        synchronized (startStopMutex) {
            if (state==ListenerState.RUNNING || (scheduledTask!=null && !scheduledTask.isDone())) {
                LOG.warn("Request to start "+this+" when already running - "+scheduledTask+"; ignoring");
                return;
            }
            state = ListenerState.RUNNING;

            Callable<Task<?>> taskFactory = new Callable<Task<?>>() {
                @Override public Task<Void> call() {
                    return Tasks.<Void>builder().dynamic(false).displayName("periodic-persister").body(new Callable<Void>() {
                        @Override
                        public Void call() {
                            persistNowSafely();
                            return null;
                        }}).build();
                }
            };
            scheduledTask = (ScheduledTask) executionContext.submit(
                ScheduledTask.builder(taskFactory).displayName("scheduled:[periodic-persister]").tagTransient().period(period).delay(period).build() );
        }
    }

    /** stops persistence, waiting for it to complete */
    void stop() {
        stop(Duration.TEN_SECONDS, Duration.ONE_SECOND);
    }
    void stop(Duration timeout, Duration graceTimeoutForSubsequentOperations) {
        synchronized (startStopMutex) {
            state = ListenerState.STOPPING;
            try {

                if (scheduledTask != null) {
                    CountdownTimer expiry = timeout.countdownTimer();
                    try {
                        scheduledTask.cancel(false);  
                        waitForPendingComplete(expiry.getDurationRemaining().lowerBound(Duration.ZERO).add(graceTimeoutForSubsequentOperations), true);
                    } catch (Exception e) {
                        throw Exceptions.propagate(e);
                    }
                    scheduledTask.blockUntilEnded(expiry.getDurationRemaining().lowerBound(Duration.ZERO).add(graceTimeoutForSubsequentOperations));
                    scheduledTask.cancel(true);
                    boolean reallyEnded = Tasks.blockUntilInternalTasksEnded(scheduledTask, expiry.getDurationRemaining().lowerBound(Duration.ZERO).add(graceTimeoutForSubsequentOperations));
                    if (!reallyEnded) {
                        LOG.warn("Persistence tasks took too long to terminate, when stopping persistence, although pending changes were persisted (ignoring): "+scheduledTask);
                    }
                    scheduledTask = null;
                }

                // Discard all state that was waiting to be persisted
                synchronized (this) {
                    deltaCollector = new DeltaCollector();
                }
            } finally {
                state = ListenerState.STOPPED;
            }
        }
    }
    
    /**
     * Resets persistence from STOPPED, back to the initial state of INIT.
     * 
     * Used when transitioning from HOT_STANDBY to MASTER. On rebinding as MASTER, we want it to 
     * behave in the same way as it would from INIT (e.g. populating the deltaCollector, etc).
     */
    void reset() {
        synchronized (startStopMutex) {
            if (state != ListenerState.STOPPED) {
                return;
            }
            state = ListenerState.INIT;
        }
    }

    /** Waits for any in-progress writes to be completed then for or any unwritten data to be written. */
    @VisibleForTesting
    public void waitForPendingComplete(Duration timeout, boolean canTrigger) throws InterruptedException, TimeoutException {
        if (!isActive() && state != ListenerState.STOPPING) return;
        
        CountdownTimer timer = timeout.isPositive() ? CountdownTimer.newInstanceStarted(timeout) : CountdownTimer.newInstancePaused(Duration.PRACTICALLY_FOREVER);
        Integer targetWriteCount = null;
        // wait for mutex, so we aren't tricked by an in-progress who has already recycled the collector
        if (persistingMutex.tryAcquire(timer.getDurationRemaining().toMilliseconds(), TimeUnit.MILLISECONDS)) {
            try {
                // now no one else is writing
                if (!deltaCollector.isEmpty()) {
                    if (canTrigger) {
                        // but there is data that needs to be written
                        persistNowSafely(true);
                    } else {
                        targetWriteCount = writeCount.get()+1;
                    }
                }
            } finally {
                persistingMutex.release();
            }
            if (targetWriteCount!=null) {
                while (writeCount.get() <= targetWriteCount) {
                    Duration left = timer.getDurationRemaining();
                    if (left.isPositive()) {
                        synchronized(writeCount) {
                            writeCount.wait(left.lowerBound(Repeater.DEFAULT_REAL_QUICK_PERIOD).toMilliseconds());
                        }
                    } else {
                        throw new TimeoutException("Timeout waiting for independent write of rebind-periodic-delta, after "+timer.getDurationElapsed());
                    }
                }
            }
        } else {
            // someone else has been writing for the entire time 
            throw new TimeoutException("Timeout waiting for completion of in-progress write of rebind-periodic-delta, after "+timer.getDurationElapsed());
        }
    }

    /** Check if there are any pending changes. May block to get mutex when checking. */
    @VisibleForTesting
    public boolean hasPending() {
        if (!isActive() && state != ListenerState.STOPPING) return false;
        
        // if can't get mutex, then some changes are being applied.
        try {
            if (persistingMutex.tryAcquire(0, TimeUnit.MILLISECONDS)) {
                try {
                    // now no one else is writing
                    return !deltaCollector.isEmpty();
                } finally {
                    persistingMutex.release();
                }
            } else {
                return false;
            }
        } catch (InterruptedException e) {
            throw Exceptions.propagate(e);
        }
    }

    /**
     * Indicates whether persistence is active. 
     * Even when not active, changes will still be tracked unless {@link #isStopped()}.
     */
    boolean isActive() {
        return state == ListenerState.RUNNING && persister != null && !isStopped();
    }

    /**
     * Whether we have been stopped, ie are stopping are or fully stopped,
     * in which case will not persist or store anything
     * (except for a final internal persistence called while STOPPING.) 
     */
    boolean isStopped() {
        return state == ListenerState.STOPPING || state == ListenerState.STOPPED || executionContext.isShutdown();
    }
    
    /**
     * @deprecated since 1.0.0; its use is enabled via BrooklynFeatureEnablement.FEATURE_REFERENCED_OBJECTS_PERSISTENCE_PROPERTY,
     *             to preserve backwards compatibility for legacy implementations of entities, policies, etc.
     */
    @Deprecated
    private void addReferencedObjects(DeltaCollector deltaCollector) {
        MutableSet<BrooklynObject> referencedObjects = MutableSet.of();
        
        // collect references
        for (Entity entity : deltaCollector.entities) {
            // FIXME How to let the policy/location tell us about changes? Don't do this every time!
            for (Location location : entity.getLocations()) {
                Collection<Location> findLocationsInHierarchy = TreeUtils.findLocationsInHierarchy(location);
                referencedObjects.addAll(findLocationsInHierarchy);
            }
            if (persistPoliciesEnabled) {
                referencedObjects.addAll(entity.policies());
            }
            if (persistEnrichersEnabled) {
                referencedObjects.addAll(entity.enrichers());
            }
            if (persistFeedsEnabled) {
                referencedObjects.addAll(((EntityInternal)entity).feeds().getFeeds());
            }
        }
        
        for (BrooklynObject instance : referencedObjects) {
            deltaCollector.addIfNotRemoved(instance);
        }
    }
    
    @VisibleForTesting
    public boolean persistNowSafely() {
        return persistNowSafely(false);
    }
    
    private boolean persistNowSafely(boolean alreadyHasMutex) {
        Stopwatch timer = Stopwatch.createStarted();
        try {
            persistNowInternal(alreadyHasMutex);
            metrics.noteSuccess(Duration.of(timer));
            return true;
        } catch (RuntimeInterruptedException e) {
            LOG.debug("Interrupted persisting change-delta (rethrowing)", e);
            metrics.noteFailure(Duration.of(timer));
            metrics.noteError(e.toString());
            Thread.currentThread().interrupt();
            return false;
        } catch (Exception e) {
            // Don't rethrow: the behaviour of executionManager is different from a scheduledExecutorService,
            // if we throw an exception, then our task will never get executed again
            LOG.error("Problem persisting change-delta", e);
            metrics.noteFailure(Duration.of(timer));
            metrics.noteError(e.toString());
            return false;
        } catch (Throwable t) {
            LOG.warn("Problem persisting change-delta (rethrowing)", t);
            metrics.noteFailure(Duration.of(timer));
            metrics.noteError(t.toString());
            throw Exceptions.propagate(t);
        }
    }
    
    protected void persistNowInternal(boolean alreadyHasMutex) {
        if (!isActive() && state != ListenerState.STOPPING) {
            return;
        }
        try {
            if (!alreadyHasMutex) persistingMutex.acquire();
            if (!isActive() && state != ListenerState.STOPPING) return;
            
            // Writes to the datastore are lossy. We'll just log failures and move on.
            // (Most) entities will get updated multiple times in their lifecycle
            // so not a huge deal. planeId does not get updated so if the first
            // write fails it's not available to the HA cluster at all. That's why it
            // gets periodically written to the datastore. 
            updatePlaneIdIfTimedOut();

            // Atomically switch the delta, so subsequent modifications will be done in the
            // next scheduled persist
            DeltaCollector prevDeltaCollector;
            synchronized (this) {
                prevDeltaCollector = deltaCollector;
                deltaCollector = new DeltaCollector();
            }
            
            if (LOG.isDebugEnabled() && shouldLogCheckpoint()) LOG.debug("Checkpointing delta of memento: "
                    + "updating entities={}, locations={}, policies={}, enrichers={}, catalog items={}, bundles={}; "
                    + "removing entities={}, locations={}, policies={}, enrichers={}, catalog items={}, bundles={}",
                    new Object[] {
                        limitedCountString(prevDeltaCollector.entities), limitedCountString(prevDeltaCollector.locations), limitedCountString(prevDeltaCollector.policies), limitedCountString(prevDeltaCollector.enrichers), limitedCountString(prevDeltaCollector.catalogItems), limitedCountString(prevDeltaCollector.bundles), 
                        limitedCountString(prevDeltaCollector.removedEntityIds), limitedCountString(prevDeltaCollector.removedLocationIds), limitedCountString(prevDeltaCollector.removedPolicyIds), limitedCountString(prevDeltaCollector.removedEnricherIds), limitedCountString(prevDeltaCollector.removedCatalogItemIds), limitedCountString(prevDeltaCollector.removedBundleIds)});

            if (rePersistReferencedObjectsEnabled) {
                addReferencedObjects(prevDeltaCollector);

                if (LOG.isTraceEnabled()) LOG.trace("Checkpointing delta of memento with references: "
                        + "updating {} entities, {} locations, {} policies, {} enrichers, {} catalog items, {} bundles; "
                        + "removing {} entities, {} locations, {} policies, {} enrichers, {} catalog items, {} bundles",
                        new Object[] {
                            prevDeltaCollector.entities.size(), prevDeltaCollector.locations.size(), prevDeltaCollector.policies.size(), prevDeltaCollector.enrichers.size(), prevDeltaCollector.catalogItems.size(), prevDeltaCollector.bundles.size(),
                            prevDeltaCollector.removedEntityIds.size(), prevDeltaCollector.removedLocationIds.size(), prevDeltaCollector.removedPolicyIds.size(), prevDeltaCollector.removedEnricherIds.size(), prevDeltaCollector.removedCatalogItemIds.size(), prevDeltaCollector.removedBundleIds.size()});
            }

            // Generate mementos for everything that has changed in this time period
            if (prevDeltaCollector.isEmpty()) {
                if (LOG.isTraceEnabled()) LOG.trace("No changes to persist since last delta");
            } else {
                PersisterDeltaImpl persisterDelta = new PersisterDeltaImpl();

                if (prevDeltaCollector.planeId != null) {
                    persisterDelta.planeId = prevDeltaCollector.planeId;
                }
                for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                    for (BrooklynObject instance: prevDeltaCollector.getCollectionOfType(type)) {
                        try {
                            persisterDelta.add(type, ((BrooklynObjectInternal)instance).getRebindSupport().getMemento());
                        } catch (Exception e) {
                            exceptionHandler.onGenerateMementoFailed(type, instance, e);
                        }
                    }
                }
                for (BrooklynObjectType type: BrooklynPersistenceUtils.STANDARD_BROOKLYN_OBJECT_TYPE_PERSISTENCE_ORDER) {
                    persisterDelta.removed(type, prevDeltaCollector.getRemovedIdsOfType(type));
                }

                /*
                 * Need to guarantee "happens before", with any thread that subsequently reads
                 * the mementos.
                 * 
                 * See MementoFileWriter.writeNow for the corresponding synchronization,
                 * that guarantees its thread has values visible for reads.
                 */
                synchronized (new Object()) {}

                // Tell the persister to persist it
                persister.delta(persisterDelta, exceptionHandler);

                // save export zip of persistence state, only for file based
                if ((persister instanceof BrooklynMementoPersisterToObjectStore) && (((BrooklynMementoPersisterToObjectStore) persister).getObjectStore() instanceof FileBasedObjectStore)){
                    BrooklynPersistenceUtils.createStateExport(((BrooklynMementoPersisterToObjectStore) persister).getManagementContext(), ((FileBasedObjectStore) ((BrooklynMementoPersisterToObjectStore) persister).getObjectStore()).getBaseDir());
                }


            }
        } catch (Exception e) {
            if (isActive()) {
                throw Exceptions.propagate(e);
            } else {
                Exceptions.propagateIfFatal(e);
                LOG.debug("Problem persisting, but no longer active (ignoring)", e);
            }
        } finally {
            synchronized (writeCount) {
                writeCount.incrementAndGet();
                writeCount.notifyAll();
            }
            if (!alreadyHasMutex) persistingMutex.release();
        }
    }
    
    private void updatePlaneIdIfTimedOut() {
        if (planeIdPersistTimer.isExpired()) {
            deltaCollector.setPlaneId(planeIdSupplier.get());
            planeIdPersistTimer = PERSIST_PLANE_ID_PERIOD.countdownTimer();
        }
        
    }

    private static String limitedCountString(Collection<?> items) {
        if (items==null) return null;
        int size = items.size();
        if (size==0) return "[]";
        
        int MAX = 12;
        
        if (size<=MAX) return items.toString();
        List<Object> itemsTruncated = Lists.newArrayList(Iterables.limit(items, MAX));
        if (items.size()>itemsTruncated.size()) itemsTruncated.add("... ("+(size-MAX)+" more)");
        return itemsTruncated.toString();
    }

    @Override
    public synchronized void onManaged(BrooklynObject instance) {
        if (LOG.isTraceEnabled()) LOG.trace("onManaged: {}", instance);
        onChanged(instance);
        addReferencedObjectsForInitialPersist(instance);
    }

    private void addReferencedObjectsForInitialPersist(BrooklynObject instance) {
        if (!(instance instanceof Entity)) return;
        Entity entity = (Entity) instance;
        
        MutableSet<BrooklynObject> referencedObjects = MutableSet.of();
        
        // collect references
        for (Location location : entity.getLocations()) {
            Collection<Location> findLocationsInHierarchy = TreeUtils.findLocationsInHierarchy(location);
            referencedObjects.addAll(findLocationsInHierarchy);
        }
        if (persistPoliciesEnabled) {
            referencedObjects.addAll(entity.policies());
        }
        if (persistEnrichersEnabled) {
            referencedObjects.addAll(entity.enrichers());
        }
        if (persistFeedsEnabled) {
            referencedObjects.addAll(((EntityInternal)entity).feeds().getFeeds());
        }
        
        for (BrooklynObject ref : referencedObjects) {
            deltaCollector.addIfNotRemoved(ref);
        }
    }
    

    @Override
    public synchronized void onUnmanaged(BrooklynObject instance) {
        if (LOG.isTraceEnabled()) LOG.trace("onUnmanaged: {}", instance);
        if (!isStopped()) {
            removeFromCollector(instance);
            if (instance instanceof Entity) {
                Entity entity = (Entity) instance;
                for (BrooklynObject adjunct : entity.policies()) removeFromCollector(adjunct);
                for (BrooklynObject adjunct : entity.enrichers()) removeFromCollector(adjunct);
                for (BrooklynObject adjunct : ((EntityInternal)entity).feeds().getFeeds()) removeFromCollector(adjunct);
            }
        }
    }
    
    private void removeFromCollector(BrooklynObject instance) {
        deltaCollector.remove(instance);
    }

    @Override
    public synchronized void onChanged(BrooklynObject instance) {
        if (LOG.isTraceEnabled()) LOG.trace("onChanged: {}", instance);
        if (!isStopped()) {
            deltaCollector.add(instance);
        }
    }
    
    public PersistenceExceptionHandler getExceptionHandler() {
        return exceptionHandler;
    }

    protected boolean shouldLogCheckpoint() {
        long logCount = checkpointLogCount.incrementAndGet();
        return (logCount < INITIAL_LOG_WRITES) || (logCount % 1000 == 0);
    }
}
