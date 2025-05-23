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
import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.AccessController;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionManager;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.TaskAdaptable;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.drivers.downloads.BasicDownloadsManager;
import org.apache.brooklyn.core.internal.BrooklynInitialization;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.internal.BrooklynProperties.Factory.Builder;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.ha.OsgiManager;
import org.apache.brooklyn.core.objs.proxy.InternalEntityFactory;
import org.apache.brooklyn.core.objs.proxy.InternalLocationFactory;
import org.apache.brooklyn.core.objs.proxy.InternalPolicyFactory;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.core.task.TaskTags;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.MoreObjects;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;

/**
 * A local (single node) implementation of the {@link ManagementContext} API.
 */
public class LocalManagementContext extends AbstractManagementContext {
    
    private static final Logger log = LoggerFactory.getLogger(LocalManagementContext.class);

    // Any usage of Brooklyn must first create a management context. Therefore do our 
    // initialisation here.
    // TODO We could delete our other calls to BrooklynInitialization.initAll(), to rely on
    // just one (e.g. AbstractEntity should not need to do it; and that is insufficient if a test
    // just deals with locations but not entities).
    static {
        BrooklynInitialization.initAll();
    }
    
    private static final Set<LocalManagementContext> INSTANCES = Collections.synchronizedSet(Collections.newSetFromMap(new WeakHashMap<LocalManagementContext, Boolean>()));
    
    private final Builder builder;
    
    private final List<ManagementContext.PropertiesReloadListener> reloadListeners = new CopyOnWriteArrayList<ManagementContext.PropertiesReloadListener>();

    @VisibleForTesting
    static Set<LocalManagementContext> getInstances() {
        synchronized (INSTANCES) {
            return ImmutableSet.copyOf(INSTANCES);
        }
    }

    // Note also called reflectively by BrooklynLeakListener
    public static void logAll(Logger logger){
        for (LocalManagementContext context : getInstances()) {
            logger.warn("Management Context "+context+" running, creation stacktrace:\n" + Throwables.getStackTraceAsString(context.constructionStackTrace));
        }
    }

    /** terminates all (best effort); returns count of sessions closed; if exceptions thrown, returns negative number.
     * semantics might change, particular in dealing with interminable mgmt contexts. */
    // Note also called reflectively by BrooklynLeakListener
    @Beta
    public static int terminateAll() {
        int closed=0,dangling=0;
        for (LocalManagementContext context : getInstances()) {
            try {
                context.terminate();
                closed++;
            }catch (Throwable t) {
                Exceptions.propagateIfFatal(t);
                log.warn("Failed to terminate management context", t);
                dangling++;
            }
        }
        if (dangling>0) return -dangling;
        return closed;
    }

    private final AtomicBoolean terminated = new AtomicBoolean(false);
    private String managementPlaneId;
    private BasicExecutionManager execution;
    private SubscriptionManager subscriptions;
    private LocalEntityManager entityManager;
    private final LocalLocationManager locationManager;
    private final LocalAccessManager accessManager;
    private final LocalUsageManager usageManager;
    private OsgiManager osgiManager;
    
    public final Throwable constructionStackTrace = new Throwable("for construction stacktrace").fillInStackTrace();
    
    private final Map<String, Object> brooklynAdditionalProperties;

    /**
     * Creates a LocalManagement with default BrooklynProperties.
     */
    public LocalManagementContext() {
        this(BrooklynProperties.Factory.builderDefault());
    }

    /**
     * Creates a new LocalManagementContext.
     *
     * @param brooklynProperties the BrooklynProperties.
     */
    @VisibleForTesting
    public LocalManagementContext(BrooklynProperties brooklynProperties) {
        this(Builder.fromProperties(brooklynProperties));
    }
    
    public LocalManagementContext(Builder builder) {
        this(builder, null);
    }
    
    public LocalManagementContext(Builder builder, Map<String, Object> brooklynAdditionalProperties) {
        super(builder.build());
        
        checkNotNull(configMap, "brooklynProperties");
        
        this.builder = builder;
        this.brooklynAdditionalProperties = brooklynAdditionalProperties;
        if (brooklynAdditionalProperties != null)
            configMap.addFromMap(brooklynAdditionalProperties);
        
        BrooklynFeatureEnablement.init(configMap);
        
        this.locationManager = new LocalLocationManager(this);
        this.accessManager = new LocalAccessManager();
        this.usageManager = new LocalUsageManager(this);
        
        if (configMap.getConfig(OsgiManager.USE_OSGI)) {
            this.osgiManager = new OsgiManager(this);
            osgiManager.start();
        }
        
        INSTANCES.add(this);
        log.debug("Created management context "+this);
    }

    @Override
    @Deprecated
    public String getManagementPlaneId() {
        return managementPlaneId;
    }
    
    @Override
    public Maybe<String> getManagementPlaneIdMaybe() {
        return Maybe.ofDisallowingNull(managementPlaneId);
    }
    
    public void setManagementPlaneId(String newPlaneId) {
        if (managementPlaneId != null && !managementPlaneId.equals(newPlaneId)) {
            log.warn("Management plane ID at {} {} changed from {} to {} (can happen on concurrent startup of multiple nodes)", new Object[] { getManagementNodeId(), getHighAvailabilityManager().getNodeState(), managementPlaneId, newPlaneId });
            log.debug("Management plane ID at {} {} changed from {} to {} (can happen on concurrent startup of multiple nodes)", new Object[] {getManagementNodeId(), getHighAvailabilityManager().getNodeState(), managementPlaneId, newPlaneId, new RuntimeException("Stack trace for setManagementPlaneId")});
        }
        this.managementPlaneId = newPlaneId;
    }

    public void generateManagementPlaneId() {
        if (this.managementPlaneId != null) {
            throw new IllegalStateException("Request to generate a management plane ID for node "+getManagementNodeId()+" but one already exists (" + managementPlaneId + ")");
        }
        this.managementPlaneId = Strings.makeRandomId(8);
    }
    
    @Override
    public void prePreManage(Entity entity) {
        getEntityManager().prePreManage(entity);
    }

    @Override
    public void prePreManage(Location location) {
        getLocationManager().prePreManage(location);
    }

    @Override
    public synchronized Collection<Application> getApplications() {
        return getEntityManager().getApplications();
    }

    @Override
    public void addEntitySetListener(CollectionChangeListener<Entity> listener) {
        getEntityManager().addEntitySetListener(listener);
    }

    @Override
    public void removeEntitySetListener(CollectionChangeListener<Entity> listener) {
        getEntityManager().removeEntitySetListener(listener);
    }

    @Override
    protected void manageIfNecessary(Entity entity, Object context) {
        getEntityManager().manageIfNecessary(entity, context);
    }

    @Override
    public synchronized LocalEntityManager getEntityManager() {
        if (!isRunning()) {
            throw new IllegalStateException("Management context no longer running");
        }

        if (entityManager == null) {
            entityManager = new LocalEntityManager(this);
        }
        return entityManager;
    }

    @Override
    public InternalEntityFactory getEntityFactory() {
        return getEntityManager().getEntityFactory();
    }

    @Override
    public InternalLocationFactory getLocationFactory() {
        return getLocationManager().getLocationFactory();
    }

    @Override
    public InternalPolicyFactory getPolicyFactory() {
        return getEntityManager().getPolicyFactory();
    }

    @Override
    public synchronized LocalLocationManager getLocationManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return locationManager;
    }

    @Override
    public synchronized LocalAccessManager getAccessManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return accessManager;
    }

    @Override
    public synchronized LocalUsageManager getUsageManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return usageManager;
    }
    
    @Override
    public synchronized Maybe<OsgiManager> getOsgiManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        if (osgiManager==null) return Maybe.absent("OSGi not available in this instance"); 
        return Maybe.of(osgiManager);
    }

    @Override
    public synchronized AccessController getAccessController() {
        return getAccessManager().getAccessController();
    }
    
    @Override
    public synchronized  SubscriptionManager getSubscriptionManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");

        if (subscriptions == null) {
            subscriptions = new LocalSubscriptionManager(getExecutionManager());
        }
        return subscriptions;
    }

    @Override
    public synchronized ExecutionManager getExecutionManager() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");

        if (execution == null) {
            execution = new BasicExecutionManager(getManagementNodeId());
            gc = new BrooklynGarbageCollector(configMap, execution, getStorage());
        }
        return execution;
    }
    
    @Override
    public void terminate() {
        if (terminated.getAndSet(true)) {
            log.trace("Already terminated management context "+this);
            // no harm in doing it twice, but it makes logs ugly!
            return;
        }
        log.debug("Terminating management context "+this);

        INSTANCES.remove(this);
        super.terminate();
        if (usageManager != null) usageManager.terminate();
        if (execution != null) execution.shutdownNow();
        if (gc != null) gc.shutdownNow();
        if (osgiManager!=null) {
            osgiManager.stop();
            osgiManager = null;
        }

        log.debug("Terminated management context "+this);
    }

    @Override
    protected void finalize() {
        terminate();
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    protected <T> Task<T> runAtEntity(Map flags, Entity entity, Callable<T> c) {
        manageIfNecessary(entity, elvis(Arrays.asList(flags.get("displayName"), flags.get("description"), flags, c)));
        return runAtEntity(entity, Tasks.<T>builder().dynamic(true).body(c).flags(flags).build());
    }

    protected <T> Task<T> runAtEntity(Entity entity, TaskAdaptable<T> task) {
        ((EntityInternal)entity).getExecutionContext().submit(task);
        if (DynamicTasks.getTaskQueuingContext()!=null) {
            // put it in the queueing context so it appears in the GUI
            // mark it inessential as this is being invoked from code,
            // the caller will do 'get' to handle errors
            TaskTags.markInessential(task);
            DynamicTasks.getTaskQueuingContext().queue(task.asTask());
        }
        return task.asTask();
    }
    
    @Override
    protected <T> Task<T> runAtEntity(final Entity entity, final Effector<T> eff, @SuppressWarnings("rawtypes") final Map parameters) {
        manageIfNecessary(entity, eff);
        // prefer to submit this from the current execution context so it sets up correct cross-context chaining
//        ExecutionContext ec = BasicExecutionContext.getCurrentExecutionContext();
//        if (ec == null) {
//            log.debug("Top-level effector invocation: {} on {}", eff, entity);
//            ec = getExecutionContext(entity);
//        }
        return runAtEntity(entity, Effectors.invocation(entity, eff, parameters));
    }

    @Override
    public boolean isManagedLocally(Entity e) {
        return true;
    }

    @Override
    public String toString() {
        String planeId = MoreObjects.firstNonNull(managementPlaneId, "?");
        return LocalManagementContext.class.getSimpleName()+"["+planeId+"-"+getManagementNodeId()+"]";
    }

    @Override
    public void reloadBrooklynProperties() {
        log.info("Reloading brooklyn properties from " + builder);
        if (builder.hasDelegateOriginalProperties())
            log.warn("When reloading, mgmt context "+this+" properties are fixed, so reload will be of limited utility");
        
        BrooklynProperties properties = builder.build();
        configMap = new DeferredBrooklynProperties(properties, this);
        if (brooklynAdditionalProperties != null && !brooklynAdditionalProperties.isEmpty()) {
            log.info("Reloading additional brooklyn properties from " + brooklynAdditionalProperties);
            configMap.addFromMap(brooklynAdditionalProperties);
        }
        this.downloadsManager = BasicDownloadsManager.newDefault(configMap);
        this.entitlementManager = Entitlements.newManager(this, configMap);
        this.configSupplierRegistry = new BasicExternalConfigSupplierRegistry(this);

        clearLocationRegistry();
        
        BrooklynFeatureEnablement.init(configMap);
        
        // Notify listeners that properties have been reloaded
        for (PropertiesReloadListener listener : reloadListeners) {
            listener.reloaded();
        }
    }

    @VisibleForTesting
    public void clearLocationRegistry() {
        // Force reload of location registry
        this.locationRegistry = null;
    }

    @Override
    public void addPropertiesReloadListener(PropertiesReloadListener listener) {
        reloadListeners.add(checkNotNull(listener, "listener"));
    }

    @Override
    public void removePropertiesReloadListener(PropertiesReloadListener listener) {
        reloadListeners.remove(listener);
    }

    private Object startupSynchObject = new Object();
    public void noteStartupComplete() {
        synchronized (startupSynchObject) {
            startupComplete = true;
            startupSynchObject.notifyAll();
        }
    }
    /** Exposed for services to advertise that startup tasks are again occurring */
    public void noteStartupTransitioning() {
        synchronized (startupSynchObject) {
            startupComplete = false;
            startupSynchObject.notifyAll();
        }
    }
    @Override
    public boolean isStartupComplete() {
        synchronized (startupSynchObject) {
            return startupComplete;
        }
    }

    @Override public void waitForManagementStartupComplete(Duration timeout) {
        if (timeout==null) timeout = Duration.minutes(5);
        CountdownTimer timer = CountdownTimer.newInstanceStarted(timeout);
        try {
            Tasks.withBlockingDetails("Waiting on management plane to completely start", () -> {
                while (true) {
                    synchronized (startupSynchObject) {
                        if (isStartupComplete()) return null;
                        if (!isRunning()) {
                            throw new IllegalStateException("Management context transitioned to not running before startup detected as completed");
                        }
                        if (!timer.waitOnForExpiryUnchecked(startupSynchObject)) {
                            throw Exceptions.propagate(new TimeoutException("Timeout waiting for management context to start"));
                        }
                    }
                }
            });
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public ManagementNodeState getNodeState() {
        synchronized (startupSynchObject) {
            if (!startupComplete) return ManagementNodeState.INITIALIZING;
            if (!errors().isEmpty()) return ManagementNodeState.FAILED;
            return getHighAvailabilityManager().getNodeState();
        }
    }
}
