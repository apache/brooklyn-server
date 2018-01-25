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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.internal.storage.BrooklynStorage;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.internal.LocationInternal;
import org.apache.brooklyn.core.mgmt.ManagementContextInjectable;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.usage.ApplicationUsage;
import org.apache.brooklyn.core.mgmt.usage.LocationUsage;
import org.apache.brooklyn.core.mgmt.usage.UsageListener;
import org.apache.brooklyn.core.mgmt.usage.UsageManager;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class LocalUsageManager implements UsageManager {

    // TODO Threading model needs revisited.
    // Synchronizes on updates to storage; but if two Brooklyn nodes were both writing to the same
    // ApplicationUsage or LocationUsage record there'd be a race. That currently won't happen
    // (at least for ApplicationUsage?) because the app is mastered in just one node at a time,
    // and because location events are just manage/unmanage which should be happening in just 
    // one place at a time for a given location.
    
    private static final Logger log = LoggerFactory.getLogger(LocalUsageManager.class);

    private static class ApplicationMetadataImpl implements UsageListener.ApplicationMetadata {
        private final Application app;
        private String applicationId;
        private String applicationName;
        private String entityType;
        private String catalogItemId;
        private Map<String, String> metadata;

        ApplicationMetadataImpl(Application app) {
            this.app = checkNotNull(app, "app");
            applicationId = app.getId();
            applicationName = app.getDisplayName();
            entityType = app.getEntityType().getName();
            catalogItemId = app.getCatalogItemId();
            metadata = ((EntityInternal)app).toMetadataRecord();
        }
        @Override public Application getApplication() {
            return app;
        }
        @Override public String getApplicationId() {
            return applicationId;
        }
        @Override public String getApplicationName() {
            return applicationName;
        }
        @Override public String getEntityType() {
            return entityType;
        }
        @Override public String getCatalogItemId() {
            return catalogItemId;
        }
        @Override public Map<String, String> getMetadata() {
            return metadata;
        }
    }
    
    private static class LocationMetadataImpl implements UsageListener.LocationMetadata {
        private final Location loc;
        private String locationId;
        private Map<String, String> metadata;

        LocationMetadataImpl(Location loc) {
            this.loc = checkNotNull(loc, "loc");
            locationId = loc.getId();
            metadata = ((LocationInternal)loc).toMetadataRecord();
        }
        @Override public Location getLocation() {
            return loc;
        }
        @Override public String getLocationId() {
            return locationId;
        }
        @Override public Map<String, String> getMetadata() {
            return metadata;
        }
    }
    
    @VisibleForTesting
    public static final String APPLICATION_USAGE_KEY = "usage-application";
    
    @VisibleForTesting
    public static final String LOCATION_USAGE_KEY = "usage-location";

    private final LocalManagementContext managementContext;
    
    private final Object mutex = new Object();

    private final List<UsageListener> listeners = Lists.newCopyOnWriteArrayList();
    
    private final AtomicInteger listenerQueueSize = new AtomicInteger();
    
    private ListeningExecutorService listenerExecutor = MoreExecutors.listeningDecorator(Executors.newSingleThreadExecutor(new ThreadFactoryBuilder()
            .setNameFormat("brooklyn-usagemanager-listener-%d")
            .build()));

    public LocalUsageManager(LocalManagementContext managementContext) {
        this.managementContext = checkNotNull(managementContext, "managementContext");
        
        // Register a coercion from String->UsageListener, so that USAGE_LISTENERS defined in brooklyn.properties
        // will be instantiated, given their class names.
        TypeCoercions.BrooklynCommonAdaptorTypeCoercions.registerInstanceForClassnameAdapter(
                new ClassLoaderUtils(this.getClass(), managementContext), 
                UsageListener.class);
        
        // Although changing listeners to Collection<UsageListener> is valid at compile time
        // the collection will contain any objects that could not be coerced by the function
        // declared above. Generally this means any string declared in brooklyn.properties
        // that is not a UsageListener.
        Collection<?> listeners = managementContext.getBrooklynProperties().getConfig(UsageManager.USAGE_LISTENERS);
        if (listeners != null) {
            for (Object obj : listeners) {
                if (obj == null) {
                    throw new NullPointerException("null listener in config " + UsageManager.USAGE_LISTENERS);
                } else if (!(obj instanceof UsageListener)) {
                    throw new ClassCastException("Configured object is not a UsageListener. This probably means coercion failed: " + obj);
                } else {
                    UsageListener listener = (UsageListener) obj;
                    if (listener instanceof ManagementContextInjectable) {
                        ((ManagementContextInjectable) listener).setManagementContext(managementContext);
                    }
                    addUsageListener(listener);
                }
            }
        }
    }

    public void terminate() {
        // Wait for the listeners to finish + close the listeners
        Duration timeout = managementContext.getBrooklynProperties().getConfig(UsageManager.USAGE_LISTENER_TERMINATION_TIMEOUT);
        if (listenerQueueSize.get() > 0) {
            log.info("Usage manager waiting for "+listenerQueueSize+" listener events for up to "+timeout);
        }
        List<ListenableFuture<?>> futures = Lists.newArrayList();
        for (final UsageListener listener : listeners) {
            ListenableFuture<?> future = listenerExecutor.submit(new Runnable() {
                @Override
                public void run() {
                    if (listener instanceof Closeable) {
                        try {
                            ((Closeable)listener).close();
                        } catch (IOException e) {
                            log.warn("Problem closing usage listener "+listener+" (continuing)", e);
                        }
                    }
                }});
            futures.add(future);
        }
        try {
            Futures.successfulAsList(futures).get(timeout.toMilliseconds(), TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Problem terminiating usage listeners (continuing)", e);
        } finally {
            listenerExecutor.shutdownNow();
        }
    }

    private void execOnListeners(final Function<UsageListener, Void> job) {
        for (final UsageListener listener : listeners) {
            listenerQueueSize.incrementAndGet();
            listenerExecutor.execute(new Runnable() {
                @Override
                public void run() {
                    try {
                        job.apply(listener);
                    } catch (RuntimeException e) {
                        log.error("Problem notifying listener "+listener+" of "+job, e);
                        Exceptions.propagateIfFatal(e);
                    } finally {
                        listenerQueueSize.decrementAndGet();
                    }
                }});
        }
    }
    
    @Override
    public void recordApplicationEvent(final Application app, final Lifecycle state) {
        log.debug("Storing application lifecycle usage event: application {} in state {}", new Object[] {app, state});
        ConcurrentMap<String, ApplicationUsage> eventMap = managementContext.getStorage().getMap(APPLICATION_USAGE_KEY);
        
        // Don't call out to alien-code (i.e. app.toMetadataRecord()) while holding mutex. It might take a while.
        // If we don't have a usage record, then generate one outside of the mutex. But then double-check while
        // holding the mutex to see if another thread has created one. If it has, stick with that rather than 
        // overwriting it.
        ApplicationUsage usage;
        synchronized (mutex) {
            usage = eventMap.get(app.getId());
        }
        if (usage == null) {
            usage = new ApplicationUsage(app.getId(), app.getDisplayName(), app.getEntityType().getName(), ((EntityInternal)app).toMetadataRecord());
        }
        final ApplicationUsage.ApplicationEvent event = new ApplicationUsage.ApplicationEvent(state, getUser());
        
        synchronized (mutex) {
            ApplicationUsage otherUsage = eventMap.get(app.getId());
            if (otherUsage != null) {
                usage = otherUsage;
            }
            usage.addEvent(event);        
            eventMap.put(app.getId(), usage);

            execOnListeners(new Function<UsageListener, Void>() {
                    @Override
                    public Void apply(UsageListener listener) {
                        listener.onApplicationEvent(new ApplicationMetadataImpl(Entities.proxy(app)), event);
                        return null;
                    }
                    @Override
                    public String toString() {
                        return "applicationEvent("+app+", "+state+")";
                    }});
        }
    }
    
    /**
     * Adds this location event to the usage record for the given location (creating the usage 
     * record if one does not already exist).
     */
    @Override
    public void recordLocationEvent(final Location loc, final Lifecycle state) {
        // TODO This approach (i.e. recording events on manage/unmanage would not work for
        // locations that are reused. For example, in a FixedListMachineProvisioningLocation
        // the ssh machine location is returned to the pool and handed back out again.
        // But maybe the solution there is to hand out different instances so that one user
        // can't change the config of the SshMachineLocation to subsequently affect the next 
        // user.
        //
        // TODO Should perhaps extract the location storage methods into their own class,
        // but no strong enough feelings yet...
        
        checkNotNull(loc, "location");
        if (loc.getConfig(AbstractLocation.TEMPORARY_LOCATION)) {
            log.info("Ignoring location lifecycle usage event for {} (state {}), because location is a temporary location", loc, state);
            return;
        }
        checkNotNull(state, "state of location %s", loc);
        if (loc.getId() == null) {
            log.error("Ignoring location lifecycle usage event for {} (state {}), because location has no id", loc, state);
            return;
        }
        if (managementContext.getStorage() == null) {
            log.warn("Cannot store location lifecycle usage event for {} (state {}), because storage not available", loc, state);
            return;
        }
        
        Object callerContext = loc.getConfig(LocationConfigKeys.CALLER_CONTEXT);
        
        if (callerContext != null && callerContext instanceof Entity) {
            Entity caller = (Entity) callerContext;
            recordLocationEvent(loc, caller, state);
        } else {
            // normal for high-level locations
            log.trace("Not recording location lifecycle usage event for {} in state {}, because no caller context", new Object[] {loc, state});
        }
    }
    
    protected void recordLocationEvent(final Location loc, final Entity caller, final Lifecycle state) {
        log.debug("Storing location lifecycle usage event: location {} in state {}; caller context {}", new Object[] {loc, state, caller});
        ConcurrentMap<String, LocationUsage> eventMap = managementContext.getStorage().<String, LocationUsage>getMap(LOCATION_USAGE_KEY);
        
        String entityTypeName = caller.getEntityType().getName();
        String appId = caller.getApplicationId();

        final LocationUsage.LocationEvent event = new LocationUsage.LocationEvent(state, caller.getId(), entityTypeName, appId, getUser());
        
        
        // Don't call out to alien-code (i.e. loc.toMetadataRecord()) while holding mutex. It might take a while,
        // e.g. ssh'ing to the machine!
        // If we don't have a usage record, then generate one outside of the mutex. But then double-check while
        // holding the mutex to see if another thread has created one. If it has, stick with that rather than 
        // overwriting it.
        LocationUsage usage;
        synchronized (mutex) {
            usage = eventMap.get(loc.getId());
        }
        if (usage == null) {
            usage = new LocationUsage(loc.getId(), ((LocationInternal)loc).toMetadataRecord());
        }
        
        synchronized (mutex) {
            LocationUsage otherUsage = eventMap.get(loc.getId());
            if (otherUsage != null) {
                usage = otherUsage;
            }
            usage.addEvent(event);
            eventMap.put(loc.getId(), usage);
            
            execOnListeners(new Function<UsageListener, Void>() {
                    @Override
                    public Void apply(UsageListener listener) {
                        listener.onLocationEvent(new LocationMetadataImpl(loc), event);
                        return null;
                    }
                    @Override
                    public String toString() {
                        return "locationEvent("+loc+", "+state+")";
                    }});
        }
    }

    /**
     * Returns the usage info for the location with the given id, or null if unknown.
     */
    @Override
    public LocationUsage getLocationUsage(String locationId) {
        BrooklynStorage storage = managementContext.getStorage();

        Map<String, LocationUsage> usageMap = storage.getMap(LOCATION_USAGE_KEY);
        return usageMap.get(locationId);
    }
    
    /**
     * Returns the usage info that matches the given predicate.
     * For example, could be used to find locations used within a given time period.
     */
    @Override
    public Set<LocationUsage> getLocationUsage(Predicate<? super LocationUsage> filter) {
        // TODO could do more efficient indexing, to more easily find locations in use during a given period.
        // But this is good enough for first-pass.

        Map<String, LocationUsage> usageMap = managementContext.getStorage().getMap(LOCATION_USAGE_KEY);
        Set<LocationUsage> result = Sets.newLinkedHashSet();
        
        for (LocationUsage usage : usageMap.values()) {
            if (filter.apply(usage)) {
                result.add(usage);
            }
        }
        return result;
    }
    
    /**
     * Returns the usage info for the location with the given id, or null if unknown.
     */
    @Override
    public ApplicationUsage getApplicationUsage(String appId) {
        BrooklynStorage storage = managementContext.getStorage();

        Map<String, ApplicationUsage> usageMap = storage.getMap(APPLICATION_USAGE_KEY);
        return usageMap.get(appId);
    }
    
    /**
     * Returns the usage info that matches the given predicate.
     * For example, could be used to find applications used within a given time period.
     */
    @Override
    public Set<ApplicationUsage> getApplicationUsage(Predicate<? super ApplicationUsage> filter) {
        // TODO could do more efficient indexing, to more easily find locations in use during a given period.
        // But this is good enough for first-pass.

        Map<String, ApplicationUsage> usageMap = managementContext.getStorage().getMap(APPLICATION_USAGE_KEY);
        Set<ApplicationUsage> result = Sets.newLinkedHashSet();
        
        for (ApplicationUsage usage : usageMap.values()) {
            if (filter.apply(usage)) {
                result.add(usage);
            }
        }
        return result;
    }

    @Override
    public void addUsageListener(UsageListener listener) {
        listeners.add(checkNotNull(listener, "listener"));
    }

    @Override
    public void removeUsageListener(UsageListener listener) {
        listeners.remove(listener);
    }

    private String getUser() {
        EntitlementContext entitlementContext = Entitlements.getEntitlementContext();
        if (entitlementContext != null) {
            return entitlementContext.user();
        }
        return null;
    }
}
