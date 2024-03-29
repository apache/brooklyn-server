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

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.EntityTypeRegistry;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.internal.BrooklynLoggingCategories;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.AccessController;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.entitlement.EntitlementContext;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.entity.trait.Startable;
import org.apache.brooklyn.core.internal.storage.BrooklynStorage;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.entitlement.Entitlements;
import org.apache.brooklyn.core.mgmt.entitlement.WebEntitlementContext;
import org.apache.brooklyn.core.objs.BasicEntityTypeRegistry;
import org.apache.brooklyn.core.objs.proxy.EntityProxy;
import org.apache.brooklyn.core.objs.proxy.EntityProxyImpl;
import org.apache.brooklyn.core.objs.proxy.InternalEntityFactory;
import org.apache.brooklyn.core.objs.proxy.InternalPolicyFactory;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.collections.SetFromLiveMap;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

public class LocalEntityManager implements EntityManagerInternal {

    private static final Logger log = LoggerFactory.getLogger(LocalEntityManager.class);

    /**
     * Regex used for validating entity ids that are passed in, for use when creating an entity.
     * 
     * Only lower-case letters and digits; min 10 chars; max 63 chars. We are this extreme because 
     * some existing entity implementations rely on the entity-id format for use in hostnames, etc.
     */
    private static final Pattern ENTITY_ID_PATTERN = Pattern.compile("[a-z0-9]{10,63}");
    
    private final LocalManagementContext managementContext;
    private final BasicEntityTypeRegistry entityTypeRegistry;
    private final InternalEntityFactory entityFactory;
    private final InternalPolicyFactory policyFactory;
    
    /** Entities that have been created, but have not yet begun to be managed */
    private final Map<String,Entity> preRegisteredEntitiesById = Collections.synchronizedMap(new WeakHashMap<String, Entity>());

    /** Entities that are in the process of being managed, but where management is not yet complete */
    private final Map<String,Entity> preManagedEntitiesById = Collections.synchronizedMap(new WeakHashMap<String, Entity>());
    
    /** Proxies of the managed entities */
    private final ConcurrentMap<String,Entity> entityProxiesById = Maps.newConcurrentMap();
    
    /** Real managed entities */
    private final Map<String,Entity> entitiesById = Maps.newLinkedHashMap();
    
    /** Management mode for each entity */
    private final Map<String,ManagementTransitionMode> entityModesById = Collections.synchronizedMap(Maps.<String,ManagementTransitionMode>newLinkedHashMap());

    /**
     * Proxies of the managed entities.
     * 
     * Access to this is always done in a synchronized block (synchronizing on `this`).
     */
    private final ObservableSet<Entity> entities = new ObservableSet<Entity>();
    
    /** Proxies of the managed entities that are applications */
    private final Set<Application> applications = Sets.newConcurrentHashSet();

    private final BrooklynStorage storage;
    private final Map<String,String> entityTypes;
    private final Set<String> applicationIds;

    public LocalEntityManager(LocalManagementContext managementContext) {
        this.managementContext = checkNotNull(managementContext, "managementContext");
        this.storage = managementContext.getStorage();
        this.entityTypeRegistry = new BasicEntityTypeRegistry();
        this.policyFactory = new InternalPolicyFactory(managementContext);
        this.entityFactory = new InternalEntityFactory(managementContext, entityTypeRegistry, policyFactory);
        
        entityTypes = storage.getMap("entities");
        applicationIds = SetFromLiveMap.create(storage.<String,Boolean>getMap("applications"));
    }

    public InternalEntityFactory getEntityFactory() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return entityFactory;
    }

    public InternalPolicyFactory getPolicyFactory() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return policyFactory;
    }

    @Override
    public EntityTypeRegistry getEntityTypeRegistry() {
        if (!isRunning()) throw new IllegalStateException("Management context no longer running");
        return entityTypeRegistry;
    }

    @Override
    public <T extends Entity> T createEntity(EntitySpec<T> spec, EntityCreationOptions options) {
        String entityId = options.getRequiredUniqueId();

        if (entityId!=null) {
            if (!ENTITY_ID_PATTERN.matcher(entityId).matches()) {
                throw new IllegalArgumentException("Invalid entity id '"+entityId+"'");
            }
        }
        
        try {
            T entity = entityFactory.createEntity(spec, options);

            if (options.isDryRun()) {
                unmanageDryRun(entity);
                // also need to do this to remove tasks etc
                managementContext.getGarbageCollector().onUnmanaged(entity);
                return entity;

            } else {
                Entity proxy = ((AbstractEntity)entity).getProxy();
                checkNotNull(proxy, "proxy for entity %s, spec %s", entity, spec);

                manage(entity);
                if (options.persistAfterCreation()) {
                    try {
                        managementContext.getRebindManager().forcePersistNow(false, null);
                    } catch (Exception e) {
                        log.warn("Error persisting "+entity+" after creation; will unmanage then rethrow: "+e);
                        try {
                            unmanage(entity);
                        } catch (Exception e2) {
                            log.error("Could not unmanage "+entity+" which failed persistence after creation; manual removal will be required: "+e2, e2);
                        }
                        throw Exceptions.propagate(e);
                    }
                }
                return (T) proxy;
            }

        } catch (Throwable e) {
            // this may be expected, eg if parent is being unmanaged; rely on catcher to handle
            log.debug("Failed to create entity using spec "+spec+" (rethrowing)", e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public <T extends Entity> T createEntity(Map<?,?> config, Class<T> type) {
        return createEntity(EntitySpec.create(config, type));
    }

    @Override
    public <T extends Policy> T createPolicy(PolicySpec<T> spec) {
        try {
            return policyFactory.createPolicy(spec);
        } catch (Throwable e) {
            log.warn("Failed to create policy using spec "+spec+" (rethrowing)", e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public <T extends Enricher> T createEnricher(EnricherSpec<T> spec) {
        try {
            return policyFactory.createEnricher(spec);
        } catch (Throwable e) {
            log.warn("Failed to create enricher using spec "+spec+" (rethrowing)", e);
            throw Exceptions.propagate(e);
        }
    }

    @Override
    public Collection<Entity> getEntities() {
        return ImmutableList.copyOf(entityProxiesById.values());
    }
    
    @Override
    public Collection<String> getEntityIds() {
        return ImmutableList.copyOf(entityProxiesById.keySet());
    }
    
    @Override
    public Collection<Entity> getEntitiesInApplication(Application application) {
        Predicate<Entity> predicate = EntityPredicates.applicationIdEqualTo(application.getId());
        return ImmutableList.copyOf(Iterables.filter(entityProxiesById.values(), predicate));
    }

    @Override
    public Collection<Entity> findEntities(Predicate<? super Entity> filter) {
        return ImmutableList.copyOf(Iterables.filter(entityProxiesById.values(), filter));
    }
    
    @Override
    public Collection<Entity> findEntitiesInApplication(Application application, Predicate<? super Entity> filter) {
        Predicate<Entity> predicate = Predicates.and(EntityPredicates.applicationIdEqualTo(application.getId()), filter);
        return ImmutableList.copyOf(Iterables.filter(entityProxiesById.values(), predicate));
    }

    @Override
    public Iterable<Entity> getAllEntitiesInApplication(Application application) {
        // To fix https://issues.apache.org/jira/browse/BROOKLYN-352, we need to synchronize on
        // preRegisteredEntitiesById and preManagedEntitiesById while iterating over them (because
        // they are synchronizedMaps). entityProxiesById is a ConcurrentMap, so no need to 
        // synchronize on that.
        // Only synchronize on one at a time, to avoid the risk of deadlock.
        
        Predicate<Entity> predicate = EntityPredicates.applicationIdEqualTo(application.getId());
        Set<Entity> result = Sets.newLinkedHashSet();
        
        synchronized (preRegisteredEntitiesById) {
            for (Entity entity : preRegisteredEntitiesById.values()) {
                if (predicate.apply(entity)) {
                    result.add(entity);
                }
            }
        }
        synchronized (preManagedEntitiesById) {
            for (Entity entity : preManagedEntitiesById.values()) {
                if (predicate.apply(entity)) {
                    result.add(entity);
                }
            }
        }
        for (Entity entity : entityProxiesById.values()) {
            if (predicate.apply(entity)) {
                result.add(entity);
            }
        }
        
        return FluentIterable.from(result)
                .transform(new Function<Entity, Entity>() {
                    @Override public Entity apply(Entity input) {
                        return Entities.proxy(input);
                    }})
                .toSet();
    }

    @Override
    public Entity getEntity(String id) {
        return entityProxiesById.get(id);
    }
    
    Collection<Application> getApplications() {
        return ImmutableList.copyOf(applications);
    }
    
    @Override
    public boolean isManaged(Entity e) {
        // Confirm we know about this entity (by id), and that it is the same entity instance
        // (rather than just a different unmanaged entity with the same id).
        return (isRunning() && getEntity(e.getId()) != null) && (entitiesById.get(e.getId()) == deproxyIfNecessary(e));
    }
    
    boolean isPreRegistered(Entity e) {
        return preRegisteredEntitiesById.containsKey(e.getId());
    }
    
    void prePreManage(Entity entity) {
        if (isPreRegistered(entity)) {
            log.warn(""+this+" redundant call to pre-pre-manage entity "+entity+"; skipping", 
                    new Exception("source of duplicate pre-pre-manage of "+entity));
            return;
        }
        preRegisteredEntitiesById.put(entity.getId(), entity);
    }
    
    @Override
    public ManagementTransitionMode getLastManagementTransitionMode(String itemId) {
        return entityModesById.get(itemId);
    }
    
    @Override
    public void setManagementTransitionMode(Entity item, ManagementTransitionMode mode) {
        entityModesById.put(item.getId(), mode);
    }
    
    // TODO synchronization issues here. We guard with isManaged(), but if another thread executing 
    // concurrently then the managed'ness could be set after our check but before we do 
    // onManagementStarting etc. However, we can't just synchronize because we're calling alien code 
    // (the user might override entity.onManagementStarting etc).
    // 
    // TODO We need to do some check about isPreManaged - i.e. is there another thread (or is this a
    // re-entrant call) where the entity is not yet full managed (i.e. isManaged==false) but we're in
    // the middle of managing it.
    // 
    // TODO Also see LocalLocationManager.manage(Entity), if fixing things here
    @Override
    public void manage(Entity e) {
        if (isManaged(e)) {
            log.warn(""+this+" redundant call to start management of entity (and descendants of) "+e+"; skipping", 
                    new Exception("source of duplicate management of "+e));
            return;
        }
        manageRecursive(e, ManagementTransitionMode.guessing(BrooklynObjectManagementMode.NONEXISTENT, BrooklynObjectManagementMode.MANAGED_PRIMARY));
    }

    @Override
    public void manageRebindedRoot(Entity item) {
        ManagementTransitionMode mode = getLastManagementTransitionMode(item.getId());
        Preconditions.checkNotNull(mode, "Mode not set for rebinding %s", item);
        manageRecursive(item, mode);
    }
    
    protected void checkManagementAllowed(Entity item) {
        AccessController.Response access = managementContext.getAccessController().canManageEntity(item);
        if (!access.isAllowed()) {
            throw new IllegalStateException("Access controller forbids management of "+item+": "+access.getMsg());
        }
    }
    
    /* TODO we sloppily use "recursive" to ensure ordering of parent-first in many places
     * (which may not be necessary but seems like a good idea),
     * and also to collect many entities when doing a big rebind,
     * ensuring all have #manageNonRecursive called before calling #onManagementStarted.
     * 
     * it would be better to have a manageAll(Map<Entity,ManagementTransitionMode> items)
     * method which did that in two phases, allowing us to selectively rebind, 
     * esp when we come to want supporting different modes and different brooklyn nodes.
     * 
     * the impl of manageAll could sort them with parents before children,
     * (and manageRecursive could simply populate a map and delegate to manageAll).
     * 
     * manageRebindRoot would then go, and the (few) callers would construct the map.
     * 
     * similarly we might want an unmanageAll(), 
     * although possibly all unmanagement should be recursive, if we assume an entity's ancestors are always at least proxied
     * (and the non-recursive RO path here could maybe be dropped)
     */
    
    /** Applies management lifecycle callbacks (onManagementStarting, for all beforehand, then onManagementStopped, for all after) */
    protected void manageRecursive(Entity e, final ManagementTransitionMode initialMode) {
        checkManagementAllowed(e);

        final List<EntityInternal> allEntities = Lists.newArrayList();
        Predicate<EntityInternal> manageEntity = new Predicate<EntityInternal>() { @Override public boolean apply(EntityInternal it) {
            ManagementTransitionMode mode = getLastManagementTransitionMode(it.getId());
            if (mode==null) {
                setManagementTransitionMode(it, mode = initialMode);
            }
            log.debug("Starting management of {} mode {}", it, mode);

            Boolean isReadOnlyFromEntity = it.getManagementSupport().isReadOnlyRaw();
            if (isReadOnlyFromEntity==null) {
                if (mode.isReadOnly()) {
                    // should have been marked by rebinder
                    log.warn("Read-only entity "+it+" not marked as such on call to manage; marking and continuing");
                }
                it.getManagementSupport().setReadOnly(mode.isReadOnly());
            } else {
                if (!isReadOnlyFromEntity.equals(mode.isReadOnly())) {
                    log.warn("Read-only status at entity "+it+" ("+isReadOnlyFromEntity+") not consistent with management mode "+mode);
                }
            }
            
            if (it.getManagementSupport().isDeployed()) {
                if (mode.wasNotLoaded()) {
                    // silently bail out
                    return false;
                } else {
                    if (mode.wasPrimary() && mode.isPrimary()) {
                        // active partial rebind; continue
                    } else if (mode.wasReadOnly() && mode.isReadOnly()) {
                        // reload in RO mode
                    } else {
                        // on initial non-RO rebind, should not have any deployed instances
                        log.warn("Already deployed "+it+" when managing "+mode+"/"+initialMode+"; ignoring this and all descendants");
                        return false;
                    }
                }
            }
            
            // check RO status is consistent
            boolean isNowReadOnly = Boolean.TRUE.equals( it.getManagementSupport().isReadOnly() );
            if (mode.isReadOnly()!=isNowReadOnly) {
                throw new IllegalStateException("Read-only status mismatch for "+it+": "+mode+" / RO="+isNowReadOnly);
            }

            allEntities.add(it);
            preManageNonRecursive(it, mode);
            it.getManagementSupport().onManagementStarting( new ManagementTransitionInfo(managementContext, mode) ); 
            return manageNonRecursive(it, mode);
        } };
        boolean isRecursive = true;
        if (initialMode.wasPrimary() && initialMode.isPrimary()) {
            // already managed, so this shouldn't be recursive 
            // (in ActivePartialRebind we cheat, calling in to this method then skipping recursion).
            // it also falls through to here when doing a redundant promotion,
            // in that case we *should* be recursive; determine by checking whether a child exists and is preregistered.
            // the TODO above removing manageRebindRoot in favour of explicit mgmt list would clean this up a lot!
            Entity aChild = Iterables.getFirst(e.getChildren(), null);
            if (aChild!=null && isPreRegistered(aChild)) {
                log.debug("Managing "+e+" in mode "+initialMode+", doing this recursively because a child is preregistered");
            } else {
                log.debug("Managing "+e+" but skipping recursion, as mode is "+initialMode);
                isRecursive = false;
            }
        }
        if (!isRecursive) {
            manageEntity.apply( (EntityInternal)e );
        } else {
            recursively(e, manageEntity);
        }
        
        for (EntityInternal it : allEntities) {
            if (!it.getManagementSupport().isFullyManaged()) {
                ManagementTransitionMode mode = getLastManagementTransitionMode(it.getId());
                ManagementTransitionInfo info = new ManagementTransitionInfo(managementContext, mode);
                
                it.getManagementSupport().onManagementStarted(info);
                managementContext.getRebindManager().getChangeListener().onManaged(it);
            }
        }
    }

    @Override
    public void unmanage(final Entity e) {
        // TODO don't want to guess; should we inspect state of e ?  or maybe it doesn't matter ?
        unmanage(e, ManagementTransitionMode.guessing(BrooklynObjectManagementMode.MANAGED_PRIMARY, BrooklynObjectManagementMode.NONEXISTENT));
    }
    
    @Override
    public void unmanage(final Entity e, final ManagementTransitionMode mode) {
        unmanage(e, mode, false);
    }

    @Override
    public void discardPremanaged(final Entity e) {
        if (e == null) return;
        if (!isRunning()) return;
        
        Set<String> todiscard = new LinkedHashSet<>();
        Stack<Entity> tovisit = new Stack<>();
        Set<Entity> visited = new LinkedHashSet<>();
        
        tovisit.push(e);
        
        while (!tovisit.isEmpty()) {
            Entity next = tovisit.pop();
            visited.add(next);
            for (Entity child : next.getChildren()) {
                if (!visited.contains(child)) {
                    tovisit.push(child);
                }
            }
            
            if (isManaged(next)) {
                throw new IllegalStateException("Cannot discard entity "+e+" because it or a descendent is already managed ("+next+")");
            }
            Entity realNext = deproxyIfNecessary(next);
            String id = next.getId();
            Entity realFound = preRegisteredEntitiesById.get(id);
            if (realFound == null) preManagedEntitiesById.get(id);

            if (realFound != null && realFound != realNext) {
                throw new IllegalStateException("Cannot discard pre-managed entity "+e+" because it or a descendent's id ("+id+") clashes with a different entity (given "+next+" but found "+realFound+")");
            }
    
            todiscard.add(id);
        }

        for (String id : todiscard) {
            preRegisteredEntitiesById.remove(id);
            preManagedEntitiesById.remove(id);
        }
    }

    private void unmanageDryRun(final Entity e) {
        final ManagementTransitionInfo info = new ManagementTransitionInfo(managementContext,
                ManagementTransitionMode.transitioning(BrooklynObjectManagementMode.NONEXISTENT, BrooklynObjectManagementMode.NONEXISTENT));
        log.debug("Unmanaging "+e+" (dry run)");
        discardPremanaged(e);
        ((EntityInternal)e).getManagementSupport().onManagementStopping(info, true);
        stopTasks(e);
        ((EntityInternal)e).getManagementSupport().onManagementStopped(info, true);
    }

    private void unmanage(final Entity e, ManagementTransitionMode mode, boolean hasBeenReplaced) {
        if (shouldSkipUnmanagement(e, hasBeenReplaced)) return;
        final ManagementTransitionInfo info = new ManagementTransitionInfo(managementContext, mode);
        log.debug("Unmanaging "+e+" (mode "+mode+", replaced "+hasBeenReplaced+")");
        if (hasBeenReplaced) {
            // we are unmanaging an old instance after having replaced it
            // don't unmanage or even clear its fields, because there might be references to it
            
            if (mode.wasReadOnly()) {
                // if coming *from* read only; nothing needed
            } else {
                if (!mode.wasPrimary()) {
                    log.warn("Unexpected mode "+mode+" for unmanage-replace "+e+" (applying anyway)");
                }
                // migrating away or in-place active partial rebind:
                ((EntityInternal)e).getManagementSupport().onManagementStopping(info, false);
                stopTasks(e);
                ((EntityInternal)e).getManagementSupport().onManagementStopped(info, false);
            }
            // do not remove from maps below, bail out now
            return;
            
        } else if (mode.wasReadOnly() && mode.isNoLongerLoaded()) {
            // we are unmanaging an instance (secondary); either stopping here or primary destroyed elsewhere
            ((EntityInternal)e).getManagementSupport().onManagementStopping(info, false);
            if (unmanageNonRecursive(e)) {
                stopTasks(e);
            }
            ((EntityInternal)e).getManagementSupport().onManagementStopped(info, false);
            managementContext.getRebindManager().getChangeListener().onUnmanaged(e);
            if (managementContext.getGarbageCollector() != null) managementContext.getGarbageCollector().onUnmanaged(e);
            
        } else if (mode.wasPrimary() && mode.isNoLongerLoaded()) {
            // unmanaging a primary; currently this is done recursively
            
            /* TODO tidy up when it is recursive and when it isn't; if something is being unloaded or destroyed,
             * that probably *is* recursive, but the old mode might be different if in some cases things are read-only.
             * or maybe nothing needs to be recursive, we just make sure the callers (e.g. HighAvailabilityModeImpl.clearManagedItems)
             * call in a good order
             * 
             * see notes above about recursive/manage/All/unmanageAll
             */
            
            // Need to store all child entities as onManagementStopping removes a child from the parent entity
            final Set<EntityInternal> allEntities =  new LinkedHashSet<>();
            int iteration = 0;

            do {
                List<Entity> entitiesToUnmanageRecursively = MutableList.of();

                if (iteration>0) {
                    log.info("Re-running descendant unmanagement on descendants of "+e+" which were added concurrently during previous iteration: "+allEntities);
                    if (iteration>=20) throw new IllegalStateException("Too many iterations detected trying to unmanage descendants of "+e+" ("+iteration+")");
                    entitiesToUnmanageRecursively.addAll(allEntities);
                } else {
                    entitiesToUnmanageRecursively.add(e);
                }

                entitiesToUnmanageRecursively.forEach(ei -> recursively(ei, new Predicate<EntityInternal>() {
                    @Override
                    public boolean apply(EntityInternal it) {
                        if (shouldSkipUnmanagement(it, false)) return false;
                        allEntities.add(it);
                        it.getManagementSupport().onManagementStopping(info, false);
                        return true;
                    }
                }));

                if (!allEntities.isEmpty()) {
                    MutableList<EntityInternal> allEntitiesExceptApp = MutableList.copyOf(allEntities);
                    EntityInternal app = allEntitiesExceptApp.remove(0);
                    Collections.reverse(allEntitiesExceptApp);
                    // log in reverse order, so that ancestor nodes logged later because they are more interesting
                    // (and application is the last one logged)
                    allEntitiesExceptApp.forEach(it -> {
                        BrooklynLoggingCategories.ENTITY_LIFECYCLE_LOG.debug("Deleting entity " + it.getId() + " (" + it + ") in application " + it.getApplicationId() + " for user " + Entitlements.getEntitlementContextUser());
                    });
                    BrooklynLoggingCategories.APPLICATION_LIFECYCLE_LOG.debug("Deleting application " + app.getId() + " (" + app + ") mode " + mode + " for user " + Entitlements.getEntitlementContextUser());
                }

                for (EntityInternal it : allEntities) {
                    if (shouldSkipUnmanagement(it, false)) continue;

                    if (unmanageNonRecursive(it)) {
                        stopTasks(it);
                    }
                }
                for (EntityInternal it : allEntities) {
                    it.getManagementSupport().onManagementStopped(info, false);
                    managementContext.getRebindManager().getChangeListener().onUnmanaged(it);
                    if (managementContext.getGarbageCollector() != null)
                        managementContext.getGarbageCollector().onUnmanaged(e);
                }

                // re-run in case a child has been added
                final Set<EntityInternal> allEntitiesAgain = new LinkedHashSet<>();
                // here loop through each entity because the children will normally be cleared during unmanagement
                allEntities.forEach(ei -> recursively(ei, new Predicate<EntityInternal>() {
                    @Override
                    public boolean apply(EntityInternal it) {
                        allEntitiesAgain.add(it);
                        return true;
                    }
                }));
                allEntitiesAgain.removeAll(allEntities);
                allEntities.clear();
                allEntities.addAll(allEntitiesAgain);
                iteration++;

            } while (!allEntities.isEmpty());

            
        } else {
            log.warn("Invalid mode for unmanage: "+mode+" on "+e+" (ignoring)");
        }
        
        preRegisteredEntitiesById.remove(e.getId());
        preManagedEntitiesById.remove(e.getId());
        entityProxiesById.remove(e.getId());
        entitiesById.remove(e.getId());
        entityModesById.remove(e.getId());
    }
    
    private void stopTasks(Entity entity) {
        stopTasks(entity, null);
    }
    
    /** stops all tasks (apart from any current one or its descendants) on this entity,
     * optionally -- if a timeout is given -- waiting for completion and warning on incomplete tasks */
    @Beta
    public void stopTasks(Entity entity, @Nullable Duration timeout) {
        CountdownTimer timeleft = timeout==null ? null : timeout.countdownTimer();
        // try forcibly interrupting tasks on managed entities
        Collection<Exception> exceptions = MutableSet.of();
        try {
            boolean inTaskForThisEntity = entity.equals(BrooklynTaskTags.getContextEntity(Tasks.current()));
            Set<String> currentAncestorIds = null;
            Set<Task<?>> tasksCancelled = MutableSet.of();
            Set<Task<?>> tasks;
            try {
                tasks = managementContext.getExecutionContext(entity).getTasks();
            } catch (Exception e) {
                log.debug("Unable to stop tasks for "+entity+"; probably it was added but management cancelled: "+e);
                log.trace("Trace for failure to stop tasks", e);
                return;
            }
            for (Task<?> t: tasks) {
                if (inTaskForThisEntity) {
                    if (currentAncestorIds==null) {
                        currentAncestorIds = getAncestorTaskIds(Tasks.current());
                    }
                    if (getAncestorTaskIds(t).stream().anyMatch(currentAncestorIds::contains)) {
                        // don't cancel the task if:
                        // - the current task is against this entity, and
                        // - the current task and target task are part of the same root tree
                        // e.g. on "stop" don't cancel ourselves, don't cancel things our ancestors have submitted
                        // (direct ancestry check is not good enough, because we might be in a subtask of a deletion which has a DST manager,
                        // and cancelling the DST manager is almost as bad as cancelling ourselves);
                        // however if our current task is from another entity we maybe do want to cancel other things running at this node
                        // (although maybe not; we could remote the "inTaskForThisEntity" check)
                        continue;
                    }
                }
                
                if (!t.isDone()) {
                    try {
                        log.debug("Cancelling "+t+" on "+entity);
                        tasksCancelled.add(t);
                        t.cancel(true);
                    } catch (Exception e) {
                        Exceptions.propagateIfFatal(e);
                        log.debug("Error cancelling "+t+" on "+entity+" (will warn when all tasks are cancelled): "+e, e);
                        exceptions.add(e);
                    }
                }
            }
            
            if (timeleft!=null) {
                Set<Task<?>> tasksIncomplete = MutableSet.of();
                // go through all tasks, not just cancelled ones, in case there are previously cancelled ones which are not complete
                for (Task<?> t: managementContext.getExecutionContext(entity).getTasks()) {
                    if (hasTaskAsAncestor(t, Tasks.current()))
                        continue;
                    if (!Tasks.blockUntilInternalTasksEnded(t, timeleft.getDurationRemaining())) {
                        tasksIncomplete.add(t);
                    }
                }
                if (!tasksIncomplete.isEmpty()) {
                    log.warn("Incomplete tasks when stopping "+entity+": "+tasksIncomplete);
                }
                if (log.isTraceEnabled())
                    log.trace("Cancelled "+tasksCancelled+" tasks for "+entity+", with "+
                            timeleft.getDurationRemaining()+" remaining (of "+timeout+"): "+tasksCancelled);
            } else {
                if (log.isTraceEnabled())
                    log.trace("Cancelled "+tasksCancelled+" tasks for "+entity+": "+tasksCancelled);
            }
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.warn("Error inspecting tasks to cancel on unmanagement: "+e, e);
        }
        if (!exceptions.isEmpty())
            log.warn("Error when cancelling tasks for "+entity+" on unmanagement: "+Exceptions.create(exceptions));
    }

    private boolean hasTaskAsAncestor(Task<?> t, Task<?> potentialAncestor) {
        if (t==null || potentialAncestor==null) return false;
        if (t.equals(potentialAncestor)) return true;
        return hasTaskAsAncestor(t.getSubmittedByTask(), potentialAncestor);
    }

    private Set<String> getAncestorTaskIds(Task<?> t) {
        List<String> ancestorIds = MutableList.of();
        while (t!=null) {
            ancestorIds.add(t.getId());
            t = t.getSubmittedByTask();
        }
        Collections.reverse(ancestorIds);
        return MutableSet.copyOf(ancestorIds);
    }

    /**
     * activates management when effector invoked, warning unless context is acceptable
     * (currently only acceptable context is "start")
     */
    void manageIfNecessary(Entity entity, Object context) {
        if (!isRunning()) {
            return; // TODO Still a race for terminate being called, and then isManaged below returning false
        } else if (((EntityInternal)entity).getManagementSupport().wasDeployed()) {
            return;
        } else if (isManaged(entity)) {
            return;
        } else if (isPreManaged(entity)) {
            return;
        } else if (Boolean.TRUE.equals(((EntityInternal)entity).getManagementSupport().isReadOnly())) {
            return;
        } else {
            Entity rootUnmanaged = entity;
            while (true) {
                Entity candidateUnmanagedParent = rootUnmanaged.getParent();
                if (candidateUnmanagedParent == null || isManaged(candidateUnmanagedParent) || isPreManaged(candidateUnmanagedParent))
                    break;
                rootUnmanaged = candidateUnmanagedParent;
            }
            if (context == Startable.START.getName())
                log.info("Activating local management for {} on start", rootUnmanaged);
            else
                log.warn("Activating local management for {} due to effector invocation on {}: {}", new Object[]{rootUnmanaged, entity, context});
            manage(rootUnmanaged);
        }
    }

    private void recursively(Entity e, Predicate<EntityInternal> action) {
        Entity otherPreregistered = preRegisteredEntitiesById.get(e.getId());
        if (otherPreregistered!=null) {
            // if something has been pre-registered, prefer it
            // (e.g. if we recursing through children, we might have a proxy from previous iteration;
            // the most recent will have been pre-registered)
            e = otherPreregistered;
        }
            
        boolean success = action.apply( (EntityInternal)e );
        if (!success) {
            return; // Don't manage children if action false/unnecessary for parent
        }
        for (Entity child : e.getChildren()) {
            recursively(child, action);
        }
    }

    /**
     * Whether the entity is in the process of being managed.
     */
    private synchronized boolean isPreManaged(Entity e) {
        return preManagedEntitiesById.containsKey(e.getId());
    }

    /**
     * Should ensure that the entity is now known about, but should not be accessible from other entities yet.
     * 
     * Records that the given entity is about to be managed (used for answering {@link #isPreManaged(Entity)}.
     * Note that refs to the given entity are stored in a a weak hashmap so if the subsequent management
     * attempt fails then this reference to the entity will eventually be discarded (if no-one else holds 
     * a reference).
     */
    private synchronized boolean preManageNonRecursive(Entity e, ManagementTransitionMode mode) {
        Entity realE = toRealEntity(e);
        
        Object old = preManagedEntitiesById.put(e.getId(), realE);
        Object pre = preRegisteredEntitiesById.remove(e.getId());
        
        if (old!=null && mode.wasNotLoaded()) {
            if (old.equals(e)) {
                log.warn("{} redundant call to pre-start management of entity {}, mode {}; ignoring", new Object[] { this, e, mode });
            } else {
                throw new IllegalStateException("call to pre-manage entity "+e+" ("+mode+") but different entity "+old+" already known under that id at "+this);
            }
            return false;
        } else {
            if (pre==null) {
                // assume everything is pre-pre-managed
                preManagedEntitiesById.remove(e.getId());
                throw new IllegalStateException("Entity "+e+" not known as pre-registered when starting management of it; probably it was unmanaged concurrently");
            }
            if (log.isTraceEnabled()) log.trace("{} pre-start management of entity {}, mode {}", 
                new Object[] { this, e, mode });
            return true;
        }
    }

    /**
     * Should ensure that the entity is now managed somewhere, and known about in all the lists.
     * Returns true if the entity has now become managed; false if it was already managed (anything else throws exception)
     */
    private synchronized boolean manageNonRecursive(Entity e, ManagementTransitionMode mode) {
        Entity old = entitiesById.get(e.getId());
        
        if (old!=null && mode.wasNotLoaded()) {
            if (old == deproxyIfNecessary(e)) {
                log.warn("{} redundant call to start management of entity {}; ignoring", this, e);
            } else {
                throw new IdAlreadyExistsException("call to manage entity "+e+" ("+mode+") but "
                        + "different entity "+old+" already known under that id '"+e.getId()+"' at "+this);
            }
            return false;
        }

        BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(e),
                "{} starting management of entity {}", this, e);
        Entity realE = toRealEntity(e);
        
        Entity oldProxy = entityProxiesById.get(e.getId());
        Entity proxyE;
        if (oldProxy!=null) {
            if (mode.wasNotLoaded()) {
                throw new IdAlreadyExistsException("call to manage entity "+e+" from unloaded "
                        + "state ("+mode+") but already had proxy "+oldProxy+" already known "
                        + "under that id '"+e.getId()+"' at "+this);
            }
            // make the old proxy point at this new delegate
            // (some other tricks done in the call below)
            ((EntityProxyImpl)(Proxy.getInvocationHandler(oldProxy))).resetDelegate(oldProxy, oldProxy, realE);
            proxyE = oldProxy;
        } else {
            proxyE = toProxyEntityIfAvailable(e);
        }
        entityProxiesById.put(e.getId(), proxyE);
        entityTypes.put(e.getId(), realE.getClass().getName());
        entitiesById.put(e.getId(), realE);

        Entity preManaged = preManagedEntitiesById.remove(e.getId());
        if (preManaged==null) {
            // assume everything is pre-managed
            log.warn(this+" cannot start management for "+e+" because it is not or no longer pre-managed; probably its ancestor was concurrently unmanaged (unmanaging then throwing)");
            unmanage(e, mode);
            throw new IllegalStateException(this+" cannot start management for "+e+" because it is not or no longer pre-managed; probably its ancestor was concurrently unmanaged");
        }

        if ((e instanceof Application) && (e.getParent()==null)) {
            applications.add((Application)proxyE);
            applicationIds.add(e.getId());
        }
        if (!entities.contains(proxyE)) 
            entities.add(proxyE);
        
        if (old!=null && old!=e) {
            // passing the transition info will ensure the right shutdown steps invoked for old instance
            log.debug("Unmanaging previous instance {} being replaced by {} {}", old, e, mode);
            unmanage(old, mode, true);
        }
        
        return true;
    }

    /**
     * Should ensure that the entity is no longer managed anywhere, remove from all lists.
     * Returns true if the entity has been removed from management; false if it is not known or pre-pre-managed (anything else throws exception)
     */
    private boolean unmanageNonRecursive(Entity e) {
        /*
         * When method is synchronized, hit deadlock: 
         * 1. thread called unmanage() on a member of a group, so we got the lock and called group.removeMember;
         *    this ties to synchronize on AbstractGroupImpl.members 
         * 2. another thread was doing AbstractGroupImpl.addMember, which is synchronized on AbstractGroupImpl.members;
         *    it tries to call Entities.manage(child) which calls LocalEntityManager.getEntity(), which is
         *    synchronized on this.
         * 
         * We MUST NOT call alien code from within the management framework while holding locks. 
         * The AbstractGroup.removeMember is effectively alien because a user could override it, and because
         * it is entity specific.
         * 
         * TODO Does getting then removing from groups risk this entity being added to other groups while 
         * this is happening? Should abstractEntity.onManagementStopped or some such remove the entity
         * from its groups?
         */

        ManagementTransitionMode lastTM = getLastManagementTransitionMode(e.getId());
        if (lastTM!=null && !lastTM.isReadOnly()) {
            e.clearParent();
            for (Group group : e.groups()) {
                if (!Entities.isNoLongerManaged(group)) group.removeMember(e);
            }
            if (e instanceof Group) {
                Collection<Entity> members = ((Group)e).getMembers();
                for (Entity member : members) {
                    if (!Entities.isNoLongerManaged(member)) ((EntityInternal)member).groups().remove((Group)e);
                }
            }
        } else {
            log.trace("No groups being updated on unmanage of read only {} (mode {})", e, lastTM);
        }

        unmanageOwnedLocations(e);

        synchronized (this) {
            Entity proxyE = toProxyEntityIfAvailable(e);
            if (e instanceof Application) {
                applications.remove(proxyE);
                applicationIds.remove(e.getId());
            }

            entities.remove(proxyE);
            entityProxiesById.remove(e.getId());
            ManagementTransitionMode oldMode = entityModesById.remove(e.getId());
            
            Object old = entitiesById.remove(e.getId());

            entityTypes.remove(e.getId());
            if (old==null) {
                if (preRegisteredEntitiesById.remove(e.getId())!=null) {
                    // concurrent IEF creation should fail if we do the above removal
                    log.info("{} stopping management of pre-pre-managed entity {} {}/{}; removed from pre-pre-managed list (management of that should fail)", this, e, oldMode, lastTM);
                    discardPremanaged(e);
                } else if (preManagedEntitiesById.remove(e.getId())!=null) {
                    // concurrent management should fail if we do the above removal
                    log.info("{} stopping management of pre-managed entity {} {}/{}; removed from pre-managed list (promotion of that should fail)", this, e, oldMode, lastTM);
                    discardPremanaged(e);
                } else {
                    // can happen even in concurrent case if removal completed by candidate adder, or two things try to unmanage
                    if (!Entities.isNoLongerManaged(e)) {
                        // slim chance we fall into this block if the concurrent unmanagement has not completed; give it a bit of time to do so
                        log.debug("Unexpected call to unmanage {}, possibly a race, delaying slightly to allow to complete");
                        log.trace("Trace for unexpected unmanage call", new Throwable("trace"));
                        Time.sleep(Duration.millis(10));
                    }
                    if (Entities.isNoLongerManaged(e)) {
                        log.debug("Confirmed redundant call to unmanage {} (e.g. an entity ensuring things are removed by policy when also unmanaging)", e);
                    } else {
                        log.warn("{} call to stop management of unknown/unexpected entity (possibly due to redundant concurrent calls) {} {}/{}; ignoring", this, e, oldMode, lastTM);
                    }
                }
                return false;

            } else if (!old.equals(e)) {
                // shouldn't happen...
                log.error("{} call to stop management of entity {} removed different entity {} {}/{}", new Object[] { this, e, old, oldMode, lastTM });
                return true;
            } else {
                if (log.isDebugEnabled()) log.debug("{} stopped management of entity {} {}/{}", this, e, oldMode, lastTM);
                return true;
            }
        }
    }

    private void unmanageOwnedLocations(Entity e) {
        for (Location loc : e.getLocations()) {
            NamedStringTag ownerEntityTag = BrooklynTags.findFirstNamedStringTag(BrooklynTags.OWNER_ENTITY_ID, loc.tags().getTags());
            if (ownerEntityTag != null) {
                if (e.getId().equals(ownerEntityTag.getContents())) {
                    managementContext.getLocationManager().unmanage(loc);
                } else {
                    // A location is "owned" if it was created as part of the EntitySpec of an entity (by Brooklyn).
                    // To share a location between entities create it yourself and pass it to any entities that needs it.
                    log.debug("Unmanaging entity {}, which contains a location {} owned by another entity {}. " +
                            "Not automatically unmanaging the location (it will be unmanaged when its owning " +
                            "entity is unmanaged).",
                            new Object[] {e, loc, ownerEntityTag.getContents()});
                }
            }
        }
    }

    void addEntitySetListener(CollectionChangeListener<Entity> listener) {
        //must notify listener in a different thread to avoid deadlock (issue #378)
        AsyncCollectionChangeAdapter<Entity> wrappedListener = new AsyncCollectionChangeAdapter<Entity>(managementContext.getExecutionManager(), listener);
        entities.addListener(wrappedListener);
    }

    void removeEntitySetListener(CollectionChangeListener<Entity> listener) {
        AsyncCollectionChangeAdapter<Entity> wrappedListener = new AsyncCollectionChangeAdapter<Entity>(managementContext.getExecutionManager(), listener);
        entities.removeListener(wrappedListener);
    }
    
    private boolean shouldSkipUnmanagement(Entity e, boolean hasBeenReplaced) {
        if (e==null) {
            log.warn(""+this+" call to unmanage null entity; skipping",  
                new IllegalStateException("source of null unmanagement call to "+this));
            return true;
        }
        if (!isManaged(e)) {
            if (!hasBeenReplaced) {

                // check duplicated in unmanageNonRecursive
                if (((EntityInternal)e).getManagementSupport().wasDeployed() && !Entities.isNoLongerManaged(e)) {
                    // slim chance we fall into this block if the concurrent unmanagement has not completed; give it a bit of time to do so
                    log.debug("Unexpected call to skippable unmanagement of {}, possibly a race, delaying slightly to allow to complete");
                    log.trace("Trace for unexpected unmanage call", new Throwable("trace"));
                    Time.sleep(Duration.millis(10));
                }

                if (Entities.isNoLongerManaged(e)) {
                    log.debug("Confirmed redundant call to skippable unmanage {} (e.g. an entity ensuring things are removed by policy when also unmanaging); skipping, and all descendants", e);
                } else if ( !((EntityInternal)e).getManagementSupport().wasDeployed() ) {
                    log.debug("Call to unmanage {} which is not (yet?) deployed; assume concurrent creator will unmanage; here we are skipping, and all descendants", e);
                } else {
                    // race can still occur with parallel creator, but unlikely so log warning
                    log.warn("{} call to skippable unmanagement of unknown/unexpected entity (possibly due to redundant concurrent calls); skipping, and all descendants", this, e);
                }

            } else {
                log.trace("{} call to unmanagement of replaced entity (previous already unmanaged?) {}; skipping, and all descendants", this, e);
            }
            return true;
        }
        return false;
    }
    
    private Entity toProxyEntityIfAvailable(Entity e) {
        checkNotNull(e, "entity");
        
        if (e instanceof EntityProxy) {
            return e;
        } else if (e instanceof AbstractEntity) {
            Entity result = ((AbstractEntity)e).getProxy();
            return (result == null) ? e : result;
        } else {
            // If we don't already know about the proxy, then use the real thing; presumably it's 
            // the legacy way of creating the entity so didn't get a preManage() call

            return e;
        }
    }
    
    private Entity toRealEntity(Entity e) {
        checkNotNull(e, "entity");
        
        if (e instanceof AbstractEntity) {
            return e;
        } else {
            Entity result = toRealEntityOrNull(e.getId());
            if (result == null) {
                throw new IllegalStateException("No concrete entity known for entity "+e+" ("+e.getId()+", "+e.getEntityType().getName()+")");
            }
            return result;
        }
    }

    public boolean isKnownEntityId(String id) {
        return entitiesById.containsKey(id) || preManagedEntitiesById.containsKey(id) || preRegisteredEntitiesById.containsKey(id);
    }
    
    private Entity toRealEntityOrNull(String id) {
        Entity result;
        // prefer the preRegistered and preManaged entities, during hot proxying, they should be newer
        result = preRegisteredEntitiesById.get(id);
        if (result==null)
            result = preManagedEntitiesById.get(id);
        if (result==null)
            entitiesById.get(id);
        return result;
    }
    
    private Entity deproxyIfNecessary(Entity e) {
        return (e instanceof AbstractEntity) ? e : Entities.deproxy(e);
    }
    

    private boolean isRunning() {
        return managementContext.isRunning();
    }

}
