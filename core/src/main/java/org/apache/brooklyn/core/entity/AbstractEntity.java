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
package org.apache.brooklyn.core.entity;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.EntityType;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.EntityManager;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionContext;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.rebind.RebindSupport;
import org.apache.brooklyn.api.mgmt.rebind.mementos.EntityMemento;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Enricher;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.config.ConfigKey.HasConfigKey;
import org.apache.brooklyn.core.BrooklynFeatureEnablement;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.catalog.internal.CatalogUtils;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.config.render.RendererHints;
import org.apache.brooklyn.core.enricher.AbstractEnricher;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.apache.brooklyn.core.entity.internal.EntityConfigMap;
import org.apache.brooklyn.core.entity.lifecycle.PolicyDescriptor;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic.ServiceNotUpLogic;
import org.apache.brooklyn.core.feed.AbstractFeed;
import org.apache.brooklyn.core.internal.BrooklynInitialization;
import org.apache.brooklyn.core.internal.storage.BrooklynStorage;
import org.apache.brooklyn.core.internal.storage.Reference;
import org.apache.brooklyn.core.internal.storage.impl.BasicReference;
import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.core.mgmt.BrooklynTags;
import org.apache.brooklyn.core.mgmt.BrooklynTags.NamedStringTag;
import org.apache.brooklyn.core.mgmt.internal.EffectorUtils;
import org.apache.brooklyn.core.mgmt.internal.EntityManagementSupport;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.internal.SubscriptionTracker;
import org.apache.brooklyn.core.mgmt.rebind.BasicEntityRebindSupport;
import org.apache.brooklyn.core.objs.AbstractBrooklynObject;
import org.apache.brooklyn.core.objs.AbstractConfigurationSupportInternal;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct.AdjunctTagSupport;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.AttributeMap;
import org.apache.brooklyn.core.sensor.BasicNotificationSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.concurrent.Locks;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.Equals;
import org.apache.brooklyn.util.text.Strings;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Objects.ToStringHelper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

/**
 * Default {@link Entity} implementation, which should be extended whenever implementing an entity.
 * <p>
 * Provides several common fields ({@link #displayName}, {@link #id}), and supports the core features of
 * an entity such as configuration keys, attributes, subscriptions and effector invocation.
 * <p>
 * If a sub-class is creating other entities, this should be done in an overridden {@link #init()}
 * method.
 * <p>
 * Note that config is typically inherited by children, whereas the fields and attributes are not.
 * <p>
 * Sub-classes should have a no-argument constructor. When brooklyn creates an entity, it will:
 * <ol>
 *   <li>Construct the entity via the no-argument constructor
 *   <li>Call {@link #setDisplayName(String)}
 *   <li>Call {@link #setManagementContext(ManagementContextInternal)}
 *   <li>Call {@link #setProxy(Entity)}; the proxy should be used by everything else when referring 
 *       to this entity (except for drivers/policies that are attached to the entity, which can be  
 *       given a reference to this entity itself).
 *   <li>Call {@link #configure(Map)} and then {@link #setConfig(ConfigKey, Object)}
 *   <li>Call {@link #init()}
 *   <li>Call {@link #addPolicy(Policy)} (for any policies defined in the {@link EntitySpec})
 *   <li>Call {@link #setParent(Entity)}, if a parent is specified in the {@link EntitySpec}
 * </ol>
 * <p>
 * The legacy (pre 0.5) mechanism for creating entities is for others to call the constructor directly.
 * This is now deprecated.
 */
public abstract class AbstractEntity extends AbstractBrooklynObject implements EntityInternal {
    
    private static final Logger LOG = LoggerFactory.getLogger(AbstractEntity.class);
    
    static { BrooklynInitialization.initAll(); }
    
    /**
     * The default name to use for this entity, if not explicitly overridden.
     */
    public static final ConfigKey<String> DEFAULT_DISPLAY_NAME = BasicConfigKey.builder(String.class)
            .name("defaultDisplayName")
            .description("Optional default display name to use (rather than auto-generating, if no name is explicitly supplied)")
            .runtimeInheritance(BasicConfigInheritance.NEVER_INHERITED)
            .build();

    public static final BasicNotificationSensor<Location> LOCATION_ADDED = new BasicNotificationSensor<Location>(
            Location.class, "entity.location.added", "Location dynamically added to entity");
    public static final BasicNotificationSensor<Location> LOCATION_REMOVED = new BasicNotificationSensor<Location>(
            Location.class, "entity.location.removed", "Location dynamically removed from entity");

    @SuppressWarnings("rawtypes")
    public static final BasicNotificationSensor<Sensor> SENSOR_ADDED = new BasicNotificationSensor<Sensor>(Sensor.class,
            "entity.sensor.added", "Sensor dynamically added to entity");
    @SuppressWarnings("rawtypes")
    public static final BasicNotificationSensor<Sensor> SENSOR_REMOVED = new BasicNotificationSensor<Sensor>(Sensor.class,
            "entity.sensor.removed", "Sensor dynamically removed from entity");

    public static final BasicNotificationSensor<String> EFFECTOR_ADDED = new BasicNotificationSensor<String>(String.class,
            "entity.effector.added", "Effector dynamically added to entity");
    public static final BasicNotificationSensor<String> EFFECTOR_REMOVED = new BasicNotificationSensor<String>(String.class,
            "entity.effector.removed", "Effector dynamically removed from entity");
    public static final BasicNotificationSensor<String> EFFECTOR_CHANGED = new BasicNotificationSensor<String>(String.class,
            "entity.effector.changed", "Effector dynamically changed on entity");

    @SuppressWarnings("rawtypes")
    public static final BasicNotificationSensor<ConfigKey> CONFIG_KEY_ADDED = new BasicNotificationSensor<ConfigKey>(ConfigKey.class,
            "entity.config_key.added", "ConfigKey dynamically added to entity");
    @SuppressWarnings("rawtypes")
    public static final BasicNotificationSensor<ConfigKey> CONFIG_KEY_REMOVED = new BasicNotificationSensor<ConfigKey>(ConfigKey.class,
            "entity.config_key.removed", "ConfigKey dynamically removed from entity");

    public static final BasicNotificationSensor<PolicyDescriptor> POLICY_ADDED = new BasicNotificationSensor<PolicyDescriptor>(PolicyDescriptor.class,
            "entity.policy.added", "Policy dynamically added to entity");
    public static final BasicNotificationSensor<PolicyDescriptor> POLICY_REMOVED = new BasicNotificationSensor<PolicyDescriptor>(PolicyDescriptor.class,
            "entity.policy.removed", "Policy dynamically removed from entity");

    public static final BasicNotificationSensor<Entity> CHILD_ADDED = new BasicNotificationSensor<Entity>(Entity.class,
            "entity.children.added", "Child dynamically added to entity");
    public static final BasicNotificationSensor<Entity> CHILD_REMOVED = new BasicNotificationSensor<Entity>(Entity.class,
            "entity.children.removed", "Child dynamically removed from entity");

    public static final BasicNotificationSensor<Group> GROUP_ADDED = new BasicNotificationSensor<Group>(Group.class,
            "entity.group.added", "Group dynamically added to entity");
    public static final BasicNotificationSensor<Group> GROUP_REMOVED = new BasicNotificationSensor<Group>(Group.class,
            "entity.group.removed", "Group dynamically removed from entity");

    public static final AttributeSensor<String> ENTITY_ID = Attributes.ENTITY_ID;
    public static final AttributeSensor<String> APPLICATION_ID = Attributes.APPLICATION_ID;
    public static final AttributeSensor<String> CATALOG_ID = Attributes.CATALOG_ID;

    static {
        RendererHints.register(Entity.class, RendererHints.displayValue(EntityFunctions.displayName()));
    }
        
    private boolean displayNameAutoGenerated = true;
    
    private Entity selfProxy;
    private volatile Application application;
    
    /**
     * Lock to be held when setting the application.
     * Must not synchronize on `this`, as then entity authors can cause horrible hanging
     * by also synchronize on this. Instead only synchronize on private fields.
     */
    private final Object appMutex = new Object();
    
    // If FEATURE_USE_BROOKLYN_LIVE_OBJECTS_DATAGRID_STORAGE, then these are just temporary values 
    // (but may still be needed if something, such as an EntityFactory in a cluster/fabric, did not
    // use EntitySpec.
    // If that feature is disabled, then these are not "temporary" values - these are the production
    // values. They must be thread-safe, and where necessary (e.g. group) they should preserve order
    // if possible.
    private Reference<Entity> parent = new BasicReference<Entity>();
    /** Synchronize on this when updating to ensure addition/removals done in order, 
     * and notifications done in order.
     * If calling other code while holding this synch lock, any synch locks it might call should be called first
     * (in particular the AttributeMap.values should be obtained before this if publishing.) */
    private Set<Entity> children = Collections.synchronizedSet(Sets.<Entity>newLinkedHashSet());
    /** Synchronize on this to ensure group and members are updated at the same time.
     * Synchronize behavior as for {@link #children} apply here, and in addition
     * the parent's "members" lock should be obtained first. */
    private Set<Group> groupsInternal = Collections.synchronizedSet(Sets.<Group>newLinkedHashSet());
    private Reference<List<Location>> locations = new BasicReference<List<Location>>(ImmutableList.<Location>of()); // dups removed in addLocations
    private Reference<Long> creationTimeUtc = new BasicReference<Long>(System.currentTimeMillis());
    private Reference<String> displayName = new BasicReference<String>();

    private Collection<AbstractPolicy> policiesInternal = Lists.newCopyOnWriteArrayList();
    private Collection<AbstractEnricher> enrichersInternal = Lists.newCopyOnWriteArrayList();
    private Collection<Feed> feedsInternal = Lists.newCopyOnWriteArrayList();

    // FIXME we do not currently support changing parents, but to implement a cluster that can shrink we need to support at least
    // orphaning (i.e. removing ownership). This flag notes if the entity has previously had a parent, and if an attempt is made to
    // set a new parent an exception will be thrown.
    private boolean previouslyOwned = false;

    /**
     * Whether we are still being constructed, in which case never warn in "assertNotYetOwned"
     */
    private boolean inConstruction = true;
    
    private final EntityDynamicType entityType;
    
    protected final EntityManagementSupport managementSupport = new EntityManagementSupport(this);

    private final BasicConfigurationSupport config = new BasicConfigurationSupport();

    private final BasicSensorSupport sensors = new BasicSensorSupport();

    private final BasicSubscriptionSupport subscriptions = new BasicSubscriptionSupport();

    private final BasicPolicySupport policies = new BasicPolicySupport();

    private final BasicEnricherSupport enrichers = new BasicEnricherSupport();

    private final BasicGroupSupport groups = new BasicGroupSupport();

    private final BasicFeedSupport feeds = new BasicFeedSupport();

    /**
     * The config values of this entity. Updating this map should be done
     * via getConfig/setConfig.
     */
    // If FEATURE_USE_BROOKLYN_LIVE_OBJECTS_DATAGRID_STORAGE, this value will be only temporary.
    private EntityConfigMap configsInternal = new EntityConfigMap(this);

    /**
     * The sensor-attribute values of this entity. Updating this map should be done
     * via getAttribute/setAttribute; it will automatically emit an attribute-change event.
     */
    // If FEATURE_USE_BROOKLYN_LIVE_OBJECTS_DATAGRID_STORAGE, this value will be only temporary.
    private AttributeMap attributesInternal = new AttributeMap(this);

    protected transient volatile SubscriptionTracker _subscriptionTracker;
    
    public AbstractEntity() {
        this(Maps.newLinkedHashMap(), null);
    }

    /**
     * @deprecated since 0.5; instead use no-arg constructor with EntityManager().createEntity(spec)
     */
    @Deprecated
    public AbstractEntity(Map flags) {
        this(flags, null);
    }

    /**
     * @deprecated since 0.5; instead use no-arg constructor with EntityManager().createEntity(spec)
     */
    @Deprecated
    public AbstractEntity(Entity parent) {
        this(Maps.newLinkedHashMap(), parent);
    }

    // FIXME don't leak this reference in constructor - even to utils
    /**
     * @deprecated since 0.5; instead use no-arg constructor with EntityManager().createEntity(spec)
     */
    @Deprecated
    public AbstractEntity(@SuppressWarnings("rawtypes") Map flags, Entity parent) {
        super(checkConstructorFlags(flags, parent));

        // TODO Don't let `this` reference escape during construction
        entityType = new EntityDynamicType(this);
        
        if (isLegacyConstruction()) {
            AbstractEntity checkWeGetThis = configure(flags);
            assert this.equals(checkWeGetThis) : this+" configure method does not return itself; returns "+checkWeGetThis+" instead of "+this;

            boolean deferConstructionChecks = (flags.containsKey("deferConstructionChecks") && TypeCoercions.coerce(flags.get("deferConstructionChecks"), Boolean.class));
            if (!deferConstructionChecks) {
                FlagUtils.checkRequiredFields(this);
            }
        }
    }
    
    private static Map<?,?> checkConstructorFlags(Map flags, Entity parent) {
        if (flags==null) {
            throw new IllegalArgumentException("Flags passed to entity must not be null (try no-arguments or empty map)");
        }
        if (flags.get("parent") != null && parent != null && flags.get("parent") != parent) {
            throw new IllegalArgumentException("Multiple parents supplied, "+flags.get("parent")+" and "+parent);
        }
        if (flags.get("owner") != null && parent != null && flags.get("owner") != parent) {
            throw new IllegalArgumentException("Multiple parents supplied with flags.parent, "+flags.get("owner")+" and "+parent);
        }
        if (flags.get("parent") != null && flags.get("owner") != null && flags.get("parent") != flags.get("owner")) {
            throw new IllegalArgumentException("Multiple parents supplied with flags.parent and flags.owner, "+flags.get("parent")+" and "+flags.get("owner"));
        }
        if (parent != null) {
            flags.put("parent", parent);
        }
        if (flags.get("owner") != null) {
            LOG.warn("Use of deprecated \"flags.owner\" instead of \"flags.parent\" for entity");
            flags.put("parent", flags.get("owner"));
            flags.remove("owner");
        }
        return flags;
    }

    /**
     * @deprecated since 0.7.0; only used for legacy brooklyn types where constructor is called directly
     */
    @Override
    @Deprecated
    public AbstractEntity configure(Map flags) {
        if (!inConstruction && getManagementSupport().isDeployed()) {
            LOG.warn("bulk/flag configuration being made to {} after deployment: may not be supported in future versions ({})", 
                    new Object[] { this, flags });
        }
        // TODO use a config bag instead
//        ConfigBag bag = new ConfigBag().putAll(flags);
        
        // FIXME Need to set parent with proxy, rather than `this`
        Entity suppliedParent = (Entity) flags.remove("parent");
        if (suppliedParent != null) {
            suppliedParent.addChild(getProxyIfAvailable());
        }
        
        Map<ConfigKey,?> suppliedOwnConfig = (Map<ConfigKey, ?>) flags.remove("config");
        if (suppliedOwnConfig != null) {
            for (Map.Entry<ConfigKey, ?> entry : suppliedOwnConfig.entrySet()) {
                setConfigEvenIfOwned(entry.getKey(), entry.getValue());
            }
        }

        if (flags.get("displayName") != null) {
            displayName.set((String) flags.remove("displayName"));
            displayNameAutoGenerated = false;
        } else if (flags.get("name") != null) {
            displayName.set((String) flags.remove("name"));
            displayNameAutoGenerated = false;
        } else if (isLegacyConstruction()) {
            displayName.set(getClass().getSimpleName()+":"+Strings.maxlen(getId(), 4));
            displayNameAutoGenerated = true;
        }

        if (flags.get(BrooklynConfigKeys.ICON_URL.getName()) != null) {
            // shouldn't be used; CAMP parser leaves it as a top-level attribute which is converted to a tag
            tags().addTag(BrooklynTags.newIconUrlTag((String) flags.remove(BrooklynConfigKeys.ICON_URL.getName())));
        }

        // allow config keys to be set by name (or deprecated name)
        //
        // The resulting `flags` will no longer contain the keys that we matched;
        // we will not also use them to for `SetFromFlag` etc.
        flags = ConfigUtilsInternal.setAllConfigKeys(flags, getEntityType().getConfigKeys(), this);

        // allow config keys, and fields, to be set from these flags if they have a SetFromFlag annotation
        // TODO the default values on flags are not used? (we should remove that support, since ConfigKeys gives a better way)
        if (flags.size() > 0) {
            FlagUtils.setFieldsFromFlags(flags, this);
            flags = FlagUtils.setAllConfigKeys(flags, this, false);
        }
        
        // finally all config keys specified in map should be set as config
        // TODO use a config bag and remove the ones set above in the code below
        for (Iterator<Map.Entry> fi = flags.entrySet().iterator(); fi.hasNext();) {
            Map.Entry entry = fi.next();
            Object k = entry.getKey();
            if (k instanceof HasConfigKey) k = ((HasConfigKey)k).getConfigKey();
            if (k instanceof ConfigKey) {
                setConfigEvenIfOwned((ConfigKey)k, entry.getValue());
                fi.remove();
            }
        }
        
        if (!flags.isEmpty()) {
            LOG.warn("Unsupported flags when configuring {}; storing: {}", this, flags);
            configsInternal.putAll(flags);
        }

        return this;
    }

    /**
     * Adds the config keys to the entity dynamic type
     * @since 0.9.0
     */
    public void configure(Iterable<ConfigKey<?>> configKeys) {
        entityType.addConfigKeys(configKeys);
    }

    @Override
    public int hashCode() {
        return getId().hashCode();
    }
    
    @Override
    public boolean equals(Object o) {
        return o != null &&
                ((o == this || o == selfProxy) ||
                (o instanceof Entity && Objects.equal(getId(), ((Entity)o).getId())));
    }
    
    /** internal use only */ @Beta
    public void setProxy(Entity proxy) {
        if (selfProxy != null) 
            throw new IllegalStateException("Proxy is already set; cannot reset proxy for "+toString());
        resetProxy(proxy);
    }
    /** internal use only */ @Beta
    public void resetProxy(Entity proxy) {
        selfProxy = checkNotNull(proxy, "proxy");
    }
    
    public Entity getProxy() {
        return selfProxy;
    }
    
    /**
     * Returns the proxy, or if not available (because using legacy code) then returns the real entity.
     * This method will be deleted in a future release; it will be kept while deprecated legacy code
     * still exists that creates entities without setting the proxy.
     */
    @Beta
    public Entity getProxyIfAvailable() {
        return getProxy()!=null ? getProxy() : this;
    }
    
    @Override
    public void setManagementContext(ManagementContextInternal managementContext) {
        super.setManagementContext(managementContext);
        getManagementSupport().setManagementContext(managementContext);
        entityType.setName(getEntityTypeName());
        if (displayNameAutoGenerated) displayName.set(getEntityType().getSimpleName()+":"+Strings.maxlen(getId(), 4));
    }

    /** 
     * Where code needs to enforce single threading, this {@link Lock} can be used.
     * <p>
     * Care must be taken to avoid deadlock, with canoncial orders carefully defined
     * if this is used by any lockholder or any other locks used while holding this lock.
     * See {@link AttributeMap#getLockInternal()} for more detail.
     */
    @Beta
    protected Lock getLockInternal() {
        return attributesInternal.getLockInternal();
    }
    
    @Override
    public Map<String, String> toMetadataRecord() {
        return ImmutableMap.of();
    }

    @Override
    public long getCreationTime() {
        return creationTimeUtc.get();
    }

    @Override
    public String getDisplayName() {
        return displayName.get();
    }
    
    @Override @Deprecated
    public String getIconUrl() {
        return RegisteredTypes.getIconUrl(this);
    }
    
    @Override
    public void setDisplayName(String newDisplayName) {
        displayName.set(newDisplayName);
        displayNameAutoGenerated = false;
        getManagementSupport().getEntityChangeListener().onChanged();
    }
    
    @Override
    public void setDefaultDisplayName(String displayNameIfDefault) {
        if (displayNameAutoGenerated) {
            displayName.set(displayNameIfDefault);
        }
    }
    
    /**
     * Gets the entity type name, to be returned by {@code getEntityType().getName()}.
     * To be called by brooklyn internals only.
     * Can be overridden to customize the name.
     */
    protected String getEntityTypeName() {
        try {
            Class<?> typeClazz = getManagementContext().getEntityManager().getEntityTypeRegistry().getEntityTypeOf(getClass());
            String typeName = typeClazz.getCanonicalName();
            if (typeName == null) typeName = typeClazz.getName();
            return typeName;
        } catch (IllegalArgumentException e) {
            String typeName = getClass().getCanonicalName();
            if (typeName == null) typeName = getClass().getName();
            LOG.debug("Entity type interface not found for entity "+this+"; instead using "+typeName+" as entity type name");
            return typeName;
        }
    }
    
    /**
     * Adds this as a child of the given entity; registers with application if necessary.
     */
    @Override
    public AbstractEntity setParent(Entity entity) {
        if (!parent.isNull()) {
            // If we are changing to the same parent...
            if (parent.contains(entity)) return this;
            // If we have a parent but changing to orphaned...
            if (entity==null) { clearParent(); return this; }
            
            if (BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_DISALLOW_REPARENTING)) {
                // We have a parent and are changing to another parent...
                throw new UnsupportedOperationException("Cannot change parent of "+this+" from "+parent+" to "+entity+" (parent change not supported)");
            }
        }
        if (BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_DISALLOW_REPARENTING)) {
            // If we have previously had a parent and are trying to change to another one...
            if (previouslyOwned && entity != null)
                throw new UnsupportedOperationException("Cannot set a parent of "+this+" as "+entity+" because it has previously had a parent");
        }
        // If reparenting disallowed, we don't have a parent, never have and are changing to having a parent...
        // If reparenting is allowed, anything could be the case, but that should be okay.

        //make sure there is no loop
        if (this.equals(entity)) throw new IllegalStateException("entity "+this+" cannot own itself");
        //this may be expensive, but preferable to throw before setting the parent!
        if (Entities.isDescendant(this, entity))
            throw new IllegalStateException("loop detected trying to set parent of "+this+" as "+entity+", which is already a descendent");
        
        parent.set(entity);
        entity.addChild(getProxyIfAvailable());
        config().refreshInheritedConfig();
        previouslyOwned = true;
        
        getApplication();
        
        return this;
    }

    @Override
    public void clearParent() {
        if (parent.isNull()) return;
        Entity oldParent = parent.get();
        parent.clear();
        if (oldParent != null) {
            if (!Entities.isNoLongerManaged(oldParent)) 
                oldParent.removeChild(getProxyIfAvailable());
        }
    }
    
    /**
     * Adds the given entity as a child of this parent <em>and</em> sets this entity as the parent of the child;
     * returns argument passed in, for convenience.
     * <p>
     * The child is NOT managed, even if the parent is already managed at this point
     * (e.g. the child is added *after* the parent's {@link AbstractEntity#init()} is invoked)
     * and so will need an explicit <code>getEntityManager().manage(childReturnedFromThis)</code> call.
     * <i>These semantics are currently under review.</i>
     */
    @Override
    public <T extends Entity> T addChild(T child) {
        checkNotNull(child, "child must not be null (for entity %s)", this);
        CatalogUtils.setCatalogItemIdOnAddition(this, child);
        
        Locks.withLock(getLockInternal(), () -> {
            // hold synch locks in this order to prevent deadlock
            synchronized (children) {
                if (Entities.isAncestor(this, child)) throw new IllegalStateException("loop detected trying to add child "+child+" to "+this+"; it is already an ancestor");
                child.setParent(getProxyIfAvailable());
                boolean changed = children.add(child);
                
                getManagementSupport().getEntityChangeListener().onChildrenChanged();
                if (changed) {
                    sensors().emit(AbstractEntity.CHILD_ADDED, child);
                }
            }
        });
        return child;
    }

    /**
     * Creates an entity using the given spec, and adds it as a child of this entity.
     * 
     * @see #addChild(Entity)
     * @see EntityManager#createEntity(EntitySpec)
     * 
     * @throws IllegalArgumentException If {@code spec.getParent()} is set and is different from this entity
     */
    @Override
    public <T extends Entity> T addChild(EntitySpec<T> spec) {
        if (spec.getParent()==null) {
            spec = EntitySpec.create(spec).parent(getProxyIfAvailable());
        }
        if (!this.equals(spec.getParent())) {
            throw new IllegalArgumentException("Attempt to create child of "+this+" with entity spec "+spec+
                " failed because spec has different parent: "+spec.getParent());
        }
        
        // The spec now includes this as the parent, so no need to call addChild; 
        // that is done by InternalEntityFactory.
        return getEntityManager().createEntity(spec);
    }
    
    @Override
    public boolean removeChild(Entity child) {
        return Locks.withLock(getLockInternal(), () -> {
            synchronized (children) {
                boolean changed = children.remove(child);
                child.clearParent();
                
                if (changed) {
                    getManagementSupport().getEntityChangeListener().onChildrenChanged();
                }
            
                if (changed) {
                    sensors().emit(AbstractEntity.CHILD_REMOVED, child);
                }
                return changed;
            }
        });
    }

    // -------- GROUPS --------------

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return EnricherSupportInternal
    // TODO revert to EnricherSupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicGroupSupport groups() {
        return groups;
    }

    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #groups()} is reverted to return {@link {GroupSupport} instead of
     * {@link BasicGroupSupport}.
     */
    @Beta
    // TODO revert to private when groups() is reverted to return GroupSupport
    public class BasicGroupSupport implements GroupSupportInternal {
        @Override
        public Iterator<Group> iterator() { 
            return asList().iterator();
        }
        @Override
        public int size() {
            return asList().size();
        }
        @Override
        public boolean isEmpty() {
            return asList().isEmpty();
        }
        
        protected List<Group> asList() {
            synchronized (groupsInternal) {
                return ImmutableList.copyOf(groupsInternal);
            }
        }
        
        @Override
        public void add(Group group) {
            boolean changed = groupsInternal.add(group);
            getApplication();
            
            if (changed) {
                sensors().emit(AbstractEntity.GROUP_ADDED, group);
            }
        }

        @Override
        public void remove(Group group) {
            boolean changed = groupsInternal.remove(group);
            getApplication();
            
            if (changed) {
                sensors().emit(AbstractEntity.GROUP_REMOVED, group);
            }
        }
    }
    
    @Override
    public Entity getParent() {
        return parent.get();
    }

    @Override
    public Collection<Entity> getChildren() {
        synchronized (children) {
            return ImmutableList.copyOf(children);
        }
    }
    
    /**
     * Returns the application, looking it up if not yet known (registering if necessary)
     */
    @Override
    public Application getApplication() {
        if (application != null) return application;
        Entity parent = this.parent.get();
        Application app = (parent != null) ? parent.getApplication() : null;
        if (app != null) {
            if (getManagementSupport().isFullyManaged())
                // only do this once fully managed, in case root app becomes parented
                setApplication(app);
        }
        return app;
    }

    // TODO Can this really be deleted? Overridden by AbstractApplication; needs careful review
    /** @deprecated since 0.4.0 should not be needed / leaked outwith brooklyn internals / mgmt support? */
    @Deprecated
    protected void setApplication(Application app) {
        synchronized (appMutex) {
            if (application != null) {
                if (application.getId() != app.getId()) {
                    throw new IllegalStateException("Cannot change application of entity (attempted for "+this+" from "+getApplication()+" to "+app);
                }
            }
            this.application = app;
        }
    }

    @Override
    public String getApplicationId() {
        Application app = getApplication();
        return (app == null) ? null : app.getId();
    }

    @Override
    public ManagementContext getManagementContext() {
        // NB Sept 2014 - removed synch keyword above due to deadlock;
        // it also synchs in ManagementSupport.getManagementContext();
        // no apparent reason why it was here also
        return getManagementSupport().getManagementContext();
    }

    protected EntityManager getEntityManager() {
        return getManagementContext().getEntityManager();
    }
    
    @Override
    public EntityType getEntityType() {
        if (entityType==null) return null;
        return entityType.getSnapshot();
    }

    @Override
    public EntityDynamicType getMutableEntityType() {
        return entityType;
    }
    
    @Override
    public Collection<Location> getLocations() {
        synchronized (locations) {
            return ImmutableList.copyOf(locations.get());
        }
    }

    @Override
    public void addLocations(Collection<? extends Location> newLocations) {
        addLocationsImpl(newLocations, true);
    }

    @Override
    @Beta
    public void addLocationsWithoutPublishing(Collection<? extends Location> newLocations) {
        addLocationsImpl(newLocations, false);
    }
    
    @Beta
    protected void addLocationsImpl(Collection<? extends Location> newLocations, boolean publish) {
        if (newLocations==null || newLocations.isEmpty()) {
            return;
        }

        for (Location loc : newLocations) {
            NamedStringTag ownerEntityTag = BrooklynTags.findFirst(BrooklynTags.OWNER_ENTITY_ID, loc.tags().getTags());
            if (ownerEntityTag != null) {
                if (!getId().equals(ownerEntityTag.getContents())) {
                    // A location is "owned" if it was created as part of the EntitySpec of an entity (by Brooklyn).
                    // To share a location between entities create it yourself and pass it to any entities that needs it.
                    LOG.info("Adding location {} to entity {}, which is already owned by another entity {}. " +
                            "Locations owned by a specific entity will be unmanaged together with their owner, " +
                            "regardless of other references to them. Therefore care should be taken if sharing " +
                            "the location with other entities.",
                            new Object[] {loc, this, ownerEntityTag.getContents()});
                }
            }
        }

        synchronized (locations) {
            List<Location> oldLocations = locations.get();
            Set<Location> trulyNewLocations = Sets.newLinkedHashSet(newLocations);
            trulyNewLocations.removeAll(oldLocations);
            if (trulyNewLocations.size() > 0) {
                locations.set(ImmutableList.<Location>builder().addAll(oldLocations).addAll(trulyNewLocations).build());
            }
            
            if (publish) {
                for (Location loc : trulyNewLocations) {
                    sensors().emit(AbstractEntity.LOCATION_ADDED, loc);
                }
            }
        }
        
        if (publish) {
            if (getManagementSupport().isDeployed()) {
                for (Location newLocation : newLocations) {
                    // Location is now reachable, so manage it
                    // TODO will not be required in future releases when creating locations always goes through LocationManager.createLocation(LocationSpec).
                    Locations.manage(newLocation, getManagementContext());
                }
            }
        }
        
        getManagementSupport().getEntityChangeListener().onLocationsChanged();
    }

    @Override
    public void removeLocations(Collection<? extends Location> removedLocations) {
        synchronized (locations) {
            List<Location> oldLocations = locations.get();
            Set<Location> trulyRemovedLocations = Sets.intersection(ImmutableSet.copyOf(removedLocations), ImmutableSet.copyOf(oldLocations));
            locations.set(MutableList.<Location>builder().addAll(oldLocations).removeAll(removedLocations).buildImmutable());
            
            for (Location loc : trulyRemovedLocations) {
                sensors().emit(AbstractEntity.LOCATION_REMOVED, loc);
            }
        }
        
        // TODO Not calling `Entities.unmanage(removedLocation)` because this location might be shared with other entities.
        // Relying on abstractLocation.removeChildLocation unmanaging it, but not ideal as top-level locations will stick
        // around forever, even if not referenced.
        // Same goes for AbstractEntity#clearLocations().
        
        getManagementSupport().getEntityChangeListener().onLocationsChanged();
    }
    
    @Override
    public void clearLocations() {
        synchronized (locations) {
            locations.set(ImmutableList.<Location>of());
        }
        getManagementSupport().getEntityChangeListener().onLocationsChanged();
    }

    public Location firstLocation() {
        synchronized (locations) {
            return Iterables.get(locations.get(), 0);
        }
    }
    
    /**
     * Should be invoked at end-of-life to clean up the item.
     */
    @Override
    public void destroy() {
    }

    @Override
    public <T> T getAttribute(AttributeSensor<T> attribute) {
        return sensors().get(attribute);
    }

    static Set<String> WARNED_READ_ONLY_ATTRIBUTES = Collections.synchronizedSet(MutableSet.<String>of());
    
    
    // -------- CONFIGURATION --------------

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return ConfigurationSupportInternal
    // TODO revert to ConfigurationSupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicConfigurationSupport config() {
        return config;
    }

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return SensorsSupport
    // TODO revert to SensorsSupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicSensorSupport sensors() {
        return sensors;
    }

    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #sensors()} is reverted to return {@link SensorSupport} instead of
     * {@link BasicSensorSupport}.
     */
    @Beta
    // TODO revert to private when config() is reverted to return SensorSupportInternal
    public class BasicSensorSupport implements SensorSupportInternal {

        @Override
        public <T> T get(AttributeSensor<T> attribute) {
            return attributesInternal.getValue(attribute);
        }

        @Override
        public <T> T set(AttributeSensor<T> attribute, T val) {
            if (LOG.isTraceEnabled())
                LOG.trace(""+AbstractEntity.this+" setAttribute "+attribute+" "+val);
            
            if (Boolean.TRUE.equals(getManagementSupport().isReadOnlyRaw())) {
                T oldVal = getAttribute(attribute);
                if (Equals.approximately(val, oldVal)) {
                    // ignore, probably an enricher resetting values or something on init
                } else {
                    String message = AbstractEntity.this+" setting "+attribute+" = "+val+" (was "+oldVal+") in read only mode; will have very little effect"; 
                    if (!getManagementSupport().isDeployed()) {
                        if (getManagementSupport().wasDeployed()) message += " (no longer deployed)"; 
                        else message += " (not yet deployed)";
                    }
                    if (WARNED_READ_ONLY_ATTRIBUTES.add(attribute.getName())) {
                        LOG.warn(message + " (future messages for this sensor logged at trace)");
                    } else if (LOG.isTraceEnabled()) {
                        LOG.trace(message);
                    }
                }
            }
            T result = attributesInternal.update(attribute, val);
            if (result == null) {
                // could be this is a new sensor
                entityType.addSensorIfAbsent(attribute);
            }
            
            if (!Objects.equal(result, val)) {
                getManagementSupport().getEntityChangeListener().onAttributeChanged(attribute);
            }
            
            return result;
        }

        @Override
        public <T> T setWithoutPublishing(AttributeSensor<T> attribute, T val) {
            if (LOG.isTraceEnabled())
                LOG.trace(""+AbstractEntity.this+" setAttributeWithoutPublishing "+attribute+" "+val);
            
            // internal code done on rebind, locking a bit redundant, but strictly speaking 
            // all updates should have the lock, and the sensor add should be done in same lock block
            return Locks.withLock(getLockInternal(), () -> {
                T result = attributesInternal.updateInternalWithoutLockOrPublish(attribute, val);
                if (result == null) {
                    // could be this is a new sensor
                    entityType.addSensorIfAbsentWithoutPublishing(attribute);
                }
                
                getManagementSupport().getEntityChangeListener().onAttributeChanged(attribute);
                return result;
            });
        }

        @Beta
        @Override
        public <T> T modify(AttributeSensor<T> attribute, Function<? super T, Maybe<T>> modifier) {
            if (LOG.isTraceEnabled())
                LOG.trace(""+AbstractEntity.this+" modifyAttribute "+attribute+" "+modifier);
            
            if (Boolean.TRUE.equals(getManagementSupport().isReadOnlyRaw())) {
                String message = AbstractEntity.this+" modifying "+attribute+" = "+modifier+" in read only mode; will have very little effect"; 
                if (!getManagementSupport().isDeployed()) {
                    if (getManagementSupport().wasDeployed()) message += " (no longer deployed)"; 
                    else message += " (not yet deployed)";
                }
                if (WARNED_READ_ONLY_ATTRIBUTES.add(attribute.getName())) {
                    LOG.warn(message + " (future messages for this sensor logged at trace)");
                } else if (LOG.isTraceEnabled()) {
                    LOG.trace(message);
                }
            }
            // ensure sensor added in same lock block
            return Locks.withLock(getLockInternal(), () -> {
                T result = attributesInternal.modify(attribute, modifier);
                if (result == null) {
                    // could be this is a new sensor
                    entityType.addSensorIfAbsent(attribute);
                }
                
                // TODO Conditionally set onAttributeChanged, only if was modified
                getManagementSupport().getEntityChangeListener().onAttributeChanged(attribute);
                return result;
            });
        }

        @Override
        public void remove(AttributeSensor<?> attribute) {
            if (LOG.isTraceEnabled())
                LOG.trace(""+AbstractEntity.this+" removeAttribute "+attribute);
            // removal should be done in same lock block
            Locks.withLock(getLockInternal(), () -> {
                attributesInternal.remove(attribute);
                entityType.removeSensor(attribute);
            });
        }

        @Override
        public Map<AttributeSensor<?>, Object> getAll() {
            Map<AttributeSensor<?>, Object> result = Maps.newLinkedHashMap();
            Map<String, Object> attribs = attributesInternal.asMap();
            for (Map.Entry<String,Object> entry : attribs.entrySet()) {
                AttributeSensor<?> attribKey = (AttributeSensor<?>) entityType.getSensor(entry.getKey());
                if (attribKey == null) {
                    // Most likely a race: e.g. persister thread calling getAllAttributes; writer thread
                    // has written attribute value and is in process of calling entityType.addSensorIfAbsent(attribute)
                    // Just use a synthetic AttributeSensor, rather than ignoring value.
                    // TODO If it's not a race, then don't log.warn every time!
                    LOG.warn("When retrieving all attributes of {}, no AttributeSensor for attribute {} (creating synthetic)", AbstractEntity.this, entry.getKey());
                    attribKey = Sensors.newSensor(Object.class, entry.getKey());
                }
                result.put(attribKey, entry.getValue());
            }
            return result;
        }
        
        @Override
        public <T> void emit(Sensor<T> sensor, T val) {
            if (sensor instanceof AttributeSensor) {
                LOG.warn("Strongly discouraged use of emit with attribute sensor "+sensor+" "+val+"; use setAttribute instead!",
                    new Throwable("location of discouraged attribute "+sensor+" emit"));
            }
            if (val instanceof SensorEvent) {
                LOG.warn("Strongly discouraged use of emit with sensor event as value "+sensor+" "+val+"; value should be unpacked!",
                    new Throwable("location of discouraged event "+sensor+" emit"));
            }
            BrooklynLogging.log(LOG, BrooklynLogging.levelDebugOrTraceIfReadOnly(AbstractEntity.this),
                "Emitting sensor notification {} value {} on {}", sensor.getName(), val, AbstractEntity.this);
            emitInternal(sensor, val);
        }
        
        public <T> void emitInternal(Sensor<T> sensor, T val) {
            if (getManagementSupport().isNoLongerManaged())
                throw new IllegalStateException("Entity "+AbstractEntity.this+" is no longer managed, when trying to publish "+sensor+" "+val);

            SubscriptionContext subsContext = subscriptions().getSubscriptionContext();
            if (subsContext != null) subsContext.publish(sensor.newEvent(getProxyIfAvailable(), val));
        }
    }
    
    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #config()} is reverted to return {@link ConfigurationSupportInternal} instead of
     * {@link BasicConfigurationSupport}.
     */
    @Beta
    // TODO revert to private when config() is reverted to return ConfigurationSupportInternal
    public class BasicConfigurationSupport extends AbstractConfigurationSupportInternal {

        @Override
        protected AbstractConfigMapImpl<?> getConfigsInternal() {
            return configsInternal;
        }

        @Override
        public void refreshInheritedConfig() {
            // no-op, for now, because the impl always looks at ancestors
            // but in a distributed impl it will need to clear any local cache
            refreshInheritedConfigOfChildren();
        }
        
        @Override
        public void refreshInheritedConfigOfChildren() {
            for (Entity it : getChildren()) {
                ((EntityInternal)it).config().refreshInheritedConfig();
            }
        }
        
        @Override
        protected <T> void onConfigChanging(ConfigKey<T> key, Object val) {
            if (!inConstruction && getManagementSupport().isDeployed()) {
                // previously we threw, then warned, but it is still quite common;
                // so long as callers don't expect miracles, it should be fine.
                // i (Alex) think the way to be stricter about this (if that becomes needed) 
                // would be to introduce a 'mutable' field on config keys
                LOG.debug("configuration being made to {} after deployment: {} = {}; change may not be visible in other contexts", 
                    new Object[] { getContainer(), key, val });
            }
        }
        
        @Override
        protected <T> void onConfigChanged(ConfigKey<T> key, Object val) {
            getManagementSupport().getEntityChangeListener().onConfigChanged(key);
        }

        @Override
        protected BrooklynObject getContainer() {
            return AbstractEntity.this;
        }
        
        @Override
        protected ExecutionContext getContext() {
            return AbstractEntity.this.getExecutionContext();
        }
    }
    
    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return config().get(key);
    }
    
    @Override
    public <T> T getConfig(HasConfigKey<T> key) {
        return config().get(key);
    }

    // kept for internal use, for convenience
    protected <T> T getConfig(ConfigKey<T> key, T defaultValue) {
        T result = configsInternal.getConfig(key);
        if (result==null) return defaultValue;
        return result;
    }

    // TODO can this be replaced with config().set ?
    // seems only used for configure(Map) -- where validation might want to be skipped;
    // and also in a couple places where i don't think it matters
    @SuppressWarnings("unchecked")
    public <T> T setConfigEvenIfOwned(ConfigKey<T> key, T val) {
        return (T) configsInternal.setConfig(key, val);
    }

    public <T> T setConfigEvenIfOwned(HasConfigKey<T> key, T val) {
        return setConfigEvenIfOwned(key.getConfigKey(), val);
    }

    // -------- SUBSCRIPTIONS --------------

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return SubscriptionSupportInternal
    // TODO revert to SubscriptionSupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicSubscriptionSupport subscriptions() {
        return subscriptions;
    }

    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #subscriptions()} is reverted to return {@link SubscriptionSupportInternal} instead of
     * {@link BasicSubscriptionSupport}.
     */
    @Beta
    // TODO revert to private when config() is reverted to return SensorSupportInternal
    public class BasicSubscriptionSupport implements EntitySubscriptionSupportInternal {
        
        @Override
        public <T> SubscriptionHandle subscribe(Entity producer, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            return getSubscriptionTracker().subscribe(producer, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribe(Map<String, ?> flags, Entity producer, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            return getSubscriptionTracker().subscribe(flags, producer, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribeToChildren(Entity parent, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            return getSubscriptionTracker().subscribeToChildren(parent, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribeToMembers(Group group, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            return getSubscriptionTracker().subscribeToMembers(group, sensor, listener);
        }

        /**
         * Unsubscribes the given producer.
         *
         * @see SubscriptionContext#unsubscribe(SubscriptionHandle)
         */
        @Override
        public boolean unsubscribe(Entity producer) {
            return getSubscriptionTracker().unsubscribe(producer);
        }

        /**
         * Unsubscribes the given handle.
         *
         * @see SubscriptionContext#unsubscribe(SubscriptionHandle)
         */
        @Override
        public boolean unsubscribe(Entity producer, SubscriptionHandle handle) {
            return getSubscriptionTracker().unsubscribe(producer, handle);
        }

        /**
         * Unsubscribes the given handle.
         * 
         * It is (currently) more efficient to also pass in the producer -
         * see {@link BasicSubscriptionSupport#unsubscribe(Entity, SubscriptionHandle)} 
         */
        @Override
        public boolean unsubscribe(SubscriptionHandle handle) {
            return getSubscriptionTracker().unsubscribe(handle);
        }

        @Override
        public void unsubscribeAll() {
            getSubscriptionTracker().unsubscribeAll();
        }
        
        @Override
        public SubscriptionContext getSubscriptionContext() {
            // Rely on synchronization in EntityManagementSupport; synchronizing on AbstractEntity.this
            // is dangerous because user's entity code might synchronize on that and call getAttribute.
            // Given that getSubscriptionContext is called by AttributeMap.update (via emitInternal),
            // that risks deadlock!
            return getManagementSupport().getSubscriptionContext();
        }

        protected SubscriptionTracker getSubscriptionTracker() {
            if (_subscriptionTracker != null) {
                return _subscriptionTracker;
            }
            // TODO Would be nice to simplify concurrent model, and not synchronize on
            // AbstractEntity.this; perhaps could get rid of lazy-initialisation, but then
            // would need to first ensure `managementSupport` is definitely initialised.
            SubscriptionContext subscriptionContext = getSubscriptionContext();
            synchronized (AbstractEntity.this) {
                if (_subscriptionTracker == null) {
                    _subscriptionTracker = new SubscriptionTracker(subscriptionContext);
                }
                return _subscriptionTracker;
            }
        }
    }
    
    @Override
    public ExecutionContext getExecutionContext() {
        // NB May 2016 - removed synch keyword above due to deadlock (see https://issues.apache.org/jira/browse/BROOKLYN-284).
        // As with getManagementContext(), it also synchs in ManagementSupport.getExecutionContext();
        // no apparent reason why it was here also.
        return getManagementSupport().getExecutionContext();
    }

    /** Default String representation is simplified name of class, together with selected fields. */
    @Override
    public String toString() {
        return toStringHelper().toString();
    }
    
    /**
     * Override this to add to the toString(), e.g. {@code return super.toStringHelper().add("port", port);}
     *
     * Cannot be used in combination with overriding the deprecated toStringFieldsToInclude.
     */
    protected ToStringHelper toStringHelper() {
        return Objects.toStringHelper(this).omitNullValues().add("id", getId());
    }
    
    // -------- INITIALIZATION --------------

    /**
     * Default entity initialization sets ID sensors and calls {@link #initEnrichers()}.
     * <p>
     * See superclass Javadoc for more info.
     * <p>
     * Note that this is invoked before {@link org.apache.brooklyn.api.entity.EntityInitializer} instances are called,
     * before policies and enrichers are added,
     * before descendants are initialized,
     * before {@link EntityPostInitializable#postInit()},
     * and before this entity is managed.
     */
    @Override
    public void init() {
        super.init();
        initEnrichers();
        if (Strings.isNonBlank(getConfig(DEFAULT_DISPLAY_NAME))) {
            setDefaultDisplayName(getConfig(DEFAULT_DISPLAY_NAME));
        }
        sensors().set(ENTITY_ID, getId());
        sensors().set(APPLICATION_ID, getApplicationId());
        sensors().set(CATALOG_ID, getCatalogItemId());
    }
    
    /**
     * By default, adds enrichers to populate {@link Attributes#SERVICE_UP} and {@link Attributes#SERVICE_STATE_ACTUAL}
     * based on {@link Attributes#SERVICE_NOT_UP_INDICATORS}, 
     * {@link Attributes#SERVICE_STATE_EXPECTED} and {@link Attributes#SERVICE_PROBLEMS}
     * (doing nothing if these sensors are not used).
     * <p>
     * Subclasses may go further and populate the {@link Attributes#SERVICE_NOT_UP_INDICATORS} 
     * and {@link Attributes#SERVICE_PROBLEMS} from children and members or other sources.
     */
    // these enrichers do nothing unless Attributes.SERVICE_NOT_UP_INDICATORS are used
    // and/or SERVICE_STATE_EXPECTED 
    protected void initEnrichers() {
        enrichers().add(ServiceNotUpLogic.newEnricherForServiceUpIfNotUpIndicatorsEmpty());
        enrichers().add(ServiceStateLogic.newEnricherForServiceStateFromProblemsAndUp());
    }
    
    // -------- POLICIES --------------------

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return PolicySupportInternal
    // TODO revert to PolicySupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicPolicySupport policies() {
        return policies;
    }

    @Override 
    @Beta
    // the concrete type rather than an interface is returned because Groovy subclasses
    // complain (incorrectly) if we return EnricherSupportInternal
    // TODO revert to EnricherSupportInternal when groovy subclasses work without this (eg new groovy version)
    public BasicEnricherSupport enrichers() {
        return enrichers;
    }

    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #policies()} is reverted to return {@link {PolicySupportInternal} instead of
     * {@link BasicPolicySupport}.
     */
    @Beta
    public class BasicPolicySupport implements PolicySupportInternal {

        @Override
        public Iterator<Policy> iterator() {
            return asList().iterator();
        }

        @Override
        public int size() {
            return policiesInternal.size();
        }
        @Override
        public boolean isEmpty() {
            return policiesInternal.isEmpty();
        }
        @Override
        public List<Policy> asList() {
            return ImmutableList.<Policy>copyOf(policiesInternal);
        }

        @Override
        public void add(Policy policy) {
            Policy old = findApparentlyEqualAndWarnIfNotSameUniqueTag(policiesInternal, policy);
            if (old!=null) {
                LOG.debug("Removing "+old+" when adding "+policy+" to "+AbstractEntity.this);
                remove(old);
            }

            CatalogUtils.setCatalogItemIdOnAddition(AbstractEntity.this, policy);
            policiesInternal.add((AbstractPolicy)policy);
            ((AbstractPolicy)policy).setEntity(AbstractEntity.this);
            ConfigConstraints.assertValid(policy);

            getManagementSupport().getEntityChangeListener().onPolicyAdded(policy);
            sensors().emit(AbstractEntity.POLICY_ADDED, new PolicyDescriptor(policy));
        }

        @Override
        public <T extends Policy> T add(PolicySpec<T> spec) {
            T policy = getManagementContext().getEntityManager().createPolicy(spec);
            add(policy);
            return policy;
        }

        @Override
        public boolean remove(Policy policy) {
            ((AbstractPolicy)policy).destroy();
            boolean changed = policiesInternal.remove(policy);

            if (changed) {
                getManagementSupport().getEntityChangeListener().onPolicyRemoved(policy);
                sensors().emit(AbstractEntity.POLICY_REMOVED, new PolicyDescriptor(policy));
            }
            return changed;
        }

        @Override
        public boolean removeAllPolicies() {
            boolean changed = false;
            for (Policy policy : policiesInternal) {
                remove(policy);
                changed = true;
            }
            return changed;
        }
    }

    /**
     * Direct use of this class is strongly discouraged. It will become private in a future release,
     * once {@link #enrichers()} is reverted to return {@link EnricherSupportInternal} instead of
     * {@link BasicEnricherSupport}.
     */
    @Beta
    public class BasicEnricherSupport implements EnricherSupportInternal {
        @Override
        public Iterator<Enricher> iterator() {
            return asList().iterator();
        }

        @Override
        public int size() {
            return enrichersInternal.size();
        }
        @Override
        public boolean isEmpty() {
            return enrichersInternal.isEmpty();
        }
        @Override
        public List<Enricher> asList() {
            return ImmutableList.<Enricher>copyOf(enrichersInternal);
        }

        @Override
        public <T extends Enricher> T add(EnricherSpec<T> spec) {
            T enricher = getManagementContext().getEntityManager().createEnricher(spec);
            add(enricher);
            return enricher;
        }

        @Override
        public void add(Enricher enricher) {
            Enricher old = findApparentlyEqualAndWarnIfNotSameUniqueTag(enrichersInternal, enricher);
            if (old!=null) {
                LOG.debug("Removing "+old+" when adding "+enricher+" to "+AbstractEntity.this);
                remove(old);
            }
            
            CatalogUtils.setCatalogItemIdOnAddition(AbstractEntity.this, enricher);
            enrichersInternal.add((AbstractEnricher) enricher);
            ((AbstractEnricher)enricher).setEntity(AbstractEntity.this);
            ConfigConstraints.assertValid(enricher);
            
            getManagementSupport().getEntityChangeListener().onEnricherAdded(enricher);
            // TODO Could add equivalent of AbstractEntity.POLICY_ADDED for enrichers; no use-case for that yet
        }
        
        @Override
        public boolean remove(Enricher enricher) {
            ((AbstractEnricher)enricher).destroy();
            boolean changed = enrichersInternal.remove(enricher);
            
            if (changed) {
                getManagementSupport().getEntityChangeListener().onEnricherRemoved(enricher);
            }
            return changed;

        }

        @Override
        public boolean removeAll() {
            boolean changed = false;
            for (AbstractEnricher enricher : enrichersInternal) {
                changed = remove(enricher) || changed;
            }
            return changed;
        }
    }
    
    private <T extends EntityAdjunct> T findApparentlyEqualAndWarnIfNotSameUniqueTag(Collection<? extends T> items, T newItem) {
        T oldItem = findApparentlyEqual(items, newItem, true);
        
        if (oldItem!=null) {
            String oldItemTag = oldItem.getUniqueTag();
            String newItemTag = newItem.getUniqueTag();
            if (oldItemTag!=null || newItemTag!=null) {
                if (Objects.equal(oldItemTag, newItemTag)) {
                    // if same tag, return old item for replacing without comment
                    return oldItem;
                }
                // if one has a tag bug not the other, and they are apparently equal,
                // transfer the tag across
                T tagged = oldItemTag!=null ? oldItem : newItem;
                T tagless = oldItemTag!=null ? newItem : oldItem;
                LOG.warn("Apparently equal items "+oldItem+" and "+newItem+"; but one has a unique tag "+tagged.getUniqueTag()+"; applying to the other");
                ((AdjunctTagSupport)tagless.tags()).setUniqueTag(tagged.getUniqueTag());
            }
            
            if (isRebinding()) {
                LOG.warn("Adding to "+this+", "+newItem+" appears identical to existing "+oldItem+"; will replace. "
                    + "Underlying addition should be modified so it is not added twice during rebind or unique tag should be used to indicate it is identical.");
                return oldItem;
            } else {
                LOG.warn("Adding to "+this+", "+newItem+" appears identical to existing "+oldItem+"; may get removed on rebind. "
                    + "Underlying addition should be modified so it is not added twice.");
                return null;
            }
        } else {
            return null;
        }
    }
    private <T extends EntityAdjunct> T findApparentlyEqual(Collection<? extends T> itemsCopy, T newItem, boolean transferUniqueTag) {
        // TODO workaround for issue where enrichers/feeds/policies can get added multiple times on rebind,
        // if it's added in onBecomingManager or connectSensors; 
        // the right fix will be more disciplined about how/where these are added;
        // furthermore unique tags should be preferred;
        // when they aren't supplied, a reflection equals is done ignoring selected fields,
        // which is okay but not great ... and if it misses something (e.g. because an 'equals' isn't implemented)
        // then you can get a new instance on every rebind
        // (and currently these aren't readily visible, except looking at the counts or in persisted state) 
        Class<?> beforeEntityAdjunct = newItem.getClass();
        while (beforeEntityAdjunct.getSuperclass()!=null && !beforeEntityAdjunct.getSuperclass().equals(AbstractEntityAdjunct.class))
            beforeEntityAdjunct = beforeEntityAdjunct.getSuperclass();
        
        String newItemTag = newItem.getUniqueTag();
        for (T oldItem: itemsCopy) {
            String oldItemTag = oldItem.getUniqueTag();
            if (oldItemTag!=null && newItemTag!=null) { 
                if (oldItemTag.equals(newItemTag)) {
                    return oldItem;
                } else {
                    continue;
                }
            }
            // either does not have a unique tag, do deep equality
            if (oldItem.getClass().equals(newItem.getClass())) {
                if (EqualsBuilder.reflectionEquals(oldItem, newItem, false,
                        // internal admin in 'beforeEntityAdjunct' should be ignored
                        beforeEntityAdjunct,
                        // known fields which shouldn't block equality checks:
                        // from aggregator
                        "transformation",
                        // from averager
                        "values", "timestamps", "lastAverage",
                        // from some feeds
                        "poller",
                        "pollerStateMutex"
                        )) {
                    
                    return oldItem;
                }
            }
        }
        return null;
    }

    // -------- FEEDS --------------------

    /**
     * Convenience, which calls {@link EntityInternal#feeds()} and {@link FeedSupport#addFeed(Feed)}.
     */
    @Override
    public <T extends Feed> T addFeed(T feed) {
        feeds().add(feed);
        return feed;
    }

    @Override
    public FeedSupport feeds() {
        return feeds;
    }
    
    protected class BasicFeedSupport implements FeedSupport {

        @Override
        public Collection<Feed> getFeeds() {
            return ImmutableList.<Feed>copyOf(feedsInternal);
        }

        @Override
        public <T extends Feed> T add(T feed) {
            return addFeed(feed);
        }
        
        /** @deprecated since 1.0.0 use {@link #add(Feed)} */
        @Deprecated
        public <T extends Feed> T addFeed(T feed) {
            Feed old = findApparentlyEqualAndWarnIfNotSameUniqueTag(feedsInternal, feed);
            if (old != null) {
                if (old == feed) {
                    if (!BrooklynFeatureEnablement.isEnabled(BrooklynFeatureEnablement.FEATURE_FEED_REGISTRATION_PROPERTY)) {
                        LOG.debug("Feed " + feed + " already added, not adding a second time.");
                    } // else expected to be added a second time through addFeed, ignore
                    return feed;
                } else {
                    // Different feed object with (seemingly) same functionality, remove previous one, will stop it.
                    LOG.debug("Removing "+old+" when adding "+feed+" to "+this);
                    removeFeed(old);
                }
            }
            
            CatalogUtils.setCatalogItemIdOnAddition(AbstractEntity.this, feed);
            feedsInternal.add(feed);
            if (!AbstractEntity.this.equals(((AbstractFeed)feed).getEntity()))
                ((AbstractFeed)feed).setEntity(AbstractEntity.this);

            getManagementContext().getRebindManager().getChangeListener().onManaged(feed);
            getManagementSupport().getEntityChangeListener().onFeedAdded(feed);
            // TODO Could add equivalent of AbstractEntity.POLICY_ADDED for feeds; no use-case for that yet
            
            return feed;
        }

        @Override
        public boolean remove(Feed feed) {
            return removeFeed(feed);
        }

        /** @deprecated since 1.0.0 use {@link #remove(Feed)} */
        @Deprecated
        public boolean removeFeed(Feed feed) {
            feed.stop();
            boolean changed = feedsInternal.remove(feed);
            
            if (changed) {
                getManagementContext().getRebindManager().getChangeListener().onUnmanaged(feed);
                getManagementSupport().getEntityChangeListener().onFeedRemoved(feed);
            }
            return changed;
        }

        @Override
        public boolean removeAll() {
            return removeAllFeeds();
        }

        @Override
        public boolean removeAllFeeds() {
            boolean changed = false;
            for (Feed feed : feedsInternal) {
                changed = removeFeed(feed) || changed;
            }
            return changed;
        }

        @Override
        public Iterator<Feed> iterator() {
            return getFeeds().iterator();
        }

        // TODO add these back when we implement AdjunctSupport (after 1.0.0)
//        @Override
//        public int size() {
//            return getFeeds().size();
//        }
//
//        @Override
//        public boolean isEmpty() {
//            return getFeeds().isEmpty();
//        }
//
//        @Override
//        public List<Feed> asList() {
//            return ImmutableList.copyOf(getFeeds());
//        }
    }
    
    // -------- EFFECTORS --------------

    /** Convenience for finding named effector in {@link EntityType#getEffectors()} {@link Map}. */
    @Override
    public Effector<?> getEffector(String effectorName) {
        return entityType.getEffector(effectorName);
    }

    /** Invoke an {@link Effector} directly. */
    public <T> Task<T> invoke(Effector<T> eff) {
        return invoke(MutableMap.of(), eff);
    }
    
    public <T> Task<T> invoke(Map parameters, Effector<T> eff) {
        return invoke(eff, parameters);
    }

    /**
     * Additional form supplied for when the parameter map needs to be made explicit.
     *
     * @see #invoke(Effector)
     */
    @Override
    public <T> Task<T> invoke(Effector<T> eff, Map<String,?> parameters) {
        return EffectorUtils.invokeEffectorAsync(this, eff, parameters);
    }

    /**
     * Invoked by {@link EntityManagementSupport} when this entity is becoming managed (i.e. it has a working
     * management context, but before the entity is visible to other entities), including during a rebind.
     */
    public void onManagementStarting() {
        if (isLegacyConstruction()) {
            entityType.setName(getEntityTypeName());
            if (displayNameAutoGenerated) displayName.set(getEntityType().getSimpleName()+":"+Strings.maxlen(getId(), 4));
        }
    }
    
    /**
     * Invoked by {@link EntityManagementSupport} when this entity is fully managed and visible to other entities
     * through the management context.
     */
    public void onManagementStarted() {}
    
    /**
     * Invoked by {@link ManagementContext} when this entity becomes managed at a particular management node,
     * including the initial management started and subsequent management node master-change for this entity.
     * @deprecated since 0.4.0 override EntityManagementSupport.onManagementStarted if customization needed
     */
    @Deprecated
    public void onManagementBecomingMaster() {}
    
    /**
     * Invoked by {@link ManagementContext} when this entity becomes mastered at a particular management node,
     * including the final management end and subsequent management node master-change for this entity.
     * @deprecated since 0.4.0 override EntityManagementSupport.onManagementStopped if customization needed
     */
    @Deprecated
    public void onManagementNoLongerMaster() {}

    /**
     * Invoked by {@link EntityManagementSupport} when this entity is fully unmanaged.
     * <p>
     * Note that the activies possible here (when unmanaged) are limited, 
     * and that this event may be caused by either a brooklyn node itself being demoted
     * (so the entity is managed elsewhere) or by a controlled shutdown.
     */
    public void onManagementStopped() {
        if (getManagementContext().isRunning()) {
            BrooklynStorage storage = ((ManagementContextInternal)getManagementContext()).getStorage();
            storage.remove(getId()+"-parent");
            storage.remove(getId()+"-groups");
            storage.remove(getId()+"-children");
            storage.remove(getId()+"-locations");
            storage.remove(getId()+"-creationTime");
            storage.remove(getId()+"-displayName");
            storage.remove(getId()+"-config");
            storage.remove(getId()+"-attributes");
        }
    }
    
    /** For use by management plane, to invalidate all fields (e.g. when an entity is changing to being proxied) */
    public void invalidateReferences() {
        // TODO Just rely on GC of this entity instance, to get rid of the children map etc.
        //      Don't clear it, as it's persisted.
        // TODO move this to EntityMangementSupport,
        application = null;
    }
    
    @Override
    public EntityManagementSupport getManagementSupport() {
        return managementSupport;
    }

    @Override
    public void requestPersist() {
        getManagementSupport().getEntityChangeListener().onChanged();
    }

    /**
     * As described in {@link EntityInternal#getRebindSupport()}...
     * Users are strongly discouraged to call or override this method.
     * It is for internal calls only, relating to persisting/rebinding entities.
     * This method may change (or be removed) in a future release without notice.
     */
    @Override
    @Beta
    public RebindSupport<EntityMemento> getRebindSupport() {
        return new BasicEntityRebindSupport(this);
    }

    @Override
    protected void onTagsChanged() {
        super.onTagsChanged();
        getManagementSupport().getEntityChangeListener().onTagsChanged();
    }

    @SuppressWarnings("unchecked")
    @Override
    public RelationSupportInternal<Entity> relations() {
        return (RelationSupportInternal<Entity>) super.relations();
    }
    
}
