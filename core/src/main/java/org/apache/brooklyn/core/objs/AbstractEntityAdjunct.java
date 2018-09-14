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
package org.apache.brooklyn.core.objs;

import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.ExecutionContext;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.objs.Configurable;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.objs.HighlightTuple;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.config.internal.AbstractConfigMapImpl;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.entity.internal.ConfigUtilsInternal;
import org.apache.brooklyn.core.mgmt.internal.SubscriptionTracker;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.FlagUtils;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;


/**
 * Common functionality for policies and enrichers
 */
public abstract class AbstractEntityAdjunct extends AbstractBrooklynObject implements BrooklynObjectInternal, EntityAdjunct, Configurable {
    private static final Logger log = LoggerFactory.getLogger(AbstractEntityAdjunct.class);

    private boolean _legacyNoConstructionInit;

    /**
     * @deprecated since 0.7.0; leftover properties are put into config, since when coming from yaml this is normal.
     */
    @Deprecated
    protected Map<String,Object> leftoverProperties = Maps.newLinkedHashMap();

    /** @deprecated since 1.0.0, going private, use {@link #getExecutionContext()} */
    @Deprecated
    protected transient ExecutionContext execution;

    private final BasicConfigurationSupport config = new BasicConfigurationSupport();

    private final BasicSubscriptionSupport subscriptions = new BasicSubscriptionSupport();

    /**
     * The config values of this entity. Updating this map should be done
     * via {@link #config()}.
     */
    private final AdjunctConfigMap configsInternal = new AdjunctConfigMap(this);

    private final AdjunctType adjunctType = new AdjunctType(this);

    @SetFromFlag
    protected String name;
    
    protected transient EntityLocal entity;
    
    /** not for direct access; refer to as 'subscriptionTracker' via getter so that it is initialized */
    protected transient SubscriptionTracker _subscriptionTracker;
    
    private AtomicBoolean destroyed = new AtomicBoolean(false);
    
    @SetFromFlag(value="uniqueTag")
    protected String uniqueTag;

    private Map<String, HighlightTuple> highlights = new HashMap<>();

    /** Name of a highlight that indicates the last action taken by this adjunct. */
    public static String HIGHLIGHT_NAME_LAST_ACTION = "lastAction";
    /** Name of a highlight that indicates the last confirmation detected by this adjunct. */
    public static String HIGHLIGHT_NAME_LAST_CONFIRMATION= "lastConfirmation";
    /** Name of a highlight that indicates the last violation detected by this adjunct. */
    public static String HIGHLIGHT_NAME_LAST_VIOLATION= "lastViolation";
    /** Name of a highlight that indicates the triggers for this adjunct. */
    public static String HIGHLIGHT_NAME_TRIGGERS = "triggers";

    public AbstractEntityAdjunct() {
        this(Collections.emptyMap());
    }
    
    public AbstractEntityAdjunct(@SuppressWarnings("rawtypes") Map properties) {
        super(properties);
        _legacyNoConstructionInit = (properties != null) && Boolean.TRUE.equals(properties.get("noConstructionInit"));
        
        if (isLegacyConstruction()) {
            AbstractEntityAdjunct checkWeGetThis = configure(properties);
            assert this.equals(checkWeGetThis) : this+" configure method does not return itself; returns "+checkWeGetThis+" instead of "+this;

            boolean deferConstructionChecks = (properties.containsKey("deferConstructionChecks") && TypeCoercions.coerce(properties.get("deferConstructionChecks"), Boolean.class));
            if (!deferConstructionChecks) {
                FlagUtils.checkRequiredFields(this);
            }
        }
    }

    /**
     * @deprecated since 0.7.0; only used for legacy brooklyn types where constructor is called directly
     */
    @Override
    @Deprecated
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public AbstractEntityAdjunct configure(Map flags) {
        // TODO only set on first time through
        boolean isFirstTime = true;
        
        // allow config keys, and fields, to be set from these flags if they have a SetFromFlag annotation
        // or if the value is a config key
        for (Iterator<Map.Entry> iter = flags.entrySet().iterator(); iter.hasNext();) {
            Map.Entry entry = iter.next();
            if (entry.getKey() instanceof ConfigKey) {
                ConfigKey key = (ConfigKey)entry.getKey();
                if (adjunctType.getConfigKeys().contains(key)) {
                    config().set(key, entry.getValue());
                } else {
                    log.warn("Unknown configuration key {} for policy {}; ignoring", key, this);
                    iter.remove();
                }
            }
        }

        // Allow config keys to be set by name (or deprecated name)
        //
        // The resulting `flags` will no longer contain the keys that we matched;
        // we will not also use them to for `SetFromFlag` etc.
        flags = ConfigUtilsInternal.setAllConfigKeys(flags, getAdjunctType().getConfigKeys(), this);

        // Must call setFieldsFromFlagsWithBag, even if properites is empty, so defaults are extracted from annotations
        ConfigBag bag = new ConfigBag().putAll(flags);
        FlagUtils.setFieldsFromFlags(this, bag, isFirstTime);
        FlagUtils.setAllConfigKeys(this, bag, false);
        leftoverProperties.putAll(bag.getUnusedConfig());

        if (!groovyTruth(name) && leftoverProperties.containsKey("displayName")) {
            //TODO inconsistent with entity and location, where name is legacy and displayName is encouraged!
            //'displayName' is a legacy way to refer to a policy's name
            Preconditions.checkArgument(leftoverProperties.get("displayName") instanceof CharSequence, "'displayName' property should be a string");
            setDisplayName(leftoverProperties.remove("displayName").toString());
        }
        
        // set leftover leftoverProperties should as config items; particularly useful when these have come from a brooklyn.config map 
        for (Object flag: leftoverProperties.keySet()) {
            ConfigKey<Object> key = ConfigKeys.newConfigKey(Object.class, Strings.toString(flag));
            if (config().getRaw(key).isPresent()) {
                log.warn("Config '"+flag+"' on "+this+" conflicts with key already set; ignoring");
            } else {
                config().set(key, leftoverProperties.get(flag));
            }
        }
        
        return this;
    }
    
    /**
     * Used for legacy-style policies/enrichers on rebind, to indicate that init() should not be called.
     * Will likely be deleted in a future release; should not be called apart from by framework code.
     */
    @Beta
    protected boolean isLegacyNoConstructionInit() {
        return _legacyNoConstructionInit;
    }

    /** If the entity has been set, returns the execution context indicating this adjunct.
     * Primarily intended for this adjunct to execute tasks, but in some cases, mainly low level,
     * it may make sense for other components to execute tasks against this adjunct. */
    @Beta
    public ExecutionContext getExecutionContext() {
        return execution;
    }
    
    @Override
    public ConfigurationSupportInternal config() {
        return config;
    }

    @Override
    public BasicSubscriptionSupport subscriptions() {
        return subscriptions;
    }

    public class BasicSubscriptionSupport implements SubscriptionSupportInternal {
        @Override
        public <T> SubscriptionHandle subscribe(Entity producer, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            if (!checkCanSubscribe()) return null;
            return getSubscriptionTracker().subscribe(producer, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribe(Map<String, ?> flags, Entity producer, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            if (!checkCanSubscribe()) return null;
            return getSubscriptionTracker().subscribe(flags, producer, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribeToMembers(Group producerGroup, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            if (!checkCanSubscribe(producerGroup)) return null;
            return getSubscriptionTracker().subscribeToMembers(producerGroup, sensor, listener);
        }

        @Override
        public <T> SubscriptionHandle subscribeToChildren(Entity producerParent, Sensor<T> sensor, SensorEventListener<? super T> listener) {
            if (!checkCanSubscribe(producerParent)) return null;
            return getSubscriptionTracker().subscribeToChildren(producerParent, sensor, listener);
        }
        
        @Override
        public boolean unsubscribe(Entity producer) {
            if (destroyed.get()) return false;
            return getSubscriptionTracker().unsubscribe(producer);
        }

        @Override
        public boolean unsubscribe(Entity producer, SubscriptionHandle handle) {
            if (destroyed.get()) return false;
            return getSubscriptionTracker().unsubscribe(producer, handle);
        }

        @Override
        public boolean unsubscribe(SubscriptionHandle handle) {
            if (destroyed.get()) return false;
            return getSubscriptionTracker().unsubscribe(handle);
        }

        @Override
        public void unsubscribeAll() {
            if (destroyed.get()) return;
            getSubscriptionTracker().unsubscribeAll();
        }

        protected SubscriptionTracker getSubscriptionTracker() {
            synchronized (AbstractEntityAdjunct.this) {
                if (_subscriptionTracker!=null) return _subscriptionTracker;
                if (entity==null) return null;
                _subscriptionTracker = new SubscriptionTracker(getManagementContext().getSubscriptionContext(entity, AbstractEntityAdjunct.this));
                return _subscriptionTracker;
            }
        }
        
        /** returns false if deleted, throws exception if invalid state, otherwise true.
         * okay if entity is not yet managed (but not if entity is no longer managed). */
        protected boolean checkCanSubscribe(Entity producer) {
            if (destroyed.get()) return false;
            if (producer==null) throw new IllegalStateException(this+" given a null target for subscription");
            if (entity==null) throw new IllegalStateException(this+" cannot subscribe to "+producer+" because it is not associated to an entity");
            if (((EntityInternal)entity).getManagementSupport().isNoLongerManaged()) throw new IllegalStateException(this+" cannot subscribe to "+producer+" because the associated entity "+entity+" is no longer managed");
            return true;
        }
        
        protected boolean checkCanSubscribe() {
            if (destroyed.get()) return false;
            if (entity==null) throw new IllegalStateException(this+" cannot subscribe because it is not associated to an entity");
            if (((EntityInternal)entity).getManagementSupport().isNoLongerManaged()) throw new IllegalStateException(this+" cannot subscribe because the associated entity "+entity+" is no longer managed");
            return true;
        }

        /**
         * @return a list of all subscription handles
         */
        protected Collection<SubscriptionHandle> getAllSubscriptions() {
            SubscriptionTracker tracker = getSubscriptionTracker();
            return (tracker != null) ? tracker.getAllSubscriptions() : Collections.<SubscriptionHandle>emptyList();
        }
    }
    
    private class BasicConfigurationSupport extends AbstractConfigurationSupportInternal {

        @SuppressWarnings("unchecked")
        @Override
        protected <T> void onConfigChanging(ConfigKey<T> key, Object val) {
            if (entity != null && isRunning()) {
                doReconfigureConfig(key, (T)val);
            }
        }

        @Override
        protected <T> void onConfigChanged(ConfigKey<T> key, Object val) {
            onChanged();
        }

        @Override
        public void refreshInheritedConfig() {
            // no-op here
        }
        
        @Override
        public void refreshInheritedConfigOfChildren() {
            // no-op here
        }

        @Override
        protected ExecutionContext getContext() {
            return AbstractEntityAdjunct.this.getExecutionContext();
        }

        @Override
        protected AbstractConfigMapImpl<?> getConfigsInternal() {
            return configsInternal;
        }

        @Override
        protected <T> void assertValid(ConfigKey<T> key, T val) {
            ConfigConstraints.assertValid(entity, key, val);
        }

        @Override
        protected BrooklynObject getContainer() {
            return AbstractEntityAdjunct.this;
        }
    }

    @Override
    public <T> T getConfig(ConfigKey<T> key) {
        return config().get(key);
    }
    
    protected <K> K getRequiredConfig(ConfigKey<K> key) {
        K result = config().get(key);
        if (result==null) 
            throw new NullPointerException("Value required for '"+key.getName()+"' in "+this);
        return result;
    }

    /**
     * Invoked whenever a config change is applied after management is started.
     * Default implementation throws an exception to disallow the change. 
     * Can be overridden to return (allowing the change) or to make other changes 
     * (if necessary), and of course it can do this selectively and call the super to disallow any others. */
    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        throw new UnsupportedOperationException("reconfiguring "+key+" unsupported for "+this);
    }
    
    @Override
    protected void onTagsChanged() {
        onChanged();
    }
    
    protected abstract void onChanged();
    
    public AdjunctType getAdjunctType() {
        return adjunctType;
    }
    
    @Override
    public String getDisplayName() {
        if (name!=null && name.length()>0) return name;
        return getClass().getCanonicalName();
    }
    
    @Override
    public void setDisplayName(String name) {
        this.name = name;
    }

    @Override
    public ManagementContext getManagementContext() {
        ManagementContext result = super.getManagementContext();
        if (result!=null) return result;
        if (entity!=null) {
            return ((EntityInternal)entity).getManagementContext();
        }
        return null;
    }
    
    public void setEntity(EntityLocal entity) {
        if (destroyed.get()) throw new IllegalStateException("Cannot set entity on a destroyed entity adjunct");
        this.entity = entity;
        this.execution = getManagementContext().getExecutionContext(entity, this);
        if (entity!=null && getCatalogItemId() == null) {
            setCatalogItemIdAndSearchPath(entity.getCatalogItemId(), entity.getCatalogItemIdSearchPath());
        }
    }
    
    public Entity getEntity() {
        return entity;
    }
    
    private synchronized SubscriptionTracker getSubscriptionTracker() {
        if (_subscriptionTracker!=null) return _subscriptionTracker;
        if (entity==null) return null;
        _subscriptionTracker = new SubscriptionTracker(((EntityInternal)entity).subscriptions().getSubscriptionContext());
        return _subscriptionTracker;
    }

    /** 
     * Unsubscribes and clears all managed subscriptions; is called by the owning entity when a policy is removed
     * and should always be called by any subclasses overriding this method
     */
    public void destroy() {
        destroyed.set(true);
        SubscriptionTracker tracker = getSubscriptionTracker();
        if (tracker != null) tracker.unsubscribeAll();
    }
    
    @Override
    public boolean isDestroyed() {
        return destroyed.get();
    }
    
    @Override
    public boolean isRunning() {
        return !isDestroyed();
    }

    @Override
    public String getUniqueTag() {
        return uniqueTag;
    }

    @Override
    public TagSupport tags() {
        return new AdjunctTagSupport();
    }

    public class AdjunctTagSupport extends BasicTagSupport {
        @Override
        public Set<Object> getTags() {
            ImmutableSet.Builder<Object> rb = ImmutableSet.builder().addAll(super.getTags());
            if (getUniqueTag()!=null) rb.add(getUniqueTag());
            return rb.build();
        }
        public String getUniqueTag() {
            return AbstractEntityAdjunct.this.getUniqueTag();
        }
        public void setUniqueTag(String uniqueTag) {
            AbstractEntityAdjunct.this.uniqueTag = uniqueTag;
        }
    }

    @Override
    public Map<String, HighlightTuple> getHighlights() {
        HashMap<String, HighlightTuple> highlightsToReturn = new HashMap<>();
        highlightsToReturn.putAll(highlights);
        return highlightsToReturn;
    }

    /** Records a named highlight against this object, for persistence and API access.
     * See common highlights including {@link #HIGHLIGHT_NAME_LAST_ACTION} and
     * {@link #HIGHLIGHT_NAME_LAST_CONFIRMATION}.
     * Also see convenience methods eg  {@link #highlightOngoing(String, String)} and {@link #highlight(String, String, Task)}
     * and {@link HighlightTuple}. 
     */
    protected void setHighlight(String name, HighlightTuple tuple) {
        highlights.put(name, tuple);
        requestPersist();
    }

    /** As {@link #setHighlight(String, HighlightTuple)}, convenience for recording an item which is intended to be ongoing. */
    protected void highlightOngoing(String name, String description) {
        setHighlight(name, new HighlightTuple(description, 0, null));
    }
    /** As {@link #setHighlight(String, HighlightTuple)}, convenience for recording an item with the current time. */
    protected void highlightNow(String name, String description) {
        setHighlight(name, new HighlightTuple(description, System.currentTimeMillis(), null));
    }
    /** As {@link #setHighlight(String, HighlightTuple)}, convenience for recording an item with the current time and given task. */
    protected void highlight(String name, String description, @Nullable Task<?> t) {
        setHighlight(name, new HighlightTuple(description, System.currentTimeMillis(), t!=null ? t.getId() : null));
    }
    
    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_TRIGGERS} (as ongoing). */
    protected void highlightTriggers(String description) {
        highlightOngoing(HIGHLIGHT_NAME_TRIGGERS, description);
    }
    /** As {@link #highlightTriggers(String)} but convenience to generate a message given a sensor and source (entity or string description) */
    protected <T> void highlightTriggers(Sensor<?> s, Object source) {
        highlightTriggers(Collections.singleton(s), Collections.singleton(source));
    }
    /** As {@link #highlightTriggers(String)} but convenience to generate a message given a list of sensors and source (entity or string description) */
    protected <T> void highlightTriggers(Iterable<? extends Sensor<? extends T>> s, Object source) {
        highlightTriggers(s, (Iterable<?>) (source instanceof Iterable ? (Iterable<?>)source : Collections.singleton(source)));
    }
    /** As {@link #highlightTriggers(String)} but convenience to generate a message given a sensor and list of sources (entity or string description) */
    protected <U> void highlightTriggers(Sensor<?> s, Iterable<U> sources) {
        highlightTriggers(Collections.singleton(s), sources);
    }
    /** As {@link #highlightTriggers(String)} but convenience to generate a message given a list of sensors and list of sources (entity or string description) */
    protected <T,U> void highlightTriggers(Iterable<? extends Sensor<? extends T>> sensors, Iterable<U> sources) {
        StringBuilder msg = new StringBuilder("Listening for ");

        if (sensors==null || Iterables.isEmpty(sensors)) {
            msg.append("<nothing>");
        } else {
            String sensorsText = MutableSet.<Object>copyOf(sensors).stream()
                    .filter(s -> s != null)
                    .map(s -> (s instanceof Sensor ? ((Sensor<?>) s).getName() : s.toString()))
                    .collect(Collectors.joining(", "));
            msg.append(sensorsText);
        }

        if (sources!=null && !Iterables.isEmpty(sources)) {
            String sourcesText = MutableSet.<Object>copyOf(sources).stream()
                    .filter(s -> s != null)
                    .map(s -> (s.equals(getEntity()) ? "self" : s.toString()))
                    .collect(Collectors.joining(", "));
            if (!"self".equals(sourcesText)) {
                msg.append(" on ").append(sourcesText);
            }
        }

        highlightTriggers(msg.toString());
    }

    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_LAST_CONFIRMATION}. */
    protected void highlightConfirmation(String description) {
        highlightNow(HIGHLIGHT_NAME_LAST_CONFIRMATION, description);
    }
    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_LAST_ACTION}. */
    protected void highlightAction(String description, Task<?> t) {
        highlight(HIGHLIGHT_NAME_LAST_ACTION, description, t);
    }
    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_LAST_ACTION} when publishing a sensor. */
    protected void highlightActionPublishSensor(Sensor<?> s, Object v) {
        highlightActionPublishSensor("Publish "+s.getName()+" "+v);
    }
    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_LAST_ACTION} when publishing a sensor (freeform text). */
    protected void highlightActionPublishSensor(String description) {
        highlight(HIGHLIGHT_NAME_LAST_ACTION, description, null);
    }
    /** As {@link #setHighlight(String, HighlightTuple)} for {@link #HIGHLIGHT_NAME_LAST_VIOLATION}. */
    protected void highlightViolation(String description) {
        highlightNow(HIGHLIGHT_NAME_LAST_VIOLATION, description);
    }
    
    /**
     * Should only be used for rebind
     * @param highlights
     */
    public void setHighlights(Map<String, HighlightTuple> highlights) {
        if(highlights != null) {
            this.highlights.putAll(highlights);
            requestPersist();
        }
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(getClass()).omitNullValues()
                .add("name", name)
                .add("uniqueTag", uniqueTag)
                .add("running", isRunning())
                .add("entity", entity)
                .add("id", getId())
                .toString();
    }
}
