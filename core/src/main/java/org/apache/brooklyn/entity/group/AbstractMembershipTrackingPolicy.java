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
package org.apache.brooklyn.entity.group;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.objs.AbstractEntityAdjunct;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

/**
 * Abstract class which helps track membership of a group, invoking (empty) methods in this class
 * on MEMBER{ADDED,REMOVED} events, as well as SERVICE_UP {true,false} for those members.
 */
public abstract class AbstractMembershipTrackingPolicy extends AbstractPolicy {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractMembershipTrackingPolicy.class);
    
    public enum EventType { ENTITY_CHANGE, ENTITY_ADDED, ENTITY_REMOVED }
    
    @SuppressWarnings("serial")
    public static final ConfigKey<Set<Sensor<?>>> SENSORS_TO_TRACK = ConfigKeys.newConfigKey(
            new TypeToken<Set<Sensor<?>>>() {},
            "sensorsToTrack",
            "Sensors of members to be monitored (implicitly adds service-up to this list, but that behaviour may be deleted in a subsequent release!)",
            ImmutableSet.<Sensor<?>>of());

    public static final ConfigKey<Boolean> NOTIFY_ON_DUPLICATES = ConfigKeys.newBooleanConfigKey("notifyOnDuplicates",
            "Whether to notify listeners when a sensor is published with the same value as last time",
            false);

    public static final ConfigKey<Group> GROUP = ConfigKeys.newConfigKey(Group.class, "group");

    private ConcurrentMap<String,Map<Sensor<Object>, Object>> entitySensorCache = new ConcurrentHashMap<String, Map<Sensor<Object>, Object>>();
    
    public AbstractMembershipTrackingPolicy() {
        super();
    }

    protected Set<Sensor<?>> getSensorsToTrack() {
        return ImmutableSet.<Sensor<?>>builder()
                .addAll(getRequiredConfig(SENSORS_TO_TRACK))
                .add(Attributes.SERVICE_UP)
                .build();
    }
    
    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        Group group = getGroup();
        if (group != null) {
            if (uniqueTag==null) {
                uniqueTag = JavaClassNames.simpleClassName(this)+":"+group;
            }
            subscribeToGroup(group);
        } else {
            LOG.warn("Deprecated use of "+AbstractMembershipTrackingPolicy.class.getSimpleName()+"; group should be set as config");
        }
    }
    
    @Override
    protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
        if (GROUP.getName().equals(key.getName())) {
            Preconditions.checkNotNull(val, "%s value must not be null", GROUP.getName());
            Preconditions.checkNotNull(val, "%s value must be a group, but was %s (of type %s)", GROUP.getName(), val, val.getClass());
            if (val.equals(getConfig(GROUP))) {
                if (LOG.isDebugEnabled()) LOG.debug("No-op for reconfigure group of "+AbstractMembershipTrackingPolicy.class.getSimpleName()+"; group is still "+val);
            } else {
                if (LOG.isInfoEnabled()) LOG.info("Membership tracker "+AbstractMembershipTrackingPolicy.class+", resubscribing to group "+val+", previously was "+getGroup());
                unsubscribeFromGroup();
                subscribeToGroup((Group)val);
            }
        } else {
            throw new UnsupportedOperationException("reconfiguring "+key+" unsupported for "+this);
        }
    }
    
    @Override
    public void suspend() {
        unsubscribeFromGroup();
        super.suspend();
    }
    
    @Override
    public void resume() {
        boolean wasSuspended = isSuspended();
        super.resume();
        
        Group group = getGroup();
        if (wasSuspended && group != null) {
            subscribeToGroup(group);
        }
    }

    protected Group getGroup() {
        return getConfig(GROUP);
    }
    
    protected void subscribeToGroup(final Group group) {
        Preconditions.checkNotNull(group, "The group must not be null");

        BrooklynLogging.log(LOG, BrooklynLogging.levelDebugOrTraceIfReadOnly(group),
            "Subscribing to group "+group+", for memberAdded, memberRemoved, and {}", getSensorsToTrack());
        
        subscriptions().subscribe(group, DynamicGroup.MEMBER_ADDED, new SensorEventListener<Entity>() {
            @Override public void onEvent(SensorEvent<Entity> event) {
                onEntityEvent(EventType.ENTITY_ADDED, event.getValue());
            }
        });
        subscriptions().subscribe(group, DynamicGroup.MEMBER_REMOVED, new SensorEventListener<Entity>() {
            @Override public void onEvent(SensorEvent<Entity> event) {
                entitySensorCache.remove(event.getSource().getId());
                onEntityEvent(EventType.ENTITY_REMOVED, event.getValue());
            }
        });

        for (Sensor<?> sensor : getSensorsToTrack()) {
            subscriptions().subscribeToMembers(group, sensor, new SensorEventListener<Object>() {
                @Override public void onEvent(SensorEvent<Object> event) {
                    boolean notifyOnDuplicates = getRequiredConfig(NOTIFY_ON_DUPLICATES);
                    String entityId = event.getSource().getId();

                    if (!notifyOnDuplicates) {
                        Map<Sensor<Object>, Object> newMap = MutableMap.<Sensor<Object>, Object>of();
                        // NOTE: putIfAbsent returns null if the key is not present, or the *previous* value if present
                        Map<Sensor<Object>, Object> sensorCache = entitySensorCache.putIfAbsent(entityId, newMap);
                        if (sensorCache == null) {
                            sensorCache = newMap;
                        }
                        
                        boolean oldExists = sensorCache.containsKey(event.getSensor());
                        Object oldVal = sensorCache.put(event.getSensor(), event.getValue());
                        
                        if (oldExists && Objects.equal(event.getValue(), oldVal)) {
                            // ignore if value has not changed
                            return;
                        }
                    }

                    onEntityEvent(EventType.ENTITY_CHANGE, event.getSource());
                }
            });
        }
        highlightTriggers(getSensorsToTrack(), "group members");

        // The policy will have already fired events for its members.
        if (!isRebinding()) {
            for (Entity it : group.getMembers()) {
                onEntityEvent(EventType.ENTITY_ADDED, it);
            }
        }
    }

    protected void unsubscribeFromGroup() {
        Group group = getGroup();
        if (group != null) subscriptions().unsubscribe(group);
    }

    /** Invoked by framework prior to all entity events, to provide default highlight info;
     * if subclasses provide their own they can overwrite this method to be no-op,
     * or they can change the message passed to {@link #defaultHighlightAction(EventType, Entity, String)}
     * which defaults to "Update on %s %s"  */
    // bit of a cheat but more informative than doing nothing; callers can override of course
    protected void defaultHighlightAction(EventType type, Entity entity) {
        defaultHighlightAction(type, entity, "Update on %s %s");
    }
    /** Records an {@link AbstractEntityAdjunct#HIGHLIGHT_NAME_LAST_ACTION} with the given message,
     * formatted with arguments entity and either 'added', 'changed', or 'removed'.
     * Can be overridden to be no-op if caller wants to manage their own such highlights,
     * or to provide further information. See also {@link #defaultHighlightAction(EventType, Entity)}. */
    protected void defaultHighlightAction(EventType type, Entity entity, String formattedMessage) {
        highlightAction(String.format(formattedMessage, entity, Strings.removeFromStart(type.toString().toLowerCase(), "entity_")), null);
    }
    
    /** All entity events pass through this method. Default impl delegates to onEntityXxxx methods, whose default behaviours are no-op.
     * Callers may override this to intercept all entity events in a single place, and to suppress subsequent processing if desired. 
     */
    protected void onEntityEvent(EventType type, Entity entity) {  
        defaultHighlightAction(type, entity);
        
        switch (type) {
        case ENTITY_CHANGE: onEntityChange(entity); break;
        case ENTITY_ADDED: onEntityAdded(entity); break;
        case ENTITY_REMOVED: onEntityRemoved(entity); break;
        }
    }
    
    /**
     * Called when a member's "up" sensor changes.
     */
    protected void onEntityChange(Entity member) {}

    /**
     * Called when a member is added.
     * Note that the change event may arrive before this event; implementations here should typically look at the last value.
     */
    protected void onEntityAdded(Entity member) {}

    /**
     * Called when a member is removed.
     * Note that entity change events may arrive after this event; they should typically be ignored.
     * The entity could already be unmanaged at this point so limited functionality is available (i.e. can't access config keys).
     */
    protected void onEntityRemoved(Entity member) {}
}
