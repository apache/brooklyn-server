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

import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.join;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ExecutionManager;
import org.apache.brooklyn.api.mgmt.SubscriptionHandle;
import org.apache.brooklyn.api.mgmt.SubscriptionManager;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.api.sensor.SensorEventListener;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.SingleThreadedScheduler;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimaps;

/**
 * A {@link SubscriptionManager} that stores subscription details locally.
 */
public class LocalSubscriptionManager extends AbstractSubscriptionManager {
    
    private static final Logger LOG = LoggerFactory.getLogger(LocalSubscriptionManager.class);

    protected final ExecutionManager em;
    
    private final String tostring = "SubscriptionContext("+Identifiers.getBase64IdFromValue(System.identityHashCode(this), 5)+")";

    private final AtomicLong totalEventsPublishedCount = new AtomicLong();
    private final AtomicLong totalEventsDeliveredCount = new AtomicLong();
    
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<String, Subscription> allSubscriptions = new ConcurrentHashMap<String, Subscription>();
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<Object, Set<Subscription>> subscriptionsBySubscriber = new ConcurrentHashMap<Object, Set<Subscription>>();
    @SuppressWarnings("rawtypes")
    protected final ConcurrentMap<Object, Set<Subscription>> subscriptionsByToken = new ConcurrentHashMap<Object, Set<Subscription>>();
    
    public LocalSubscriptionManager(ExecutionManager m) {
        this.em = m;
    }
        
    public long getNumSubscriptions() {
        return allSubscriptions.size();
    }

    /** The total number of sensor change events generated (irrespective of number subscribers, see {@link #getTotalEventsDelivered()}) */
    public long getTotalEventsPublished() {
        return totalEventsPublishedCount.get();
    }
    
    /** The total number of sensor change events submitted for delivery, counting multiple deliveries for multipe subscribers (see {@link #getTotalEventsPublished()}),
     * but excluding initial notifications, and incremented when submitted ie prior to delivery */
    public long getTotalEventsDelivered() {
        return totalEventsDeliveredCount.get();
    }
    
    @Override
    @SuppressWarnings("unchecked")
    protected synchronized <T> SubscriptionHandle subscribe(Map<String, Object> flags, final Subscription<T> s) {
        Entity producer = s.producer;
        Sensor<T> sensor= s.sensor;
        s.subscriber = getSubscriber(flags, s);
        s.subscriptionDescription = getSubscriptionDescription(flags, s);
        if (flags.containsKey("tags") || flags.containsKey("tag")) {
            Iterable<?> tags = (Iterable<?>) flags.get("tags");
            Object tag = flags.get("tag");
            s.subscriberExtraExecTags = (tag == null) ? tags : (tags == null ? ImmutableList.of(tag) : MutableList.builder().addAll(tags).add(tag).build());
        }

        if (flags.containsKey("subscriberExecutionManagerTag")) {
            s.subscriberExecutionManagerTag = flags.remove("subscriberExecutionManagerTag");
            s.subscriberExecutionManagerTagSupplied = true;
        } else {
            s.subscriberExecutionManagerTag = 
                s.subscriber instanceof Entity ? "subscription-delivery-entity-"+((Entity)s.subscriber).getId() : 
                s.subscriber instanceof String ? "subscription-delivery-string["+s.subscriber+"]" : 
                "subscription-delivery-object["+s.subscriber+"]";
            s.subscriberExecutionManagerTagSupplied = false;
        }
        s.eventFilter = (Predicate<SensorEvent<T>>) flags.remove("eventFilter");
        boolean notifyOfInitialValue = Boolean.TRUE.equals(flags.remove("notifyOfInitialValue"));
        s.flags = flags;
        
        if (LOG.isDebugEnabled()) LOG.debug("Creating subscription {} for {} on {} {} in {}", new Object[] {s.id, s.subscriber, producer, sensor, this});
        allSubscriptions.put(s.id, s);
        addToMapOfSets(subscriptionsByToken, makeEntitySensorToken(s.producer, s.sensor), s);
        if (s.subscriber!=null) {
            addToMapOfSets(subscriptionsBySubscriber, s.subscriber, s);
        }
        if (!s.subscriberExecutionManagerTagSupplied && s.subscriberExecutionManagerTag!=null) {
            ((BasicExecutionManager) em).setTaskSchedulerForTag(s.subscriberExecutionManagerTag, SingleThreadedScheduler.class);
        }

        if (notifyOfInitialValue) {
            if (producer == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wildcard producer: "+s);
            } else if (sensor == null) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with wilcard sensor: "+s);
            } else if (!(sensor instanceof AttributeSensor)) {
                LOG.warn("Cannot notifyOfInitialValue for subscription with non-attribute sensor: "+s);
            } else {
                if (LOG.isTraceEnabled()) LOG.trace("sending initial value of {} -> {} to {}", new Object[] {s.producer, s.sensor, s});
                em.submit(
                    MutableMap.of("tags", ImmutableList.of(BrooklynTaskTags.tagForContextEntity(s.producer)),
                        "displayName", "Initial publication of "+s.sensor.getName()),
                    () -> {
                        T val = (T) s.producer.getAttribute((AttributeSensor<?>) s.sensor);
                        submitPublishEvent(s, new BasicSensorEvent<T>(s.sensor, s.producer, val), true);
                    });
            }
        }
        
        return s;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Set<SubscriptionHandle> getSubscriptionsForSubscriber(Object subscriber) {
        return (Set<SubscriptionHandle>) ((Set<?>) elvis(subscriptionsBySubscriber.get(subscriber), Collections.emptySet()));
    }

    @Override
    public synchronized Set<SubscriptionHandle> getSubscriptionsForEntitySensor(Entity source, Sensor<?> sensor) {
        Set<SubscriptionHandle> subscriptions = new LinkedHashSet<SubscriptionHandle>();
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(source, sensor)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(null, sensor)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(source, null)), Collections.emptySet()));
        subscriptions.addAll(elvis(subscriptionsByToken.get(makeEntitySensorToken(null, null)), Collections.emptySet()));
        return subscriptions;
    }

    /**
     * Unsubscribe the given subscription id.
     *
     * @see #subscribe(Map, Entity, Sensor, SensorEventListener)
     */
    @Override
    @SuppressWarnings("rawtypes")
    public synchronized boolean unsubscribe(SubscriptionHandle sh) {
        if (!(sh instanceof Subscription)) throw new IllegalArgumentException("Only subscription handles of type Subscription supported: sh="+sh+"; type="+(sh != null ? sh.getClass().getCanonicalName() : null));
        Subscription s = (Subscription) sh;
        boolean result = allSubscriptions.remove(s.id) != null;
        boolean b2 = removeFromMapOfCollections(subscriptionsByToken, makeEntitySensorToken(s.producer, s.sensor), s);
        assert result==b2;
        if (s.subscriber!=null) {
            boolean b3 = removeFromMapOfCollections(subscriptionsBySubscriber, s.subscriber, s);
            assert b3 == b2;
        }

        // FIXME ALEX - this seems wrong
        ((BasicExecutionManager) em).setTaskSchedulerForTag(s.subscriberExecutionManagerTag, SingleThreadedScheduler.class);
        return result;
    }

    @Override
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> void publish(final SensorEvent<T> event) {
        // REVIEW 1459 - execution
        
        // delivery in parallel/background, using execution manager
        
        // subscriptions, should define SingleThreadedScheduler for any subscriber ID tag
        // in order to ensure callbacks are invoked in the order they are submitted
        // (recommend exactly one per subscription to prevent deadlock)
        // this is done with:
        // em.setTaskSchedulerForTag(subscriberId, SingleThreadedScheduler.class);
        
        //note, generating the notifications must be done in the calling thread to preserve order
        //e.g. emit(A); emit(B); should cause onEvent(A); onEvent(B) in that order
        if (LOG.isTraceEnabled()) LOG.trace("{} got event {}", this, event);
        totalEventsPublishedCount.incrementAndGet();
        
        Set<Subscription> subs = (Set<Subscription>) ((Set<?>) getSubscriptionsForEntitySensor(event.getSource(), event.getSensor()));
        if (groovyTruth(subs)) {
            if (LOG.isTraceEnabled()) LOG.trace("sending {}, {} to {}", new Object[] {event.getSensor().getName(), event, join(subs, ",")});
            for (Subscription s : subs) {
                submitPublishEvent(s, event, false);
                // excludes initial so only do it here
                totalEventsDeliveredCount.incrementAndGet();
            }
        }
    }
    
    @SuppressWarnings({ "unchecked", "rawtypes" })
    private void submitPublishEvent(final Subscription s, final SensorEvent<?> event, final boolean isInitial) {
        if (s.eventFilter!=null && !s.eventFilter.apply(event))
            return;
        
        List<Object> tags = MutableList.builder()
            .addAll(s.subscriberExtraExecTags == null ? ImmutableList.of() : s.subscriberExtraExecTags)
            .add(s.subscriberExecutionManagerTag)
            .add(BrooklynTaskTags.SENSOR_TAG)
            // associate the publish event with the publisher (though on init it might be triggered by subscriber)
            .addIfNotNull(event.getSource()!=null ? BrooklynTaskTags.tagForTargetEntity(event.getSource()) : null)
            .build()
            .asUnmodifiable();
        
        StringBuilder name = new StringBuilder("sensor ");
        StringBuilder description = new StringBuilder("Sensor ");
        String sensorName = s.sensor==null ? "<null-sensor>" : s.sensor.getName();
        String sourceName = event.getSource()==null ? null : event.getSource().getId();
        if (Strings.isNonBlank(sourceName)) {
            name.append(sourceName);
            name.append(":");
        }
        name.append(sensorName);
        
        description.append(sensorName);
        description.append(" on ");
        description.append(sourceName==null ? "<null-source>" : sourceName);
        description.append(" publishing to ");
        description.append(s.subscriber instanceof Entity ? ((Entity)s.subscriber).getId() : s.subscriber);
        if (Strings.isNonBlank(s.subscriptionDescription)) {
            description.append(", ");
            description.append(s.subscriptionDescription);
        }
        
        if (includeDescriptionForSensorTask(event)) {
            name.append(" ");
            name.append(event.getValue());
            description.append(", value: ");
            description.append(event.getValue());
        }
        Map<String, Object> execFlags = MutableMap.of("tags", tags, 
            "displayName", name.toString(),
            "description", description.toString());
        
        boolean isEntityStarting = s.subscriber instanceof Entity && isInitial;
        // will have entity (and adjunct) execution context from tags, so can skip getting exec context
        em.submit(execFlags, new Runnable() {
            @Override
            public String toString() {
                if (isInitial) {
                    return "LSM.publishInitial("+event+")";
                } else {
                    return "LSM.publish("+event+")";
                }
            }
            @Override
            public void run() {
                try {
                    if (isEntityStarting) {
                        /* don't let sub deliveries start until this is completed;
                         * this is a pragmatic way to ensure the publish events 
                         * if submitted during management starting, aren't executed
                         * until after management is starting.
                         *   without this we can get deadlocks as this goes to publish,
                         * has the attribute sensors lock, and waits on the publish lock
                         * (any of management support, local subs, queueing subs).
                         * meanwhile the management startup has those three locks,
                         * then goes to publish and in the process looks up a sensor value.
                         *   usually this is not an issue because some other task
                         * does something (eg entity.getExecutionContext()) which
                         * also has a wait-on-management-support semantics.
                         */
                        synchronized (((EntityInternal)s.subscriber).getManagementSupport()) {}
                    }
                    int count = s.eventCount.incrementAndGet();
                    if (count > 0 && count % 1000 == 0) LOG.debug("{} events for subscriber {}", count, s);
                    
                    s.listener.onEvent(event);
                } catch (Throwable t) {
                    if (event!=null && event.getSource()!=null && Entities.isNoLongerManaged(event.getSource())) {
                        LOG.debug("Error processing subscriptions to "+this+", after entity unmanaged: "+t, t);
                    } else {
                        LOG.warn("Error processing subscriptions to "+this+": "+t, t);
                    }
                }
            }});
    }
    
    protected boolean includeDescriptionForSensorTask(SensorEvent<?> event) {
        // just do it for simple/quick things to avoid expensive toStrings
        // (info is rarely useful, but occasionally it will be)
        if (event.getValue()==null) return true;
        Class<?> clazz = event.getValue().getClass();
        if (clazz.isEnum() || clazz.isPrimitive() || Number.class.isAssignableFrom(clazz) || 
            clazz.equals(String.class)) return true;
        return false;
    }

    @Override
    public String toString() {
        return tostring;
    }
    
    /**
     * Copied from LanguageUtils.groovy, to remove dependency.
     * 
     * Adds the given value to a collection in the map under the key.
     * 
     * A collection (as {@link LinkedHashMap}) will be created if necessary,
     * synchronized on map for map access/change and set for addition there
     *
     * @return the updated set (instance, not copy)
     * 
     * @deprecated since 0.5; use {@link HashMultimap}, and {@link Multimaps#synchronizedSetMultimap(com.google.common.collect.SetMultimap)}
     */
    @Deprecated
    private static <K,V> Set<V> addToMapOfSets(Map<K,Set<V>> map, K key, V valueInCollection) {
        Set<V> coll;
        synchronized (map) {
            coll = map.get(key);
            if (coll==null) {
                coll = new LinkedHashSet<V>();
                map.put(key, coll);
            }
            if (coll.isEmpty()) {
                synchronized (coll) {
                    coll.add(valueInCollection);
                }
                //if collection was empty then add to the collection while holding the map lock, to prevent removal
                return coll;
            }
        }
        synchronized (coll) {
            if (!coll.isEmpty()) {
                coll.add(valueInCollection);
                return coll;
            }
        }
        //if was empty, recurse, because someone else might be removing the collection
        return addToMapOfSets(map, key, valueInCollection);
    }

    /**
     * Copied from LanguageUtils.groovy, to remove dependency.
     * 
     * Removes the given value from a collection in the map under the key.
     *
     * @return the updated set (instance, not copy)
     * 
     * @deprecated since 0.5; use {@link ArrayListMultimap} or {@link HashMultimap}, and {@link Multimaps#synchronizedListMultimap(com.google.common.collect.ListMultimap)} etc
     */
    @Deprecated
    private static <K,V> boolean removeFromMapOfCollections(Map<K,? extends Collection<V>> map, K key, V valueInCollection) {
        Collection<V> coll;
        synchronized (map) {
            coll = map.get(key);
            if (coll==null) return false;
        }
        boolean result;
        synchronized (coll) {
            result = coll.remove(valueInCollection);
        }
        if (coll.isEmpty()) {
            synchronized (map) {
                synchronized (coll) {
                    if (coll.isEmpty()) {
                        //only remove from the map if no one is adding to the collection or to the map, and the collection is still in the map
                        if (map.get(key)==coll) {
                            map.remove(key);
                        }
                    }
                }
            }
        }
        return result;
    }
}
