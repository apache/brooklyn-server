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
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.entity.Group;
import org.apache.brooklyn.api.mgmt.ha.ManagementNodeState;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.feed.function.FunctionFeed;
import org.apache.brooklyn.feed.function.FunctionPollConfig;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

public class DynamicMultiGroupImpl extends DynamicGroupImpl implements DynamicMultiGroup {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicMultiGroupImpl.class);

    /**
     * {@link Function} for deriving bucket names from a sensor value.
     */
    public static class BucketFromAttribute implements Function<Entity, String> {
        private final AttributeSensor<?> sensor;
        private final String defaultValue;

        public BucketFromAttribute(AttributeSensor<?> sensor, String defaultValue) {
            this.sensor = Preconditions.checkNotNull(sensor, "sensor");
            this.defaultValue = defaultValue;
        }

        @Override
        public String apply(@Nullable Entity input) {
            Object value = input.getAttribute(sensor);
            if (value == null) {
                return defaultValue;
            } else {
                return String.valueOf(value);
            }
        };
    }

    /**
     * Convenience factory method for the common use-case of deriving the bucket directly from a sensor value.
     *
     * @see DynamicMultiGroup#BUCKET_FUNCTION
     */
    public static Function<Entity, String> bucketFromAttribute(final AttributeSensor<?> sensor, final String defaultValue) {
        return new BucketFromAttribute(sensor, defaultValue);
    }

    /**
     * Convenience factory method for the common use-case of deriving the bucket directly from a sensor value.
     *
     * @see DynamicMultiGroup#BUCKET_FUNCTION
     */
    public static Function<Entity, String> bucketFromAttribute(final AttributeSensor<?> sensor) {
        return bucketFromAttribute(sensor, null);
    }

    private transient FunctionFeed rescan;

    @Override
    public void init() {
        super.init();
        sensors().set(BUCKETS, ImmutableMap.<String, BasicGroup>of());
        connectScanner();
    }

    private void connectScanner() {
        Long interval = getConfig(RESCAN_INTERVAL);
        if (interval != null && interval > 0L) {
            rescan = FunctionFeed.builder()
                    .entity(this)
                    .poll(new FunctionPollConfig<Object, Void>(RESCAN)
                            .period(interval, TimeUnit.SECONDS)
                            .callable(new Callable<Void>() {
                                    @Override
                                    public Void call() throws Exception {
                                        rescanEntities();
                                        return null;
                                    }
                                }))
                    .build();
        }
    }

    @Override
    public void rebind() {
        super.rebind();

        if (rescan == null) {
            // The rescan can (in a different thread) cause us to remove the empty groups - i.e. remove 
            // it as a child, and unmanage it. That is dangerous during rebind, because the rebind-thread 
            // may concurrently (or subsequently) be initialising that child entity. It has caused 
            // rebind errors where the child's entity-rebind complains in setParent() that it was
            // "previouslyOwned". Therefore we defer registering/executing our scanner until rebind is
            // complete, so all entities are reconstituted.
            // We don't worry about other managementNodeStates, such as standby: if we were told to
            // rebind then we are free to fully initialise ourselves. But we do double-check that we
            // are still managed before trying to execute.
            
            getExecutionContext().execute(new Runnable() {
                @Override public void run() {
                    LOG.debug("Deferring scanner for {} until management context initialisation complete", DynamicMultiGroupImpl.this);
                    while (!isRebindComplete()) {
                        Time.sleep(100); // avoid thrashing
                    }
                    LOG.debug("Connecting scanner for {}", DynamicMultiGroupImpl.this);
                    connectScanner();
                }
                private boolean isRebindComplete() {
                    // TODO Want to determine if finished rebinding (either success or fail is fine).
                    // But not a clean way to do this that works for both unit tests and live server?!
                    //  * In RebindTestFixtureWithApp tests, mgmt.getHighAvailabilityManager().getNodeState()
                    //    always returns INITIALIZING.
                    //  * The rebind metrics is a hack, and feels very risky for HOT_STANDBY nodes that 
                    //    may have executed the rebind code multiple times.
                    Map<String, Object> metrics = getManagementContext().getRebindManager().getMetrics();
                    Object count = (metrics.get("rebind") instanceof Map) ? ((Map<?,?>)metrics.get("rebind")).get("count") : null;
                    return (count instanceof Number) && ((Number)count).intValue() > 0;
                }});
        }
    }

    @Override
    public void stop() {
        super.stop();

        if (rescan != null && rescan.isActivated()) {
            rescan.stop();
        }
    }

    @Override
    protected void onEntityAdded(Entity item) {
        synchronized (memberChangeMutex) {
            super.onEntityAdded(item);
            distributeEntities();
        }
    }

    @Override
    protected void onEntityRemoved(Entity item) {
        synchronized (memberChangeMutex) {
            super.onEntityRemoved(item);
            distributeEntities();
        }
    }
    
    @Override
    protected void onEntityChanged(Entity item) {
        synchronized (memberChangeMutex) {
            super.onEntityChanged(item);
            distributeEntities();
        }
    }

    @Override
    public void rescanEntities() {
        synchronized (memberChangeMutex) {
            super.rescanEntities();
            distributeEntities();
        }
    }

    @Override
    public void distributeEntities() {
        synchronized (memberChangeMutex) {
            Function<Entity, String> bucketFunction = getConfig(BUCKET_FUNCTION);
            EntitySpec<? extends BasicGroup> bucketSpec = getConfig(BUCKET_SPEC);
            if (bucketFunction == null || bucketSpec == null) return;
            Map<String, BasicGroup> buckets = MutableMap.copyOf(getAttribute(BUCKETS));

            // Bucketize the members where the function gives a non-null bucket
            Multimap<String, Entity> entityMapping = Multimaps.index(
                    Iterables.filter(getMembers(), Predicates.compose(Predicates.notNull(), bucketFunction)), bucketFunction);

            // Now fill the buckets
            for (String name : entityMapping.keySet()) {
                BasicGroup bucket = buckets.get(name);
                if (bucket == null) {
                    bucket = addChild(EntitySpec.create(bucketSpec).displayName(name));
                    buckets.put(name, bucket);
                }
                bucket.setMembers(entityMapping.get(name));
            }

            // Remove any now-empty buckets
            Set<String> empty = ImmutableSet.copyOf(Sets.difference(buckets.keySet(), entityMapping.keySet()));
            for (String name : empty) {
                Group removed = buckets.remove(name);
                LOG.debug(this+" removing empty child-bucket "+name+" -> "+removed);
                removeChild(removed);
                Entities.unmanage(removed);
            }

            // Save the bucket mappings
            sensors().set(BUCKETS, ImmutableMap.copyOf(buckets));
        }
    }

}
