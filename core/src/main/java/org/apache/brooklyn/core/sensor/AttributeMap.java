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
package org.apache.brooklyn.core.sensor;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.BrooklynLogging;
import org.apache.brooklyn.core.entity.AbstractEntity;
import org.apache.brooklyn.util.concurrent.Locks;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

/**
 * A {@link Map} of {@link Entity} attribute values.
 */
public final class AttributeMap {

    static final Logger log = LoggerFactory.getLogger(AttributeMap.class);

    private static enum Marker {
        NULL;
    }
    
    private final AbstractEntity entity;

    // Assumed to be something like a ConcurrentMap passed in.
    private final Map<Collection<String>, Object> values;

    /**
     * Creates a new AttributeMap.
     *
     * @param entity the Entity this AttributeMap belongs to.
     * @throws NullPointerException if entity is null
     */
    public AttributeMap(AbstractEntity entity) {
        // Not using ConcurrentMap, because want to (continue to) allow null values.
        // Could use ConcurrentMapAcceptingNullVals (with the associated performance hit on entrySet() etc).
        this(entity, Collections.synchronizedMap(Maps.<Collection<String>, Object>newLinkedHashMap()));
    }

    /**
     * Creates a new AttributeMap.
     *
     * @param entity  the Entity this AttributeMap belongs to.
     * @param storage the Map in which to store the values - should be concurrent or synchronized.
     * @throws NullPointerException if entity is null
     */
    public AttributeMap(AbstractEntity entity, Map<Collection<String>, Object> storage) {
        this.entity = checkNotNull(entity, "entity must be specified");
        this.values = checkNotNull(storage, "storage map must not be null");
    }

    /** 
     * This externally visible lock ensures only one thread can write and publish
     * any sensor value at a time.  Methods which set, modify, and publish values
     * acquire this lock.
     * <p>
     * Reads are not blocked by this, although this synchs on {@link #values}
     * when actually changing the low-level value to ensure consistency there.
     * <p>
     * See {@link #getLockInternal()}
     */
    private final transient ReentrantLock writeLock = new ReentrantLock();
    
    /** Internal {@link Lock} used when publishing values.
     * When needed -- and carefully -- callers can lock on this to ensure no values are changed by other
     * threads while they are active.
     * <p>
     * To prevent deadlock, Brooklyn code generally call this _before_ getting any lower-level
     * monitor (ie {@code synchronized}) and before locking on any descendant entity.
     * <p>
     * In other words, this lock should generally *not* be acquired inside a {@code synchronized} block
     * nor by the holder of a similar {@link Lock} on an entity except self or ancestor,
     * unless a clear canonical order is set forth.
     */
    @Beta
    public Lock getLockInternal() {
        return writeLock;
    }
    
    public Map<Collection<String>, Object> asRawMap() {
        synchronized (values) {
            return ImmutableMap.copyOf(values);
        }
    }

    public Map<String, Object> asMap() {
        Map<String, Object> result = Maps.newLinkedHashMap();
        synchronized (values) {
            for (Map.Entry<Collection<String>, Object> entry : values.entrySet()) {
                String sensorName = Joiner.on('.').join(entry.getKey());
                Object val = (isNull(entry.getValue())) ? null : entry.getValue();
                result.put(sensorName, val);
            }
        }
        return result;
    }
    
    /**
     * Updates the value.
     *
     * @param path the path to the value.
     * @param newValue the new value
     * @return the old value.
     * @throws IllegalArgumentException if path is null or empty
     * 
     * @deprecated since 0.13.0 becoming private; callers should only ever use {@link #update(AttributeSensor, Object)}
     */
    @Deprecated
    // TODO path must be ordered(and legal to contain duplicates like "a.b.a"; list would be better
    // (is there even any point to the path?  it was for returning maps by querying a prefix but that's ancient!)
    public <T> T update(Collection<String> path, T newValue) {
        checkPath(path);

        if (newValue == null) {
            newValue = typedNull();
        }

        if (log.isTraceEnabled()) {
            log.trace("setting sensor {}={} for {}", new Object[] {path, newValue, entity});
        }

        @SuppressWarnings("unchecked")
        T oldValue = (T) values.put(path, newValue);
        return (isNull(oldValue)) ? null : oldValue;
    }

    private void checkPath(Collection<String> path) {
        Preconditions.checkNotNull(path, "path can't be null");
        Preconditions.checkArgument(!path.isEmpty(), "path can't be empty");
    }

    /**
     * Sets a value and published it.
     * <p>
     * Constraints of {@link #getLockInternal()} apply, with lock-holder being the supplied {@link Function}.
     */
    public <T> T update(AttributeSensor<T> attribute, T newValue) {
        // 2017-10 this was unsynched which meant if two threads updated
        // the last publication would not correspond to the last value.
        // a crude `synchronized (values)` could cause deadlock as
        // this does publish (doing `synch(LSM)`) whereas _subscribe_ 
        // would have the LSM lock and try to `synch(values)`
        // as described at https://issues.apache.org/jira/browse/BROOKLYN-544.
        // the addition of getLockInternal should make this much cleaner,
        // as no one holding `synch(LSM)` should be updating here, 
        // ands gets here won't call any other code at all
        return withLock(() -> {
            T oldValue = updateInternalWithoutLockOrPublish(attribute, newValue);
            entity.emitInternal(attribute, newValue);
            return oldValue;
        });
    }
    
    /** @deprecated since 0.13.0 this is becoming an internal method, {@link #updateInternalWithoutLockOrPublish(AttributeSensor, Object)} */
    @Deprecated
    public <T> T updateWithoutPublishing(AttributeSensor<T> attribute, T newValue) {
        return updateInternalWithoutLockOrPublish(attribute, newValue);
    }
    
    @Beta
    public <T> T updateInternalWithoutLockOrPublish(AttributeSensor<T> attribute, T newValue) {
        synchronized (values) {
            if (log.isTraceEnabled()) {
                Object oldValue = getValue(attribute);
                if (!Objects.equal(oldValue, newValue != null)) {
                    log.trace("setting attribute {} to {} (was {}) on {}", new Object[] {attribute.getName(), newValue, oldValue, entity});
                } else {
                    log.trace("setting attribute {} to {} (unchanged) on {}", new Object[] {attribute.getName(), newValue, this});
                }
            }
    
            T oldValue = update(attribute.getNameParts(), newValue);
            return (isNull(oldValue)) ? null : oldValue;
        }
    }

    private <T> T withLock(Callable<T> body) { return Locks.withLock(getLockInternal(), body); }
    private void withLock(Runnable body) { Locks.withLock(getLockInternal(), body); }
    
    /**
     * Where atomicity is desired, this method wraps an acquisition of {@link #getLockInternal()}
     * while applying the given {@link Function}.
     * <p>
     * Constraints of {@link #getLockInternal()} apply, with lock-holder being the supplied {@link Function}.
     */
    public <T> T modify(AttributeSensor<T> attribute, Function<? super T, Maybe<T>> modifier) {
        return withLock(() -> {
            T oldValue = getValue(attribute);
            Maybe<? extends T> newValue = modifier.apply(oldValue);
            
            if (newValue.isPresent()) {
                if (log.isTraceEnabled()) log.trace("modified attribute {} to {} (was {}) on {}", new Object[] {attribute.getName(), newValue, oldValue, entity});
                return update(attribute, newValue.get());
            } else {
                if (log.isTraceEnabled()) log.trace("modified attribute {} unchanged; not emitting on {}", new Object[] {attribute.getName(), newValue, this});
                return oldValue;
            }
        });
    }

    /** Removes a sensor.  The {@link #getLockInternal()} is acquired,
     * and constraints on the caller described there apply to callers of this method.
     * <p>
     * The caller is responsible for any subsequent actions needed, including publishing
     * the removal of the sensor and triggering persistence. Caller may wish to 
     * have the {@link #getLockInternal()} while doing that. */
    public void remove(AttributeSensor<?> attribute) {
        BrooklynLogging.log(log, BrooklynLogging.levelDebugOrTraceIfReadOnly(entity),
            "removing attribute {} on {}", attribute.getName(), entity);
        withLock(() -> remove(attribute.getNameParts()) );
    }

    // TODO path must be ordered(and legal to contain duplicates like "a.b.a"; list would be better
    /** @deprecated since 0.13.0 becoming private; callers should only ever use {@link #remove(AttributeSensor)}
     */
    @Deprecated
    public void remove(Collection<String> path) {
        checkPath(path);

        if (log.isTraceEnabled()) {
            log.trace("removing sensor {} for {}", new Object[] {path, entity});
        }

        values.remove(path);
    }

    /**
     * Gets the value
     *
     * @param path the path of the value to get
     * @return the value
     * @throws IllegalArgumentException path is null or empty.
     */
    public Object getValue(Collection<String> path) {
        checkPath(path);
        Object result = values.get(path);
        return (isNull(result)) ? null : result;
    }

    @SuppressWarnings("unchecked")
    public <T> T getValue(AttributeSensor<T> sensor) {
        return (T) TypeCoercions.coerce(getValue(sensor.getNameParts()), sensor.getType());
    }

    @SuppressWarnings("unchecked")
    private <T> T typedNull() {
        return (T) Marker.NULL;
    }
    
    private boolean isNull(Object t) {
        return t == Marker.NULL;
    }
}
