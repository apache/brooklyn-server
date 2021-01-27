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

package org.apache.brooklyn.policy;

import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.Nonnull;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.MoreExecutors;

public abstract class AbstractInvokeEffectorPolicy extends AbstractPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractInvokeEffectorPolicy.class);

    public static final ConfigKey<String> IS_BUSY_SENSOR_NAME = ConfigKeys.newStringConfigKey(
            "isBusySensor",
            "Name of the sensor to publish on the entity that indicates that this policy has incomplete effectors. " +
                    "If unset running tasks will not be tracked.");

    private final AtomicInteger taskCounter = new AtomicInteger();

    /**
     * Indicates that onEvent was notified of an value of is not the latest sensor value.
     */
    private boolean moreUpdatesComing;
    /**
     * The timestamp of the event that informed moreUpdatesComing. Subsequent notifications
     * of earlier events will not cause updates of moreUpdatesComing.
     */
    private long mostRecentUpdate = 0;
    /**
     * Guards {@link #moreUpdatesComing} and {@link #mostRecentUpdate}.
     */
    private final Object[] moreUpdatesLock = new Object[0];

    @Override
    public void setEntity(EntityLocal entity) {
        super.setEntity(entity);
        if (isBusySensorEnabled()) {
            // Republishes when the entity rebinds.
            publishIsBusy();
        }
    }

    /**
     * Invoke effector with parameters on the entity that the policy is attached to.
     */
    protected <T> Task<T> invoke(Effector<T> effector, Map<String, ?> parameters) {
        if (isBusySensorEnabled()) {
            getTaskCounter().incrementAndGet();
            publishIsBusy();
        }
        Task<T> task = entity.invoke(effector, parameters);
        if (isBusySensorEnabled()) {
            task.addListener(new EffectorListener(), MoreExecutors.sameThreadExecutor());
        }
        return task;
    }

    protected boolean isBusy() {
        synchronized (moreUpdatesLock) {
            return getTaskCounter().get() != 0 || moreUpdatesComing;
        }
    }

    protected boolean isBusySensorEnabled() {
        return Strings.isNonBlank(getIsBusySensorName());
    }

    protected Maybe<Effector<?>> getEffectorNamed(String name) {
        return entity.getEntityType().getEffectorByName(name);
    }

    @Nonnull
    protected String getIsBusySensorName() {
        return getConfig(IS_BUSY_SENSOR_NAME);
    }

    /**
     * Indicates that when the policy was notified of eventValue, occurring at time
     * eventTimestamp, it observed the current sensor value to be current. This
     * informs the value for {@link #moreUpdatesComing}.
     */
    protected <T> void setMoreUpdatesComing(long eventTimestamp, T eventValue, T current) {
        if (eventTimestamp >= mostRecentUpdate) {
            synchronized (moreUpdatesLock) {
                if (eventTimestamp >= mostRecentUpdate) {
                    moreUpdatesComing = !Objects.equal(eventValue, current);
                    mostRecentUpdate = eventTimestamp;
                }
            }
        }
    }

    private AtomicInteger getTaskCounter() {
        return taskCounter;
    }

    private void publishIsBusy() {
        final boolean busy = isBusy();
        LOG.trace("{} taskCount={}, isBusy={}", new Object[]{this, getTaskCounter().get(), busy});
        AttributeSensor<Boolean> sensor = Sensors.newBooleanSensor(getIsBusySensorName());
        entity.sensors().set(sensor, busy);
    }

    private class EffectorListener implements Runnable {
        @Override
        public void run() {
            getTaskCounter().decrementAndGet();
            publishIsBusy();
        }
    }

}
