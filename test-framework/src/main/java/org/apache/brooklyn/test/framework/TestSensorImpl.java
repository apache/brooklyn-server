/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAbortConditions;
import static org.apache.brooklyn.test.framework.TestFrameworkAssertions.getAssertions;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.entity.lifecycle.ServiceStateLogic;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.test.framework.TestFrameworkAssertions.AssertionOptions;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.collect.Lists;

/**
 * {@inheritDoc}
 */
public class TestSensorImpl extends TargetableTestComponentImpl implements TestSensor {

    private static final Logger LOG = LoggerFactory.getLogger(TestSensorImpl.class);

    /**
     * {@inheritDoc}
     */
    public void start(Collection<? extends Location> locations) {
        final AtomicReference<String> sensor = new AtomicReference<>();
        
        ServiceStateLogic.setExpectedState(this, Lifecycle.STARTING);
        try {
            sensor.set(getRequiredConfig(SENSOR_NAME));
            final Entity target = resolveTarget();
            final Duration timeout = getConfig(TIMEOUT);
            final List<Map<String, Object>> assertions = getAssertions(this, ASSERTIONS);
            final List<Map<String, Object>> abortConditions = getAbortConditions(this, ABORT_CONDITIONS);
            if (!getChildren().isEmpty()) {
                throw new RuntimeException(String.format("The entity [%s] cannot have child entities", getClass().getName()));
            }
            
            Supplier<?> supplier = new Supplier<Object>() {
                @Override
                public Object get() {
                    final Object sensorValue = target.sensors().get(Sensors.newSensor(Object.class, sensor.get()));
                    return sensorValue;
                }
            };
            TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(sensor.get(), supplier).timeout(timeout)
                    .assertions(assertions).abortConditions(abortConditions));

            setUpAndRunState(true, Lifecycle.RUNNING);
        } catch (Throwable t) {
            if (sensor.get() != null) {
                LOG.debug("Sensor [{}] test failed for {} (rethrowing)", sensor, TestSensorImpl.this);
            } else {
                LOG.debug("Sensor test failed for {} (no sensor; rethrowing)", TestSensorImpl.this);
            }
            setUpAndRunState(false, Lifecycle.ON_FIRE);
            throw Exceptions.propagate(t);
        }
    }


    /**
     * {@inheritDoc}
     */
    public void stop() {
        setUpAndRunState(false, Lifecycle.STOPPED);
    }

    /**
     * {@inheritDoc}
     */
    public void restart() {
        final Collection<Location> locations = Lists.newArrayList(getLocations());
        stop();
        start(locations);
    }

    /**
     * Predicate to check the equality of object
     *
     * @param value
     * @return The created {@link Predicate}
     */
    private Predicate<Object> isEqualTo(final Object value) {
        return new Predicate<Object>() {
            public boolean apply(final Object input) {
                return (input != null) && Objects.equal(TypeCoercions.coerce(value, input.getClass()), input);
            }
        };
    }
}
