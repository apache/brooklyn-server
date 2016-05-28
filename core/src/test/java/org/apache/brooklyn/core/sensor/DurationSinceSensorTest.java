/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.brooklyn.core.sensor;

import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsContinually;
import static org.apache.brooklyn.core.entity.EntityAsserts.assertAttributeEqualsEventually;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.effector.AddSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableMap;

public class DurationSinceSensorTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSensorAddedAndUpdated() {
        final AtomicLong ticker = new AtomicLong(0);
        Supplier<Long> timeSupplier = new Supplier<Long>() {
            @Override
            public Long get() {
                return ticker.get();
            }
        };
        Entity entity = app.createAndManageChild(EntitySpec.create(TestEntity.class)
                .addInitializer(new DurationSinceSensor(ConfigBag.newInstance(ImmutableMap.of(
                        AddSensor.SENSOR_NAME, "sensor",
                        AddSensor.SENSOR_TYPE, Duration.class.getName(),
                        AddSensor.SENSOR_PERIOD, Duration.ONE_MILLISECOND,
                        DurationSinceSensor.EPOCH_SUPPLIER, Suppliers.ofInstance(0L),
                        DurationSinceSensor.TIME_SUPPLIER, timeSupplier)))));

        final Map<?, ?> continuallyTimeout = ImmutableMap.of("timeout", Duration.millis(10));
        final AttributeSensor<Duration> duration = Sensors.newSensor(Duration.class, "sensor");
        assertAttributeEqualsEventually(entity, duration, Duration.millis(0));
        assertAttributeEqualsContinually(continuallyTimeout, entity, duration, Duration.millis(0));
        ticker.incrementAndGet();
        assertAttributeEqualsEventually(entity, duration, Duration.millis(1));
        assertAttributeEqualsContinually(continuallyTimeout, entity, duration, Duration.millis(1));
        ticker.incrementAndGet();
        assertAttributeEqualsEventually(entity, duration, Duration.millis(2));
        assertAttributeEqualsContinually(continuallyTimeout, entity, duration, Duration.millis(2));
    }

}
