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
package org.apache.brooklyn.core.config;

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;

public class DeferredConfigTest extends BrooklynAppUnitTestSupport {
    private static ConfigKey<Object> SENSORS_UNTYPED = ConfigKeys.newConfigKey(Object.class, "sensors");
    @SuppressWarnings("serial")
    private static ConfigKey<List<? extends Sensor<?>>> SENSORS = ConfigKeys.newConfigKey(new TypeToken<List<? extends Sensor<?>>>() {}, "sensors");
    
    @Test
    public void testDeferredConfigInListWhenAvailable() throws Exception {
        doTestDeferredConfigInList(false);
    }
    
    @Test
    public void testDeferredConfigInListNotAvailable() throws Exception {
        doTestDeferredConfigInList(true);
    }
    
    void doTestDeferredConfigInList(final boolean delay) throws Exception {
        // Simulate a deferred value
        Task<Sensor<?>> sensorFuture = app.getExecutionContext().submit(new Callable<Sensor<?>>() {
            @Override
            public Sensor<?> call() throws Exception {
                if (delay) Time.sleep(Duration.FIVE_SECONDS);
                return TestApplication.MY_ATTRIBUTE;
            }
        });
        app.config().set(SENSORS_UNTYPED, (Object)ImmutableList.of(sensorFuture));

        if (!delay) sensorFuture.get(Duration.ONE_SECOND);
        // should resolve if future completed
        Maybe<List<? extends Sensor<?>>> sensors = app.config().getNonBlocking(SENSORS);
        if (delay) {
            Assert.assertFalse(sensors.isPresent(), "value shouldn't be available");
        } else {
            Assert.assertTrue(sensors.isPresent(), "value should be available");
            Sensor<?> sensor = Iterables.getOnlyElement(sensors.get());
            assertEquals(sensor, TestApplication.MY_ATTRIBUTE);
        }
    }

}
