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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.effector.AddEffector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.checkerframework.checker.units.qual.A;
import org.testng.Assert;
import org.testng.annotations.Test;

public class InvokeEffectorOnSensorChangeIntegrationTest extends BrooklynAppUnitTestSupport {

    @Test(groups = "Integration")
    public void testIsBusySensorAlwaysFalseAtEnd() throws InterruptedException {
        /*
         * Stress-test isBusy. Reliably failed with insufficient synchronisation
         * in AbstractInvokeEffectorPolicy.
         */
        final AttributeSensor<String> sensor = Sensors.newStringSensor("sensor-being-watched");
        final AttributeSensor<Boolean> isBusy = Sensors.newBooleanSensor("is-busy");
        Effector<Void> effector = Effectors.effector(Void.class, "effector")
                .impl(new DoNothingEffector())
                .build();
        final BasicEntity entity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new AddEffector(effector))
                .policy(PolicySpec.create(InvokeEffectorOnSensorChange.class)
                        .configure(InvokeEffectorOnSensorChange.SENSOR, sensor)
                        .configure(InvokeEffectorOnSensorChange.EFFECTOR, "effector")
                        .configure(InvokeEffectorOnSensorChange.IS_BUSY_SENSOR_NAME, isBusy.getName())));
        final AtomicInteger threadId = new AtomicInteger();
        Thread[] threads = new Thread[10];
        for (int i = 0; i < threads.length; i++) {
            threads[i] = new Thread(new Runnable() {
                private int count = 0;
                @Override
                public void run() {
                    int id = threadId.incrementAndGet();
                    while (count++ < 1000) {
                        entity.sensors().set(sensor, "thread-" + id + "-" + count);
                    }
                }
            });
            threads[i].start();
        }
        for (Thread thread : threads) {
            thread.join();
        }

        EntityAsserts.assertAttributeEqualsEventually(entity, isBusy, false);
    }

    private static class DoNothingEffector extends EffectorBody<Void> {
        @Override
        public Void call(ConfigBag config) {
            return null;
        }
    }

    /**
     * Tests {@link InvokeEffectorOnSensorChange} to watch configured {@link InvokeEffectorOnSensorChange#SENSOR} that
     * defaults to the same entity it attached to, when {@link InvokeEffectorOnSensorChange#PRODUCER} is not set.
     */
    @Test
    public void testCallEffectorOnSensorChange_WatchSensorAtThisEntity() {

        final String sensorModifiedValue = "Can be anything...";

        // Prepare sensor for policy to watch.
        final AttributeSensor<String> sensorBeingWatched = Sensors.newStringSensor("sensor-being-watched");
        final AttributeSensor<String> sensorBeingModifiedByEffector = Sensors.newStringSensor("sensor-being-modified");

        // Prepare effector to call, set value 'B' to sensor-being-watched.
        Effector<Void> effector = Effectors.effector(Void.class, "my-effector")
                .impl((entity, _effector, parameters) -> {
                    entity.sensors().set(sensorBeingModifiedByEffector, sensorModifiedValue);
                    return null;
                })
                .build();

        // Create entity, configure effector and policy to test.
        final BasicEntity thisEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new AddEffector(effector))
                .policy(PolicySpec.create(InvokeEffectorOnSensorChange.class)
                        .configure(InvokeEffectorOnSensorChange.SENSOR, sensorBeingWatched)
                        .configure(InvokeEffectorOnSensorChange.EFFECTOR, "my-effector")));

        // Run the test.
        EntityAsserts.assertAttributeContinuallyNotEqualTo(thisEntity, sensorBeingModifiedByEffector, sensorModifiedValue);
        thisEntity.sensors().set(sensorBeingWatched, "Something changed in sensor (at this entity) that triggers policy!");
        EntityAsserts.assertAttributeEqualsEventually(thisEntity, sensorBeingModifiedByEffector, sensorModifiedValue);
    }

    /**
     * Tests {@link InvokeEffectorOnSensorChange} to watch configured {@link InvokeEffectorOnSensorChange#SENSOR} on a
     * configured {@link InvokeEffectorOnSensorChange#PRODUCER}, which is some other entity.
     */
    @Test
    public void testCallEffectorOnSensorChange_WatchSensorAtOtherEntity() {

        final String sensorModifiedValue = "Can be anything...";

        // Prepare sensor for policy to watch.
        final AttributeSensor<String> sensorBeingWatched = Sensors.newStringSensor("sensor-being-watched");
        final AttributeSensor<String> sensorBeingModifiedByEffector = Sensors.newStringSensor("sensor-being-modified");

        // Prepare effector to call, set value 'B' to sensor-being-watched.
        AtomicInteger effectorCalledCount = new AtomicInteger(0);
        Effector<Void> effector = Effectors.effector(Void.class, "my-effector")
                .impl((entity, _effector, parameters) -> {
                    effectorCalledCount.incrementAndGet();
                    entity.sensors().set(sensorBeingModifiedByEffector, sensorModifiedValue);
                    return null;
                })
                .build();

        // Create 'other' entity where sensor is being watched from 'this' entity.
        final BasicEntity otherEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create 'this' entity, configure effector and policy to test.
        final BasicEntity thisEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new AddEffector(effector))
                .policy(PolicySpec.create(InvokeEffectorOnSensorChange.class)
                        .configure(InvokeEffectorOnSensorChange.SENSOR, sensorBeingWatched)
                        .configure(InvokeEffectorOnSensorChange.PRODUCER, otherEntity)
                        .configure(InvokeEffectorOnSensorChange.EFFECTOR, "my-effector")));

        // Run the test - change sensor on 'other' entity and expect effector called on 'this' entity.
        EntityAsserts.assertAttributeContinuallyNotEqualTo(thisEntity, sensorBeingModifiedByEffector, sensorModifiedValue);
        otherEntity.sensors().set(sensorBeingWatched, "Something changed in sensor (at other entity) that triggers policy!");
        EntityAsserts.assertAttributeEqualsEventually(thisEntity, sensorBeingModifiedByEffector, sensorModifiedValue);
    }

    /**
     * Tests {@link InvokeEffectorOnSensorChange} to watch configured {@link InvokeEffectorOnSensorChange#SENSOR} on a
     * configured {@link InvokeEffectorOnSensorChange#PRODUCER} and call effector only when sensor changes the value.
     * Overriding the same value at the sensor must be ignored by this policy.
     */
    @Test
    public void testCallEffectorOnSensorChange_CallEffectorIfSensorValueChanges() {

        // Prepare sensor for policy to watch.
        final AttributeSensor<String> sensorBeingWatched = Sensors.newStringSensor("sensor-being-watched");
        final AttributeSensor<Integer> sensorBeingModifiedByEffector = Sensors.newIntegerSensor("sensor-being-modified");

        // Prepare effector to call, set value 'B' to sensor-being-watched.
        AtomicInteger effectorCalledCount = new AtomicInteger(0);
        Effector<Void> effector = Effectors.effector(Void.class, "my-effector")
                .impl((entity, _effector, parameters) -> {
                    int count = effectorCalledCount.incrementAndGet();
                    entity.sensors().set(sensorBeingModifiedByEffector, count);
                    return null;
                })
                .build();

        // Create 'other' entity where sensor is being watched from 'this' entity.
        final BasicEntity otherEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class));

        // Create 'this' entity, configure effector and policy to test.
        final BasicEntity thisEntity = app.createAndManageChild(EntitySpec.create(BasicEntity.class)
                .addInitializer(new AddEffector(effector))
                .policy(PolicySpec.create(InvokeEffectorOnSensorChange.class)
                        .configure(InvokeEffectorOnSensorChange.SENSOR, sensorBeingWatched)
                        .configure(InvokeEffectorOnSensorChange.PRODUCER, otherEntity)
                        .configure(InvokeEffectorOnSensorChange.EFFECTOR, "my-effector")));

        // Run the test - change sensor on 'other' entity and expect effector called on 'this' entity.
        EntityAsserts.assertAttributeContinuallyNotEqualTo(thisEntity, sensorBeingModifiedByEffector, 0);
        otherEntity.sensors().set(sensorBeingWatched, "Something changed in sensor (at other entity) that triggers policy!");

        // Expect effector called to change sensor value.
        EntityAsserts.assertAttributeEqualsEventually(thisEntity, sensorBeingModifiedByEffector, 1);

        // Override same sensor value on 'other' entity, expect no new effector calls.
        otherEntity.sensors().set(sensorBeingWatched, "Something changed in sensor (at other entity) that triggers policy!");
        EntityAsserts.assertAttributeEqualsEventually(thisEntity, sensorBeingModifiedByEffector, 1);
        EntityAsserts.assertAttributeContinuallyNotEqualTo(thisEntity, sensorBeingModifiedByEffector, 2);

        // Set another sensor value on 'other' entity, expect expect effector called on 'this' entity.
        otherEntity.sensors().set(sensorBeingWatched, "Something else has changed!");
        EntityAsserts.assertAttributeEqualsEventually(thisEntity, sensorBeingModifiedByEffector, 2);
        EntityAsserts.assertAttributeContinuallyNotEqualTo(thisEntity, sensorBeingModifiedByEffector, 3);
    }
}