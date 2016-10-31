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
import org.testng.annotations.Test;

public class InvokeEffectorOnSensorChangeIntegrationTest extends BrooklynAppUnitTestSupport {

    @Test(groups = "Integration")
    public void testIsBusySensorAlwaysFalseAtEnd() throws InterruptedException {
        /*
         * Stress-test isBusy. Reliably failed with insufficient synchronisation
         * in AbstractInvokeEffectorPolicy.
         */
        final AttributeSensor<String> sensor = Sensors.newStringSensor("my-sensor");
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

}