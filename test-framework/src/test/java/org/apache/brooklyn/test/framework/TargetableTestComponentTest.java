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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TargetableTestComponentTest extends BrooklynAppUnitTestSupport {

    private static final AttributeSensor<String> STRING_SENSOR = Sensors.newStringSensor("string-sensor");

    @Test
    public void testTargetEntity() {
        app.sensors().set(STRING_SENSOR, "myval");

        app.addChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, ImmutableList.of(ImmutableMap.of("equals", "myval"))));

        app.start(ImmutableList.<Location>of());
    }
    
    @Test
    public void testTargetEntityById() {
        TestEntity target = app.addChild(EntitySpec.create(TestEntity.class)
                .configure(BrooklynCampConstants.PLAN_ID, "myTargetId"));
        target.sensors().set(STRING_SENSOR, "myval");

        app.addChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ID, "myTargetId")
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, ImmutableList.of(ImmutableMap.of("equals", "myval"))));

        app.start(ImmutableList.<Location>of());
    }
    
    @Test
    public void testTargetEntityByIdWithDelayedEntityCreation() {
        final Duration entityCreationDelay = Duration.millis(250);
        final Duration overheadDuration = Duration.seconds(10);
        ExecutorService executor = Executors.newCachedThreadPool();
        
        try {
            executor.submit(new Runnable() {
                @Override public void run() {
                    Time.sleep(entityCreationDelay);
                    TestEntity target = app.addChild(EntitySpec.create(TestEntity.class)
                            .configure(BrooklynCampConstants.PLAN_ID, "myTargetId"));
                    target.sensors().set(STRING_SENSOR, "myval");
                }});
    
            app.addChild(EntitySpec.create(TestSensor.class)
                    .configure(TestSensor.TARGET_ID, "myTargetId")
                    .configure(TestSensor.TARGET_RESOLUTION_TIMEOUT, Duration.of(entityCreationDelay).add(overheadDuration))
                    .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                    .configure(TestSensor.ASSERTIONS, ImmutableList.of(ImmutableMap.of("equals", "myval"))));

            
            app.start(ImmutableList.<Location>of());
            
        } finally {
            executor.shutdownNow();
        }
    }
}
