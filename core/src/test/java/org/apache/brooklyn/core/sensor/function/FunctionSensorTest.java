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
package org.apache.brooklyn.core.sensor.function;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class FunctionSensorTest extends BrooklynAppUnitTestSupport {
    final static AttributeSensor<String> SENSOR_STRING = Sensors.newStringSensor("aString");
    final static String STRING_TARGET_TYPE = "java.lang.String";

    TestEntity entity;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        entity = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        app.start(ImmutableList.<Location>of());
    }

    @Test
    public void testFunction() throws Exception {
        AtomicReference<String> val = new AtomicReference<String>("first");
        Callable<String> callable = new Callable<String>() {
            @Override public String call() throws Exception {
                return val.get();
            }
        };
        
        FunctionSensor<Integer> initializer = new FunctionSensor<Integer>(ConfigBag.newInstance()
                .configure(FunctionSensor.SENSOR_PERIOD, Duration.millis(10))
                .configure(FunctionSensor.SENSOR_NAME, SENSOR_STRING.getName())
                .configure(FunctionSensor.SENSOR_TYPE, STRING_TARGET_TYPE)
                .configure(FunctionSensor.FUNCTION, callable));
        initializer.apply(entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);
        
        initializer.apply(entity);
        entity.sensors().set(Attributes.SERVICE_UP, true);

        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "first");
        
        val.set("second");
        EntityAsserts.assertAttributeEqualsEventually(entity, SENSOR_STRING, "second");
    }
}
