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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.SensorEvent;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.core.sensor.SensorPredicates;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;

public class SensorEventPredicatesTest extends BrooklynAppUnitTestSupport {

    private AttributeSensor<String> sensor1 = Sensors.newStringSensor("sensor1");
    private AttributeSensor<String> sensor2 = Sensors.newStringSensor("sensor2");

    @Test
    public void testSensorEqualTo() throws Exception {
        Predicate<SensorEvent<?>> predicate = SensorEventPredicates.sensorEqualTo(sensor1);
        assertTrue(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "myval")));
        assertFalse(predicate.apply(new BasicSensorEvent<String>(sensor2, app, "myval")));
    }
    
    @Test
    public void testSensorSatisfies() throws Exception {
        Predicate<SensorEvent<?>> predicate = SensorEventPredicates.sensorSatisfies(SensorPredicates.nameEqualTo("sensor1"));
        assertTrue(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "myval")));
        assertFalse(predicate.apply(new BasicSensorEvent<String>(sensor2, app, "myval")));
    }
    
    @Test
    public void testValueEqualTo() throws Exception {
        Predicate<SensorEvent<String>> predicate = SensorEventPredicates.valueEqualTo("myval");
        assertTrue(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "myval")));
        assertFalse(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "wrongVal")));
    }
    
    @Test
    public void testValueSatisfies() throws Exception {
        Predicate<SensorEvent<String>> predicate = SensorEventPredicates.valueSatisfies(StringPredicates.containsLiteral("my"));
        assertTrue(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "myval")));
        assertFalse(predicate.apply(new BasicSensorEvent<String>(sensor1, app, "wrongVal")));
    }
}
