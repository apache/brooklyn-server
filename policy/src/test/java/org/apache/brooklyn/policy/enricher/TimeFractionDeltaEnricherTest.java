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
package org.apache.brooklyn.policy.enricher;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.SubscriptionContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class TimeFractionDeltaEnricherTest extends BrooklynAppUnitTestSupport {

    private static final double PRECISION = 0.000001;
    
    private Entity producer;

    Sensor<Integer> intSensor;
    AttributeSensor<Double> fractionSensor;
    SubscriptionContext subscription;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        producer = app.addChild(EntitySpec.create(TestEntity.class));
        
        intSensor = Sensors.newIntegerSensor("int sensor");
        fractionSensor = Sensors.newDoubleSensor("fraction sensor");
    }

    @Test
    public void testCalculatesFractions() {
        @SuppressWarnings("unchecked")
        TimeFractionDeltaEnricher<Integer> enricher = producer.enrichers().add(EnricherSpec.create(TimeFractionDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", fractionSensor)
                .configure("durationPerOrigUnit", Duration.millis(1)));
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 0, 1000000L));
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 0, 1001000L));
        assertEquals(producer.getAttribute(fractionSensor), 0d);
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 100, 1002000L));
        assertEquals(producer.getAttribute(fractionSensor), 0.1d, PRECISION);
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 300, 1003000L));
        assertEquals(producer.getAttribute(fractionSensor), 0.2d, PRECISION);
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 2300, 1004000L));
        assertEquals(producer.getAttribute(fractionSensor), 2d, PRECISION);
    }
    
    @Test
    public void testConvertsTimeUnits() {
        @SuppressWarnings("unchecked")
        TimeFractionDeltaEnricher<Integer> enricher = producer.enrichers().add(EnricherSpec.create(TimeFractionDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", fractionSensor)
                .configure("durationPerOrigUnit", Duration.micros(1)));
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 0, 1000000L));
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 1000000, 1001000L));
        assertEquals(producer.getAttribute(fractionSensor), 1d);
    }
    
    @Test
    public void testConverts100NanosTimeBlocks() {
        @SuppressWarnings("unchecked")
        TimeFractionDeltaEnricher<Integer> enricher = producer.enrichers().add(EnricherSpec.create(TimeFractionDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", fractionSensor)
                .configure("durationPerOrigUnit", Duration.nanos(100)));
        
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 0, 1000000L));
        enricher.onEvent(new BasicSensorEvent<Integer>(intSensor, producer, 10000000, 1001000L));
        assertEquals(producer.getAttribute(fractionSensor), 1d);
    }
}
