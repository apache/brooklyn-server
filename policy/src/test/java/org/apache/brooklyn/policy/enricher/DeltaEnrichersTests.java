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
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Function;

public class DeltaEnrichersTests extends BrooklynAppUnitTestSupport {
    
    Entity producer;

    Sensor<Integer> intSensor;
    Sensor<Double> avgSensor;
    SubscriptionContext subscription;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        producer = app.addChild(EntitySpec.create(TestEntity.class));
        intSensor = new BasicAttributeSensor<Integer>(Integer.class, "int sensor");
    }

    @Test
    public void testDeltaEnricher() {
        AttributeSensor<Integer> deltaSensor = new BasicAttributeSensor<Integer>(Integer.class, "delta sensor");
        @SuppressWarnings("unchecked")
        DeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(DeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", deltaSensor));

        delta.onEvent(intSensor.newEvent(producer, 0));
        delta.onEvent(intSensor.newEvent(producer, 0));
        assertEquals(producer.getAttribute(deltaSensor), (Integer)0);
        delta.onEvent(intSensor.newEvent(producer, 1));
        assertEquals(producer.getAttribute(deltaSensor), (Integer)1);
        delta.onEvent(intSensor.newEvent(producer, 3));
        assertEquals(producer.getAttribute(deltaSensor), (Integer)2);
        delta.onEvent(intSensor.newEvent(producer, 8));
        assertEquals(producer.getAttribute(deltaSensor), (Integer)5);
    }
    
    @Test
    public void testMonospaceTimeWeightedDeltaEnricher() {
        AttributeSensor<Double> deltaSensor = new BasicAttributeSensor<Double>(Double.class, "per second delta delta sensor");
        @SuppressWarnings("unchecked")
        TimeWeightedDeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(TimeWeightedDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", deltaSensor)
                .configure("unitMillis", 1000));
        
        // Don't start with timestamp=0: that may be treated special 
        delta.onEvent(intSensor.newEvent(producer, 0), 1000);
        assertEquals(producer.getAttribute(deltaSensor), null);
        delta.onEvent(intSensor.newEvent(producer, 0), 2000);
        assertEquals(producer.getAttribute(deltaSensor), 0d);
        delta.onEvent(intSensor.newEvent(producer, 1), 3000);
        assertEquals(producer.getAttribute(deltaSensor), 1d);
        delta.onEvent(intSensor.newEvent(producer, 3), 4000);
        assertEquals(producer.getAttribute(deltaSensor), 2d);
        delta.onEvent(intSensor.newEvent(producer, 8), 5000);
        assertEquals(producer.getAttribute(deltaSensor), 5d);
    }
    
    @Test
    public void testVariableTimeWeightedDeltaEnricher() {
        AttributeSensor<Double> deltaSensor = new BasicAttributeSensor<Double>(Double.class, "per second delta delta sensor");
        @SuppressWarnings("unchecked")
        TimeWeightedDeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(TimeWeightedDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", deltaSensor)
                .configure("unitMillis", 1000));
        
        delta.onEvent(intSensor.newEvent(producer, 0), 1000);
        delta.onEvent(intSensor.newEvent(producer, 0), 3000);
        assertEquals(producer.getAttribute(deltaSensor), 0d);
        delta.onEvent(intSensor.newEvent(producer, 3), 6000);
        assertEquals(producer.getAttribute(deltaSensor), 1d);
        delta.onEvent(intSensor.newEvent(producer, 7), 8000);
        assertEquals(producer.getAttribute(deltaSensor), 2d);
        delta.onEvent(intSensor.newEvent(producer, 12), 8500);
        assertEquals(producer.getAttribute(deltaSensor), 10d);
        delta.onEvent(intSensor.newEvent(producer, 15), 10500);
        assertEquals(producer.getAttribute(deltaSensor), 1.5d);
    }

    @Test
    public void testPostProcessorCalledForDeltaEnricher() {
        AttributeSensor<Double> deltaSensor = new BasicAttributeSensor<Double>(Double.class, "per second delta delta sensor");
        @SuppressWarnings("unchecked")
        TimeWeightedDeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(TimeWeightedDeltaEnricher.class)
                .configure("producer", producer)
                .configure("source", intSensor)
                .configure("target", deltaSensor)
                .configure("unitMillis", 1000)
                .configure("postProcessor", new AddConstant(123d)));
        
        delta.onEvent(intSensor.newEvent(producer, 0), 1000);
        delta.onEvent(intSensor.newEvent(producer, 0), 2000);
        assertEquals(producer.getAttribute(deltaSensor), 123+0d);
        delta.onEvent(intSensor.newEvent(producer, 1), 3000);
        assertEquals(producer.getAttribute(deltaSensor), 123+1d);
    }

    private static class AddConstant implements Function<Double, Double> {
        private Double constant;

        public AddConstant(Double constant) {
            super();
            this.constant = constant;
        }

        @Override
        public Double apply(Double input) {
            return input + constant;
        }
    }
}
