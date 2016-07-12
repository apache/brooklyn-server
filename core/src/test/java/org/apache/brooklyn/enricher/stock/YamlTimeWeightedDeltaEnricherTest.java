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
package org.apache.brooklyn.enricher.stock;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.SubscriptionContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.sensor.BasicSensorEvent;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class YamlTimeWeightedDeltaEnricherTest extends BrooklynAppUnitTestSupport {
    
    BasicEntity producer;

    AttributeSensor<Integer> intSensor;
    AttributeSensor<Double> avgSensor, deltaSensor;
    SubscriptionContext subscription;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        producer = app.addChild(EntitySpec.create(BasicEntity.class));

        intSensor = new BasicAttributeSensor<Integer>(Integer.class, "int sensor");
        deltaSensor = new BasicAttributeSensor<Double>(Double.class, "delta sensor");
    }

    /**
     * TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins (e.g. brooklyn-server-pull-requests # 732)
     * 
     * The problem is that the enricher's subscription uses "notifyOfInitialValue", 
     * so another thread will execute with that value. If the other thread executes after we've 
     * done onEvent(2000), but before we do the assertion, then the test will fail (because the 
     * other thread will have set the sensor's initial value to null).
     * 
     * This is just an issue with the way we've written the test, rather than for production
     * usage. In production, all events would go to the enricher sequentially.
     * 
     * We wrote the test like this because we want to inject specific timestamps.
     * We *could* refactor LocalSubscriptionManager to be configured with a 
     * {@link com.google.common.base.Ticker} that we can explicitly supply and configure.
     * That's a bigger change than I'd like to make right at this moment!
     */
    @Test(groups={"Broken"})
    public void testMonospaceTimeWeightedDeltaEnricher() {
        @SuppressWarnings("unchecked")
        YamlTimeWeightedDeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(YamlTimeWeightedDeltaEnricher.class)
            .configure(YamlTimeWeightedDeltaEnricher.PRODUCER, producer)
            .configure(YamlTimeWeightedDeltaEnricher.SOURCE_SENSOR, intSensor)
            .configure(YamlTimeWeightedDeltaEnricher.TARGET_SENSOR, deltaSensor));
        
        delta.onEvent(newIntSensorEvent(0, 0));
        assertEquals(producer.getAttribute(deltaSensor), null);
        delta.onEvent(newIntSensorEvent(0, 1000));
        assertEquals(producer.getAttribute(deltaSensor), 0d);
        delta.onEvent(newIntSensorEvent(1, 2000));
        assertEquals(producer.getAttribute(deltaSensor), 1d);
        delta.onEvent(newIntSensorEvent(3, 3000));
        assertEquals(producer.getAttribute(deltaSensor), 2d);
        delta.onEvent(newIntSensorEvent(8, 4000));
        assertEquals(producer.getAttribute(deltaSensor), 5d);
    }
    
    protected BasicSensorEvent<Integer> newIntSensorEvent(int value, long timestamp) {
        return new BasicSensorEvent<Integer>(intSensor, producer, value, timestamp);
    }
    
    /**
     * TODO BROOKLYN-272, Disabled, because fails non-deterministically in jenkins (brooklyn-server-master #84):
     * 
     * testVariableTimeWeightedDeltaEnricher(org.apache.brooklyn.enricher.stock.YamlTimeWeightedDeltaEnricherTest)  Time elapsed: 0.005 sec  <<< FAILURE!
     * java.lang.AssertionError: expected [0.0] but found [null]
     *     at org.testng.Assert.fail(Assert.java:94)
     *     at org.testng.Assert.failNotEquals(Assert.java:494)
     *     at org.testng.Assert.assertEquals(Assert.java:123)
     *     at org.testng.Assert.assertEquals(Assert.java:165)
     *     at org.apache.brooklyn.enricher.stock.YamlTimeWeightedDeltaEnricherTest.testVariableTimeWeightedDeltaEnricher(YamlTimeWeightedDeltaEnricherTest.java:96)
     */
    @Test(groups={"Broken"})
    public void testVariableTimeWeightedDeltaEnricher() {
        @SuppressWarnings("unchecked")
        YamlTimeWeightedDeltaEnricher<Integer> delta = producer.enrichers().add(EnricherSpec.create(YamlTimeWeightedDeltaEnricher.class)
            .configure(YamlTimeWeightedDeltaEnricher.PRODUCER, producer)
            .configure(YamlTimeWeightedDeltaEnricher.SOURCE_SENSOR, intSensor)
            .configure(YamlTimeWeightedDeltaEnricher.TARGET_SENSOR, deltaSensor));
        
        delta.onEvent(newIntSensorEvent(0, 0));
        delta.onEvent(newIntSensorEvent(0, 2000));
        assertEquals(producer.getAttribute(deltaSensor), 0d);
        delta.onEvent(newIntSensorEvent(3, 5000));
        assertEquals(producer.getAttribute(deltaSensor), 1d);
        delta.onEvent(newIntSensorEvent(7, 7000));
        assertEquals(producer.getAttribute(deltaSensor), 2d);
        delta.onEvent(newIntSensorEvent(12, 7500));
        assertEquals(producer.getAttribute(deltaSensor), 10d);
        delta.onEvent(newIntSensorEvent(15, 9500));
        assertEquals(producer.getAttribute(deltaSensor), 1.5d);
    }

}
