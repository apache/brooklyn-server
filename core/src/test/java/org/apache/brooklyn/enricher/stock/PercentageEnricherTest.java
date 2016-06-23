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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;

public class PercentageEnricherTest extends BrooklynAppUnitTestSupport {

    public static final Logger log = LoggerFactory.getLogger(PercentageEnricherTest.class);

    AttributeSensor<Double> currentSensor;
    AttributeSensor<Double> totalSensor;
    AttributeSensor<Double> targetSensor;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        currentSensor = new BasicAttributeSensor<Double>(Double.class, "current");
        totalSensor = new BasicAttributeSensor<Double>(Double.class, "total");
        targetSensor = new BasicAttributeSensor<Double>(Double.class, "target");

        app.start(ImmutableList.of(new SimulatedLocation()));
    }

    private void addEnricher() {
        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor));
    }

    @Test
    public void vanillaTest() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, 100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
    }

    @Test
    public void currentNullTest() {
        addEnricher();

        app.sensors().set(currentSensor, null);
        app.sensors().set(totalSensor, 100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void totalNullTest() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, null);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void bothInputNullTest() {
        addEnricher();

        app.sensors().set(currentSensor, null);
        app.sensors().set(totalSensor, null);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void currentZeroTest() {
        addEnricher();

        app.sensors().set(currentSensor, 0d);
        app.sensors().set(totalSensor, 100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 0d);
    }

    @Test
    public void totalZeroTest() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, 0d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void totalLessThanCurrentTest() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, 25d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void oneHundredPercentTest() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, 50d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 100d);
    }

    @Test
    public void negativeCurrent() {
        addEnricher();

        app.sensors().set(currentSensor, -50d);
        app.sensors().set(totalSensor, 100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void negativeTotal() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, -100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void bothSourceNegative() {
        addEnricher();

        app.sensors().set(currentSensor, -50d);
        app.sensors().set(totalSensor, -100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, null);
    }

    @Test
    public void totalDoubleMaxValue() {
        addEnricher();

        app.sensors().set(currentSensor, Double.MAX_VALUE);
        app.sensors().set(totalSensor, Double.MAX_VALUE);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 100d);
    }

    //SETUP TESTS
    @Test(expectedExceptions = IllegalArgumentException.class)
    public void totalNoCurrentSensor() {
        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void totalNoTotalSensor() {
        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor));
    }

    @Test(expectedExceptions = IllegalArgumentException.class)
    public void totalNoTargetSensor() {
        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor));
    }

    @Test
    public void testDifferentProducer() {
        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class)
                .configure(BrooklynConfigKeys.SKIP_ON_BOX_BASE_DIR_RESOLUTION, shouldSkipOnBoxBaseDirResolution());

        TestApplication producer = mgmt.getEntityManager().createEntity(appSpec);

        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor)
                .configure(PercentageEnricher.PRODUCER, producer)
        );

        producer.sensors().set(currentSensor, 25d);
        producer.sensors().set(totalSensor, 50d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
        EntityAsserts.assertAttributeEqualsEventually(producer, targetSensor, null);
    }

    @Test
    public void testLong() {
        AttributeSensor<Long> currentSensor = new BasicAttributeSensor<Long>(Long.class, "current");
        AttributeSensor<Long> totalSensor = new BasicAttributeSensor<Long>(Long.class, "total");

        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor)
        );

        app.sensors().set(currentSensor, 25l);
        app.sensors().set(totalSensor, 50l);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
    }

    @Test
    public void testInteger() {
        AttributeSensor<Integer> currentSensor = new BasicAttributeSensor<Integer>(Integer.class, "current");
        AttributeSensor<Integer> totalSensor = new BasicAttributeSensor<Integer>(Integer.class, "total");

        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor)
        );

        app.sensors().set(currentSensor, 25);
        app.sensors().set(totalSensor, 50);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
    }

    @Test
    public void testFloat() {
        AttributeSensor<Float> currentSensor = new BasicAttributeSensor<Float>(Float.class, "current");
        AttributeSensor<Float> totalSensor = new BasicAttributeSensor<Float>(Float.class, "total");

        app.enrichers().add(EnricherSpec.create(PercentageEnricher.class)
                .configure(PercentageEnricher.SOURCE_CURRENT_SENSOR, currentSensor)
                .configure(PercentageEnricher.SOURCE_TOTAL_SENSOR, totalSensor)
                .configure(PercentageEnricher.TARGET_SENSOR, targetSensor)
        );

        app.sensors().set(currentSensor, 25f);
        app.sensors().set(totalSensor, 50f);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
    }

    @Test
    public void validThenInvalid() {
        addEnricher();

        app.sensors().set(currentSensor, 50d);
        app.sensors().set(totalSensor, 100d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
        app.sensors().set(totalSensor, 0d);
        EntityAsserts.assertAttributeEqualsEventually(app, targetSensor, 50d);
    }

}
