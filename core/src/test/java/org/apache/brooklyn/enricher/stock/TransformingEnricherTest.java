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

import java.util.List;

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.EnricherSpec;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.location.SimulatedLocation;
import org.apache.brooklyn.core.sensor.BasicAttributeSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.math.MathFunctions;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TransformingEnricherTest extends BrooklynAppUnitTestSupport {

    public static final Logger log = LoggerFactory.getLogger(TransformingEnricherTest.class);

    private static final Duration SHORT_WAIT = Duration.millis(250);
    private static final Duration VERY_SHORT_WAIT = Duration.millis(100);
    
    TestEntity producer;
    AttributeSensor<Integer> intSensorA;
    AttributeSensor<Integer> intSensorB;
    AttributeSensor<Long> target;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        producer = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        intSensorA = new BasicAttributeSensor<Integer>(Integer.class, "int.sensor.a");
        intSensorB = new BasicAttributeSensor<Integer>(Integer.class, "int.sensor.b");
        target = new BasicAttributeSensor<Long>(Long.class, "long.sensor.target");
        
        app.start(ImmutableList.of(new SimulatedLocation()));
    }
    
    @Test
    public void testTransformingEnricher() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        producer.enrichers().add(Enrichers.builder()
                .transforming(intSensorA)
                //.computing(MathFunctions.times(2)) // TODO calling it before "publishing" means it doesn't check return type!
                .publishing(target)
                .computing((Function)MathFunctions.times(2)) // TODO doesn't match strongly typed int->long
                .build());

        EntityAsserts.assertAttributeEqualsEventually(producer, target, 6L);
    }
    
    @Test
    public void testTargetValueTriggeredBySourceSensor() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.TARGET_SENSOR, target)
                .configure(Transformer.SOURCE_SENSOR, intSensorA)
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TARGET_VALUE, new DeferredSupplier<Long>() {
                    @Override public Long get() {
                        Integer a =  producer.sensors().get(intSensorA);
                        return (a == null ? null : a.longValue());
                    }}));

        EntityAsserts.assertAttributeEqualsEventually(producer, target, 3L);
        
        producer.sensors().set(intSensorA, 4);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 4L);
    }
    
    @Test
    public void testTargetValueTriggeredByOtherSensors() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.TARGET_SENSOR, target)
                .configure(Transformer.TRIGGER_SENSORS, ImmutableList.of(intSensorA, intSensorB))
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TARGET_VALUE, new DeferredSupplier<Long>() {
                    @Override public Long get() {
                        Integer a =  producer.sensors().get(intSensorA);
                        Integer b =  producer.sensors().get(intSensorB);
                        return (long) ((a == null ? 0 : a) + (b == null ? 0 : b));
                    }}));

        EntityAsserts.assertAttributeEqualsEventually(producer, target, 3L);
        
        producer.sensors().set(intSensorA, 4);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 4L);
        
        producer.sensors().set(intSensorB, 1);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 5L);
    }
    
    @Test
    public void testTransformationTriggeredByOtherSensors() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.TARGET_SENSOR, target)
                .configure(Transformer.SOURCE_SENSOR, intSensorA)
                .configure(Transformer.TRIGGER_SENSORS, ImmutableList.of(intSensorB))
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TRANSFORMATION_FROM_VALUE, (Function)MathFunctions.times(2))); // TODO doesn't match strongly typed int->long

        EntityAsserts.assertAttributeEqualsEventually(producer, target, 6L);
        
        producer.sensors().set(intSensorA, 4);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 8L);
        
        producer.sensors().set(target, -1L);
        producer.sensors().set(intSensorB, 1);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 8L);
    }
    
    @Test
    public void testTriggeringSensorNamesResolvedFromStrings() throws Exception {
        // Doing nasty casting here, but in YAML we could easily get passed this.
        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.TARGET_SENSOR, target)
                .configure(Transformer.TRIGGER_SENSORS, (List<Sensor<?>>)(List)ImmutableList.of(intSensorA.getName(), intSensorB.getName()))
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TARGET_VALUE, new DeferredSupplier<Long>() {
                    @Override public Long get() {
                        Integer a =  producer.sensors().get(intSensorA);
                        Integer b =  producer.sensors().get(intSensorB);
                        return (long) ((a == null ? 0 : a) + (b == null ? 0 : b));
                    }}));

        producer.sensors().set(intSensorA, 1);
        EntityAsserts.assertAttributeEqualsEventually(producer, target, 1L);
    }
    
    @Test
    public void testTransformerAvoidsInfiniteLoopWhereSourceAndTargetSame() throws Exception {
        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.SOURCE_SENSOR, intSensorA)
                .configure(Transformer.TARGET_SENSOR, intSensorA)
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TRANSFORMATION_FROM_VALUE, (Function)MathFunctions.times(2))); // TODO doesn't match strongly typed int->long

        // Short wait; expect us to never re-publish the source-sensor as that would cause infinite loop.
        producer.sensors().set(intSensorA, 1);
        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", SHORT_WAIT), producer, intSensorA, 1);
    }
    
    @Test
    public void testTransformerAvoidsInfiniteLoopWhereTriggerAndTargetSame() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        producer.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.TRIGGER_SENSORS, ImmutableList.of(intSensorA))
                .configure(Transformer.TARGET_SENSOR, intSensorA)
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TARGET_VALUE, new DeferredSupplier<Integer>() {
                    @Override public Integer get() {
                        Integer a =  producer.sensors().get(intSensorA);
                        return (a == null ? 0 : a+1);
                    }}));

        producer.sensors().set(intSensorA, 1);
        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", SHORT_WAIT), producer, intSensorA, 1);
    }
    
    @Test
    public void testTransformerFailsIfMissingTriggers() throws Exception {
        //ensure previous values get picked up
        producer.sensors().set(intSensorA, 3);

        try {
            producer.enrichers().add(EnricherSpec.create(Transformer.class)
                    .configure(Transformer.TARGET_SENSOR, target)
                    .configure(Transformer.PRODUCER, producer)
                    .configure(Transformer.TRANSFORMATION_FROM_VALUE, Functions.identity()));
            Asserts.shouldHaveFailedPreviously();
        } catch (IllegalArgumentException e) {
            Asserts.expectedFailureContains(e, "has no "+Transformer.SOURCE_SENSOR.getName()+" and no "+Transformer.TRIGGER_SENSORS.getName());
        }
    }
    
    @Test
    public void testInfersTargetSensorSameAsSource() throws Exception {
        producer.sensors().set(intSensorA, 3);
        
        app.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.SOURCE_SENSOR, intSensorA)
                .configure(Transformer.PRODUCER, producer)
                .configure(Transformer.TRANSFORMATION_FROM_VALUE, Functions.identity()));
        
        EntityAsserts.assertAttributeEqualsEventually(app, intSensorA, 3);
    }

    @Test
    public void testAllowCyclicPublishing() throws Exception {
        app.enrichers().add(EnricherSpec.create(Transformer.class)
                .configure(Transformer.SOURCE_SENSOR, intSensorA)
                .configure(Transformer.TARGET_SENSOR, intSensorA)
                .configure(Transformer.ALLOW_CYCLIC_PUBLISHING, true)
                .configure(Transformer.TRANSFORMATION_FROM_VALUE, new Function<Integer, Object>() {
                    @Override public Object apply(Integer input) {
                        if (input != null && input < 10) {
                            return input + 1;
                        } else {
                            return Entities.UNCHANGED;
                        }
                    }}));

        app.sensors().set(intSensorA, 3);

        EntityAsserts.assertAttributeEqualsEventually(app, intSensorA, 10);
        EntityAsserts.assertAttributeEqualsContinually(ImmutableMap.of("timeout", VERY_SHORT_WAIT), app, intSensorA, 10);
    }

    @Test
    public void testTransformerFailsWithEmptyConfig() throws Exception {
        EnricherSpec<?> spec = EnricherSpec.create(Transformer.class);

        assertAddEnricherThrowsIllegalArgumentException(spec, "Transformer has no");
    }
    
    private void assertAddEnricherThrowsIllegalArgumentException(EnricherSpec<?> spec, String expectedPhrase) {
        try {
            app.enrichers().add(spec);
            Asserts.shouldHaveFailedPreviously();
        } catch (IllegalArgumentException e) {
            Asserts.expectedFailureContains(e, expectedPhrase);
        }
    }
}
