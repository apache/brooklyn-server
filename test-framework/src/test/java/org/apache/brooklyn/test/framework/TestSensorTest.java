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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.sensor.AttributeSensorAndConfigKey;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class TestSensorTest extends BrooklynAppUnitTestSupport {

    private static final AttributeSensorAndConfigKey<Boolean, Boolean> BOOLEAN_SENSOR = ConfigKeys.newSensorAndConfigKey(Boolean.class, "boolean-sensor", "Boolean Sensor");
    private static final AttributeSensorAndConfigKey<String, String> STRING_SENSOR = ConfigKeys.newSensorAndConfigKey(String.class, "string-sensor", "String Sensor");
    private static final AttributeSensorAndConfigKey<Integer, Integer> INTEGER_SENSOR = ConfigKeys.newIntegerSensorAndConfigKey("integer-sensor", "Integer Sensor");
    private static final AttributeSensorAndConfigKey<Object, Object> OBJECT_SENSOR = ConfigKeys.newSensorAndConfigKey(Object.class, "object-sensor", "Object Sensor");

    private List<Location> locs = ImmutableList.of();
    private String testId;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testId = Identifiers.makeRandomId(8);
    }

    @Test
    public void testAssertEqual() throws Exception {
        int testInteger = 100;

        //Add Sensor Test for BOOLEAN sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, BOOLEAN_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("equals", true)));
        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("equals", testId)));
        //Add Sensor Test for INTEGER sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, INTEGER_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("equals", testInteger)));

        //Set BOOLEAN Sensor to true
        app.sensors().set(BOOLEAN_SENSOR, Boolean.TRUE);

        // Give a value to INTEGER sensor
        app.sensors().set(INTEGER_SENSOR, testInteger);

        //Set STRING sensor to random string
        app.sensors().set(STRING_SENSOR, testId);

        app.start(locs);

    }

    @Test
    public void testAssertEqualFailure() throws Exception {
        //Add Sensor Test for BOOLEAN sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.millis(10))
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, BOOLEAN_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("equals", true)));

        //Set BOOLEAN Sensor to false
        app.sensors().set(BOOLEAN_SENSOR, Boolean.FALSE);
        assertStartFails(app, AssertionError.class);
    }

    @Test
    public void testAssertEqualOnNullSensor() throws Exception {
        //Add Sensor Test for BOOLEAN sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.millis(10))
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, BOOLEAN_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("equals", false)));

        assertStartFails(app, AssertionError.class);
    }

    @Test
    public void testAssertNull() throws Exception {
        //Add Sensor Test for BOOLEAN sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, BOOLEAN_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS,  newMapAssertion("isNull", true)));
        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("notNull", true)));

        //Set STRING sensor to random string
        app.sensors().set(STRING_SENSOR, testId);

        app.start(locs);

    }


    @Test
    public void testAssertNullFail() throws Exception {
        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.millis(10))
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("isNull", true)));

        //Set STRING sensor to random string
        app.sensors().set(STRING_SENSOR, testId);
        assertStartFails(app, AssertionError.class);
    }

    @Test
    public void testAssertMatches() throws Exception {
        final long time = System.currentTimeMillis();
        final String sensorValue = String.format("%s%s%s", Identifiers.makeRandomId(8), time, Identifiers.makeRandomId(8));

        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("matches", String.format(".*%s.*", time))));
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, BOOLEAN_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("matches", "true")));

        //Set STRING sensor
        app.sensors().set(STRING_SENSOR, sensorValue);
        app.sensors().set(BOOLEAN_SENSOR, true);


        app.start(locs);
    }

    @Test
    public void testAssertMatchesFail() throws Exception {
        final String sensorValue = String.format("%s%s%s", Identifiers.makeRandomId(8), System.currentTimeMillis(), Identifiers.makeRandomId(8));

        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.millis(10))
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("matches", String.format(".*%s.*", Identifiers.makeRandomId(8)))));

        //Set STRING sensor
        app.sensors().set(STRING_SENSOR, sensorValue);
        assertStartFails(app, AssertionError.class);
    }

    @Test
    public void testAssertMatchesOnNullSensor() throws Exception {
        //Add Sensor Test for STRING sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.millis(10))
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("matches", String.format(".*%s.*", Identifiers.makeRandomId(8)))));

        assertStartFails(app, AssertionError.class);
    }


    @Test
    public void testAssertMatchesOnNonStringSensor() throws Exception {
        //Add Sensor Test for OBJECT sensor
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.SENSOR_NAME, OBJECT_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newListAssertion("matches", ".*TestObject.*id=.*")));

        app.sensors().set(OBJECT_SENSOR, new TestObject());

        app.start(locs);

    }

    @Test
    public void testFailFastIfNoTargetEntity() throws Exception {
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.ONE_MINUTE)
                .configure(TestSensor.SENSOR_NAME, STRING_SENSOR.getName())
                .configure(TestSensor.ASSERTIONS, newMapAssertion("isNull", true)));

        assertStartFails(app, IllegalStateException.class, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    @Test
    public void testFailFastIfNoSensor() throws Exception {
        app.createAndManageChild(EntitySpec.create(TestSensor.class)
                .configure(TestSensor.TIMEOUT, Duration.ONE_MINUTE)
                .configure(TestSensor.TARGET_ENTITY, app)
                .configure(TestSensor.ASSERTIONS, newMapAssertion("isNull", true)));

        assertStartFails(app, NullPointerException.class, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    protected void assertStartFails(TestApplication app, Class<? extends Throwable> clazz) throws Exception {
        assertStartFails(app, clazz, null);
    }
    
    protected void assertStartFails(final TestApplication app, final Class<? extends Throwable> clazz, Duration execTimeout) throws Exception {
        Runnable task = new Runnable() {
            public void run() {
                try {
                    app.start(locs);
                    Asserts.shouldHaveFailedPreviously();
                } catch (final PropagatedRuntimeException pre) {
                    final Throwable throwable = Exceptions.getFirstThrowableOfType(pre, clazz);
                    if (throwable == null) {
                        throw pre;
                    }
                }
            }
        };

        if (execTimeout == null) {
            task.run();
        } else {
            Asserts.assertReturnsEventually(task, execTimeout);
        }

        Entity entity = Iterables.find(Entities.descendantsWithoutSelf(app), Predicates.instanceOf(TestSensor.class));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
    }
    
    private List<Map<String, Object>> newListAssertion(final String assertionKey, final Object assertionValue) {
        final List<Map<String, Object>> result = new ArrayList<>();
        result.add(ImmutableMap.<String, Object>of(assertionKey, assertionValue));
        return result;
    }

    private Map<String, Object> newMapAssertion(final String assertionKey, final Object assertionValue) {
        return ImmutableMap.<String, Object>of(assertionKey, assertionValue);
    }


    class TestObject {
        private final String id;

        public TestObject() {
            id = Identifiers.makeRandomId(8);
        }

        public String getId() {
            return id;
        }

        @Override
        public String toString() {
            return ToStringBuilder.reflectionToString(this);
        }
    }

}
