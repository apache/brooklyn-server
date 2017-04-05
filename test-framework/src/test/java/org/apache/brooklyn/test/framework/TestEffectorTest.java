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

import static org.apache.brooklyn.core.entity.trait.Startable.SERVICE_UP;
import static org.assertj.core.api.Assertions.assertThat;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.entity.TestEntity;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicates;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class TestEffectorTest extends BrooklynAppUnitTestSupport {

    private List<Location> locs = ImmutableList.of();
    private String testId;

    private TestCase testCase;
    private TestEntity testEntity;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        testId = Identifiers.makeRandomId(8);
        testCase = app.createAndManageChild(EntitySpec.create(TestCase.class));
        testEntity = testCase.addChild(EntitySpec.create(TestEntity.class));
    }

    @Test
    public void testSimpleEffector() throws Exception {
        final TestEffector testEffector = testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "simpleEffector"));

        app.start(locs);

        assertThat(testEntity.sensors().get(TestEntity.SIMPLE_EFFECTOR_INVOKED)).isNotNull();
        assertThat(testEntity.sensors().get(TestEntity.SIMPLE_EFFECTOR_INVOKED)).isTrue();

        assertThat(testEffector.sensors().get(TestEffector.EFFECTOR_RESULT)).isNull();
    }

    @Test
    public void testEffectorReturnsInt() throws Exception {
        int intToReturn = 123;
        
        List<Map<String, Object>> assertions = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.EQUAL_TO, intToReturn));

        final TestEffector testEffector = testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorReturnsInt")
                .configure(TestEffector.EFFECTOR_PARAMS, ImmutableMap.of("intToReturn", intToReturn))
                .configure(TestEffector.ASSERTIONS, assertions));

        app.start(locs);

        assertThat(testEffector.sensors().get(TestEffector.EFFECTOR_RESULT)).isEqualTo(123);
        assertThat(testEffector.sensors().get(SERVICE_UP)).isTrue();
    }

    @Test
    public void testEffectorPositiveAssertions() throws Exception {
        String stringToReturn = "Hello World!";

        Map<String, String> effectorParams = ImmutableMap.of("stringToReturn", stringToReturn);

        List<Map<String, Object>> assertions = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.EQUAL_TO, stringToReturn),
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.CONTAINS, "Hello")
        );

        final TestEffector testEffector = testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorReturnsString")
                .configure(TestEffector.EFFECTOR_PARAMS, effectorParams)
                .configure(TestEffector.ASSERTIONS, assertions));

        app.start(locs);

        assertThat(testEffector.sensors().get(TestEffector.EFFECTOR_RESULT)).isEqualTo(stringToReturn);
        assertThat(testEffector.sensors().get(SERVICE_UP)).isTrue().withFailMessage("Service should be up");
    }

    @Test
    public void testEffectorNegativeAssertions() throws Exception {
        String stringToReturn = "Goodbye World!";

        Map<String, String> effectorParams = ImmutableMap.of("stringToReturn", stringToReturn);

        List<Map<String, Object>> assertions = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.EQUAL_TO, "Not the string I expected"),
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.CONTAINS, "Hello")
        );

        final TestEffector testEffector = testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorReturnsString")
                .configure(TestEffector.EFFECTOR_PARAMS, effectorParams)
                .configure(TestEffector.ASSERTIONS, assertions));

        assertStartFails(app, AssertionError.class);
        assertThat(testEffector.sensors().get(SERVICE_UP)).isFalse().withFailMessage("Service should not be up");
    }

    @Test
    public void testComplexffector() throws Exception {
        final long expectedLongValue = System.currentTimeMillis();
        final boolean expectedBooleanValue = expectedLongValue % 2 == 0;

        final TestEffector testEffector = testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "complexEffector")
                .configure(TestEffector.EFFECTOR_PARAMS, ImmutableMap.of(
                        "stringValue", testId,
                        "booleanValue", expectedBooleanValue,
                        "longValue", expectedLongValue)));

        app.start(locs);

        assertThat(testEntity.sensors().get(TestEntity.SIMPLE_EFFECTOR_INVOKED)).isNull();
        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_INVOKED)).isNotNull();
        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_INVOKED)).isTrue();

        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_STRING)).isNotNull();
        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_STRING)).isEqualTo(testId);

        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_BOOLEAN)).isNotNull();
        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_BOOLEAN)).isEqualTo(expectedBooleanValue);

        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_LONG)).isNotNull();
        assertThat(testEntity.sensors().get(TestEntity.COMPLEX_EFFECTOR_LONG)).isEqualTo(expectedLongValue);

        assertThat(testEffector.sensors().get(TestEffector.EFFECTOR_RESULT)).isNotNull();
        assertThat(testEffector.sensors().get(TestEffector.EFFECTOR_RESULT)).isInstanceOf(TestEntity.TestPojo.class);

        final TestEntity.TestPojo effectorResult = (TestEntity.TestPojo) testEffector.sensors().get(TestEffector.EFFECTOR_RESULT);
        assertThat(effectorResult.getBooleanValue()).isEqualTo(expectedBooleanValue);
        assertThat(effectorResult.getStringValue()).isEqualTo(testId);
        assertThat(effectorResult.getLongValue()).isEqualTo(expectedLongValue);

    }

    @Test
    public void testEffectorTimeout() throws Exception {
        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TIMEOUT, Duration.millis(10))
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorHangs"));

        assertStartFails(app, AssertionError.class, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    @Test
    public void testEffectorTimeoutAppliesOnlyToCallAndNotToAssertionCheck() throws Exception {
        String stringToReturn = "Goodbye World!";

        Map<String, String> effectorParams = ImmutableMap.of("stringToReturn", stringToReturn);

        List<Map<String, Object>> assertions = ImmutableList.<Map<String, Object>>of(
                ImmutableMap.<String, Object>of(TestFrameworkAssertions.EQUAL_TO, "Not the string I expected")
        );

        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TIMEOUT, Duration.minutes(1))
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorReturnsString")
                .configure(TestEffector.EFFECTOR_PARAMS, effectorParams)
                .configure(TestEffector.ASSERTIONS, assertions));

        Stopwatch stopwatch = Stopwatch.createStarted();

        assertStartFails(app, AssertionError.class, Asserts.DEFAULT_LONG_TIMEOUT);
        
        Duration duration = Duration.of(stopwatch);
        assertTrue(duration.isShorterThan(Asserts.DEFAULT_LONG_TIMEOUT), "duration="+duration);
    }

    @Test
    public void testEffectorFailureNotRetried() throws Exception {
        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TIMEOUT, Duration.minutes(1))
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorFails"));

        assertStartFails(app, TestEntity.EffectorFailureException.class, Asserts.DEFAULT_LONG_TIMEOUT);
        
        assertEquals(testEntity.sensors().get(TestEntity.FAILING_EFFECTOR_INVOCATION_COUNT), Integer.valueOf(1));
    }

    @Test
    public void testEffectorFailureRetriedUpToMaxAttempts() throws Exception {
        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.MAX_ATTEMPTS, 2)
                .configure(TestEffector.TIMEOUT, Duration.minutes(1))
                .configure(TestEffector.TARGET_ENTITY, testEntity)
                .configure(TestEffector.EFFECTOR_NAME, "effectorFails"));
        Stopwatch stopwatch = Stopwatch.createStarted();

        assertStartFails(app, TestEntity.EffectorFailureException.class, Asserts.DEFAULT_LONG_TIMEOUT);

        Duration duration = Duration.of(stopwatch);
        assertTrue(duration.isShorterThan(Asserts.DEFAULT_LONG_TIMEOUT), "duration="+duration);
        
        assertEquals(testEntity.sensors().get(TestEntity.FAILING_EFFECTOR_INVOCATION_COUNT), Integer.valueOf(2));
    }

    @Test
    public void testFailFastIfNoTargetEntity() throws Exception {
        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.EFFECTOR_NAME, "simpleEffector"));

        assertStartFails(app, IllegalStateException.class, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    @Test
    public void testFailFastIfNoEffector() throws Exception {
        testCase.addChild(EntitySpec.create(TestEffector.class)
                .configure(TestEffector.TARGET_ENTITY, testEntity));

        assertStartFails(app, NullPointerException.class, Asserts.DEFAULT_LONG_TIMEOUT);
    }

    protected void assertStartFails(TestApplication app, Class<? extends Throwable> clazz) throws Exception {
        assertStartFails(app, clazz, null);
    }
    
    protected void assertStartFails(final TestApplication app, final Class<? extends Throwable> clazz, Duration execTimeout) throws Exception {
        Runnable task = new Runnable() {
            @Override
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

        Entity entity = Iterables.find(Entities.descendantsWithoutSelf(app), Predicates.instanceOf(TestEffector.class));
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        EntityAsserts.assertAttributeEqualsEventually(entity, Attributes.SERVICE_UP, false);
    }
}
