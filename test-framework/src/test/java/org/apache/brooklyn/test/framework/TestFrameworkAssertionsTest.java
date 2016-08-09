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

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.framework.TestFrameworkAssertions.AssertionOptions;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class TestFrameworkAssertionsTest {
    private static final Logger LOG = LoggerFactory.getLogger(TestFrameworkAssertionsTest.class);

    @BeforeMethod
    public void setup() {

    }

    @DataProvider
    public Object[][] positiveTestsDP() {
        return new Object[][]{
                {"some-sensor-value", Arrays.asList(ImmutableMap.of("isEqualTo", "some-sensor-value"))},
                {"some-sensor-value", Arrays.asList(ImmutableMap.of("equalTo", "some-sensor-value"))},
                {"some-sensor-value", Arrays.asList(ImmutableMap.of("equals", "some-sensor-value"))},
                {"some-sensor-value", Arrays.asList(ImmutableMap.of("notEqual", "other-sensor-value"))},
                {10, Arrays.asList(ImmutableMap.of("notEqual", 20))},
                {"some-regex-value-to-match", Arrays.asList(ImmutableMap.of("matches", "some.*match", "isEqualTo", "some-regex-value-to-match"))},
                {null, Arrays.asList(ImmutableMap.of("isNull", Boolean.TRUE))},
                {"some-non-null-value", Arrays.asList(ImmutableMap.of("isNull", Boolean.FALSE))},
                {null, Arrays.asList(ImmutableMap.of("notNull", Boolean.FALSE))},
                {"some-non-null-value", Arrays.asList(ImmutableMap.of("notNull", Boolean.TRUE))},
                {"<html><body><h1>Im a H1 tag!</h1></body></html>", Arrays.asList(ImmutableMap.of("contains", "Im a H1 tag!"))},
                {"{\"a\":\"b\",\"c\":\"d\",\"e\":123,\"g\":false}", Arrays.asList(ImmutableMap.of("contains", "false"))},
                {"", Arrays.asList(ImmutableMap.of("isEmpty", Boolean.TRUE))},
                {"some-non-null-value", Arrays.asList(ImmutableMap.of("isEmpty", Boolean.FALSE))},
                {null, Arrays.asList(ImmutableMap.of("notEmpty", Boolean.FALSE))},
                {"some-non-null-value", Arrays.asList(ImmutableMap.of("notEmpty", Boolean.TRUE))},
                {"true", Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.TRUE))},
                {"false", Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.FALSE))},
                {"some-non-null-value", Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.FALSE))},
        };
    }

    @Test(dataProvider = "positiveTestsDP")
    public void positiveTest(final Object data, final List<Map<String, ?>> assertions) {
        final Supplier<Object> supplier = new Supplier<Object>() {
            @Override
            public Object get() {
                LOG.info("Supplier invoked for data [{}]", data);
                return data;
            }
        };
        TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(Objects.toString(data), supplier)
                .timeout(Asserts.DEFAULT_LONG_TIMEOUT).assertions(assertions));
    }

    @Test(dataProvider = "positiveTestsDP")
    public void positiveAbortTest(final Object data, final List<Map<String, ?>> abortConditions) {
        final Supplier<Object> supplier = new Supplier<Object>() {
            @Override
            public Object get() {
                LOG.info("Supplier invoked for data [{}]", data);
                return data;
            }
        };
        
        for (Map<String, ?> map : abortConditions) {
            try {
                TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(Objects.toString(data), supplier)
                        .timeout(Asserts.DEFAULT_LONG_TIMEOUT).abortConditions(map)
                        .assertions(ImmutableMap.of("equals", "wrong-value-never-equals")));
                Asserts.shouldHaveFailedPreviously();
            } catch (AbortError e) {
                // success
            }
        }
    }

    @DataProvider
    public Object[][] negativeTestsDP() {
        String arbitrary = Identifiers.makeRandomId(8);
        return new Object[][]{
                {"some-sensor-value", "isEqualTo", arbitrary, Arrays.asList(ImmutableMap.of("isEqualTo", arbitrary))},
                {"some-sensor-value", "equalTo", arbitrary, Arrays.asList(ImmutableMap.of("equalTo", arbitrary))},
                {"some-sensor-value", "equals", arbitrary, Arrays.asList(ImmutableMap.of("equals", arbitrary))},

                {"some-sensor-value", "notEqual", "some-sensor-value", Arrays.asList(ImmutableMap.of("notEqual", "some-sensor-value"))},
                {10, "notEqual", new Integer(10), Arrays.asList(ImmutableMap.of("notEqual", new Integer(10)))},

                {"some-regex-value-to-match", "matches", "some.*not-match", Arrays.asList(ImmutableMap.of("matches", "some.*not-match", "isEqualTo", "oink"))},

                {null, "notNull", Boolean.TRUE, Arrays.asList(ImmutableMap.of("notNull", Boolean.TRUE))},
                {"some-not-null-value", "notNull", Boolean.FALSE, Arrays.asList(ImmutableMap.of("notNull", Boolean.FALSE))},
                {"some-non-null-value", "isNull", Boolean.TRUE, Arrays.asList(ImmutableMap.of("isNull", Boolean.TRUE))},
                {null, "isNull", Boolean.FALSE, Arrays.asList(ImmutableMap.of("isNull", Boolean.FALSE))},

                {null, "notEmpty", Boolean.TRUE, Arrays.asList(ImmutableMap.of("notEmpty", Boolean.TRUE))},
                {"some-not-null-value", "notEmpty", Boolean.FALSE, Arrays.asList(ImmutableMap.of("notEmpty", Boolean.FALSE))},
                {"some-non-null-value", "isEmpty", Boolean.TRUE, Arrays.asList(ImmutableMap.of("isEmpty", Boolean.TRUE))},
                {null, "isEmpty", Boolean.FALSE, Arrays.asList(ImmutableMap.of("isEmpty", Boolean.FALSE))},

                {"<html><body><h1>Im a H1 tag!</h1></body></html>", "contains", "quack", Arrays.asList(ImmutableMap.of("contains", "quack"))},
                {"{\"a\":\"b\",\"c\":\"d\",\"e\":123,\"g\":false}", "contains", "moo", Arrays.asList(ImmutableMap.of("contains", "moo"))},

                {"true", "hasTruthValue", Boolean.FALSE, Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.FALSE))},
                {"false", "hasTruthValue", Boolean.TRUE, Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.TRUE))},
                {"some-not-null-value", "hasTruthValue", Boolean.TRUE, Arrays.asList(ImmutableMap.of("hasTruthValue", Boolean.TRUE))}
        };
    }

    @Test(dataProvider = "negativeTestsDP")
    public void negativeTests(final Object data, String condition, Object expected, final List<Map<String, ?>> assertions) {
        final Supplier<Object> supplier = new Supplier<Object>() {
            @Override
            public Object get() {
                LOG.info("Supplier invoked for data [{}]", data);
                return data;
            }
        };
        
        // It should always try at least once, so we can use a very small timeout
        Duration timeout = Duration.millis(1);
        
        try {
            TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(Objects.toString(data), supplier).timeout(timeout).assertions(assertions));
            Asserts.shouldHaveFailedPreviously();
        } catch (AssertionError e) {
            Asserts.expectedFailureContains(e, Objects.toString(data), condition, expected.toString());
        }
    }

    @Test(dataProvider = "negativeTestsDP")
    public void negativeAbortTest(final Object data, String condition, Object expected, final List<Map<String, ?>> assertions) {
        final Supplier<Object> supplier = new Supplier<Object>() {
            @Override
            public Object get() {
                LOG.info("Supplier invoked for data [{}]", data);
                return data;
            }
        };
        
        // It should always try at least once, so we can use a very small timeout
        Duration timeout = Duration.millis(1);
        
        // The abort-condition should never hold, so it should always fail due to the timeout rather than
        // aborting.
        try {
            TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions(Objects.toString(data), supplier)
                    .timeout(timeout).abortConditions(assertions)
                    .assertions(assertions));
            Asserts.shouldHaveFailedPreviously();
        } catch (AssertionError e) {
            Asserts.expectedFailureContains(e, Objects.toString(data), condition, expected.toString());
        }
    }

    @Test
    public void testUnknownAssertion() {
        final String randomId = Identifiers.makeRandomId(8);
        final Map<String, Object> assertions = new HashMap<>();
        assertions.put(randomId, randomId);

        final Supplier<String> supplier = new Supplier<String>() {
            @Override
            public String get() {
                LOG.info("Supplier invoked for data [{}]", randomId);
                return randomId;
            }
        };
        try {
            TestFrameworkAssertions.checkAssertionsEventually(new AssertionOptions("anyTarget", supplier).timeout(Duration.millis(1))
                    .assertions(assertions));
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable e) {
            Asserts.expectedFailureOfType(e, AssertionError.class);
            Asserts.expectedFailureContains(e, TestFrameworkAssertions.UNKNOWN_CONDITION);
        }
    }


}
