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
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.exceptions.FatalConfigurationRuntimeException;
import org.apache.brooklyn.util.exceptions.RuntimeInterruptedException;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.repeat.Repeater;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableList;
import com.google.common.reflect.TypeToken;


/**
 * Utility class to evaluate test-framework assertions
 */
public class TestFrameworkAssertions {

    public static final String IS_NULL = "isNull";
    public static final String NOT_NULL = "notNull";
    public static final String IS_EQUAL_TO = "isEqualTo";
    public static final String EQUAL_TO = "equalTo";
    public static final String EQUALS = "equals";
    public static final String NOT_EQUAL = "notEqual";
    public static final String MATCHES = "matches";
    public static final String CONTAINS = "contains";
    public static final String IS_EMPTY = "isEmpty";
    public static final String NOT_EMPTY = "notEmpty";
    public static final String HAS_TRUTH_VALUE = "hasTruthValue";
    public static final String UNKNOWN_CONDITION = "unknown condition";

    public static class AssertionOptions {
        protected Map<String,Object> flags = MutableMap.of();
        protected List<? extends Map<String, ?>> assertions = ImmutableList.of();
        protected List<? extends Map<String, ?>> abortConditions = ImmutableList.of();
        protected String target;
        protected Supplier<?> supplier;
        
        public AssertionOptions(String target, Supplier<?> supplier) {
            this.target = target;
            this.supplier = supplier;
        }
        public AssertionOptions flags(Map<String,?> val) {
            this.flags.putAll(val);
            return this;
        }
        public AssertionOptions timeout(Duration val) {
            this.flags.put("timeout", val);
            return this;
        }
        public AssertionOptions backoffToPeriod(Duration val) {
            this.flags.put("backoffToPeriod", val);
            return this;
        }
        public AssertionOptions assertions(Map<String, ?> val) {
            this.assertions = ImmutableList.of(val);
            return this;
        }
        public AssertionOptions assertions(List<? extends Map<String, ?>> val) {
            this.assertions = val;
            return this;
        }
        public AssertionOptions abortConditions(Map<String, ?> val) {
            this.abortConditions = ImmutableList.of(val);
            return this;
        }
        public AssertionOptions abortConditions(List<? extends Map<String, ?>> val) {
            this.abortConditions = val;
            return this;
        }
        public AssertionOptions target(String val) {
            this.target = val;
            return this;
        }
        public AssertionOptions supplier(Supplier<?> val) {
            this.supplier = val;
            return this;
        }
    }

    private TestFrameworkAssertions() {
    }


    /**
     * Get assertions tolerantly from a configuration key.
     * This supports either a simple map of assertions, such as
     *
     * <pre>
     * assertOut:
     *   contains: 2 users
     *   matches: .*[\d]* days.*
     * </pre>
     * or a list of such maps, (which allows you to repeat keys):
     * <pre>
     * assertOut:
     * - contains: 2 users
     * - contains: 2 days
     * </pre>
     */
    public static List<Map<String, Object>> getAssertions(Entity entity, ConfigKey<Object> key) {
        return getAsListOfMaps(entity, key);
    }
    
    /**
     * Get abort-condition tolerantly from a configuration key.
     * This supports either a simple map of assertions, such as
     *
     * <pre>
     * abortCondition:
     *   equals: ON_FIRE
     * </pre>
     * or a list of such maps, (which allows you to repeat keys):
     * <pre>
     * abortCondition:
     * - equals: ON_FIRE
     * - equals: STOPPING
     * - equals: STOPPED
     * - equals: DESTROYED
     * </pre>
     */
    public static List<Map<String, Object>> getAbortConditions(Entity entity, ConfigKey<Object> key) {
        return getAsListOfMaps(entity, key);
    }
    
    protected static List<Map<String, Object>> getAsListOfMaps(Entity entity, ConfigKey<Object> key) {
        Object config = entity.getConfig(key);
        Maybe<Map<String, Object>> maybeMap = TypeCoercions.tryCoerce(config, new TypeToken<Map<String, Object>>() {});
        if (maybeMap.isPresent()) {
            return Collections.singletonList(maybeMap.get());
        }

        Maybe<List<Map<String, Object>>> maybeList = TypeCoercions.tryCoerce(config,
            new TypeToken<List<Map<String, Object>>>() {});
        if (maybeList.isPresent()) {
            return maybeList.get();
        }

        throw new FatalConfigurationRuntimeException(key.getDescription() + " is not a map or list of maps");
    }

    /**
     * @Deprecated since 0.10.0; use {@link #checkAssertionsEventually(AssertionOptions)}
     */
    @Deprecated
    public static <T> void checkAssertions(Map<String,?> flags, List<? extends Map<String, ?>> assertions,
            String target, Supplier<T> supplier) {
        checkAssertionsEventually(new AssertionOptions(target, supplier).flags(flags).assertions(assertions));
    }
    
    /**
     * @Deprecated since 0.10.0; use {@link #checkAssertionsEventually(AssertionOptions)}; don't pass in own {@link AssertionSupport}.
     */
    @Deprecated
    public static <T> void checkAssertions(AssertionSupport support, Map<String,?> flags,
            List<? extends Map<String, ?>> assertions, String target, Supplier<T> supplier) {
        checkAssertionsEventually(support, new AssertionOptions(target, supplier).flags(flags).assertions(assertions));
    }

    /**
     * @Deprecated since 0.10.0; use {@link #checkAssertionsEventually(AssertionOptions)}
     */
    @Deprecated
    public static <T> void checkAssertions(AssertionSupport support, Map<String,?> flags,
            Map<String, ?> assertions, String target, Supplier<T> supplier) {
        checkAssertionsEventually(support, new AssertionOptions(target, supplier).flags(flags).assertions(assertions));
    }

    /**
     * @Deprecated since 0.10.0; use {@link #checkAssertionsEventually(AssertionOptions)}
     */
    @Deprecated
    public static <T> void checkAssertions(Map<String,?> flags, Map<String, ?> assertions,
            String target, Supplier<T> supplier) {
        checkAssertionsEventually(new AssertionOptions(target, supplier).flags(flags).assertions(assertions));
    }

    public static <T> void checkAssertionsEventually(AssertionOptions options) {
        AssertionSupport support = new AssertionSupport();
        checkAssertionsEventually(support, options);
        support.validate();
    }

    // TODO Copied from Asserts.toDuration
    private static Duration toDuration(Object duration, Duration defaultVal) {
        if (duration == null)
            return defaultVal;
        else 
            return Duration.of(duration);
    }

    protected static <T> void checkAssertionsEventually(AssertionSupport support, final AssertionOptions options) {
        if (options.assertions == null || options.assertions.isEmpty()) {
            return;
        }
        Map<String, ?> flags = options.flags;
        
        // To speed up tests, the period starts small and increases.
        Integer maxAttempts = (Integer) flags.get("maxAttempts");
        Duration timeout = toDuration(flags.get("timeout"), (maxAttempts == null ? Asserts.DEFAULT_LONG_TIMEOUT : Duration.PRACTICALLY_FOREVER));
        Duration backoffToPeriod = toDuration(flags.get("backoffToPeriod"), Duration.millis(500));
        Predicate<Throwable> rethrowImmediatelyPredicate = Predicates.or(ImmutableList.of(
                Predicates.instanceOf(AbortError.class), 
                Predicates.instanceOf(InterruptedException.class), 
                Predicates.instanceOf(RuntimeInterruptedException.class)));

        try {
            Repeater.create()
                    .until(new Callable<Boolean>() {
                        public Boolean call() {
                            try {
                                Object actual = options.supplier.get();
                                
                                for (Map<String, ?> abortMap : options.abortConditions) {
                                    checkActualAgainstAbortConditions(abortMap, options.target, actual);
                                }
                                for (Map<String, ?> assertionMap : options.assertions) {
                                    checkActualAgainstAssertions(assertionMap, options.target, actual);
                                }
                                return true;
                            } catch (AssertionError e) {
                                throw e;
                            } catch (Throwable t) {
                                throw t;
                            }
                        }})
                    .limitIterationsTo(maxAttempts != null ? maxAttempts : Integer.MAX_VALUE)
                    .limitTimeTo(timeout)
                    .backoffTo(backoffToPeriod)
                    .rethrowExceptionImmediately(rethrowImmediatelyPredicate)
                    .runRequiringTrue();

        } catch (AssertionError t) {
            support.fail(t);
        } catch (Throwable t) {
            Exceptions.propagateIfFatal(t);
            support.fail(t);
        }
    }

    protected static <T> void checkActualAgainstAssertions(AssertionSupport support, Map<String, ?> assertions, 
            String target, T actual) {
        try {
            checkActualAgainstAssertions(assertions, target, actual);
        } catch (Throwable t) {
            support.fail(t);
        }
    }

    protected static <T> void checkActualAgainstAssertions(Map<String, ?> assertions,
            String target, T actual) {
        for (Map.Entry<String, ?> assertion : assertions.entrySet()) {
            String condition = assertion.getKey().toString();
            Object expected = assertion.getValue();
            switch (condition) {

                case IS_EQUAL_TO :
                case EQUAL_TO :
                case EQUALS :
                    if (null == actual || !actual.equals(expected)) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case NOT_EQUAL :
                    if (Objects.equals(actual, expected)) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;
                    
                case IS_NULL :
                    if (isTrue(expected) != (null == actual)) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case NOT_NULL :
                    if (isTrue(expected) != (null != actual)) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case CONTAINS :
                    if (null == actual || !actual.toString().contains(expected.toString())) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case IS_EMPTY :
                    if (isTrue(expected) != (null == actual || Strings.isEmpty(actual.toString()))) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case NOT_EMPTY :
                    if (isTrue(expected) != ((null != actual && Strings.isNonEmpty(actual.toString())))) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case MATCHES :
                    if (null == actual || !actual.toString().matches(expected.toString())) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                case HAS_TRUTH_VALUE :
                    if (isTrue(expected) != isTrue(actual)) {
                        failAssertion(target, condition, expected, actual);
                    }
                    break;

                default:
                    failAssertion(target, UNKNOWN_CONDITION, condition, actual);
            }
        }
    }

    protected static <T> void checkActualAgainstAbortConditions(Map<String, ?> assertions, String target, T actual) {
        for (Map.Entry<String, ?> assertion : assertions.entrySet()) {
            String condition = assertion.getKey().toString();
            Object expected = assertion.getValue();
            switch (condition) {

                case IS_EQUAL_TO :
                case EQUAL_TO :
                case EQUALS :
                    if (null != actual && actual.equals(expected)) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case NOT_EQUAL :
                    if (!Objects.equals(actual, expected)) {
                        abort(target, condition, expected, actual);
                    }
                    break;
                    
                case IS_NULL :
                    if (isTrue(expected) == (null == actual)) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case NOT_NULL :
                    if (isTrue(expected) == (null != actual)) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case CONTAINS :
                    if (null != actual && actual.toString().contains(expected.toString())) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case IS_EMPTY :
                    if (isTrue(expected) == (null == actual || Strings.isEmpty(actual.toString()))) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case NOT_EMPTY :
                    if (isTrue(expected) == ((null != actual && Strings.isNonEmpty(actual.toString())))) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case MATCHES :
                    if (null != actual && actual.toString().matches(expected.toString())) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                case HAS_TRUTH_VALUE :
                    if (isTrue(expected) == isTrue(actual)) {
                        abort(target, condition, expected, actual);
                    }
                    break;

                default:
                    abort(target, condition, expected, actual);
            }
        }
    }
    
    static void failAssertion(String target, String assertion, Object expected, Object actual) {
        throw new AssertionError(Joiner.on(' ').join(
            Objects.toString(target),
            "expected",
            Objects.toString(assertion),
            Objects.toString(expected),
            "but found",
            Objects.toString(actual)));
    }

    static void abort(String target, String assertion, Object expected, Object actual) {
        throw new AbortError(Objects.toString(target) + " matched abort criteria '" 
                + Objects.toString(assertion) + " " + Objects.toString(expected) + "', found "
                + Objects.toString(actual));
    }

    private static boolean isTrue(Object object) {
        return null != object && Boolean.valueOf(object.toString());
    }

    /**
     * A convenience to collect multiple assertion failures.
     */
    public static class AssertionSupport {
        private List<AssertionError> failures = new ArrayList<>();

        public void fail(String target, String assertion, Object expected) {
            failures.add(new AssertionError(Joiner.on(' ').join(
                null != target ? target : "null",
                null != assertion ? assertion : "null",
                null != expected ? expected : "null")));
        }

        public void fail(Throwable throwable) {
            failures.add(new AssertionError(throwable.getMessage(), throwable));
        }

        /**
         * @throws AssertionError if any failures were collected.
         */
        public void validate() {
            if (0 < failures.size()) {

                if (1 == failures.size()) {
                    throw failures.get(0);
                }

                StringBuilder builder = new StringBuilder();
                for (AssertionError assertionError : failures) {
                    builder.append(assertionError.getMessage()).append("\n");
                }
                throw new AssertionError("Assertions failed:\n" + builder, new CompoundRuntimeException("Assertions", failures));
            }
        }
    }
}
