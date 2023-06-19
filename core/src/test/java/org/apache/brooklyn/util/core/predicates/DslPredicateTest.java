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
package org.apache.brooklyn.util.core.predicates;

import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

public class DslPredicateTest extends BrooklynMgmtUnitTestSupport {

    static {
        DslPredicates.init();
    }

    @Test
    public void testSimpleEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "equals", "x"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertFalse(p.test("y"));
    }

    @Test
    public void testSimpleEqualsAsPredicate() {
        Predicate p = TypeCoercions.coerce(MutableMap.of(
                "equals", "x"), Predicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertFalse(p.test("y"));
    }

    @Test
    public void testImplicitEqualsAsDslPredicate() {
        Predicate p = TypeCoercions.coerce("x", DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertFalse(p.test("y"));
    }

    @Test
    public void testImplicitEqualsAsRawPredicateNotSupported() {
        Asserts.assertFailsWith(() -> {
            Predicate p = TypeCoercions.coerce("x", Predicate.class);
            // we deliberately don't support this, because predicates of the form 'notNull' should resolve in a different way;
            // but _given_ a predicate which should be DslPredicate, we _do_ want to allow it;
            // see DslPredicates.JsonDeserializer registered/permitted classes
            Asserts.fail("Should have failed, instead gave: "+p);  // probably implicit equals
        }, e -> {
            Asserts.assertStringContainsIgnoreCase(e.toString(), "cannot convert", "given input", " x ", "predicate");
            return true;
        });
    }

    DslPredicates.DslPredicate predicate(String key, Object value) {
        return TypeCoercions.coerce(MutableMap.of(
               key, value), DslPredicates.DslPredicate.class);
    }

    @Test
    public void testGlob() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "glob", "x*z"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("xz"));
        Asserts.assertTrue(p.test("xaz"));
        Asserts.assertFalse(p.test("yxaz"));
    }

    @Test
    public void testRegex() {
        DslPredicates.DslPredicate p = predicate("regex", "x.*z");
        Asserts.assertTrue(p.test("xz"));
        Asserts.assertTrue(p.test("xaz"));
        Asserts.assertFalse(p.test("yxaz"));

        Asserts.assertFalse(predicate("regex", "y").test("xyz"));
        Asserts.assertFalse(predicate("regex", "y").test("y\nx"));
        Asserts.assertTrue(predicate("regex", "y\\s+x").test("y\nx"));
        Asserts.assertTrue(predicate("regex", ".*y.*").test("y\nx"));
    }

    @Test
    public void testLessThan() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "less-than", "5"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("4"));
        Asserts.assertTrue(p.test("-0.2"));
        Asserts.assertFalse(p.test("10"));
        Asserts.assertFalse(p.test("5"));
    }

    @Test
    public void testGreaterThan() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "greater-than", "5"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test("4"));
        Asserts.assertFalse(p.test("-0.2"));
        Asserts.assertTrue(p.test("10"));
        Asserts.assertFalse(p.test("5"));
    }

    @Test
    public void testGreaterThanOtherTypes() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "greater-than", "25.45"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(26.01d));
        Asserts.assertTrue(p.test(101.01d));
        Asserts.assertTrue(p.test(101));
        Asserts.assertFalse(p.test(24.9d));
        Asserts.assertFalse(p.test(3.3d));
        Asserts.assertTrue(p.test(25.55d));
        Asserts.assertFalse(p.test(25.35d));
        Asserts.assertFalse(p.test(1d));
        Asserts.assertFalse(p.test(1));
        Asserts.assertFalse(p.test("1"));
        Asserts.assertFalse(p.test(-1d));
        Asserts.assertFalse(p.test(-1));
        Asserts.assertFalse(p.test("-1"));
        Asserts.assertFalse(p.test("."));
        Asserts.assertTrue(p.test("A"));

        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than", 25.45d), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(26.01d));
        Asserts.assertTrue(p.test(101.01d));
        Asserts.assertTrue(p.test(101));
        Asserts.assertFalse(p.test(24.9d));
        Asserts.assertFalse(p.test(3.3d));
        Asserts.assertTrue(p.test(25.55d));
        Asserts.assertFalse(p.test(25.35d));
        Asserts.assertFalse(p.test(-25.55d));
        Asserts.assertFalse(p.test(1d));
        Asserts.assertFalse(p.test(1));
        Asserts.assertFalse(p.test("1"));
        Asserts.assertFalse(p.test(-1));
        Asserts.assertFalse(p.test(-1d));
        Asserts.assertFalse(p.test("-1"));
        Asserts.assertFalse(p.test("."));
        Asserts.assertTrue(p.test("A"));

        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than", -25.45), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(-26.01d));
        Asserts.assertFalse(p.test(-101.01d));
        Asserts.assertFalse(p.test(-101));
        Asserts.assertTrue(p.test(-24.9d));
        Asserts.assertTrue(p.test(-3.3d));
        Asserts.assertFalse(p.test(-25.55d));
        Asserts.assertTrue(p.test(-25.35d));
        Asserts.assertTrue(p.test(25.35d));
        Asserts.assertTrue(p.test(1d));
        Asserts.assertTrue(p.test(1));
        Asserts.assertTrue(p.test("1"));
        Asserts.assertTrue(p.test(-1d));
        Asserts.assertTrue(p.test(-1));
        Asserts.assertTrue(p.test("-1"));
        Asserts.assertTrue(p.test("."));
        Asserts.assertTrue(p.test("A"));

        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than", "-25.45"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(-26.01d));
        Asserts.assertFalse(p.test(-101.01d));
        Asserts.assertFalse(p.test(-101));
        Asserts.assertTrue(p.test(-24.9d));
        Asserts.assertTrue(p.test(-3.3d));
        Asserts.assertFalse(p.test(-25.55d));
        Asserts.assertTrue(p.test(-25.35d));
        Asserts.assertTrue(p.test(25.35d));
        Asserts.assertTrue(p.test(1d));
        Asserts.assertTrue(p.test(1));
        Asserts.assertTrue(p.test("1"));
        Asserts.assertTrue(p.test(-1d));
        Asserts.assertTrue(p.test(-1));
        Asserts.assertTrue(p.test("-1"));
        Asserts.assertTrue(p.test("."));  // . comes first alphabetically
        Asserts.assertTrue(p.test("A"));
    }

    @Test
    public void testLessThanOrEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "less-than-or-equal-to", "5"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("4"));
        Asserts.assertTrue(p.test("-0.2"));
        Asserts.assertFalse(p.test("10"));
        Asserts.assertTrue(p.test("5"));
    }

    @Test
    public void testGreaterThanOrEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "greater-than-or-equal-to", "5"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test("4"));
        Asserts.assertFalse(p.test("-0.2"));
        Asserts.assertTrue(p.test("10"));
        Asserts.assertTrue(p.test("5"));
    }

    @Test
    public void testDateGreaterThanOrEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "greater-than-or-equal-to", "2022-06-01"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Time.parseInstant("2022-06-02")));
        Asserts.assertFalse(p.test(Time.parseInstant("2022-05-31")));

        // ensure it isn't doing string compare if either side is strongly typed
        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than-or-equal-to", "2022.06.01"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Time.parseInstant("2022-06-02")));
        Asserts.assertFalse(p.test(Time.parseInstant("2022-05-31")));

        // whereas if none are strongly typed it does string compare
        Asserts.assertFalse(p.test("2022-06-02"));
        Asserts.assertFalse(p.test("2022-05-31"));

        Asserts.assertFalse(p.test("0"));
        Asserts.assertTrue(p.test("2023"));
        Asserts.assertTrue(p.test("a"));
        Asserts.assertTrue(p.test("10000"));

        // if argument to test against is not coercible to a date, it's always false if value to test is a date
        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than-or-equal-to", "2022-06-not-a-date"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(Time.parseInstant("2022-07-02")));
        Asserts.assertFalse(p.test(Time.parseInstant("2022-05-31")));
        // whereas a string it just does string compare
        Asserts.assertTrue(p.test("2022-07-02"));
        Asserts.assertFalse(p.test("2022-05-31"));
        Asserts.assertTrue(p.test("2022.07.02"));
        Asserts.assertTrue(p.test("2022.05.31"));
    }

    @Test
    public void testLessThanPrefersNonStringComparison() {
        DslPredicates.DslPredicate p;
        p = TypeCoercions.coerce(MutableMap.of(
                "less-than", "2m"), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(Duration.of("5s")));
        Asserts.assertFalse(p.test(Duration.of("5m")));
        Asserts.assertTrue(p.test(Duration.of("1m")));
        Asserts.assertFalse(p.test(Duration.of("1h")));

        p = TypeCoercions.coerce(MutableMap.of(
                "less-than", Duration.of("2m")), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(Duration.of("5s")));
        Asserts.assertFalse(p.test(Duration.of("5m")));
        Asserts.assertTrue(p.test(Duration.of("1m")));
        Asserts.assertFalse(p.test(Duration.of("1h")));

        p = TypeCoercions.coerce(MutableMap.of(
                "less-than", Duration.of("2m")), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(("5s")));
        Asserts.assertFalse(p.test(("5m")));
        Asserts.assertTrue(p.test(("1m")));
        Asserts.assertFalse(p.test(("1h")));
    }

    @Test
    public void testInRange() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "in-range", MutableList.of(2, 5)), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("4"));
        Asserts.assertFalse(p.test("-0.2"));
        Asserts.assertFalse(p.test("10"));
        Asserts.assertTrue(p.test("2.00"));
        Asserts.assertTrue(p.test("5"));
    }

    @Test
    public void testAny() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "any", MutableList.of("x", MutableMap.of("equals", "y"))), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertTrue(p.test("y"));
        Asserts.assertFalse(p.test("z"));
    }

    @Test
    public void testInstanceOf() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "java-instance-of", String.class), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("some-str"));
        Asserts.assertFalse(p.test(18));
    }

    static class TestException extends Throwable implements Supplier<Object> {
        public TestException(String msg, String x, Throwable cause) {
            super(msg, cause);
            this.x = x;
        }
        String x;

        @Override
        public String toString() {
            return super.toString()+"; x="+x;
        }

        @Override
        public Object get() {
            return null;
        }
    }

    @Test
    public void testErrorInstanceOf() {
        // checks can apply to any superclasses and interfaces

        BiConsumer<Object,Object> checkInstanceOf = (instanceOfTest,target) -> {
            DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                    "java-instance-of", instanceOfTest), DslPredicates.DslPredicate.class);
            Asserts.assertTrue(p.test(target));
        };

        TestException exc = new TestException("hello mezzage", "hello world", null);

        checkInstanceOf.accept(Throwable.class, exc);
        checkInstanceOf.accept(MutableMap.of("equals", "TestException"), exc);
        checkInstanceOf.accept("Supplier", exc);
        checkInstanceOf.accept(Object.class.getName(), exc);
        checkInstanceOf.accept(MutableMap.of("glob", "*.brooklyn.*$TestException"), exc);

        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "java-instance-of", "UnknownType"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(exc));
    }

    @Test
    public void testErrorToString() {
        // toString
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "glob", "*world*"), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(new TestException("hello mezzage", "hello world", null)));
        Asserts.assertTrue(p.test(new TestException("hello world", "hello mezzge", null)));
        Asserts.assertFalse(p.test(new TestException("hello mezzage", "hello planet", null)));
    }

    @Test
    public void testErrorField() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "error-field", "x", "glob", "*world*"), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(new TestException("hello mezzage", "hello world", null)));
        Asserts.assertFalse(p.test(new TestException("hello mezzage", "hello planet", null)));
        Asserts.assertFalse(p.test(new TestException("hello mezzage", null, null)));

        p = TypeCoercions.coerce(MutableMap.of(
                "error-field", "message", "glob", "*world*"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(new TestException("hello world", null, null)));
        Asserts.assertFalse(p.test(new TestException("hello planet", null, null)));

        Asserts.assertFalse(p.test(MutableMap.of("message", "hello message")));
    }

    @Test
    public void testErrorCause() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "error-cause", MutableMap.of("java-instance-of", "TestException")), DslPredicates.DslPredicate.class);

        Asserts.assertTrue(p.test(new TestException("hello mezzage", "hello world", null)));
        Asserts.assertTrue(p.test(new IllegalStateException("hello root", new TestException("hello mezzage", "hello planet", null))));
        Asserts.assertFalse(p.test(new IllegalStateException("hello mezzage")));

        p = TypeCoercions.coerce(MutableMap.of(
                "glob", "*TestException*"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(new IllegalStateException("hello root", new TestException("hello mezzage", "hello planet", null))));
        p = TypeCoercions.coerce(MutableMap.of(
                "error-cause", MutableMap.of("glob", "*TestException*")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(new IllegalStateException("hello root", new TestException("hello mezzage", "hello planet", null))));
        p = TypeCoercions.coerce(MutableMap.of(
                "error-cause", MutableMap.of("glob", "*root*")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(new IllegalStateException("hello root", new TestException("hello mezzage", "hello planet", null))));
        p = TypeCoercions.coerce(MutableMap.of(
                "error-cause", MutableMap.of("glob", "*mezzage*")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(new IllegalStateException("hello root", new TestException("hello mezzage", "hello planet", null))));
        Asserts.assertTrue(p.test(new IllegalStateException("hello mezzage", new TestException("hello sub", "hello planet", null))));
        Asserts.assertFalse(p.test(new IllegalStateException("hello root", new TestException("hello sub", "hello planet", null))));
    }

    @Test
    public void testAll() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "all", MutableList.of(MutableMap.of("regex", ".*x"), MutableMap.of("less-than", "z"))), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("xx"));
        Asserts.assertTrue(p.test("yx"));
        Asserts.assertFalse(p.test("zx"));
        Asserts.assertFalse(p.test("xa"));
    }

    private static class SetAllowingEqualsToList<T> extends MutableSet<T> {
        public static <T> SetAllowingEqualsToList<T> of(Iterable<T> items) {
            SetAllowingEqualsToList<T> result = new SetAllowingEqualsToList<>();
            result.putAll(items);
            return result;
        }

        @Override
        public boolean equals(Object o) {
            if (o instanceof Iterable && !(o instanceof Set)) o = MutableSet.copyOf((Iterable)o);
            return super.equals(o);
        }
    }

    @Test
    public void testListsAndElement() {
        Asserts.assertTrue(SetAllowingEqualsToList.of(MutableSet.of("y", "x")).equals(MutableList.of("x", "y")));

        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("equals", MutableList.of("x", "y")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("x", "y")));
        // list not equal because of order and equal
        Asserts.assertFalse(p.test(Arrays.asList("y", "x")));
        Asserts.assertFalse(p.test(Arrays.asList("x", "y", "z")));
        // set equality _does_ match without order
        Asserts.assertTrue(p.test(SetAllowingEqualsToList.of(MutableSet.of("y", "x"))));

        // no longer automatically unflatted
        p = TypeCoercions.coerce(MutableMap.of("equals", "x"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(Arrays.asList("y", "x")));
        Asserts.assertFalse(p.test(Arrays.asList("x")));
        Asserts.assertTrue(p.test("x"));
        // and implicit equals gives error
        Asserts.assertFailsWith(() -> {
                    DslPredicates.DslPredicate p2 = TypeCoercions.coerce("x", DslPredicates.DslPredicate.class);
                    Asserts.assertFalse(p2.test(Arrays.asList("x")));
                }, e -> Asserts.expectedFailureContainsIgnoreCase(e, "explicit", "equals"));

        // "has-element" now supported
        p = TypeCoercions.coerce(MutableMap.of("has-element", "x"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("y", "x")));
        Asserts.assertFalse(p.test(Arrays.asList("y", "z")));

        // and can be nested to require multiple elements
        p = TypeCoercions.coerce(MutableMap.of("all", MutableList.of(MutableMap.of("has-element", "x"), MutableMap.of("has-element","y"))), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("x", "y", "z")));
        Asserts.assertFalse(p.test(Arrays.asList("x", "z")));

        // and can take another predicate
        p = TypeCoercions.coerce(MutableMap.of("has-element", MutableMap.of("glob", "?")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("xx", "y")));
        Asserts.assertFalse(p.test(Arrays.asList("xx", "yy")));
    }

    @Test
    public void testKeyAndAtIndex() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("key", "name", "regex", "[Bb].*"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableMap.of("id", 123, "name", "Bob")));
        Asserts.assertFalse(p.test(MutableMap.of("id", 124, "name", "Astrid")));

        p = TypeCoercions.coerce(MutableMap.of("index", -1, "regex", "[Bb].*"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableList.of("Astrid", "Bob")));
        Asserts.assertFalse(p.test(MutableList.of("Astrid", "Bob", "Carver")));

        // nested check
        p = TypeCoercions.coerce(MutableMap.of("index", 1,
                "check", MutableMap.of("key", "name", "regex", "[Bb].*")), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(MutableList.of(MutableMap.of("name", "Bob"))));
        Asserts.assertTrue(p.test(MutableList.of("Astrid", MutableMap.of("name", "Bob"))));
        Asserts.assertFalse(p.test(MutableList.of("Astrid", MutableMap.of("name", "Carver"))));
    }

    @Test
    public void testNotAndNotEmpty() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("not", "foo"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("bar"));
        Asserts.assertTrue(p.test(null));
        Asserts.assertFalse(p.test("foo"));

        p = TypeCoercions.coerce(MutableMap.of("not", MutableMap.of("size", "0")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableMap.of("id", 123, "name", "Bob")));
        Asserts.assertFalse(p.test(MutableMap.of()));
        Asserts.assertTrue(p.test(MutableList.of("Astrid", "Bob")));
        Asserts.assertFalse(p.test(MutableList.of()));
    }

    @Test
    public void testJsonpath() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("jsonpath", "name", "regex", "[Bb].*"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableMap.of("id", 123, "name", "Bob")));
        Asserts.assertFalse(p.test(MutableMap.of("id", 124, "name", "Astrid")));
        Asserts.assertFalse(p.test(MutableMap.of("id", 0)));
        Asserts.assertFalse(p.test(MutableList.of("id", 0)));
        Asserts.assertFalse(p.test("not json"));

        p = TypeCoercions.coerce(MutableMap.of("jsonpath", "$.[*].name", "has-element", MutableMap.of("regex", "[Bb].*")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableList.of(MutableMap.of("id", 123, "name", "Bob"), MutableMap.of("id", 124, "name", "Astrid"))));
        Asserts.assertFalse(p.test(MutableList.of(MutableMap.of("id", 125), MutableMap.of("id", 124, "name", "Astrid"))));
        Asserts.assertFalse(p.test(MutableList.of(MutableMap.of("id", 125))));

        p = TypeCoercions.coerce(MutableMap.of("jsonpath", "[*].name",
                "check",
                    MutableMap.of("filter", MutableMap.of("regex", "[Bb].*"),
                        "size", 1))
                , DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(MutableList.of(MutableMap.of("id", 123, "name", "Bob"), MutableMap.of("id", 124, "name", "Astrid"))));
        Asserts.assertFalse(p.test(MutableList.of(MutableMap.of("id", 125), MutableMap.of("id", 124, "name", "Astrid"))));
        Asserts.assertFalse(p.test(MutableList.of(MutableMap.of("id", 125))));
    }

    @Test
    public void testLocationTagImplicitEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "target", "location",
                "tag", "locationTagValueMatched"), DslPredicates.DslPredicate.class);
        Asserts.assertInstanceOf(p, DslPredicates.DslPredicateDefault.class);
        Asserts.assertInstanceOf( ((DslPredicates.DslPredicateDefault)p).tag, DslPredicates.DslPredicateDefault.class);
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault) ((DslPredicates.DslPredicateDefault)p).tag).implicitEqualsUnwrapped(), "locationTagValueMatched");
    }

    @Test
    public void testEqualsNestedObject() {
        MutableMap<String, String> kvMap = MutableMap.of("k1", "v1", "k2", "v2");
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("target", "location", "equals", kvMap, "config", "x"),
                DslPredicates.DslPredicate.class);
        Asserts.assertInstanceOf(p, DslPredicates.DslPredicateDefault.class);
        Asserts.assertEquals( WrappedValue.get( ((DslPredicates.DslPredicateDefault)p).equals ), kvMap);
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault)p).config, "x");
    }

    @Test
    public void testAssertPresent() {
        // implicit assertion
        Consumer<DslPredicates.DslPredicate> check = p -> {
            Asserts.assertTrue(p.test(MutableList.of("x", "a")));
            Asserts.assertFalse(p.test(MutableList.of("x", "b")));
            Asserts.assertFailsWith(() -> { p.test(MutableList.of("x")); }, e -> Asserts.expectedFailureContainsIgnoreCase(e, "assert", "no element", "index 1", "MutableList", "size 1"));
            Asserts.assertFailsWith(() -> { p.test(null); }, e -> Asserts.expectedFailureContainsIgnoreCase(e, "assert", "non-list", "null"));
        };

        DslPredicates.DslPredicate p1 = TypeCoercions.coerce(MutableMap.of("index", 1, "equals", "a", "assert", "present"), DslPredicates.DslPredicate.class);
        check.accept(p1);

        // explicit more complex assertion
        DslPredicates.DslPredicate p2 = TypeCoercions.coerce(MutableMap.of("index", 1, "equals", "a", "assert", MutableMap.of("when", "present", "size", 1)), DslPredicates.DslPredicate.class);
        check.accept(p2);
        Asserts.assertFailsWith(() -> { p2.test(MutableList.of("x", "zz")); }, e -> Asserts.expectedFailureContainsIgnoreCase(e, "assert", "value", "zz"));
    }

}
