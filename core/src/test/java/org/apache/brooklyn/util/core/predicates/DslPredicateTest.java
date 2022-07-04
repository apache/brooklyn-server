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

import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.time.Timestamp;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;

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
    public void testImplicitEquals() {
        Predicate p = TypeCoercions.coerce("x", DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("x"));
        Asserts.assertFalse(p.test("y"));
    }

    @Test
    public void testImplicitEqualsAsPredicateNotSupported() {
        Asserts.assertFailsWith(() -> {
                Predicate p = TypeCoercions.coerce("x", Predicate.class);
//                Asserts.assertTrue(p.test("x"));
//                Asserts.assertFalse(p.test("y"));
        }, e -> {
                Asserts.assertStringContainsIgnoreCase(e.toString(), "cannot coerce", "string", "predicate");
                return true;
        });
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
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "regex", "x.*z"), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("xz"));
        Asserts.assertTrue(p.test("xaz"));
        Asserts.assertFalse(p.test("yxaz"));
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
        Asserts.assertFalse(p.test(Time.parseInstant("2022-06-02")));
        Asserts.assertTrue(p.test(Time.parseInstant("2022-05-31")));

        // ensure it isn't doing string compare if either side is strongly typed
        p = TypeCoercions.coerce(MutableMap.of(
                "greater-than-or-equal-to", "2022.06.01"), DslPredicates.DslPredicate.class);
        Asserts.assertFalse(p.test(Time.parseInstant("2022-06-02")));
        Asserts.assertTrue(p.test(Time.parseInstant("2022-05-31")));

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
    public void testTypeName() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "java-type-name", String.class), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("some-str"));
        Asserts.assertFalse(p.test(18));
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
    public void testAllWithListWithVariousFlattening() {
        Asserts.assertTrue(SetAllowingEqualsToList.of(MutableSet.of("y", "x")).equals(MutableList.of("x", "y")));

        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("equals", MutableList.of("x", "y")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("x", "y")));
        // list not equal because of order and equal
        Asserts.assertFalse(p.test(Arrays.asList("y", "x")));
        Asserts.assertFalse(p.test(Arrays.asList("x", "y", "z")));
        // set equality _does_ match without order
        Asserts.assertTrue(p.test(SetAllowingEqualsToList.of(MutableSet.of("y", "x"))));

        // "all" works because it attempts unflattened at all, then flattens on each test
        p = TypeCoercions.coerce(MutableMap.of("all", MutableList.of("x", "y")), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("y", "x")));
        Asserts.assertTrue(p.test(Arrays.asList("x", "y", "z")));
        // set equality _does_ match!
        Asserts.assertTrue(p.test(SetAllowingEqualsToList.of(MutableSet.of("y", "x"))));

        // specify unflattening also works, but is unnecessary
        p = TypeCoercions.coerce(MutableMap.of("unflattened", MutableMap.of("all", MutableList.of("x", "y"))), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test(Arrays.asList("x", "y", "z")));
    }

    @Test
    public void testLocationTagImplicitEquals() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "target", "location",
                "tag", "locationTagValueMatched"), DslPredicates.DslPredicate.class);
        Asserts.assertInstanceOf(p, DslPredicates.DslPredicateDefault.class);
        Asserts.assertInstanceOf( ((DslPredicates.DslPredicateDefault)p).tag, DslPredicates.DslPredicateDefault.class);
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault) ((DslPredicates.DslPredicateDefault)p).tag).implicitEquals, "locationTagValueMatched");
    }

    @Test
    public void testEqualsNestedObject() {
        MutableMap<String, String> kvMap = MutableMap.of("k1", "v1", "k2", "v2");
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of("target", "location", "equals", kvMap, "config", "x"),
                DslPredicates.DslPredicate.class);
        Asserts.assertInstanceOf(p, DslPredicates.DslPredicateDefault.class);
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault)p).equals, kvMap);
        Asserts.assertEquals( ((DslPredicates.DslPredicateDefault)p).config, "x");
    }

}
