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
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.testng.annotations.Test;

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
    public void testAll() {
        DslPredicates.DslPredicate p = TypeCoercions.coerce(MutableMap.of(
                "all", MutableList.of(MutableMap.of("regex", ".*x"), MutableMap.of("less-than", "z"))), DslPredicates.DslPredicate.class);
        Asserts.assertTrue(p.test("xx"));
        Asserts.assertTrue(p.test("yx"));
        Asserts.assertFalse(p.test("zx"));
        Asserts.assertFalse(p.test("xa"));
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
