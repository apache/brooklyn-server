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
package org.apache.brooklyn.core.objs;

import java.util.List;

import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;

public class ConstraintSerializationTest extends BrooklynMgmtUnitTestSupport {

    @Test
    public void testSimple() {
        assertPredJsonBidi(ConfigConstraints.required(), MutableList.of("required"));
    }

    @Test
    public void testInteresting() {
        assertPredJsonBidi(Predicates.and(ConfigConstraints.required(), StringPredicates.matchesRegex(".*")),
            MutableList.of("required", MutableMap.of("regex", ".*")));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNestedAnd() {
        Predicate<String> p = Predicates.<String>and(
            ConfigConstraints.required(), 
            Predicates.and(Predicates.alwaysTrue()),
            Predicates.<String>and(StringPredicates.matchesRegex(".*")));
        Assert.assertEquals(ConstraintSerialization.INSTANCE.toJsonList(p), 
            MutableList.of("required", MutableMap.of("regex", ".*")));
    }

    @Test
    public void testAltName() {
        Predicate<String> p = StringPredicates.matchesGlob("???*");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(
            MutableList.of(MutableMap.of("matchesGlob", "???*"))), p);
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(
            MutableList.of(MutableMap.of("glob", "???*"))), p);
        Assert.assertEquals(ConstraintSerialization.INSTANCE.toJsonList(p),
            MutableList.of(MutableMap.of("glob", "???*")));
    }

    @Test
    public void testAcceptsMap() {
        Predicate<String> p = StringPredicates.matchesGlob("???*");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(MutableMap.of("matchesGlob", "???*")), p);
    }

    @Test
    public void testAcceptsForbiddenIfMap() {
        Predicate<Object> p = ConfigConstraints.forbiddenIf("x");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(MutableMap.of("forbiddenIf", "x")), p);
    }

    @Test
    public void testAcceptsString() {
        Predicate<String> p = StringPredicates.matchesGlob("???*");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson("matchesGlob(\"???*\")"), p);
    }
    
    @Test
    public void testAltPred() {
        Predicate<?> p = Predicates.notNull();
        Assert.assertEquals(ConstraintSerialization.INSTANCE.toJsonList(p),
            MutableList.of("required"));
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson("required"),
            ConfigConstraints.required());
    }

    @Test
    public void testFlattens() {
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(MutableList.of("required", "required")),
            ConfigConstraints.required());
    }
    
    @Test
    public void testEmpty() {
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(MutableList.of()),
            Predicates.alwaysTrue());
        Assert.assertEquals(ConstraintSerialization.INSTANCE.toJsonList(Predicates.alwaysTrue()), 
            MutableList.of());
    }

    private void assertPredJsonBidi(Predicate<?> pred, List<?> json) {
        Assert.assertEquals(ConstraintSerialization.INSTANCE.toJsonList(pred), json);
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(json), pred);
    }

    private static void assertSamePredicate(Predicate<?> p1, Predicate<?> p2) {
        // some predicates don't support equals, but all (the ones we use) must support toString
        Assert.assertEquals(p1.toString(), p2.toString());
        Assert.assertEquals(p1.getClass(), p2.getClass());
    }

}
