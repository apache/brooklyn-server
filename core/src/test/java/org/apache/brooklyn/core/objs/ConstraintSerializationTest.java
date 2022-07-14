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

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.gson.Gson;
import org.apache.brooklyn.core.config.ConfigConstraints;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourcePredicates;
import org.apache.brooklyn.util.text.StringPredicates;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class ConstraintSerializationTest extends BrooklynMgmtUnitTestSupport {

    @Test
    public void testSimple() {
        assertPredJsonBidi(ConfigConstraints.required(), MutableList.of("required"));
        assertPredJsonBidi(Predicates.alwaysFalse(), MutableList.of("Predicates.alwaysFalse"));
        assertPredJsonBidi(Predicates.alwaysTrue(), MutableList.of()); // simplified to no-op
        assertPredJsonBidi(ResourcePredicates.urlExists(), MutableList.of("urlExists"));
        assertPredJsonBidi(StringPredicates.isBlank(), MutableList.of("isBlank"));
        assertPredJsonBidi(StringPredicates.matchesRegex("a.*b"), MutableList.of(MutableMap.of("regex", "a.*b")));
        assertPredJsonBidi(StringPredicates.matchesGlob("a?b"), MutableList.of(MutableMap.of("glob", "a?b")));
        assertPredJsonBidi(ConfigConstraints.forbiddenIf("myother"), MutableList.of(MutableMap.of("forbiddenIf", "myother")));
        assertPredJsonBidi(ConfigConstraints.forbiddenUnless("myother"), MutableList.of(MutableMap.of("forbiddenUnless", "myother")));
        assertPredJsonBidi(ConfigConstraints.requiredIf("myother"), MutableList.of(MutableMap.of("requiredIf", "myother")));
        assertPredJsonBidi(ConfigConstraints.requiredUnless("myother"), MutableList.of(MutableMap.of("requiredUnless", "myother")));
        assertPredJsonBidi(ConfigConstraints.requiredUnlessAnyOf(ImmutableList.of("myother1", "myother2")), MutableList.of(MutableMap.of("requiredUnlessAnyOf", ImmutableList.of("myother1", ("myother2")))));
        assertPredJsonBidi(ConfigConstraints.forbiddenUnlessAnyOf(ImmutableList.of("myother1", "myother2")), MutableList.of(MutableMap.of("forbiddenUnlessAnyOf", ImmutableList.of("myother1", ("myother2")))));
    }
    
    @Test
    public void testSimpleEquivalents() {
        Gson gson = new Gson();
        
        assertPredicateFromJson(ConfigConstraints.required(), 
                "required", "required()", "isNonBlank");
        
        assertPredicateFromJson(StringPredicates.matchesRegex("a"), 
                "regex(\"a\")", "matchesRegex(\"a\")");
        
        assertPredicateFromJson(StringPredicates.matchesGlob("a"), 
                "glob(\"a\")", "matchesGlob(\"a\")");
        
        assertPredicateFromJson(Predicates.and(StringPredicates.matchesRegex("a"), StringPredicates.matchesRegex("b")), 
                gson.fromJson("[{and: [{regex: a},{regex: b}]}]", List.class),
                gson.fromJson("[{all: [{regex: a},{regex: b}]}]", List.class));
        
        assertPredicateFromJson(Predicates.or(StringPredicates.matchesRegex("a"), StringPredicates.matchesRegex("b")), 
                gson.fromJson("[{or: [{regex: a},{regex: b}]}]", List.class),
                gson.fromJson("[{any: [{regex: a},{regex: b}]}]", List.class));
    }

    @Test
    public void testInteresting() {
        assertPredJsonBidi(Predicates.and(ConfigConstraints.required(), StringPredicates.matchesRegex(".*")),
                MutableList.of("required", MutableMap.of("regex", ".*")));
    }

    @Test
    public void testAnd() {
        Predicate<String> p = Predicates.<String>and(
                StringPredicates.matchesRegex("my.*first"), 
                StringPredicates.matchesRegex("my.*second"));
        List<?> json = MutableList.of(
                MutableMap.of("regex", "my.*first"), 
                MutableMap.of("regex", "my.*second"));
        
        assertPredJsonBidi(p, json);
    }

    @Test
    public void testOr() {
        Predicate<String> p = Predicates.<String>or(
                StringPredicates.matchesRegex("my.*first"), 
                StringPredicates.matchesRegex("my.*second"));
        List<?> json = MutableList.of(
                MutableMap.of("any", MutableList.of(
                        MutableMap.of("regex", "my.*first"), 
                        MutableMap.of("regex", "my.*second"))));
        
        assertPredJsonBidi(p, json);
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testNestedAndIsSimplified() {
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
    public void testAcceptsArray() {
        Predicate<String> p1 = StringPredicates.matchesGlob("???*");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson("matchesGlob([???*])"), p1);

        Predicate<String> p2 = StringPredicates.matchesGlob("???*, b, 1");
        assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson("matchesGlob([???*, b, 1])"), p2);
    }

    @Test
    public void testAltPred() {
        Predicate<?> p = StringPredicates.isNonBlank();
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

    private void assertPredicateFromJson(Predicate<?> expected, Object... inputs) {
        for (Object input : inputs) {
            assertSamePredicate(ConstraintSerialization.INSTANCE.toPredicateFromJson(input), expected);
        }
    }
    
    private static void assertSamePredicate(Predicate<?> p1, Predicate<?> p2) {
        // some predicates don't support equals, but all (the ones we use) must support toString
        Assert.assertEquals(p1.toString(), p2.toString());
        Assert.assertEquals(p1.getClass(), p2.getClass());
    }
}
