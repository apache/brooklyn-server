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
package org.apache.brooklyn.core.workflow;

import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Predicate;

public class ShorthandProcessorTest extends BrooklynMgmtUnitTestSupport {

    void assertShorthandOfGives(String template, String input, Map<String,Object> expected) {
        Asserts.assertEquals(new ShorthandProcessor(template).process(input).get(), expected);
    }

    void assertShorthandFailsWith(String template, String input, Consumer<Exception> check) {
        try {
            new ShorthandProcessor(template).process(input).get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            check.accept(e);
        }
    }

    @Test
    public void testShorthandQuoted() {
        assertShorthandOfGives("${x}", "hello world", MutableMap.of("x", "hello world"));

        assertShorthandOfGives("${x} \" is \" ${y}", "a is b c", MutableMap.of("x", "a", "y", "b c"));
        assertShorthandOfGives("${x} \" is \" ${y}", "a is b \"c\"", MutableMap.of("x", "a", "y", "b c"));

        // don't allow intermediate multi-token matching; we could add but not needed yet
//        assertShorthandOfGives("${x} \" is \" ${y}", "a b is b c", MutableMap.of("x", "a b", "y", "b c"));

        assertShorthandOfGives("${x} \" is \" ${y}", "this is b c", MutableMap.of("x", "this", "y", "b c"));
        assertShorthandOfGives("${x} \"is\" ${y}", "this is b c", MutableMap.of("x", "th", "y", "is b c"));
        assertShorthandOfGives("${x} \"is\" ${y}", "\"this\" is b c", MutableMap.of("x", "this", "y", "b c"));
        assertShorthandOfGives("${x} \" is \" ${y}", "\"this is b\" is c", MutableMap.of("x", "this is b", "y", "c"));
    }

    @Test
    public void testShorthandOptional() {
        assertShorthandOfGives("[\"hello\"] ${x}", "hello world", MutableMap.of("x", "world"));
        assertShorthandOfGives("[\"hello\"] ${x}", "hi world", MutableMap.of("x", "hi world"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "x = 1", MutableMap.of("key", "x", "value", "1"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "x=1", MutableMap.of("key", "x", "value", "1"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "integer x=1", MutableMap.of("type", "integer", "key", "x", "value", "1"));

        // this matches -- more than one whitespace is not important
        assertShorthandOfGives("[${type}] ${key} \"  =\" ${value}", "x =1", MutableMap.of("key", "x", "value", "1"));
        // but this does not match
        assertShorthandFailsWith("[${type}] ${key} \" =\" ${value}", "x=1", e -> Asserts.expectedFailureContainsIgnoreCase(e, " =", "end of input"));
    }

}
