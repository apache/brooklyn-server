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

    void assertShorthandFinalMatchRawOfGives(String template, String input, Map<String,Object> expected) {
        Asserts.assertEquals(new ShorthandProcessor(template).withFinalMatchRaw(true).process(input).get(), expected);
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

        // quotes with spaces before/after removed
        assertShorthandOfGives("${x} \" is \" ${y}", "\"this is b\" is c is \"is quoted\"", MutableMap.of("x", "this is b", "y", "c is is quoted"));
        // if you want quotes, you have to wrap them in quotes
        assertShorthandOfGives("${x} \" is \" ${y}", "\"this is b\" is \"\\\"c is is quoted\\\"\"", MutableMap.of("x", "this is b", "y", "\"c is is quoted\""));
        // and only quotes at word end are considered, per below
        assertShorthandOfGives("${x} \" is \" ${y}", "\"this is b\" is \"\\\"c is \"is  quoted\"\\\"\"", MutableMap.of("x", "this is b", "y", "\"c is \"is  quoted\"\""));
        assertShorthandOfGives("${x} \" is \" ${y}", "\"this is b\" is \"\\\"c is \"is  quoted\"\\\"\"  too", MutableMap.of("x", "this is b", "y", "\"c is \"is  quoted\"\" too"));

        // preserve spaces in a word
        assertShorthandOfGives("${x}", "\"  sp a  ces \"", MutableMap.of("x", "  sp a  ces "));
        assertShorthandOfGives("${x}", "\"  sp a  ces \"  and  then  some", MutableMap.of("x", "  sp a  ces  and then some"));

        // if you want quotes, you have to wrap them in quotes
        assertShorthandOfGives("${x}", "\"\\\"c is is quoted\\\"\"", MutableMap.of("x", "\"c is is quoted\""));
        // or use final match quoted
        assertShorthandFinalMatchRawOfGives("${x}", "\"\\\"c is is quoted\\\"\"", MutableMap.of("x", "\"\\\"c is is quoted\\\"\""));
        // a close quote must come at a word end to be considered
        // so this gives an error
        assertShorthandFailsWith("${x}", "\"c is  \"is", e -> Asserts.expectedFailureContainsIgnoreCase(e, "mismatched", "quot"));
        // and this is treated as one quoted string
        assertShorthandOfGives("${x}", "\"\\\"c  is \"is  quoted\"\\\"\"", MutableMap.of("x", "\"c  is \"is  quoted\"\""));
    }

    @Test
    public void testShorthandWithOptionalPart() {
        assertShorthandOfGives("[?${hello} \"hello\" ] ${x}", "hello world", MutableMap.of("hello", true, "x", "world"));

        assertShorthandOfGives("[ \"hello\" ] ${x}", "hello world", MutableMap.of("x", "world"));
        assertShorthandOfGives("[\"hello\"] ${x}", "hello world", MutableMap.of("x", "world"));
        assertShorthandOfGives("[\"hello\"] ${x}", "hi world", MutableMap.of("x", "hi world"));
        assertShorthandOfGives("[?${hello} \"hello\" ] ${x}", "hello world", MutableMap.of("hello", true, "x", "world"));
        assertShorthandOfGives("[?${hello} \"hello\" ] ${x}", "hi world", MutableMap.of("hello", false, "x", "hi world"));

        assertShorthandOfGives("[${type}] ${key}", "x", MutableMap.of("key", "x"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "x = 1", MutableMap.of("key", "x", "value", "1"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "x=1", MutableMap.of("key", "x", "value", "1"));
        assertShorthandOfGives("[${type}] ${key} \"=\" ${value}", "integer x=1", MutableMap.of("type", "integer", "key", "x", "value", "1"));

        // this matches -- more than one whitespace is not important
        assertShorthandOfGives("[${type}] ${key} \"  =\" ${value}", "x =1", MutableMap.of("key", "x", "value", "1"));
        // but this does not match
        assertShorthandFailsWith("[${type}] ${key} \" =\" ${value}", "x=1", e -> Asserts.expectedFailureContainsIgnoreCase(e, " =", "end of input"));

        assertShorthandOfGives("${x} [ ${y} ]", "hi world", MutableMap.of("x", "hi", "y", "world"));
        assertShorthandOfGives("${x} [ ${y} ]", "hi world 1 and 2", MutableMap.of("x", "hi", "y", "world 1 and 2"));
    }

}
