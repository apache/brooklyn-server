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

import java.util.List;
import java.util.stream.Collectors;

import org.apache.brooklyn.core.workflow.utils.ExpressionParser;
import org.apache.brooklyn.core.workflow.utils.ExpressionParserImpl.ParseNodeOrValue;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.annotations.Test;

public class ExpressionParserTest {

    static String s(List<ParseNodeOrValue> pr) {
        return pr.stream().map(ParseNodeOrValue::toString).collect(Collectors.joining(""));
    }
    static String parseFull(ExpressionParser ep, String expression) {
        return getParseTreeString(ep.parseEverything(expression).get());
    }
    static String parseFull(String expression) {
        return parseFull(ExpressionParser.newDefaultAllowingUnquotedLiteralValues(), expression);
    }
    static String parseFullWhitespace(String expression) {
        return parseFull(ExpressionParser.newDefaultAllowingUnquotedAndSplittingOnWhitespace(), expression);
    }
    static String parsePartial(ExpressionParser ep, String expression) {
        return getParseTreeString(ep.parse(expression).get().getContents());
    }

    static String getParseTreeString(List<ParseNodeOrValue> result) {
        return result.stream().map(ParseNodeOrValue::toString).collect(Collectors.joining(""));
    }

    @Test
    public void testCommon() {
        Asserts.assertEquals(parseFull("hello"), "[hello]");
        Asserts.assertEquals(parseFull("\"hello\""), "double_quote[hello]");
        Asserts.assertEquals(parseFull("${a}"), "interpolated_expression[a]");

        Asserts.assertEquals(parseFull("\"hello\"world"), "double_quote[hello][world]");
        Asserts.assertEquals(parseFull("\"hello\" with 'sq \"'"), "double_quote[hello][ with ]single_quote[sq \"]");
        Asserts.assertEquals(parseFullWhitespace("\"hello\" with 'sq \"'"), "double_quote[hello]whitespace[ ][with]whitespace[ ]single_quote[sq \"]");

        Asserts.assertEquals(parseFull("\"hello ${a}\""), "double_quote[[hello ]interpolated_expression[a]]");
        Asserts.assertEquals(parseFull("x[\"v\"] =1"), "[x[]double_quote[v][] =1]");
    }

    @Test
    public void testError() {
        Asserts.expectedFailureContains(Maybe.Absent.getException(ExpressionParser.newDefaultAllowingUnquotedLiteralValues().parseEverything("\"non-quoted string")),
                "Non-terminated double_quote");
    }

    @Test
    public void testPartial() {
        ExpressionParser ep = ExpressionParser.newDefaultAllowingUnquotedLiteralValues().
                stoppingAt("equals", s -> s.startsWith("="), true);
        Asserts.assertEquals(parsePartial(ep, "x=1"), "[x]equals[]");
        Asserts.assertEquals(parsePartial(ep, "x[\"v\"] =1"), "[x[]double_quote[v][] ]equals[]");
        // equals not matched in expression
        Asserts.assertFailsWith(() -> parsePartial(ep, "x[\"=\"] is 1"),
                Asserts.expectedFailureContains("value", "should", "end", "with", "required", "equals"));
    }

    @Test
    public void testBrackets() {
        ExpressionParser ep1 = ExpressionParser.newDefaultAllowingUnquotedLiteralValues().
                includeGroupingBracketsAtUsualPlaces();
        Asserts.assertEquals(parseFull(ep1, "x[\"v\"] =1"), "[x]square_bracket[double_quote[v]][ =1]");

        ExpressionParser ep2 = ExpressionParser.newDefaultAllowingUnquotedLiteralValues().
                includeGroupingBracketsAtUsualPlaces().
                stoppingAt("equals", s -> s.startsWith("="), true);
        Asserts.assertEquals(parsePartial(ep2, "x[\"v\"] =1"), "[x]square_bracket[double_quote[v]][ ]equals[]");
        Asserts.assertFailsWith(() -> parsePartial(ep2, "x[\"=\"] is 1"),
                Asserts.expectedFailureContains("value", "should", "end", "with", "required", "equals"));
    }

}
