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
package org.apache.brooklyn.util.text;

import static org.testng.Assert.assertEquals;

import java.io.IOException;
import java.io.Reader;
import java.io.StreamTokenizer;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.text.QuotedStringTokenizer;
import org.testng.Assert;
import org.testng.annotations.Test;

public class QuotedStringTokenizerTest {

    // have to initialise to use the methods (instance as it can take custom tokens)
    private QuotedStringTokenizer defaultTokenizer = new QuotedStringTokenizer("", true);

    @Test
    public void testQuoting() throws Exception {
        assertQuoteUnquoteFor("a=b");
        assertQuoteUnquoteFor("a=\"things\",b=c");
        assertQuoteUnquoteFor("thing=\"\"");
        assertQuoteUnquoteFor("\"thing\"=\"\"");
        assertQuoteUnquoteFor("");
        assertQuoteUnquoteFor("\"");
        assertQuoteUnquoteFor("\"\"");

        assertUnquoteFor("", "''");
        assertUnquoteFor("thing=", "\"thing\"=\"\"");
        assertUnquoteFor("a=", "a=\"\"");
    }

    @Test
    public void testTokenizingStrippingInternalQuotes() throws Exception {
        testResultingTokens("foo,bar,baz", "\"", false, ",", false, "foo", "bar", "baz");
        testResultingTokens("\"foo,bar\",baz", "\"", false, ",", false, "foo,bar", "baz");
        testResultingTokens("\"foo,,bar\",baz", "\"", false, ",", false, "foo,,bar", "baz");
        testResultingTokens("\"foo,',bar\",baz", "\"", false, ",", false, "foo,',bar", "baz");
        testResultingTokens("foo \"\"bar\"\" baz", "\"", false, ",", false, "foo bar baz");
        testResultingTokens("\"foo \"\"bar\"\" baz\"", "\"", false, ",", false, "foo bar baz");

        // NOTE: would like to return empty tokens when we encounter adjacent delimiters, but need
        // to work around brain-dead java.util.StringTokenizer to do this.
        // testResultingTokens("foo,,baz", "\"", false, ",", false, "foo", "", "baz");
    }

    @Test
    public void testTokenizingKeepingInternalQuotes() throws Exception {
        testResultingTokens("foo,bar,baz", "\"", false, ",", false, true, true, "foo", "bar", "baz");
        testResultingTokens("\"foo,bar\",baz", "\"", false, ",", false, true, true, "foo,bar", "baz");
        testResultingTokens("\"foo,,bar\",baz", "\"", false, ",", false, true, true, "foo,,bar", "baz");
        testResultingTokens("\"foo,',bar\",baz", "\"", false, ",", false, true, true, "foo,',bar", "baz");
        testResultingTokens("\"foo,\',bar\",baz", "\"", false, ",", false, true, true, "foo,',bar", "baz");
        testResultingTokens("foo \"\"bar\"\" baz", "\"", false, ",", false, true, true, "foo \"\"bar\"\" baz");
        testResultingTokens("foo \"\"bar\"\" baz", "\"", false, ", ", false, true, true, "foo", "\"bar\"", "baz");
        testResultingTokens("\"foo \"\"bar\"\" baz\"", "\"", false, ",", false, true, true, "foo \"\"bar\"\" baz");
        testResultingTokens("\"foo \"\"bar\"\" baz\"", "\"", false, ", ", false, true, true, "foo \"\"bar\"", "baz\"");
    }

    @Test
    public void testTokenizingUsingInternalQuotesWithJavaStreamTokenizerWhitespaceOrStrings() throws Exception {
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("foo,bar,baz", "foo,bar,baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"foo,bar\",baz", "foo,bar", ",baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"foo,,bar\",baz", "foo,,bar", ",baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"foo,',bar\",baz", "foo,',bar", ",baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"foo,\',bar\",baz", "foo,',bar", ",baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("foo \"\"bar\"\" baz", "foo", "", "bar", "", "baz");
        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"foo \"\"bar\"\" baz\"", "foo ", "bar", " baz");

//        // this is the one irritant
//        testResultingTokensJavaStreamTokenizerWhitespaceOrStrings("\"hi\" and\"hi\"", "hi andhi");
    }

    @Test
    public void testTokenizingUsintestResultingTokensJavaStreamTokenizerIdentifiers() throws Exception {
        testResultingTokensJavaStreamTokenizerIdentifiers("foo,bar,baz", "foo",",","bar",",","baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("\"foo,bar\",baz", "foo,bar", ",", "baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("\"foo,,bar\",baz", "foo,,bar", ",", "baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("\"foo,',bar\",baz", "foo,',bar", ",", "baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("\"foo,\',bar\",baz", "foo,',bar", ",", "baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("foo \"\"bar\"\" baz", "foo", "", "bar", "", "baz");
        testResultingTokensJavaStreamTokenizerIdentifiers("\"foo \"\"bar\"\" baz\"", "foo ", "bar", " baz");
    }

    @Test
    public void testTokenizingBuilder() throws Exception {
        Assert.assertEquals(Arrays.asList("foo", "bar"), QuotedStringTokenizer.builder().buildList("foo bar"));
        Assert.assertEquals(Arrays.asList("foo,bar"), QuotedStringTokenizer.builder().buildList("foo,bar"));
        Assert.assertEquals(Arrays.asList("foo", "bar"), QuotedStringTokenizer.builder().delimiterChars(",").buildList("foo,bar"));
        Assert.assertEquals(Arrays.asList("foo", " bar"), QuotedStringTokenizer.builder().delimiterChars(",").buildList("foo, bar"));
        Assert.assertEquals(Arrays.asList("foo", "bar"), QuotedStringTokenizer.builder().addDelimiterChars(",").buildList("foo, bar"));
    }

    @Test
    public void testCommaInQuotes() throws Exception {
        List<String> l = QuotedStringTokenizer.builder().addDelimiterChars(",").buildList("location1,byon:(hosts=\"loc2,loc3\"),location4");
        Assert.assertEquals(Arrays.asList("location1", "byon:(hosts=\"loc2,loc3\")", "location4"), l);
    }

    /** not implemented yet */
    @Test(enabled=false)
    public void testCommaInParentheses() throws Exception {
        List<String> l = QuotedStringTokenizer.builder().addDelimiterChars(",").buildList("location1, byon:(hosts=\"loc2,loc3\",user=foo),location4");
        Assert.assertEquals(Arrays.asList("location1", "byon:(hosts=\"loc2,loc3\",user=foo)", "location4"), l);
    }

    private void testResultingTokens(String input, String quoteChars, boolean includeQuotes, String delimiterChars, boolean includeDelimiters, String... expectedTokens) {
        testResultingTokens(input, quoteChars, includeQuotes, delimiterChars, includeDelimiters, false, false, expectedTokens);
    }
    private void testResultingTokens(String input, String quoteChars, boolean includeQuotes, String delimiterChars, boolean includeDelimiters, boolean keepInternalQuotes, boolean failOnOpenQuote, String... expectedTokens) {
        QuotedStringTokenizer tok = new QuotedStringTokenizer(input, quoteChars, includeQuotes, delimiterChars, includeDelimiters, keepInternalQuotes, failOnOpenQuote);
        testResultingTokens(input, tok, expectedTokens);
    }

    private void testResultingTokensJavaStreamTokenizerIdentifiers(String input, String... expectedTokens) {
        Asserts.assertEquals(QuotedStringTokenizer.parseAsStreamTokenizerIdentifierStrings(input), Arrays.asList(expectedTokens));
    }

    private void testResultingTokensJavaStreamTokenizerWhitespaceOrStrings(String input, String... expectedTokens) {
        Asserts.assertEquals(QuotedStringTokenizer.parseAsStreamTokenizerWhitespaceOrStrings(input), Arrays.asList(expectedTokens));
    }

    private void testResultingTokens(String input, QuotedStringTokenizer tok, String... expectedTokens) {
        List<String> actual = new LinkedList<String>();
        while (tok.hasMoreTokens()) actual.add(tok.nextToken());
        assertEquals(actual, Arrays.asList(expectedTokens), "Wrong tokens returned.");
    }

    private void assertQuoteUnquoteFor(String unquoted) {
        String quoted = defaultTokenizer.quoteToken(unquoted);
        String reunquoted = defaultTokenizer.unquoteToken(quoted);
        //System.out.println("orig="+unquoted+"  quoted="+quoted+"   reunquoted="+reunquoted);
        assertEquals(reunquoted, unquoted);
    }

    private void assertUnquoteFor(String expected, String quoted) {
        String unquoted = defaultTokenizer.unquoteToken(quoted);
        //System.out.println("expected="+expected+"  quoted="+quoted+"   unquoted="+unquoted);
        assertEquals(unquoted, expected);
    }
}
