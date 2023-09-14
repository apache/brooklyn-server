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

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.exceptions.Exceptions;
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

    String testIsQuoted(String expression, boolean isQuotedExpected, boolean shouldFailInStrictMode, String ...expectedTokens) {
        QuotedStringTokenizer.Builder qb = QuotedStringTokenizer.builder().expectQuotesDelimited(true);
        QuotedStringTokenizer qbi = qb.build(expression);

        Asserts.assertEquals(qbi.remainderAsList(), Arrays.asList(expectedTokens));
        Asserts.assertEquals(qbi.isQuoted(expression), isQuotedExpected);
        Object result = null;
        try {
            QuotedStringTokenizer qb2 = QuotedStringTokenizer.builder().expectQuotesDelimited(true).failOnOpenQuote(true).build(expression);
            result = qb2.remainderAsList();
        } catch (Exception e) {
            if (!shouldFailInStrictMode) throw Exceptions.propagate(e);
        }
        if (shouldFailInStrictMode && result!=null) Asserts.fail("Should have failed on input: "+expression+" -- instead returned "+result);

        return qbi.unwrapIfQuoted(expression);
    }

    void testUnquoted(String expression, boolean shouldFailInStrictMode, String expectedTokenWrapped, String expectedTokenUnwrapped) {
        String unq = testIsQuoted(expression, true, shouldFailInStrictMode, expectedTokenWrapped);
        Asserts.assertEquals(unq, expectedTokenUnwrapped);
    }

    @Test
    public void testIsQuoted() {
        QuotedStringTokenizer.Builder qb = QuotedStringTokenizer.builder().expectQuotesDelimited(true);
        QuotedStringTokenizer qbi = qb.build("");
        testIsQuoted("\"x \" ''y z \"1'", false, true, "\"x \"", "''y z \"1'");

        testUnquoted("\"x \\\"y z \\\"1\"", false, "\"x \\\"y z \\\"1\"", "x \"y z \"1");
        testUnquoted("\"x \"y z \"1\"", true, "\"x \"y z \"1\"", "x \"y z \"1");
        testIsQuoted("\"x \" ''y z \"1'", false, true, "\"x \"", "''y z \"1'");
        testIsQuoted("\"x \" 'y z \"1'", false, false, "\"x \"", "'y z \"1'");
        testIsQuoted("\"x \"y\" z \"1\"", false, true, "\"x \"y\"", "z", "\"1\"");
        testIsQuoted("\\\"x \"y\" z \"1\"", false, false, "\\\"x", "\"y\"", "z", "\"1\"");
    }

    @Test
    public void testTokenizingWithQuotesDelimited() throws Exception {
        testResultingTokens("foo,bar,baz", "\"", false, ",", false, true, true, "foo", "bar", "baz");
        testResultingTokens("\"foo,bar\",baz", "\"", false, ",", false, true, true, "foo,bar", "baz");
        testResultingTokens("\"foo,,bar\",baz", "\"", false, ",", false, true, true, "foo,,bar", "baz");
        testResultingTokens("\"foo,',bar\",baz", "\"", false, ",", false, true, true, "foo,',bar", "baz");
        testResultingTokens("\"foo,\',bar\",baz", "\"", false, ",", false, true, true, "foo,',bar", "baz");
        testResultingTokens("foo \"\"bar\"\" baz", "\"", false, ",", false, true, true, "foo \"\"bar\"\" baz");
        testResultingTokens("foo \"\"bar\"\" baz", "\"", false, ", ", false, true, false, "foo", "\"bar\"", "baz");

        testResultingTokens("\"foo \"\"bar\"\" baz\"", "\"", false, ",", false, true, false, "foo \"\"bar\"\" baz");
        testResultingTokens("\"foo \"\"bar\"\" baz\"", "\"", false, ", ", false, true, false, "foo \"\"bar\"", "baz\"");
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

    @Test
    public void testBashLike() throws Exception {
        Asserts.assertEquals(new QuotedStringTokenizer("This is a test\\ of spaces and \"some in quotes\" too").remainderAsList(),
                MutableList.of("This", "is", "a", "test\\", "of", "spaces", "and", "\"some in quotes\"", "too"));
        // note: backslashed escapes are not handled at present; ideally it would give us back this:
                //MutableList.of("This", "is", "a", "test\\ of", "spaces", "and", "\"some in quotes\"", "too"));
    }

    @Test
    public void testForArgsList() throws Exception {
        // per above, ideally note: backslashed escapes are not handled at present; ideally it would give us back this:
        Asserts.assertEquals(new QuotedStringTokenizer("test \"quoted phrases\" and [\"embedded_quotes\"]", false).remainderAsList(),
                MutableList.of("test", "quoted phrases", "and", "[embedded_quotes]"));
        // above is the same as includeQuotes below; but for args lists we will usually want to keepInternalQuotes
        Asserts.assertEquals(QuotedStringTokenizer.builder().includeQuotes(false).keepInternalQuotes(true).buildList("test \"quoted phrases\" and [\"embedded_quotes\"]"),
                MutableList.of("test", "quoted phrases", "and", "[\"embedded_quotes\"]"));
    }
}
