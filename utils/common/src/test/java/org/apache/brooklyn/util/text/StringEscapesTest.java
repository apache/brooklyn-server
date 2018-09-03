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

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.StringEscapes.BashStringEscapes;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.testng.Assert;
import org.testng.annotations.Test;

public class StringEscapesTest {

    @Test
    public void testEscapeSql() {
        Assert.assertEquals(StringEscapes.escapeSql("I've never been to Brooklyn"), "I''ve never been to Brooklyn");
    }

    
    @Test
    public void testBashEscaping() {
        Assert.assertEquals(
            BashStringEscapes.doubleQuoteLiteralsForBash("-Dname=Bob Johnson", "-Dnet.worth=$100"),
            "\"-Dname=Bob Johnson\" \"-Dnet.worth=\\$100\"");
    }

    @Test
    public void testBashEscapable() {
        Assert.assertTrue(BashStringEscapes.isValidForDoubleQuotingInBash("Bob Johnson"));
        Assert.assertFalse(BashStringEscapes.isValidForDoubleQuotingInBash("\""));
        Assert.assertTrue(BashStringEscapes.isValidForDoubleQuotingInBash("\\\""));
    }    
    
    /** Bash handles ampersand in double quoted strings without escaping. */
    @Test
    public void testBashEscapableAmpersand() {
        Assert.assertTrue(BashStringEscapes.isValidForDoubleQuotingInBash("&"));
        Assert.assertTrue(BashStringEscapes.isValidForDoubleQuotingInBash("Marks & Spencer"));
    }

    @Test
    public void testJavaUnwrap() {
        Assert.assertEquals(JavaStringEscapes.unwrapJavaString("\"Hello World\""), "Hello World");
        Assert.assertEquals(JavaStringEscapes.unwrapJavaString("\"Hello \\\"Bob\\\"\""), "Hello \"Bob\"");
        try {
            JavaStringEscapes.unwrapJavaString("Hello World");
            Assert.fail("Should have thrown");
        } catch (Exception e) { /* expected */ }
        try {
            // missing final quote
            JavaStringEscapes.unwrapJavaString("\"Hello \\\"Bob\\\"");
            Assert.fail("Should have thrown");
        } catch (Exception e) { /* expected */ }
        
        Assert.assertEquals(JavaStringEscapes.unwrapJavaStringIfWrapped("\"Hello World\""), "Hello World");
        Assert.assertEquals(JavaStringEscapes.unwrapJavaStringIfWrapped("\"Hello \\\"Bob\\\"\""), "Hello \"Bob\"");
        Assert.assertEquals(JavaStringEscapes.unwrapJavaStringIfWrapped("Hello World"), "Hello World");
        try {
            // missing final quote
            JavaStringEscapes.unwrapJavaStringIfWrapped("\"Hello \\\"Bob\\\"");
            Assert.fail("Should have thrown");
        } catch (Exception e) { /* expected */ }
    }
    
    @Test
    public void testJavaEscape() {
        Assert.assertEquals(JavaStringEscapes.wrapJavaString("Hello \"World\""), "\"Hello \\\"World\\\"\"");
    }
    
    @SuppressWarnings("deprecation")
    @Test
    public void testJavaListDeprecated() {
        Assert.assertEquals(
                JavaStringEscapes.unwrapQuotedJavaStringList("\"hello\", \"world\"", ","), 
                MutableList.of("hello", "world"));
        try {
            JavaStringEscapes.unwrapQuotedJavaStringList("\"hello\", world", ",");
            Assert.fail("Should have thrown");
        } catch (Exception e) { /* expected */ }
        
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("\"hello\", \"world\""),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("hello"),
                MutableList.of("hello"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("hello, world"),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("\"hello\", world"),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("[ \"hello\", world ]"),
                MutableList.of("hello", "world"));
        // if can't parse e.g. because contains double quote, then returns original string as single element list
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("hello\", \"world\""),
                MutableList.of("hello\", \"world\""));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible(" "),
                MutableList.of());
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("\"\""),
                MutableList.of(""));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible(",,x,"),
                MutableList.of("x"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListIfPossible("\"\",,x,\"\""),
                MutableList.of("","x",""));
    }

    @Test
    public void testJavaListString() {
        Assert.assertEquals(
                JavaStringEscapes.unwrapQuotedJavaStringList("\"hello\", \"world\"", ","),
                MutableList.of("hello", "world"));
        try {
            JavaStringEscapes.unwrapQuotedJavaStringList("\"hello\", world", ",");
            Assert.fail("Should have thrown");
        } catch (Exception e) { /* expected */ }
        
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("\"hello\", \"world\""),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("hello"),
                MutableList.of("hello"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("hello, world"),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("\"hello\", world"),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("[ \"hello\", world ]"),
                MutableList.of("hello", "world"));
        // if can't parse e.g. because contains double quote, then returns original string as single element list
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("hello\", \"world\""),
                MutableList.of("hello\", \"world\""));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible(" "),
                MutableList.of());
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("\"\""),
                MutableList.of(""));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible(",,x,"),
                MutableList.of("x"));
        Assert.assertEquals(
                JavaStringEscapes.unwrapJsonishListStringIfPossible("\"\",,x,\"\""),
                MutableList.of("","x",""));
    }

    @Test
    public void testJavaListObject() {
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("\"hello\", \"world\"").get(),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("hello").get(),
                MutableList.of("hello"));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("hello, world").get(),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("\"hello\", world").get(),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("[ \"hello\", world ]").get(),
                MutableList.of("hello", "world"));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList(" ").get(),
                MutableList.of());
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("\"\"").get(),
                MutableList.of(""));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList(",,x,").get(),
                MutableList.of("","","x",""));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("\"\",,x,\"\"").get(),
                MutableList.of("","","x",""));
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("[ a : 1, b : 2 ]").get(),
                MutableList.<Object>of(MutableMap.of("a", 1),MutableMap.of("b", 2)));

        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("1, 2.0, buckle my shoe, true, \"true\", null, \"null\", \",\"").get(),
                MutableList.<Object>of(1, 2.0, "buckle my shoe", true, "true", null, "null", ","));

        try {
            JavaStringEscapes.tryUnwrapJsonishList("\"hello").get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) { Asserts.expectedFailureDoesNotContain(e, "position"); }
        try {
            JavaStringEscapes.tryUnwrapJsonishList(", \"hello").get();
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) { 
            Asserts.expectedFailureContains(e, "position 1"); 
        }
        
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("[ { a: b }, world ]").get(),
                MutableList.of(MutableMap.of("a", "b"), "world"));
        
        Assert.assertEquals(
                JavaStringEscapes.tryUnwrapJsonishList("{ a: [ b, 2 ] }, world").get(),
                MutableList.of(MutableMap.of("a", MutableList.<Object>of("b", 2)), "world"));
    }

}
