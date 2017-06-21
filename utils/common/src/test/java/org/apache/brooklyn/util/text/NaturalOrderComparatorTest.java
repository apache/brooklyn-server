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

import org.testng.Assert;
import org.testng.annotations.Test;

public class NaturalOrderComparatorTest {

    public static final NaturalOrderComparator noc = new NaturalOrderComparator();
    
    @Test
    public void testNoc() {
        Assert.assertEquals(noc.compare("0", "1"), -1);
        Assert.assertEquals(noc.compare("1", "10"), -1);
        Assert.assertEquals(noc.compare("9", "10"), -1);
        Assert.assertEquals(noc.compare("a", "b"), -1);
        Assert.assertEquals(noc.compare("a9", "a10"), -1);
        
        Assert.assertEquals(noc.compare("1", "02"), -1);
        Assert.assertEquals(noc.compare("02", "3"), -1);
        Assert.assertEquals(noc.compare("02", "2"), -1);
        
        Assert.assertEquals(noc.compare("1.b", "02.a"), -1);
        Assert.assertEquals(noc.compare("02.b", "3.a"), -1);
        // leading zero considered _after_ remainder of string
        Assert.assertEquals(noc.compare("02.a", "2.b"), -1);
        
        Assert.assertEquals(noc.compare("0.9", "0.91"), -1);
        Assert.assertEquals(noc.compare("0.90", "0.91"), -1);
        Assert.assertEquals(noc.compare("1.2.x", "1.09.x"), -1);
        Assert.assertEquals(noc.compare("2", "2.1"), -1);
        Assert.assertEquals(noc.compare("2", "2.01"), -1);
        Assert.assertEquals(noc.compare("2.01", "2.1"), -1);
        Assert.assertEquals(noc.compare("2.1", "2.02"), -1);
        Assert.assertEquals(noc.compare("2", "02.1"), -1);
        Assert.assertEquals(noc.compare("2", "02.01"), -1);
        
        Assert.assertEquals(noc.compare("0", "1a"), -1);
        Assert.assertEquals(noc.compare("0a", "1"), -1);
    }
    
    @Test
    public void testBasicOnes() {
        Assert.assertEquals(0, noc.compare("a", "a"));
        Assert.assertTrue(noc.compare("a", "b") < 0);
        Assert.assertTrue(noc.compare("b", "a") > 0);
        
        Assert.assertTrue(noc.compare("9", "10") < 0);
        Assert.assertTrue(noc.compare("10", "9") > 0);
        
        Assert.assertTrue(noc.compare("b10", "a9") > 0);
        Assert.assertTrue(noc.compare("b9", "a10") > 0);
        
        Assert.assertTrue(noc.compare(" 9", "10") < 0);
        Assert.assertTrue(noc.compare("10", " 9") > 0);
    }

    @Test
    public void testVersionNumbers() {
        Assert.assertEquals(noc.compare("10.5.8", "10.5.8"), 0);
        Assert.assertTrue(noc.compare("10.5", "9.9") > 0);
        Assert.assertTrue(noc.compare("10.5.1", "10.5") > 0);
        Assert.assertTrue(noc.compare("10.5.1", "10.6") < 0);
        Assert.assertTrue(noc.compare("10.5.1-1", "10.5.1-0") > 0);
    }

    @Test
    public void testWordsNumsPunctuation() {
        // it's basically ascii except for where we're comparing numbers in the same place
        
        Assert.assertTrue(noc.compare("1", "") > 0);
        Assert.assertTrue(noc.compare("one", "1") > 0);
        Assert.assertTrue(noc.compare("some1", "some") > 0);
        Assert.assertTrue(noc.compare("someone", "some") > 0);
        Assert.assertTrue(noc.compare("someone", "some1") > 0);
        Assert.assertTrue(noc.compare("someone", "some0") > 0);
        Assert.assertTrue(noc.compare("some-one", "some-1") > 0);
        
        Assert.assertTrue(noc.compare("a1", "aa1") < 0);
        Assert.assertTrue(noc.compare("a", "aa") < 0);
        Assert.assertTrue(noc.compare("a", "a-") < 0);
        Assert.assertTrue(noc.compare("a-", "a.") < 0);
        Assert.assertTrue(noc.compare("a-", "a1") < 0);
        Assert.assertTrue(noc.compare("a-", "aa") < 0);
        Assert.assertTrue(noc.compare("a1", "aa") < 0);
        Assert.assertTrue(noc.compare("aA", "a_") < 0);
        Assert.assertTrue(noc.compare("a_", "aa") < 0);
        Assert.assertTrue(noc.compare("a-1", "a1") < 0);
        Assert.assertTrue(noc.compare("a0", "a-1") < 0);
        Assert.assertTrue(noc.compare("a0", "aa0") < 0);
        Assert.assertTrue(noc.compare("a-9", "a-10") < 0);
        Assert.assertTrue(noc.compare("a-0", "a-01") < 0);
        Assert.assertTrue(noc.compare("a-01", "a-1") < 0);
    }

}