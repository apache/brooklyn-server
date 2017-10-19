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
package org.apache.brooklyn.core.catalog.internal;

import static org.testng.Assert.assertEquals;

import java.util.List;

import org.apache.brooklyn.core.catalog.internal.BundleUpgradeParser.VersionRangedName;
import org.osgi.framework.Version;
import org.osgi.framework.VersionRange;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

public class BundleUpgradeParserTest {

    private VersionRange from0lessThan1 = new VersionRange('[', Version.valueOf("0"), Version.valueOf("1.0.0"), ')');
    private VersionRange exactly0dot1 = new VersionRange('[', Version.valueOf("0.1.0"), Version.valueOf("0.1.0"), ']');
    private VersionRangedName fooFrom0lessThan1 = new VersionRangedName("foo", from0lessThan1);
    private VersionRangedName barFrom0lessThan1 = new VersionRangedName("bar", from0lessThan1);

    @Test
    public void testParseSingleQuotedVal() throws Exception {
        String input = "\"foo:[0,1.0.0)\"";
        assertParsed(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseSingleQuotedValWithNestedQuotes() throws Exception {
        String input = "\"foo:[0,\"1.0.0\")\"";
        assertParsed(input, ImmutableList.of(fooFrom0lessThan1));
    }
    
    @Test
    public void testParseMultipleVals() throws Exception {
        String input = "\"foo:[0,1.0.0)\",\"bar:[0,1.0.0)\"";
        assertParsed(input, ImmutableList.of(fooFrom0lessThan1, barFrom0lessThan1));
    }

    @Test
    public void testParseValWithExactVersion() throws Exception {
        String input = "\"foo:0.1.0\"";
        assertParsed(input, ImmutableList.of(new VersionRangedName("foo", exactly0dot1)));
    }

    protected void assertParsed(String input, List<VersionRangedName> expected) throws Exception {
        List<VersionRangedName> actual = BundleUpgradeParser.parseVersionRangedNameList(input, false);
        assertEquals(actual.size(), expected.size(), "actual="+actual); 
        for (int i = 0; i < actual.size(); i++) {
            assertEquals(actual.get(i).getSymbolicName(), expected.get(i).getSymbolicName());
            assertEquals(actual.get(i).getOsgiVersionRange(), expected.get(i).getOsgiVersionRange());
        }
    }
}
