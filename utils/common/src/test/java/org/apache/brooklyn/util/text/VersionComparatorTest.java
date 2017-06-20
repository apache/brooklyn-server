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

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.testng.Assert;
import org.testng.annotations.Test;

public class VersionComparatorTest {
    
    @Test
    public void testSnapshotSuffixComparison() {
        assertVersionOrder("0.1-SNAPSHOT", "0.1-R", "0.1-S", "0.1-T", "0.1");
    }

    @Test
    public void testComparison() {
        assertVersionOrder("1beta", "1");
        
        assertVersionOrder("0", "1");
        assertVersionOrder("0", "0.0", "0.9", "0.10", "0.10.0", "1");
        
        assertVersionOrder("a", "b");
        
        assertVersionOrder("1beta", "1", "2beta", "11beta");
        assertVersionOrder("beta", "0", "1alpha", "1-alpha", "1beta", "1", "11-alpha", "11beta", "11_beta", "11.beta", "11-beta", "11");
        assertVersionOrder("1.0-a", "1.0-b", "1.0");
        
        assertVersionOrder("qualifier", "0qualifier", "0-qualifier", "0", "1-qualifier", "1");

        assertVersionOrder("2.0.qualifier", "2.0", "2.0.0qualifier", "2.0.0.qualifier", "2.0.0-qualifier", "2.0.0");
        assertVersionOrder("2.0-0", "2.0-2", "2.0-10", "2.0.qualifier.0", "2.0", 
            "2.0.0.0", "2.0.0-0", "2.0.0.1", "2.0.0-1", "2.0.0-q", "2.0.0qualifier.0", "2.0.0.qualifier.0", "2.0.0-qualifier.0", "2.0.0");
        
        assertVersionOrder("0", "0.0", "0.1", "0.1.0", "0.1.1", "0.2", "0.2.1", "1", "1.0", "2");
        assertVersionOrder("0", "0.0", "0.1", "0.1.0", "0.1.1", "0.2", "0.2.1", "1", "1.0", "02", "2", "02.01", "2.01", "02.1", "2.1", "02.02", "02.2", "2.2");
        // case sensitive
        assertVersionOrder("AA", "Aa", "aa");
        // non-version-strings sorted as per NaturalOrderComparator, all < 0
        assertVersionOrder("A", "B", "B0", "B-2", "B-10", "C", "b", "b-9", "b1", "b9", "b10", "ba1", "c", "0");
        // and non-letter symbols are compared as follows
        assertVersionOrder("0-qual", "0", "0.1", 
            // _after_ qualifier (after leading zeroes) if it's the qualifier separator
            "01.qualA", "01-qualC", "01", 
            "1_qualA", "1.qualA", "1-qualA", "1.qualB", "1_qualC", "1-qualC",
            // in ascii order (-, ., _) otherwise (always after leading zeroes)
            "1-x-qualC", "1-x.qualB", "1-x_qualA",
            // with unqualified always preferred
            "1");
        
        // numeric comparison works with qualifiers, preferring unqualified
        assertVersionOrder("0--qual", "0-qual", "0.qualA", "0-qualA", "0-qualB", "0-qualB2", "0-qualB10", "0-qualC", "0", "0.1.qual", "0.1", "1");
        
        // all snapshots rated lower
        assertVersionOrder(
            "0_SNAPSHOT", "0.1.SNAPSHOT", "1-SNAPSHOT", "1-SNAPSHOT-X", "1-SNAPSHOT-X-X", "1-SNAPSHOT-XX", "1-SNAPSHOT-XX-X", 
            "1.0.SNAPSHOT-A", "1.0-SNAPSHOT-B", 
            "1.2-SNAPSHOT", "1.10.SNAPSHOT",
            "qualifer",
            "0", "0.1", "1");

        assertVersionOrder("0.10.0.SNAPSHOT", "0.10.0-SNAPSHOT", "0.10.0.GA", "0.10.0-GA", "0.10.0");
    }
    
    @Test
    public void testIsSnapshot() {
        Assert.assertTrue(VersionComparator.isSnapshot("0.10.0-SNAPSHOT"));
        Assert.assertTrue(VersionComparator.isSnapshot("0.10.0.snapshot"));
        Assert.assertFalse(VersionComparator.isSnapshot("0.10.0"));
    }
    
    private static void assertVersionOrder(String v1, String v2, String ...otherVersions) {
        List<String> versions = MutableList.<String>of().append(v1, v2, otherVersions);
        
        for (int i=0; i<versions.size(); i++) {
            for (int j=0; j<versions.size(); j++) {
                assertEquals(VersionComparator.getInstance().compare(
                        versions.get(i), versions.get(j)),
                    new Integer(i).compareTo(j), "comparing "+versions.get(i)+" and "+versions.get(j));
            }
        }
    }

    @Test
    public void testComparableVersion() {
        ComparableVersion v = new ComparableVersion("10.5.8");
        ComparableVersion v_rc2 = new ComparableVersion("10.5.8-rc2");

        Assert.assertTrue(v.isGreaterThanAndNotEqualTo(v_rc2.version));
        Assert.assertTrue(v_rc2.isLessThanAndNotEqualTo(v.version));
    }

}
