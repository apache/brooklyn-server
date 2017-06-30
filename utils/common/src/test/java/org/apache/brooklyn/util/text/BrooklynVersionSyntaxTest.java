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

@Test
public class BrooklynVersionSyntaxTest {

    public void testVersions() {
        assertVersion("1", true, true, true);
        assertVersion("1.0.0", true, true, true);
        assertVersion("1.0.0.SNAPSHOT", true, true, false);
        assertVersion("1.0.0-SNAPSHOT", true, false, true);
        
        assertVersion("", false, false, false);
        assertVersion(null, false, false, false);
        assertVersion("1:1", false, false, false);
        
        assertVersion("1.SNAPSHOT", true, false, false);
        assertVersion("1.0.0_SNAPSHOT", true, false, false);
        assertVersion(".1", true, false, false);
        assertVersion("v1", true, false, false);
        assertVersion("!$&", true, false, false);
    }
    
    public void testConvert() {
        assertConverts("0", "0", "0.0.0");
        assertConverts("1.1", "1.1", "1.1.0");
        assertConverts("x", "0.0.0-x", "0.0.0.x");
        assertConverts("0.x", "0-x", "0.0.0.x");
        assertConverts("1.1.x", "1.1-x", "1.1.0.x");
        assertConverts("1.1.1.1.x", "1.1.1-1_x", "1.1.1.1_x");
        assertConverts("1x", "1-x", "1.0.0.x");
        assertConverts("1$", "1-_", "1.0.0._");
        assertConverts("1-", "1-_", "1.0.0._");
        assertConverts("1.1.", "1.1-_", "1.1.0._");
        assertConverts("1$$$", "1-_", "1.0.0._");
        assertConverts("1._$", "1-__", "1.0.0.__");
        assertConverts("1a$$$", "1-a_", "1.0.0.a_");
        assertConverts("1a$$$SNAPSHOT", "1-a_SNAPSHOT", "1.0.0.a_SNAPSHOT");
    }
    
    private void assertConverts(String input, String bklyn, String osgi) {
        Assert.assertEquals(BrooklynVersionSyntax.toGoodBrooklynVersion(input), bklyn, "conversion to good brooklyn");
        Assert.assertEquals(BrooklynVersionSyntax.toValidOsgiVersion(input), osgi, "conversion to valid osgi");
    }

    private void assertVersion(String candidate, boolean isUsable, boolean isOsgi, boolean isGood) {
        Assert.assertEquals(BrooklynVersionSyntax.isUsableVersion(candidate), isUsable, "usable version '"+candidate+"'");
        Assert.assertEquals(BrooklynVersionSyntax.isValidOsgiVersion(candidate), isOsgi, "osgi version '"+candidate+"'");
        Assert.assertEquals(BrooklynVersionSyntax.isGoodBrooklynVersion(candidate), isGood, "good version '"+candidate+"'");
    }
    
    private void assertOsgiVersion(String input, String osgi) {
        Assert.assertEquals(BrooklynVersionSyntax.toValidOsgiVersion(input), osgi, "conversion to valid osgi");
    }
    
    public void testOsgiVersions() {
        assertOsgiVersion("0.10.0-20160713.1653", "0.10.0.20160713_1653");

        assertOsgiVersion("2.1.0-SNAPSHOT", "2.1.0.SNAPSHOT");
        assertOsgiVersion("2.1-SNAPSHOT", "2.1.0.SNAPSHOT");
        assertOsgiVersion("0.1-SNAPSHOT", "0.1.0.SNAPSHOT");
        assertOsgiVersion("2-SNAPSHOT", "2.0.0.SNAPSHOT");
        assertOsgiVersion("2", "2.0.0");
        assertOsgiVersion("2.1", "2.1.0");
        assertOsgiVersion("2.1.3", "2.1.3");
        assertOsgiVersion("2.1.3.4", "2.1.3.4");
        assertOsgiVersion("1.1-alpha-2", "1.1.0.alpha-2");
        assertOsgiVersion("1.0-alpha-16-20070122.203121-13", "1.0.0.alpha-16-20070122_203121-13");
        assertOsgiVersion("1.0-20070119.021432-1", "1.0.0.20070119_021432-1");
        assertOsgiVersion("1-20070119.021432-1", "1.0.0.20070119_021432-1");
        assertOsgiVersion("1.4.1-20070217.082013-7", "1.4.1.20070217_082013-7");
        assertOsgiVersion("0.0.0.4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
        assertOsgiVersion("0-4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
        assertOsgiVersion("-4aug2000r7-dev", "0.0.0.4aug2000r7-dev");
        // potentially surprising, and different to maven (0.0.0.4aug..)
        assertOsgiVersion("4aug2000r7-dev", "4.0.0.aug2000r7-dev");
    }
}
