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
package org.apache.brooklyn.core.typereg;

import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class RegisteredTypeNamingTest {

    public void testNames() {
        assertName("foo", true, true);
        assertName("foo-bar", true, true);
        assertName("a-package.1-foo-bar", true, true);
        
        assertName("", false, false);
        assertName(null, false, false);
        assertName("foo:1", false, false);
        assertName("foo bar", false, false);
        
        assertName(".foo", true, false);
        assertName("package..foo", true, false);
        assertName("package..foo", true, false);
        assertName("!$&", true, false);
    }

    public void testVersions() {
        assertVersion("1", true, true);
        assertVersion("1.0.0", true, true);
        assertVersion("1.0.0.SNAPSHOT", true, true);
        
        assertVersion("", false, false);
        assertVersion(null, false, false);
        assertVersion("1:1", false, false);
        
        assertVersion("1.SNAPSHOT", true, false);
        assertVersion("1.0.0_SNAPSHOT", true, false);
        assertVersion(".1", true, false);
        assertVersion("v1", true, false);
        assertVersion("!$&", true, false);
    }
    
    public void testNameColonVersion() {
        assertNameColonVersion("foo:1", true, true);
        assertNameColonVersion("1:1", true, true);
        assertNameColonVersion("a-package.1-foo-bar:1.0.0.SNAPSHOT_dev", true, true);
        
        assertNameColonVersion("", false, false);
        assertNameColonVersion(null, false, false);
        assertNameColonVersion("foo:", false, false);
        assertNameColonVersion(":1", false, false);
        
        assertNameColonVersion("foo:1.SNAPSHOT", true, false);
        assertNameColonVersion("foo:v1", true, false);
        assertNameColonVersion("foo...bar!:1", true, false);
    }
    
    private void assertName(String candidate, boolean isUsable, boolean isGood) {
        Assert.assertEquals(RegisteredTypeNaming.isUsableTypeName(candidate), isUsable, "usable name '"+candidate+"'");
        Assert.assertEquals(RegisteredTypeNaming.isGoodTypeName(candidate), isGood, "good name '"+candidate+"'");
    }
    private void assertVersion(String candidate, boolean isUsable, boolean isGood) {
        Assert.assertEquals(RegisteredTypeNaming.isUsableVersion(candidate), isUsable, "usable version '"+candidate+"'");
        Assert.assertEquals(RegisteredTypeNaming.isOsgiLegalVersion(candidate), isGood, "good version '"+candidate+"'");
    }
    private void assertNameColonVersion(String candidate, boolean isUsable, boolean isGood) {
        Assert.assertEquals(RegisteredTypeNaming.isUsableTypeColonVersion(candidate), isUsable, "usable name:version '"+candidate+"'");
        Assert.assertEquals(RegisteredTypeNaming.isGoodTypeColonVersion(candidate), isGood, "good name:version '"+candidate+"'");
    }

}
