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
package org.apache.brooklyn.core.location;

import org.apache.brooklyn.api.location.OsDetails;
import org.testng.annotations.Test;

import static org.testng.Assert.*;

public class BasicOsDetailsTest {

    @Test
    public void testMacIntel2019() {
        OsDetails testMacOsIntel2019 = new BasicOsDetails("Mac OS X","x86_64","10.16");
        assertTrue(testMacOsIntel2019.isMac());
        assertFalse(testMacOsIntel2019.isArm());
        assertTrue(testMacOsIntel2019.is64bit());
    }

    @Test
    public void testMacM12021() {
        OsDetails testMacOsIntel2019 = new BasicOsDetails("macOS","aarch64","10.19");
        assertTrue(testMacOsIntel2019.isMac());
        assertTrue(testMacOsIntel2019.isArm());
        assertTrue(testMacOsIntel2019.is64bit());
    }

    @Test
    public void testMyOs() {
        OsDetails myOs = new BasicOsDetails("myname", "myarch", "myversion");
        assertFalse(myOs.isMac());
        assertFalse(myOs.isArm());
        assertFalse(myOs.is64bit());
    }

    @Test
    public void testMyOs64() {
        OsDetails myOs64 = new BasicOsDetails("myname", "myarch64", "myversion");
        assertFalse(myOs64.isMac());
        assertFalse(myOs64.isArm());
        assertTrue(myOs64.is64bit());
    }
}
