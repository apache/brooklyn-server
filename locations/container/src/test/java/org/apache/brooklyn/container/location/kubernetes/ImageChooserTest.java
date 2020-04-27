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
package org.apache.brooklyn.container.location.kubernetes;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class ImageChooserTest {

    private ImageChooser chooser;

    @BeforeMethod(alwaysRun = true)
    public void setUp() {
        chooser = new ImageChooser();
    }

    @Test
    public void testDefault() {
        assertEquals(chooser.chooseImage((String) null, null).orNull(), "brooklyncentral/centos:7");
    }

    @Test
    public void testCentos() {
        assertEquals(chooser.chooseImage("cEnToS", null).orNull(), "brooklyncentral/centos:7");
    }

    @Test
    public void testCentos7() {
        assertEquals(chooser.chooseImage("cEnToS", "7").orNull(), "brooklyncentral/centos:7");
    }

    @Test
    public void testUbuntu() {
        assertEquals(chooser.chooseImage("uBuNtU", null).orNull(), "brooklyncentral/ubuntu:14.04");
    }

    @Test
    public void testUbuntu14() {
        assertEquals(chooser.chooseImage("uBuNtU", "14.*").orNull(), "brooklyncentral/ubuntu:14.04");
    }

    @Test
    public void testUbuntu16() {
        assertEquals(chooser.chooseImage("uBuNtU", "16.*").orNull(), "brooklyncentral/ubuntu:16.04");
    }

    @Test
    public void testUbuntu18() {
        assertEquals(chooser.chooseImage("uBuNtU", "18.*").orNull(), "brooklyncentral/ubuntu:18.04");
    }

    @Test
    public void testAbsentForCentos6() {
        assertFalse(chooser.chooseImage("cEnToS", "6").isPresent());
    }

    @Test
    public void testAbsentForUbuntu15() {
        assertFalse(chooser.chooseImage("uBuNtU", "15").isPresent());
    }

    @Test
    public void testAbsentForDebian() {
        assertFalse(chooser.chooseImage("debian", null).isPresent());
    }

    @Test
    public void testAbsentForWrongOsFamily() {
        assertFalse(chooser.chooseImage("weirdOsFamily", null).isPresent());
    }
}
