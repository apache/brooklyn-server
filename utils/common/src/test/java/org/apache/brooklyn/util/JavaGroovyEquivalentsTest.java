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
package org.apache.brooklyn.util;

import static org.apache.brooklyn.util.JavaGroovyEquivalents.elvis;
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.codehaus.groovy.runtime.GStringImpl;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

import groovy.lang.GString;

public class JavaGroovyEquivalentsTest {

    private String gstringVal = "exampleGString";
    private GString gstring = new GStringImpl(new Object[0], new String[] {gstringVal});
    private GString emptyGstring = new GStringImpl(new Object[0], new String[] {""});

    @Test
    public void testTruth() {
        assertFalse(groovyTruth((Object)null));
        assertTrue(groovyTruth("someString"));
        assertFalse(groovyTruth(""));
        assertTrue(groovyTruth(1));
        assertTrue(groovyTruth((byte)1));
        assertTrue(groovyTruth((short)1));
        assertTrue(groovyTruth(1L));
        assertTrue(groovyTruth(1F));
        assertTrue(groovyTruth(1D));
        assertFalse(groovyTruth(0));
        assertFalse(groovyTruth((byte)0));
        assertFalse(groovyTruth((short)0));
        assertFalse(groovyTruth(0L));
        assertFalse(groovyTruth(0F));
        assertFalse(groovyTruth(0D));
        assertTrue(groovyTruth(true));
        assertFalse(groovyTruth(false));
        assertTrue(groovyTruth(gstring));
        assertFalse(groovyTruth(emptyGstring));
    }

    @Test
    public void testElvis() {
        final List<?> emptyList = ImmutableList.of();
        final List<?> singletonList = ImmutableList.of("myVal");
        final List<?> differentList = ImmutableList.of("differentVal");
        
        assertEquals(elvis("", "string2"), "string2");
        assertEquals(elvis("string1", "string2"), "string1");
        assertEquals(elvis(null, "string2"), "string2");
        assertEquals(elvis("", "string2"), "string2");
        assertEquals(elvis(1, 2), Integer.valueOf(1));
        assertEquals(elvis(0, 2), Integer.valueOf(2));
        assertEquals(elvis(singletonList, differentList), singletonList);
        assertEquals(elvis(emptyList, differentList), differentList);
        assertEquals(elvis(gstring, "other"), gstringVal);
        assertEquals(elvis(emptyGstring, "other"), "other");
        assertEquals(elvis(emptyGstring, gstring), gstringVal);
    }
}
