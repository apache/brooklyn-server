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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.location.AbstractLocation;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class LocationSetFromFlagTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSetFromFlagUsingFieldName() {
        MyLocation loc = newLocation(MutableMap.of("str1", "myval"));
        assertEquals(loc.str1, "myval");
    }
    
    @Test
    public void testSetFromFlagUsingOverridenName() {
        MyLocation loc = newLocation(MutableMap.of("altStr2", "myval"));
        assertEquals(loc.str2, "myval");
    }
    
    @Test
    public void testSetFromFlagWhenNoDefaultIsNull() {
        MyLocation loc = newLocation();
        assertEquals(loc.str1, null);
    }
    
    @Test
    public void testSetFromFlagUsesDefault() {
        MyLocation loc = newLocation();
        assertEquals(loc.str3, "default str3");
    }
    
    @Test
    public void testSetFromFlagOverridingDefault() {
        MyLocation loc = newLocation(MutableMap.of("str3", "overridden str3"));
        assertEquals(loc.str3, "overridden str3");
    }

    @Test
    public void testSetFromFlagCastsPrimitives() {
        MyLocation loc = newLocation(MutableMap.of("double1", 1f));
        assertEquals(loc.double1, 1d);
    }

    @Test
    public void testSetFromFlagCastsDefault() {
        MyLocation loc = newLocation();
        assertEquals(loc.byte1, (byte)1);
        assertEquals(loc.short1, (short)2);
        assertEquals(loc.int1, 3);
        assertEquals(loc.long1, 4l);
        assertEquals(loc.float1, 5f);
        assertEquals(loc.double1, 6d);
         assertEquals(loc.char1, 'a');
        assertEquals(loc.bool1, true);
        
        assertEquals(loc.byte2, Byte.valueOf((byte)1));
        assertEquals(loc.short2, Short.valueOf((short)2));
        assertEquals(loc.int2, Integer.valueOf(3));
        assertEquals(loc.long2, Long.valueOf(4l));
        assertEquals(loc.float2, Float.valueOf(5f));
        assertEquals(loc.double2, Double.valueOf(6d));
        assertEquals(loc.char2, Character.valueOf('a'));
        assertEquals(loc.bool2, Boolean.TRUE);
    }
    
    @Test
    public void testSetFromFlagCoercesDefaultToPortRange() {
        MyLocation loc = newLocation();
        assertEquals(loc.portRange1, PortRanges.fromInteger(1234));
    }
    
    @Test
    public void testSetFromFlagCoercesStringValueToPortRange() {
        MyLocation loc = newLocation(MutableMap.of("portRange1", "1-3"));
        assertEquals(loc.portRange1, new PortRanges.LinearPortRange(1, 3));
    }
    
    @Test
    public void testSetFromFlagCoercesStringValueToInt() {
        MyLocation loc = newLocation(MutableMap.of("int1", "123"));
        assertEquals(loc.int1, 123);
    }

    @Test
    public void testFailsFastOnInvalidCoercion() {
        try {
            newLocation(MutableMap.of("int1", "thisisnotanint"));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, IllegalArgumentException.class) == null) {
                throw e;
            }
        }
    }
    
    @Test
    public void testSetFromFlagWithFieldThatIsExplicitySet() {
        MyLocation loc = newLocation(MutableMap.of("str4", "myval"));
        assertEquals(loc.str4, "myval");
        
        MyLocation loc2 = newLocation();
        assertEquals(loc2.str4, "explicit str4");
    }
    
    private MyLocation newLocation() {
        return newLocation(ImmutableMap.of());
    }
    
    private MyLocation newLocation(Map<?, ?> config) {
        return mgmt.getLocationManager().createLocation(LocationSpec.create(MyLocation.class)
                .configure(config));
    }
    
    public static class MyLocation extends AbstractLocation {

        @SetFromFlag(defaultVal="1234")
        PortRange portRange1;

        @SetFromFlag
        String str1;
        
        @SetFromFlag("altStr2")
        String str2;
        
        @SetFromFlag(defaultVal="default str3")
        String str3;

        @SetFromFlag
        String str4 = "explicit str4";
        
        @SetFromFlag(defaultVal="1")
        byte byte1;

        @SetFromFlag(defaultVal="2")
        short short1;

        @SetFromFlag(defaultVal="3")
        int int1;

        @SetFromFlag(defaultVal="4")
        long long1;

        @SetFromFlag(defaultVal="5")
        float float1;

        @SetFromFlag(defaultVal="6")
        double double1;

        @SetFromFlag(defaultVal="a")
        char char1;

        @SetFromFlag(defaultVal="true")
        boolean bool1;

        @SetFromFlag(defaultVal="1")
        Byte byte2;

        @SetFromFlag(defaultVal="2")
        Short short2;

        @SetFromFlag(defaultVal="3")
        Integer int2;

        @SetFromFlag(defaultVal="4")
        Long long2;

        @SetFromFlag(defaultVal="5")
        Float float2;

        @SetFromFlag(defaultVal="6")
        Double double2;

        @SetFromFlag(defaultVal="a")
        Character char2;

        @SetFromFlag(defaultVal="true")
        Boolean bool2;
    }
}
