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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.PortRange;
import org.apache.brooklyn.core.location.PortRanges;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

public class EntitySetFromFlagTest extends BrooklynAppUnitTestSupport {

    @Test
    public void testSetFromFlagUsingFieldName() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("str1", "myval"));
        assertEquals(entity.str1, "myval");
    }
    
    @Test
    public void testSetFromFlagUsingOverridenName() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("altStr2", "myval"));
        assertEquals(entity.str2, "myval");
    }
    
    @Test
    public void testSetFromFlagWhenNoDefaultIsNull() {
        MyEntity entity = newDeproxiedEntity();
        assertEquals(entity.str1, null);
    }
    
    @Test
    public void testSetFromFlagUsesDefault() {
        MyEntity entity = newDeproxiedEntity();
        assertEquals(entity.str3, "default str3");
    }
    
    @Test
    public void testSetFromFlagOverridingDefault() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("str3", "overridden str3"));
        assertEquals(entity.str3, "overridden str3");
    }

    @Test
    public void testSetFromFlagCastsPrimitives() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("double1", 1f));
        assertEquals(entity.double1, 1d);
    }

    @Test
    public void testSetFromFlagCastsDefault() {
        MyEntity entity = newDeproxiedEntity();
        assertEquals(entity.byte1, (byte)1);
        assertEquals(entity.short1, (short)2);
        assertEquals(entity.int1, 3);
        assertEquals(entity.long1, 4l);
        assertEquals(entity.float1, 5f);
        assertEquals(entity.double1, 6d);
         assertEquals(entity.char1, 'a');
        assertEquals(entity.bool1, true);
        
        assertEquals(entity.byte2, Byte.valueOf((byte)1));
        assertEquals(entity.short2, Short.valueOf((short)2));
        assertEquals(entity.int2, Integer.valueOf(3));
        assertEquals(entity.long2, Long.valueOf(4l));
        assertEquals(entity.float2, Float.valueOf(5f));
        assertEquals(entity.double2, Double.valueOf(6d));
        assertEquals(entity.char2, Character.valueOf('a'));
        assertEquals(entity.bool2, Boolean.TRUE);
    }
    
    @Test
    public void testSetFromFlagCoercesDefaultToPortRange() {
        MyEntity entity = newDeproxiedEntity();
        assertEquals(entity.portRange1, PortRanges.fromInteger(1234));
    }
    
    @Test
    public void testSetFromFlagCoercesStringValueToPortRange() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("portRange1", "1-3"));
        assertEquals(entity.portRange1, new PortRanges.LinearPortRange(1, 3));
    }
    
    @Test
    public void testSetFromFlagCoercesStringValueToInt() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("int1", "123"));
        assertEquals(entity.int1, 123);
    }

    @Test
    public void testSetIconUrl() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("iconUrl", "/img/myicon.gif"));
        assertEquals(entity.getIconUrl(), "/img/myicon.gif");
    }

    @Test
    public void testFailsFastOnInvalidCoercion() {
        try {
            newDeproxiedEntity(MutableMap.of("int1", "thisisnotanint"));
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            if (Exceptions.getFirstThrowableOfType(e, IllegalArgumentException.class) == null) {
                throw e;
            }
        }
    }
    
    @Test
    public void testSetFromFlagWithFieldThatIsExplicitySet() {
        MyEntity entity = newDeproxiedEntity(MutableMap.of("str4", "myval"));
        assertEquals(entity.str4, "myval");
        
        MyEntity entity2 = newDeproxiedEntity();
        assertEquals(entity2.str4, "explicit str4");
    }
    
    private MyEntity newDeproxiedEntity() {
        return newDeproxiedEntity(ImmutableMap.of());
    }
    
    private MyEntity newDeproxiedEntity(Map<?, ?> config) {
        Entity result = app.addChild(EntitySpec.create(Entity.class).impl(MyEntity.class)
                .configure(config));
        return (MyEntity) Entities.deproxy(result);
    }
    
    public static class MyEntity extends AbstractEntity {

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
