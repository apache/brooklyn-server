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
package org.apache.brooklyn.location.jclouds;

import static org.apache.brooklyn.util.core.flags.TypeCoercions.coerce;
import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.coerce.ClassCoercionException;
import org.jclouds.json.SerializedNames;
import org.testng.annotations.Test;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JcloudsTypeCoercionsWithCreateTest {

    static {
        JcloudsTypeCoercions.init();
    }
    
    @Test
    public void testCallsCreate() {
        assertEquals(
                coerce(ImmutableMap.of("arg1", "val1", "arg2", "val2"), MyClazz.class), 
                MyClazz.create("val1", "val2"));
        assertEquals(
                coerce(ImmutableMap.of("arg2", "val2", "arg1", "val1"), MyClazz.class), 
                MyClazz.create("val1", "val2"));
        assertEquals(
                coerce(ImmutableMap.of("arg1", "val1"), MyClazz.class), 
                MyClazz.create("val1", null));
        assertEquals(
                coerce(ImmutableMap.of("arg2", "val2"), MyClazz.class), 
                MyClazz.create(null, "val2"));
    }

    @Test
    public void testFailsIfExtraArgs() {
        try {
            coerce(ImmutableMap.of("arg1", "val1", "arg2", "val2", "arg3", "val3"), MyClazz.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "create()", "MyClazz", "does not accept extra args [arg3]");
        }
    }

    @Test
    public void testCallsCreateWithPrimitives() {
        assertEquals(
                coerce(ImmutableMap.builder().put("boolArg", true).put("byteArg", (byte)1).put("shortArg", (short)2)
                        .put("intArg", (int)3).put("longArg", (long)4).put("floatArg", (float)5.0)
                        .put("doubleArg", (double)6.0).build(), MyClazzWithPrimitives.class), 
                MyClazzWithPrimitives.create(true, (byte)1, (short)2, (int)3, (long)4, (float)5.0, (double)6.0));
        assertEquals(
                coerce(ImmutableMap.builder().put("boolArg", "true").put("byteArg", "1").put("shortArg", "2")
                        .put("intArg", "3").put("longArg", "4").put("floatArg", "5.0")
                        .put("doubleArg", "6.0").build(), MyClazzWithPrimitives.class), 
                MyClazzWithPrimitives.create(true, (byte)1, (short)2, (int)3, (long)4, (float)5.0, (double)6.0));
    }
    @Test
    public void testListOfCreatedObjs() {
        assertEquals(
                coerce(ImmutableMap.of("vals", ImmutableList.of()), ListOfMyClazz.class), 
                ListOfMyClazz.create(ImmutableList.of()));
        assertEquals(
                coerce(ImmutableMap.of("vals", ImmutableList.of(ImmutableMap.of("arg1", "val1", "arg2", "val2"))), ListOfMyClazz.class), 
                ListOfMyClazz.create(ImmutableList.of(MyClazz.create("val1", "val2"))));
    }

    @Test
    public void testCompositeOfCreatedObjs() {
        assertEquals(
                coerce(ImmutableMap.of("val",ImmutableMap.of("arg1", "val1", "arg2", "val2")), MyCompositeClazz.class), 
                MyCompositeClazz.create(MyClazz.create("val1", "val2")));
    }
    
    @Test
    public void testMapOfCreatedObjs() {
        assertEquals(
                coerce(ImmutableMap.of("vals", ImmutableMap.of()), MapOfMyClazz.class), 
                MapOfMyClazz.create(ImmutableMap.of()));
        assertEquals(
                coerce(ImmutableMap.of("vals", ImmutableMap.of("key1", ImmutableMap.of("arg1", "val1", "arg2", "val2"))), MapOfMyClazz.class), 
                MapOfMyClazz.create(ImmutableMap.of("key1", MyClazz.create("val1", "val2"))));
    }

    public static class MyClazz {
        private final String arg1;
        private final String arg2;
        
        @SerializedNames({"arg1", "arg2"})
        public static MyClazz create(String arg1, String arg2) {
            return new MyClazz(arg1, arg2);
        }
        
        private MyClazz(String arg1, String arg2) {
            this.arg1 = arg1;
            this.arg2 = arg2;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyClazz)) return false;
            MyClazz o = (MyClazz) obj;
            return Objects.equal(arg1, o.arg1) && Objects.equal(arg2, o.arg2);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(arg1, arg2);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("arg1", arg1).add("arg2", arg2).toString();
        }
    }
    
    public static class MyCompositeClazz {
        private final MyClazz val;
        
        @SerializedNames({"val"})
        public static MyCompositeClazz create(MyClazz val) {
            return new MyCompositeClazz(val);
        }
        
        private MyCompositeClazz(MyClazz val) {
            this.val = val;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyCompositeClazz)) return false;
            MyCompositeClazz o = (MyCompositeClazz) obj;
            return Objects.equal(val, o.val);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(val);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("val", val).toString();
        }
    }
    
    public static class MyClazzWithPrimitives {
        private final boolean boolArg;
        private final byte byteArg;
        private final short shortArg;
        private final int intArg;
        private final long longArg;
        private final float floatArg;
        private final double doubleArg;
        
        @SerializedNames({"boolArg", "byteArg", "shortArg", "intArg", "longArg", "floatArg", "doubleArg"})
        public static MyClazzWithPrimitives create(boolean boolArg, byte byteArg, short shortArg, int intArg, long longArg, float floatArg, double doubleArg) {
            return new MyClazzWithPrimitives(boolArg, byteArg, shortArg, intArg, longArg, floatArg, doubleArg);
        }
        
        private MyClazzWithPrimitives(boolean boolArg, byte byteArg, short shortArg, int intArg, long longArg, float floatArg, double doubleArg) {
            this.boolArg = boolArg;
            this.byteArg = byteArg;
            this.shortArg = shortArg;
            this.intArg = intArg;
            this.longArg = longArg;
            this.floatArg = floatArg;
            this.doubleArg = doubleArg;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MyClazzWithPrimitives)) return false;
            MyClazzWithPrimitives o = (MyClazzWithPrimitives) obj;
            return Objects.equal(boolArg, o.boolArg) && Objects.equal(byteArg, o.byteArg)
                    && Objects.equal(shortArg, o.shortArg) && Objects.equal(intArg, o.intArg)
                    && Objects.equal(longArg, o.longArg) && Objects.equal(floatArg, o.floatArg)
                    && Objects.equal(doubleArg, o.doubleArg);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(boolArg, byteArg, shortArg, intArg, longArg, floatArg, doubleArg);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("boolArg", boolArg).add("byteArg", byteArg)
                    .add("shortArg", shortArg).add("intArg", intArg).add("longArg", longArg)
                    .add("floatArg", floatArg).add("doubleArg", doubleArg)
                    .toString();
        }
    }
    
    public static class ListOfMyClazz {
        private final List<MyClazz> vals;
        
        @SerializedNames({"vals"})
        public static ListOfMyClazz create(List<MyClazz> vals) {
            return new ListOfMyClazz(vals);
        }
        
        private ListOfMyClazz(List<MyClazz> vals) {
            this.vals = vals;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ListOfMyClazz)) return false;
            ListOfMyClazz o = (ListOfMyClazz) obj;
            return Objects.equal(vals, o.vals);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(vals);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("vals", vals).toString();
        }
    }
    
    public static class MapOfMyClazz {
        private final Map<String, MyClazz> vals;
        
        @SerializedNames({"vals"})
        public static MapOfMyClazz create(Map<String, MyClazz> vals) {
            return new MapOfMyClazz(vals);
        }
        
        private MapOfMyClazz(Map<String, MyClazz> vals) {
            this.vals = vals;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof MapOfMyClazz)) return false;
            MapOfMyClazz o = (MapOfMyClazz) obj;
            return Objects.equal(vals, o.vals);
        }
        
        @Override
        public int hashCode() {
            return Objects.hashCode(vals);
        }
        
        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this).add("vals", vals).toString();
        }
    }
}
