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
package org.apache.brooklyn.util.javalang.coerce;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertSame;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.StringPredicates;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import groovy.lang.GString;

public class TypeCoercionsTest {

    private static final Logger log = LoggerFactory.getLogger(TypeCoercionsTest.class);
    
    TypeCoercerExtensible coercer = TypeCoercerExtensible.newDefault();
    
    protected <T> T coerce(Object x, Class<T> type) {
        return coercer.coerce(x, type);
    }
    protected <T> T coerce(Object x, TypeToken<T> type) {
        return coercer.coerce(x, type);
    }
    
    @Test
    public void testCoerceCharSequenceToString() {
        assertEquals(coerce(new StringBuilder("abc"), String.class), "abc");
        assertEquals(coerce(GString.EMPTY, String.class), "");
    }
    
    @Test
    public void testCoerceStringToPrimitive() {
        assertEquals(coerce("1", Character.class), (Character)'1');
        assertEquals(coerce(" ", Character.class), (Character)' ');
        assertEquals(coerce("1", Short.class), (Short)((short)1));
        assertEquals(coerce("1", Integer.class), (Integer)1);
        assertEquals(coerce("1", Long.class), (Long)1l);
        assertEquals(coerce("1", Float.class), (Float)1f);
        assertEquals(coerce("1", Double.class), (Double)1d);
        assertEquals(coerce("true", Boolean.class), (Boolean)true);
        assertEquals(coerce("False", Boolean.class), (Boolean)false);
        assertEquals(coerce("true ", Boolean.class), (Boolean)true);
        assertNull(coerce(null, Boolean.class), null);

        assertEquals(coerce("1", char.class), (Character)'1');
        assertEquals(coerce("1", short.class), (Short)((short)1));
        assertEquals(coerce("1", int.class), (Integer)1);
        assertEquals(coerce("1", long.class), (Long)1l);
        assertEquals(coerce("1", float.class), (Float) 1f);
        assertEquals(coerce("1", double.class), (Double)1d);
        assertEquals(coerce("TRUE", boolean.class), (Boolean)true);
        assertEquals(coerce("false", boolean.class), (Boolean)false);
    }

    @Test
    public void testCoercePrimitivesToSameType() {
        assertEquals(coerce('1', Character.class), (Character)'1');
        assertEquals(coerce((short)1, Short.class), (Short)((short)1));
        assertEquals(coerce(1, Integer.class), (Integer)1);
        assertEquals(coerce(1l, Long.class), (Long)1l);
        assertEquals(coerce(1f, Float.class), (Float) 1f);
        assertEquals(coerce(1d, Double.class), (Double) 1d);
        assertEquals(coerce(true, Boolean.class), (Boolean)true);
    }
    
    @Test
    public void testCastPrimitives() {
        assertEquals(coerce(1L, Character.class), (Character)(char)1);
        assertEquals(coerce(1L, Byte.class), (Byte)(byte)1);
        assertEquals(coerce(1L, Short.class), (Short)(short)1);
        assertEquals(coerce(1L, Integer.class), (Integer)1);
        assertEquals(coerce(1L, Long.class), (Long)(long)1);
        assertEquals(coerce(1L, Float.class), (Float)(float)1);
        assertEquals(coerce(1L, Double.class), (Double)(double)1);
        
        assertEquals(coerce(1L, char.class), (Character)(char)1);
        assertEquals(coerce(1L, byte.class), (Byte)(byte)1);
        assertEquals(coerce(1L, short.class), (Short)(short)1);
        assertEquals(coerce(1L, int.class), (Integer)1);
        assertEquals(coerce(1L, long.class), (Long)(long)1);
        assertEquals(coerce(1L, float.class), (Float)(float)1);
        assertEquals(coerce(1L, double.class), (Double)(double)1);
        
        assertEquals(coerce((char)1, Integer.class), (Integer)1);
        assertEquals(coerce((byte)1, Integer.class), (Integer)1);
        assertEquals(coerce((short)1, Integer.class), (Integer)1);
        assertEquals(coerce(1, Integer.class), (Integer)1);
        assertEquals(coerce((long)1, Integer.class), (Integer)1);
        assertEquals(coerce((float)1, Integer.class), (Integer)1);
        assertEquals(coerce((double)1, Integer.class), (Integer)1);
    }
    
    @Test
    public void testCoercePrimitiveFailures() {
        // error messages don't have to be this exactly, but they should include sufficient information...
        assertCoercionFailsWithErrorMatching("maybe", boolean.class, StringPredicates.containsAllLiterals("String", "Boolean", "maybe"));
        assertCoercionFailsWithErrorMatching("NaN", int.class, StringPredicates.containsAllLiterals("Integer", "NaN"));
        assertCoercionFailsWithErrorMatching('c', boolean.class, StringPredicates.containsAllLiterals("Boolean", "(c)"));  // will say 'string' rather than 'char'
        assertCoercionFailsWithErrorMatching(0, boolean.class, StringPredicates.containsAllLiterals("Integer", "Boolean", "0"));
    }
    
    protected void assertCoercionFailsWithErrorMatching(Object input, Class<?> type, Predicate<? super String> errorMessageRequirement) {
        try {
            Object result = coerce(input, type);
            Assert.fail("Should have failed type coercion of "+input+" to "+type+", instead got: "+result);
        } catch (Exception e) {
            if (errorMessageRequirement==null || errorMessageRequirement.apply(e.toString()))
                log.info("Primitive coercion failed as expected, with: "+e);
            else
                Assert.fail("Error from type coercion of "+input+" to "+type+" failed with wrong exception; expected match of "+errorMessageRequirement+" but got: "+e);
        }
        
    }

    @Test
    public void testCastToNumericPrimitives() {
        assertEquals(coerce(BigInteger.ONE, Integer.class), (Integer)1);
        assertEquals(coerce(BigInteger.ONE, int.class), (Integer)1);
        assertEquals(coerce(BigInteger.valueOf(Long.MAX_VALUE), Long.class), (Long)Long.MAX_VALUE);
        assertEquals(coerce(BigInteger.valueOf(Long.MAX_VALUE), long.class), (Long)Long.MAX_VALUE);
        
        assertEquals(coerce(BigDecimal.valueOf(0.5), Double.class), 0.5d, 0.00001d);
        assertEquals(coerce(BigDecimal.valueOf(0.5), double.class), 0.5d, 0.00001d);
    }

    @Test
    public void testCoerceStringToBigNumber() {
    	assertEquals(coerce("0.5", BigDecimal.class), BigDecimal.valueOf(0.5));
    	assertEquals(coerce("1", BigInteger.class), BigInteger.valueOf(1));
    }

    @Test
    public void testCoerceStringToTimeZone() {
        assertEquals(coerce("UTC", TimeZone.class).getID(), TimeZone.getTimeZone("UTC").getID());
    }

    @Test
    public void testCoerceNumberToDate() {
        assertEquals(coerce(1000L, Date.class), new Date(1000));
        assertEquals(coerce(1000, Date.class), new Date(1000));
    }

    @Test
    public void testCoerceStringToEnum() {
        assertEquals(coerce("LOWERCASE", PerverseEnum.class), PerverseEnum.lowercase);
        assertEquals(coerce("CAMELCASE", PerverseEnum.class), PerverseEnum.camelCase);
        assertEquals(coerce("upper", PerverseEnum.class), PerverseEnum.UPPER);
        assertEquals(coerce("upper_with_underscore", PerverseEnum.class), PerverseEnum.UPPER_WITH_UNDERSCORE);
        assertEquals(coerce("LOWER_WITH_UNDERSCORE", PerverseEnum.class), PerverseEnum.lower_with_underscore);
    }
    public static enum PerverseEnum {
        lowercase,
        camelCase,
        UPPER,
        UPPER_WITH_UNDERSCORE,
        lower_with_underscore;
    }
    
    @Test
    public void testArrayToListCoercion() {
        @SuppressWarnings("serial")
        List<?> s = coerce(new String[] {"1", "2"}, new TypeToken<List<String>>() { });
        Assert.assertEquals(s, ImmutableList.of("1", "2"));
    }
    
    @Test
    public void testArrayEntryCoercion() {
        @SuppressWarnings("serial")
        Integer[] val = coerce(new String[] {"1", "2"}, new TypeToken<Integer[]>() { });
        Assert.assertTrue(Arrays.equals(val, new Integer[] {1, 2}), "val="+Arrays.toString(val)+" of type "+val.getClass());
    }
    
    @Test
    public void testArrayEntryInvalidCoercion() {
        try {
            @SuppressWarnings("serial")
            Integer[] val = coerce(new String[] {"myWrongVal"}, new TypeToken<Integer[]>() { });
            Asserts.shouldHaveFailedPreviously("val="+val);
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "Cannot coerce", "myWrongVal", "to java.lang.Integer");
        }
    }
    
    @Test
    public void testListToArrayInvalidCoercion() {
        try {
            @SuppressWarnings("serial")
            Integer[] val = coerce(ImmutableList.of("myWrongVal"), new TypeToken<Integer[]>() { });
            Asserts.shouldHaveFailedPreviously("val="+val);
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, "Cannot coerce", "myWrongVal", "to java.lang.Integer");
        }
    }
    
    @Test
    public void testArrayMultiDimensionEntryCoercion() {
        @SuppressWarnings("serial")
        Integer[][] val = coerce(new String[][] {{"1", "2"}, {"3", "4"}}, new TypeToken<Integer[][]>() { });
        Assert.assertTrue(EqualsBuilder.reflectionEquals(val, new Integer[][] {{1, 2}, {3, 4}}), 
                "val="+Arrays.toString(val)+" of type "+val.getClass());
    }
    
    @Test
    public void testArrayEntryToListCoercion() {
        @SuppressWarnings("serial")
        List<?> s = coerce(new String[] {"1", "2"}, new TypeToken<List<Integer>>() { });
        Assert.assertEquals(s, ImmutableList.of(1, 2));
    }
    
    @Test
    public void testListToSetCoercion() {
        Set<?> s = coerce(ImmutableList.of(1), Set.class);
        Assert.assertEquals(s, ImmutableSet.of(1));
    }
    
    @Test
    public void testSetToListCoercion() {
        List<?> s = coerce(ImmutableSet.of(1), List.class);
        Assert.assertEquals(s, ImmutableList.of(1));
    }
    
    @Test
    public void testIterableToArrayCoercion() {
        String[] s = coerce(ImmutableList.of("a", "b"), String[].class);
        Assert.assertTrue(Arrays.equals(s, new String[] {"a", "b"}), "result="+Arrays.toString(s));
        
        Integer[] i = coerce(ImmutableList.of(1, 2), Integer[].class);
        Assert.assertTrue(Arrays.equals(i, new Integer[] {1, 2}), "result="+Arrays.toString(i));
        
        int[] i2 = coerce(ImmutableList.of(1, 2), int[].class);
        Assert.assertTrue(Arrays.equals(i2, new int[] {1, 2}), "result="+Arrays.toString(i2));
        
        int[] i3 = coerce(MutableSet.of("1", 2), int[].class);
        Assert.assertTrue(Arrays.equals(i3, new int[] {1, 2}), "result="+Arrays.toString(i3));
    }

    @Test
    public void testListEntryCoercion() {
        @SuppressWarnings("serial")
        List<?> s = coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<List<Class<?>>>() { });
        Assert.assertEquals(s, ImmutableList.of(Integer.class, Double.class));
    }
    
    @Test
    public void testListEntryToSetCoercion() {
        @SuppressWarnings("serial")
        Set<?> s = coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<Set<Class<?>>>() { });
        Assert.assertEquals(s, ImmutableSet.of(Integer.class, Double.class));
    }
    
    @Test
    public void testListEntryToCollectionCoercion() {
        @SuppressWarnings("serial")
        Collection<?> s = coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<Collection<Class<?>>>() { });
        Assert.assertEquals(s, ImmutableList.of(Integer.class, Double.class));
    }

    @Test
    public void testListEntryToIterableCoercion() {
        @SuppressWarnings("serial")
        Iterable<?> val = coerce(ImmutableList.of("1", "2"), new TypeToken<Iterable<Integer>>() {});
        Assert.assertEquals(ImmutableList.copyOf(val), ImmutableList.of(1, 2));
    }

    @Test
    public void testMapValueCoercion() {
        @SuppressWarnings("serial")
        Map<?,?> s = coerce(ImmutableMap.of("int", "java.lang.Integer", "double", "java.lang.Double"), new TypeToken<Map<String, Class<?>>>() { });
        Assert.assertEquals(s, ImmutableMap.of("int", Integer.class, "double", Double.class));
    }
    
    @Test
    public void testMapKeyCoercion() {
        @SuppressWarnings("serial")
        Map<?,?> s = coerce(ImmutableMap.of("java.lang.Integer", "int", "java.lang.Double", "double"), new TypeToken<Map<Class<?>, String>>() { });
        Assert.assertEquals(s, ImmutableMap.of(Integer.class, "int", Double.class, "double"));
    }

    @Test
    public void testStringToListCoercion() {
        List<?> s = coerce("a,b,c", List.class);
        Assert.assertEquals(s, ImmutableList.of("a", "b", "c"));
    }

    @Test
    @SuppressWarnings("serial")
    public void testCoerceRecursivelyStringToGenericsCollection() {
        assertEquals(coerce("1,2", new TypeToken<List<Integer>>() {}), ImmutableList.of(1, 2));
    }
    
    @Test
    public void testJsonStringToMapCoercion() {
        Map<?,?> s = coerce("{ \"a\" : \"1\", b : 2 }", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @Test
    public void testJsonStringWithoutQuotesToMapCoercion() {
        Map<?,?> s = coerce("{ a : 1 }", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", 1));
    }

    @Test
    public void testJsonComplexTypesToMapCoercion() {
        Map<?,?> s = coerce("{ a : [1, \"2\", '\"3\"'], b: { c: d, 'e': \"f\" } }", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", ImmutableList.<Object>of(1, "2", "\"3\""), 
            "b", ImmutableMap.of("c", "d", "e", "f")));
    }

    @Test
    public void testJsonStringWithoutBracesToMapCoercion() {
        Map<?,?> s = coerce("a : 1", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", 1));
    }

    @Test
    public void testJsonStringWithoutBracesWithMultipleToMapCoercion() {
        Map<?,?> s = coerce("a : 1, b : 2", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", 1, "b", 2));
    }

    @Test
    public void testKeyEqualsValueStringToMapCoercion() {
        Map<?,?> s = coerce("a=1,b=2", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", "1", "b", "2"));
    }

    @Test
    public void testJsonStringWithoutBracesOrSpaceDisallowedAsMapCoercion() {
        Map<?,?> s = coerce("a:1,b:2", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", "1", "b", "2"));
        // NB: snakeyaml 1.17 required spaces after the colon, but 1.21 accepts the above
    }
    
    @Test
    public void testEqualsInBracesMapCoercion() {
        Map<?,?> s = coerce("{ a = 1, b = '2' }", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", 1, "b", "2"));
    }

    @Test
    public void testKeyEqualsOrColonValueWithBracesStringToMapCoercion() {
        Map<?,?> s = coerce("{ a=1, b: 2 }", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @Test
    public void testKeyEqualsOrColonValueWithoutBracesStringToMapCoercion() {
        Map<?,?> s = coerce("a=1, b: 2", Map.class);
        Assert.assertEquals(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @Test
    public void testURItoStringCoercion() {
        String s = coerce(URI.create("http://localhost:1234/"), String.class);
        Assert.assertEquals(s, "http://localhost:1234/");
    }

    @Test
    public void testStringToUriCoercion() {
        Assert.assertEquals(coerce("http://localhost:1234/", URI.class), URI.create("http://localhost:1234/"));
        
        // Empty string is coerced to null; in contrast, `URI.create("")` gives a URI
        // with null scheme, host, etc (which would be very surprising for Brooklyn users!).
        Assert.assertEquals(coerce("", URI.class), null);
    }

    @Test
    public void testURLtoStringCoercion() throws MalformedURLException {
        String s = coerce(new URL("http://localhost:1234/"), String.class);
        Assert.assertEquals(s, "http://localhost:1234/");
    }

    @Test
    public void testAs() {
        Integer x = coerce(new WithAs("3"), Integer.class);
        Assert.assertEquals(x, (Integer)3);
    }

    @Test
    public void testFrom() {
        WithFrom x = coerce("3", WithFrom.class);
        Assert.assertEquals(x.value, 3);
    }

    @Test
    public void testFromThrowingException() {
        String expectedGenericErr = "Cannot coerce type class " + String.class.getName() + " to " + WithFromThrowingException.class.getCanonicalName();
        String expectedSpecificErr = "Simulating problem in fromString";
        
        try {
            coercer.coerce("myval", WithFromThrowingException.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, expectedGenericErr, expectedSpecificErr);
        }
        
        try {
            coercer.tryCoerce("myval", WithFromThrowingException.class).get();
            Asserts.shouldHaveFailedPreviously();
        } catch (ClassCoercionException e) {
            Asserts.expectedFailureContains(e, expectedGenericErr, expectedSpecificErr);
        }
    }

    @Test
    public void testCoerceStringToNumber() {
        assertEquals(coerce("1", Number.class), Double.valueOf(1));
        assertEquals(coerce("1.0", Number.class), Double.valueOf(1.0));
    }

    @Test
    public void testCoerceToSameTypeIsNoop() {
        assertCoerceToSame(Integer.valueOf(1), Integer.class);
        assertCoerceToSame(Integer.valueOf(1), Number.class);
        assertCoerceToSame("myval", String.class);
    }

    @Test
    public void testCoerceStringToPredicate() {
        assertEquals(coerce("alwaysFalse", Predicate.class), Predicates.alwaysFalse());
        assertEquals(coerce("alwaysTrue", Predicate.class), Predicates.alwaysTrue());
        assertEquals(coerce("isNull", Predicate.class), Predicates.isNull());
        assertEquals(coerce("notNull", Predicate.class), Predicates.notNull());
        
        try {
            coerce("wrongInput", Predicate.class);
            Asserts.shouldHaveFailedPreviously();
        } catch (RuntimeException e) {
            Asserts.expectedFailureContains(e, "Cannot convert string 'wrongInput' to predicate");
        }
    }

    @Test(expectedExceptions = org.apache.brooklyn.util.javalang.coerce.ClassCoercionException.class)
    public void testInvalidCoercionThrowsClassCoercionException() {
        coerce(new Object(), TypeToken.of(Integer.class));
    }

    @Test
    public void testCoercionFunction() {
        assertEquals(coercer.function(Double.class).apply("1"), Double.valueOf(1));
    }

    private <T> void assertCoerceToSame(T val, Class<? super T> type) {
        assertSame(coerce(val, type), val);
    }
    
    public static class WithAs {
        String value;
        public WithAs(Object x) { value = ""+x; }
        public Integer asInteger() {
            return Integer.parseInt(value);
        }
    }

    public static class WithFrom {
        int value;
        public static WithFrom fromString(String s) {
            WithFrom result = new WithFrom();
            result.value = Integer.parseInt(s);
            return result;
        }
    }

    public static class WithFromThrowingException {
        int value;
        public static WithFrom fromString(String s) {
            throw new RuntimeException("Simulating problem in fromString(\"" + s + "\")");
        }
    }
}
