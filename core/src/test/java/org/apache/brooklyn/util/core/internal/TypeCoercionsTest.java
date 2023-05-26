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
package org.apache.brooklyn.util.core.internal;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.ClassLoaderUtils;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.coerce.ClassCoercionException;
import org.apache.brooklyn.util.javalang.coerce.CommonAdaptorTypeCoercions;
import org.apache.brooklyn.util.text.StringPredicates;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.TypeToken;

import groovy.lang.GString;

import static org.testng.Assert.*;

public class TypeCoercionsTest {

    private static final Logger log = LoggerFactory.getLogger(TypeCoercionsTest.class);

    // largely a duplicate of upstream common coerce/TypeCoercionsTest
    // but with a few extras; ideally would be reconciled

    private static void assertMapsEqual(Map actual, Map expected) {
        Assert.assertEquals(actual, expected);

        // workaround for bug in testng assert when values are null
        // PR https://github.com/testng-team/testng/pull/2914 opened, hopefully included in testng 7.9 then this can be removed
        if (actual instanceof Map && expected instanceof Map) {
            assertEquals( ((Map)actual).keySet(), ((Map)expected).keySet(), "Keys in map differ: "+actual.keySet()+" / "+expected.keySet() );
        }
    }

    @Test
    public void testCoerceCharSequenceToString() {
        assertEquals(TypeCoercions.coerce(new StringBuilder("abc"), String.class), "abc");
        assertEquals(TypeCoercions.coerce(GString.EMPTY, String.class), "");
    }
    
    @Test
    public void testCoerceStringToPrimitive() {
        assertEquals(TypeCoercions.coerce("1", Character.class), (Character) '1');
        assertEquals(TypeCoercions.coerce(" ", Character.class), (Character) ' ');
        assertEquals(TypeCoercions.coerce("1", Short.class), (Short) ((short) 1));
        assertEquals(TypeCoercions.coerce("1", Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce("1", Long.class), (Long) 1l);
        assertEquals(TypeCoercions.coerce("1", Float.class), (Float) 1f);
        assertEquals(TypeCoercions.coerce("1", Double.class), (Double) 1d);
        assertEquals(TypeCoercions.coerce("true", Boolean.class), (Boolean) true);
        assertEquals(TypeCoercions.coerce("False", Boolean.class), (Boolean) false);
        assertEquals(TypeCoercions.coerce("true ", Boolean.class), (Boolean) true);
        assertNull(TypeCoercions.coerce(null, Boolean.class), null);

        assertEquals(TypeCoercions.coerce("1", char.class), (Character) '1');
        assertEquals(TypeCoercions.coerce("1", short.class), (Short) ((short) 1));
        assertEquals(TypeCoercions.coerce("1", int.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce("1", long.class), (Long) 1l);
        assertEquals(TypeCoercions.coerce("1", float.class), (Float) 1f);
        assertEquals(TypeCoercions.coerce("1", double.class), (Double) 1d);
        assertEquals(TypeCoercions.coerce("TRUE", boolean.class), (Boolean) true);
        assertEquals(TypeCoercions.coerce("false", boolean.class), (Boolean) false);
    }

    @Test
    public void testCoercePrimitivesToSameType() {
        assertEquals(TypeCoercions.coerce('1', Character.class), (Character) '1');
        assertEquals(TypeCoercions.coerce((short) 1, Short.class), (Short) ((short) 1));
        assertEquals(TypeCoercions.coerce(1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce(1l, Long.class), (Long) 1l);
        assertEquals(TypeCoercions.coerce(1f, Float.class), (Float) 1f);
        assertEquals(TypeCoercions.coerce(1d, Double.class), (Double) 1d);
        assertEquals(TypeCoercions.coerce(true, Boolean.class), (Boolean) true);
    }
    
    @Test
    public void testCastPrimitives() {
        assertEquals(TypeCoercions.coerce(1L, Character.class), (Character) (char) 1);
        assertEquals(TypeCoercions.coerce(1L, Byte.class), (Byte) (byte) 1);
        assertEquals(TypeCoercions.coerce(1L, Short.class), (Short) (short) 1);
        assertEquals(TypeCoercions.coerce(1L, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce(1L, Long.class), (Long) (long) 1);
        assertEquals(TypeCoercions.coerce(1L, Float.class), (Float) (float) 1);
        assertEquals(TypeCoercions.coerce(1L, Double.class), (Double) (double) 1);

        assertEquals(TypeCoercions.coerce(1L, char.class), (Character) (char) 1);
        assertEquals(TypeCoercions.coerce(1L, byte.class), (Byte) (byte) 1);
        assertEquals(TypeCoercions.coerce(1L, short.class), (Short) (short) 1);
        assertEquals(TypeCoercions.coerce(1L, int.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce(1L, long.class), (Long) (long) 1);
        assertEquals(TypeCoercions.coerce(1L, float.class), (Float) (float) 1);
        assertEquals(TypeCoercions.coerce(1L, double.class), (Double) (double) 1);

        assertEquals(TypeCoercions.coerce((char) 1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce((byte) 1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce((short) 1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce(1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce((long) 1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce((float) 1, Integer.class), (Integer) 1);
        assertEquals(TypeCoercions.coerce((double) 1, Integer.class), (Integer) 1);
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
            Object result = TypeCoercions.coerce(input, type);
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
        assertEquals(TypeCoercions.coerce(BigInteger.ONE, Integer.class), (Integer)1);
        assertEquals(TypeCoercions.coerce(BigInteger.ONE, int.class), (Integer)1);
        assertEquals(TypeCoercions.coerce(BigInteger.valueOf(Long.MAX_VALUE), Long.class), (Long)Long.MAX_VALUE);
        assertEquals(TypeCoercions.coerce(BigInteger.valueOf(Long.MAX_VALUE), long.class), (Long)Long.MAX_VALUE);

        assertEquals(TypeCoercions.coerce(BigDecimal.valueOf(0.5), Double.class), 0.5d, 0.00001d);
        assertEquals(TypeCoercions.coerce(BigDecimal.valueOf(0.5), double.class), 0.5d, 0.00001d);
    }

    @Test
    public void testCoerceStringToBigNumber() {
    	assertEquals(TypeCoercions.coerce("0.5", BigDecimal.class), BigDecimal.valueOf(0.5));
    	assertEquals(TypeCoercions.coerce("1", BigInteger.class), BigInteger.valueOf(1));
    }

    @Test
    public void testCoerceStringToEnum() {
        assertEquals(TypeCoercions.coerce("STARTING", Lifecycle.class), Lifecycle.STARTING);
        assertEquals(TypeCoercions.coerce("Starting", Lifecycle.class), Lifecycle.STARTING);
        assertEquals(TypeCoercions.coerce("starting", Lifecycle.class), Lifecycle.STARTING);
        
        assertEquals(TypeCoercions.coerce("LOWERCASE", PerverseEnum.class), PerverseEnum.lowercase);
        assertEquals(TypeCoercions.coerce("CAMELCASE", PerverseEnum.class), PerverseEnum.camelCase);
        assertEquals(TypeCoercions.coerce("upper", PerverseEnum.class), PerverseEnum.UPPER);
        assertEquals(TypeCoercions.coerce("upper_with_underscore", PerverseEnum.class), PerverseEnum.UPPER_WITH_UNDERSCORE);
        assertEquals(TypeCoercions.coerce("LOWER_WITH_UNDERSCORE", PerverseEnum.class), PerverseEnum.lower_with_underscore);
    }
    public static enum PerverseEnum {
        lowercase,
        camelCase,
        UPPER,
        UPPER_WITH_UNDERSCORE,
        lower_with_underscore;
    }
    
    @Test(expectedExceptions = ClassCoercionException.class)
    public void testCoerceStringToEnumFailure() {
        TypeCoercions.coerce("scrambled-eggs", Lifecycle.class);
    }

    @Test
    public void testListToSetCoercion() {
        Set<?> s = TypeCoercions.coerce(ImmutableList.of(1), Set.class);
        assertEquals(s, ImmutableSet.of(1));
    }
    
    @Test
    public void testSetToListCoercion() {
        List<?> s = TypeCoercions.coerce(ImmutableSet.of(1), List.class);
        assertEquals(s, ImmutableList.of(1));
    }
    
    @Test
    public void testIterableToArrayCoercion() {
        String[] s = TypeCoercions.coerce(ImmutableList.of("a", "b"), String[].class);
        Assert.assertTrue(Arrays.equals(s, new String[] {"a", "b"}), "result="+Arrays.toString(s));
        
        Integer[] i = TypeCoercions.coerce(ImmutableList.of(1, 2), Integer[].class);
        Assert.assertTrue(Arrays.equals(i, new Integer[] {1, 2}), "result="+Arrays.toString(i));
        
        int[] i2 = TypeCoercions.coerce(ImmutableList.of(1, 2), int[].class);
        Assert.assertTrue(Arrays.equals(i2, new int[] {1, 2}), "result="+Arrays.toString(i2));
        
        int[] i3 = TypeCoercions.coerce(MutableSet.of("1", 2), int[].class);
        Assert.assertTrue(Arrays.equals(i3, new int[] {1, 2}), "result="+Arrays.toString(i3));
    }

    @Test
    public void testListEntryCoercion() {
        @SuppressWarnings("serial")
        List<?> s = TypeCoercions.coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<List<Class<?>>>() { });
        assertEquals(s, ImmutableList.of(Integer.class, Double.class));
    }
    
    @Test
    public void testListEntryToSetCoercion() {
        @SuppressWarnings("serial")
        Set<?> s = TypeCoercions.coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<Set<Class<?>>>() { });
        assertEquals(s, ImmutableSet.of(Integer.class, Double.class));
    }
    
    @Test
    public void testListEntryToCollectionCoercion() {
        @SuppressWarnings("serial")
        Collection<?> s = TypeCoercions.coerce(ImmutableList.of("java.lang.Integer", "java.lang.Double"), new TypeToken<Collection<Class<?>>>() { });
        assertEquals(s, ImmutableList.of(Integer.class, Double.class));
    }

    @Test
    public void testMapValueCoercion() {
        @SuppressWarnings("serial")
        Map<?,?> s = TypeCoercions.coerce(ImmutableMap.of("int", "java.lang.Integer", "double", "java.lang.Double"), new TypeToken<Map<String, Class<?>>>() { });
        assertMapsEqual(s, ImmutableMap.of("int", Integer.class, "double", Double.class));
    }
    
    @Test
    public void testMapKeyCoercion() {
        @SuppressWarnings("serial")
        Map<?,?> s = TypeCoercions.coerce(ImmutableMap.of("java.lang.Integer", "int", "java.lang.Double", "double"), new TypeToken<Map<Class<?>, String>>() { });
        assertMapsEqual(s, ImmutableMap.of(Integer.class, "int", Double.class, "double"));
    }

    @Test
    public void testStringToListCoercion() {
        List<?> s = TypeCoercions.coerce("a,b,c", List.class);
        assertEquals(s, ImmutableList.of("a", "b", "c"));
    }

    @Test
    @SuppressWarnings("serial")
    public void testCoerceRecursivelyStringToGenericsCollection() {
        assertEquals(TypeCoercions.coerce("1,2", new TypeToken<List<Integer>>() {}), ImmutableList.of(1, 2));
    }
    
    @Test
    public void testJsonStringToMapCoercion() {
        Map<?,?> s = TypeCoercions.coerce("{ \"a\" : \"1\", b : 2 }", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @Test
    public void testJsonStringWithoutQuotesToMapCoercion() {
        Map<?,?> s = TypeCoercions.coerce("{ a : 1 }", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", 1));
    }

    @Test
    public void testJsonComplexTypesToMapCoercion() {
        Map<?,?> s = TypeCoercions.coerce("{ a : [1, \"2\", '\"3\"'], b: { c: d, 'e': \"f\" } }", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", ImmutableList.<Object>of(1, "2", "\"3\""),
            "b", ImmutableMap.of("c", "d", "e", "f")));
    }

    @Test
    public void testJsonStringWithoutBracesToMapCoercion() {
        Map<?,?> s = TypeCoercions.coerce("a : 1", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", 1));
    }

    @Test
    public void testJsonStringWithoutBracesWithMultipleToMapCoercion() {
        Map<?,?> s = TypeCoercions.coerce("a : 1, b : 2", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", 1, "b", 2));
    }

    @Test(enabled = false) //never actually worked, only seemed to because of bug in assertEquals(Map,Map)
    public void testKeyEqualsValueStringToMapCoercion() {
        if (!CommonAdaptorTypeCoercions.PARSE_MAPS_WITH_EQUALS_SYMBOL) Assert.fail("Known to be unsupported");
        Map<?,?> s = TypeCoercions.coerce("a=1,b=2", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", "1", "b", "2"));
    }

    @Test
    public void testJsonStringWithoutBracesOrSpaceDisallowedAsMapCoercion() {
        Maybe<Map> s1 = TypeCoercions.tryCoerce("a:1,b:2", Map.class);
        // NB: snakeyaml 1.17 required spaces after the colon, 1.21 accepts the above, but we explicitly disallow it
        // (mileage may vary if you do something like a:1,b=2)
        if (s1.isPresent()) Asserts.fail("Shouldn't allow colon without space when processing map; instead got: "+s1.get());

        Map<?,?> s = TypeCoercions.coerce("a: 1,b: 2", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", 1, "b", 2));
    }

    @Test(enabled = false) //never actually worked, only seemed to because of bug in assertEquals(Map,Map)
    public void testEqualsInBracesMapCoercionLax() {
        if (!CommonAdaptorTypeCoercions.PARSE_MAPS_WITH_EQUALS_SYMBOL) Assert.fail("Known to be unsupported");
        Map<?,?> s = TypeCoercions.coerce("{ a = 1, b = '2' }", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", 1, "b", "2"));
    }

    @Test(enabled = false) //never actually worked, only seemed to because of bug in assertEquals(Map,Map)
    public void testKeyEqualsOrColonValueWithBracesStringToMapCoercion() {
        if (!CommonAdaptorTypeCoercions.PARSE_MAPS_WITH_EQUALS_SYMBOL) Assert.fail("Known to be unsupported");
        Map<?,?> s = TypeCoercions.coerce("{ a=1, b: 2 }", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @Test(enabled = false) //never actually worked, only seemed to because of bug in assertEquals(Map,Map)
    public void testKeyEqualsOrColonValueWithoutBracesStringToMapCoercion() {
        if (!CommonAdaptorTypeCoercions.PARSE_MAPS_WITH_EQUALS_SYMBOL) Assert.fail("Known to be unsupported");
        Map<?,?> s = TypeCoercions.coerce("a=1, b: 2", Map.class);
        assertMapsEqual(s, ImmutableMap.of("a", "1", "b", 2));
    }

    @SuppressWarnings("serial")
    @Test
    public void testYamlMapsDontGoTooFarWhenWantingListOfString() {
        List<?> s = TypeCoercions.coerce("[ a: 1, b: 2 ]", List.class);
        assertEquals(s, ImmutableList.of(MutableMap.of("a", 1), MutableMap.of("b", 2)));
        
        s = TypeCoercions.coerce("[ a: 1, b : 2 ]", new TypeToken<List<String>>() {});
        assertEquals(s, ImmutableList.of("a: 1", "b : 2"));
    }

    @Test
    public void testURItoStringCoercion() {
        String s = TypeCoercions.coerce(URI.create("http://localhost:1234/"), String.class);
        assertEquals(s, "http://localhost:1234/");
    }

    @Test
    public void testURLtoStringCoercion() throws MalformedURLException {
        String s = TypeCoercions.coerce(new URL("http://localhost:1234/"), String.class);
        assertEquals(s, "http://localhost:1234/");
    }

    @Test
    public void testAs() {
        Integer x = TypeCoercions.coerce(new WithAs("3"), Integer.class);
        assertEquals(x, (Integer)3);
    }

    @Test
    public void testFrom() {
        WithFrom x = TypeCoercions.coerce("3", WithFrom.class);
        assertEquals(x.value, 3);
    }

    @Test
    public void testCoerceStringToNumber() {
        assertEquals(TypeCoercions.coerce("1", Number.class), 1);  // since Jackson is permtitted to coerce, this prefers integers over doubles, 2023-05
        assertEquals(TypeCoercions.coerce("1.0", Number.class), Double.valueOf(1.0));
    }

    @Test(expectedExceptions = org.apache.brooklyn.util.javalang.coerce.ClassCoercionException.class)
    public void testInvalidCoercionThrowsClassCoercionException() {
        TypeCoercions.coerce(new Object(), TypeToken.of(Integer.class));
    }

    @Test
    public void testCoercionFunction() {
        assertEquals(TypeCoercions.function(Double.class).apply("1"), Double.valueOf(1));
    }

    @Test
    public void testCoerceInstanceForClassnameAdapter() {
        List<String> loaderCalls = new CopyOnWriteArrayList<>();
        ClassLoaderUtils loader = new ClassLoaderUtils(getClass()) {
            @Override
            public Class<?> loadClass(String name) throws ClassNotFoundException {
                loaderCalls.add(name);
                return super.loadClass(name);
            }
        };
        
        TypeCoercions.BrooklynCommonAdaptorTypeCoercions.registerInstanceForClassnameAdapter(loader, MyInterface.class);
        MyInterface val = TypeCoercions.coerce(MyClazz.class.getName(), MyInterface.class);
        assertTrue(val instanceof MyClazz, "val="+val);
        assertEquals(loaderCalls, ImmutableList.of(MyClazz.class.getName()));
    }
    
    @Test(expectedExceptions = org.apache.brooklyn.util.javalang.coerce.ClassCoercionException.class)
    public void testInvalidCoerceInstanceForClassnameAdapterThrows() {
        TypeCoercions.BrooklynCommonAdaptorTypeCoercions.registerInstanceForClassnameAdapter(new ClassLoaderUtils(getClass()), MyInterface.class);
        TypeCoercions.coerce("wrongClassNameDoesNotExist", MyInterface.class);
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

    public static interface MyInterface {
    }
    
    public static class MyClazz implements MyInterface {
    }

    public static class ClassWithMap {
        Map<String,Object> properties = MutableMap.of();
        Map<String,List<MyClazz>> propsList = MutableMap.of();
    }

    @Test
    public void testObjectInMapCoercion() {
        ClassWithMap r1 =
                TypeCoercions.coerce(MutableMap.of("properties", MutableMap.of("x", 1)), ClassWithMap.class);
        assertEquals(r1.properties.get("x"), 1);

        r1 = TypeCoercions.coerce(MutableMap.of("properties", MutableMap.of("x", new MyClazz())), ClassWithMap.class);
        Asserts.assertInstanceOf(r1.properties.get("x"), MyClazz.class);

        r1 = TypeCoercions.coerce(MutableMap.of(
                "properties", MutableMap.of("x", new MyClazz()),
                "propsList", MutableMap.of("y", MutableList.of(new MyClazz()))
                ), ClassWithMap.class);
        Asserts.assertInstanceOf(((List)r1.propsList.get("y")).get(0), MyClazz.class);
    }

}
