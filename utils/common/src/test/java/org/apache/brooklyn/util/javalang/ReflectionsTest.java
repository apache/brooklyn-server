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
package org.apache.brooklyn.util.javalang;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

public class ReflectionsTest {

    @Test
    public void testFindPublicMethodsOrderedBySuper() throws Exception {
        List<Method> methods = Reflections.findPublicMethodsOrderedBySuper(MySubClass.class);
        assertContainsInOrder(methods, ImmutableList.of(
                MyInterface.class.getMethod("mymethod"), 
                MySuperClass.class.getMethod("mymethod"), 
                MySubClass.class.getMethod("mymethod")));
        assertNoDuplicates(methods);
    }
    
    @Test
    public void testFindPublicFieldsOrdereBySuper() throws Exception {
        List<Field> fields = Reflections.findPublicFieldsOrderedBySuper(MySubClass.class);
        assertContainsInOrder(fields, ImmutableList.of(
                MyInterface.class.getField("MY_FIELD"), 
                MySuperClass.class.getField("MY_FIELD"), 
                MySubClass.class.getField("MY_FIELD")));
        assertNoDuplicates(fields);
    }
    
    @Test
    public void testConstructLangObject() {
        // special test for this because the lang object might have classloader null
        Assert.assertTrue(Reflections.invokeConstructorFromArgs(java.util.Date.class).get() instanceof java.util.Date);
    }
    
    public static interface MyInterface {
        public static final int MY_FIELD = 0;
        public void mymethod();
    }
    public static class MySuperClass implements MyInterface {
        public static final int MY_FIELD = 0;
        
        @Override public void mymethod() {}
    }
    public static class MySubClass extends MySuperClass implements MyInterface {
        public static final int MY_FIELD = 0;
        @Override public void mymethod() {}
    }
    
    private void assertContainsInOrder(List<?> actual, List<?> subsetExpected) {
        int lastIndex = -1;
        for (Object e : subsetExpected) {
            int index = actual.indexOf(e);
            assertTrue(index >= 0 && index > lastIndex, "actual="+actual);
            lastIndex = index;
        }
    }
    
    private void assertNoDuplicates(List<?> actual) {
        assertEquals(actual.size(), Sets.newLinkedHashSet(actual).size(), "actual="+actual);
    }
    
    public static class CI1 {
        public final List<Object> constructorArgs;
        
        public CI1() {
            constructorArgs = ImmutableList.of();
        }
        public CI1(String x, int y) {
            constructorArgs = ImmutableList.<Object>of(x, y);
        }
        public CI1(String x, int y0, int y1, int ...yy) {
            constructorArgs = Lists.newArrayList();
            constructorArgs.addAll(ImmutableList.of(x, y0, y1));
            for (int yi: yy) constructorArgs.add((Integer)yi);
        }
        public static String m1(String x, int y) {
            return x+y;
        }
        public static String m1(String x, int y0, int y1, int ...yy) {
            int Y = y0 + y1;;
            for (int yi: yy) Y += yi;
            return x+Y;
        }
    }

    @Test
    public void testTypesMatch() throws Exception {
        Assert.assertTrue(Reflections.typesMatch(new Object[] { 3 }, new Class[] { Integer.class } ));
        Assert.assertTrue(Reflections.typesMatch(new Object[] { 3 }, new Class[] { int.class } ), "auto-boxing failure");
    }
    
    @Test
    public void testInvocation() throws Exception {
        Method m = CI1.class.getMethod("m1", String.class, int.class, int.class, int[].class);
        Assert.assertEquals(m.invoke(null, "hello", 1, 2, new int[] { 3, 4}), "hello10");
        
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList("hello", 3)).get(), "hello3");
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList("hello", 3, 4, 5)).get(), "hello12");
    }
    
    @Test
    public void testFindConstructors() throws Exception {
        Asserts.assertPresent(Reflections.findConstructorExactMaybe(String.class, String.class));
        Asserts.assertNotPresent(Reflections.findConstructorExactMaybe(String.class, Object.class));
    }

    @Test
    public void testConstruction() throws Exception {
        Assert.assertEquals(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"hello", 3}).get().constructorArgs, ImmutableList.of("hello", 3));
        Assert.assertEquals(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"hello", 3, 4, 5}).get().constructorArgs, ImmutableList.of("hello", 3, 4, 5));
        Assert.assertFalse(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"wrong", "args"}).isPresent());
    }

    interface I { };
    interface J extends I { };
    interface K extends I, J { };
    interface L { };
    interface M { };
    class A implements I { };
    class B extends A implements L { };
    class C extends B implements M, K { };
    
    @Test
    public void testGetAllInterfaces() throws Exception {
        Assert.assertEquals(Reflections.getAllInterfaces(A.class), ImmutableList.of(I.class));
        Assert.assertEquals(Reflections.getAllInterfaces(B.class), ImmutableList.of(L.class, I.class));
        Assert.assertEquals(Reflections.getAllInterfaces(C.class), ImmutableList.of(M.class, K.class, I.class, J.class, L.class));
    }

    public static class FF1 {
        int y;
        public int x;
        public FF1(int x, int y) { this.x = x; this.y = y; }
    }
    public static class FF2 extends FF1 {
        public int z;
        int x;
        public FF2(int x, int y, int x2, int z) { super(x, y); this.x = x2; this.z = z; }
    }
    
    @Test
    public void testFindPublicFields() throws Exception {
        List<Field> fields = Reflections.findPublicFieldsOrderedBySuper(FF2.class);
        if (fields.size() != 2) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "z");
    }
    
    @Test
    public void testFindAllFields() throws Exception {
        List<Field> fields = Reflections.findFields(FF2.class, null, null);
        // defaults to SUB_BEST_FIELD_LAST_THEN_ALPHA
        if (fields.size() != 4) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "y");
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "z");
    }

    @Test
    public void testFindAllFieldsSubBestFirstThenAlpha() throws Exception {
        List<Field> fields = Reflections.findFields(FF2.class, null, FieldOrderings.SUB_BEST_FIELD_FIRST_THEN_ALPHABETICAL);
        if (fields.size() != 4) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "z");
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "y");
    }

    @Test
    public void testFindAllFieldsSubBestLastThenAlpha() throws Exception {
        List<Field> fields = Reflections.findFields(FF2.class, null, FieldOrderings.SUB_BEST_FIELD_LAST_THEN_ALPHABETICAL);
        if (fields.size() != 4) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "y");
        Assert.assertEquals(fields.get(i++).getName(), "x");
        Assert.assertEquals(fields.get(i++).getName(), "z");
    }

    @Test
    public void testFindAllFieldsAlphaSubBestFirst() throws Exception {
        List<Field> fields = Reflections.findFields(FF2.class, null, FieldOrderings.ALPHABETICAL_FIELD_THEN_SUB_BEST_FIRST);
        if (fields.size() != 4) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        Assert.assertEquals(fields.get(i).getName(), "x");
        Assert.assertEquals(fields.get(i++).getDeclaringClass(), FF2.class);
        Assert.assertEquals(fields.get(i).getName(), "x");
        Assert.assertEquals(fields.get(i++).getDeclaringClass(), FF1.class);
        Assert.assertEquals(fields.get(i++).getName(), "y");
        Assert.assertEquals(fields.get(i++).getName(), "z");
    }

    @Test
    public void testFindAllFieldsNotAlpha() throws Exception {
        // ?? - does this test depend on the JVM?  it preserves the default order of fields
        List<Field> fields = Reflections.findFields(FF2.class, null, FieldOrderings.SUB_BEST_FIELD_LAST_THEN_DEFAULT);
        if (fields.size() != 4) Assert.fail("Wrong number of fields: "+fields);
        int i=0;
        // can't say more about order than this
        Assert.assertEquals(MutableSet.of(fields.get(i++).getName(), fields.get(i++).getName()), 
            MutableSet.of("x", "y"));
        Assert.assertEquals(MutableSet.of(fields.get(i++).getName(), fields.get(i++).getName()), 
            MutableSet.of("x", "z"));
    }

    @Test
    public void testFindField() throws Exception {
        FF2 f2 = new FF2(1,2,3,4);
        Field fz = Reflections.findField(FF2.class, "z");
        Assert.assertEquals(fz.get(f2), 4);
        Field fx2 = Reflections.findField(FF2.class, "x");
        Assert.assertEquals(fx2.get(f2), 3);
        Field fy = Reflections.findField(FF2.class, "y");
        Assert.assertEquals(fy.get(f2), 2);
        Field fx1 = Reflections.findField(FF1.class, "x");
        Assert.assertEquals(fx1.get(f2), 1);
        
        Field fxC2 = Reflections.findField(FF2.class, FF2.class.getCanonicalName()+"."+"x");
        Assert.assertEquals(fxC2.get(f2), 3);
        Field fxC1 = Reflections.findField(FF2.class, FF1.class.getCanonicalName()+"."+"x");
        Assert.assertEquals(fxC1.get(f2), 1);
    }

    @Test
    public void testGetFieldValue() {
        FF2 f2 = new FF2(1,2,3,4);
        Assert.assertEquals(Reflections.getFieldValueMaybe(f2, "x").get(), 3);
        Assert.assertEquals(Reflections.getFieldValueMaybe(f2, "y").get(), 2);
        
        Assert.assertEquals(Reflections.getFieldValueMaybe(f2, FF2.class.getCanonicalName()+"."+"x").get(), 3);
        Assert.assertEquals(Reflections.getFieldValueMaybe(f2, FF1.class.getCanonicalName()+"."+"x").get(), 1);
    }

    @SuppressWarnings("rawtypes")
    static class MM1 {
        public void foo(List l) {}
        @SuppressWarnings("unused")
        private void bar(List l) {}
    }

    @SuppressWarnings("rawtypes")
    static class MM2 extends MM1 {
        public void foo(ArrayList l) {}
    }

    @Test
    public void testFindMethods() {
        Asserts.assertSize(Reflections.findMethodsCompatible(MM2.class, "foo", ArrayList.class), 2);
        Asserts.assertSize(Reflections.findMethodsCompatible(MM2.class, "foo", List.class), 1);
        Asserts.assertSize(Reflections.findMethodsCompatible(MM2.class, "foo", Object.class), 0);
        Asserts.assertSize(Reflections.findMethodsCompatible(MM2.class, "foo", Map.class), 0);
        Asserts.assertSize(Reflections.findMethodsCompatible(MM2.class, "bar", List.class), 1);
        Asserts.assertSize(Reflections.findMethodsCompatible(MM1.class, "bar", ArrayList.class), 1);
    }

    @Test
    public void testFindMethod() {
        Asserts.assertTrue(Reflections.findMethodMaybe(MM2.class, "foo", ArrayList.class).isPresent());
        Asserts.assertTrue(Reflections.findMethodMaybe(MM2.class, "foo", List.class).isPresent());
        Asserts.assertTrue(Reflections.findMethodMaybe(MM2.class, "foo", Object.class).isAbsent());
        Asserts.assertTrue(Reflections.findMethodMaybe(MM2.class, "bar", List.class).isPresent());
        Asserts.assertTrue(Reflections.findMethodMaybe(MM2.class, "bar", ArrayList.class).isAbsent());
    }
    
    @Test
    public void testHasSerializableMethods() {
        Asserts.assertFalse(Reflections.hasSpecialSerializationMethods(MM2.class));
        Asserts.assertTrue(Reflections.hasSpecialSerializationMethods(LinkedHashMap.class));
    }
    
}
