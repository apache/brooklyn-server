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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import org.apache.brooklyn.util.javalang.coerce.CommonAdaptorTypeCoercions;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercer;
import org.apache.brooklyn.util.javalang.coerce.TypeCoercerExtensible;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
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
            for (int yi: yy) constructorArgs.add(yi);
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
    public void testInvocationCoercingArgs() throws Exception {
        TypeCoercerExtensible rawCoercer = TypeCoercerExtensible.newDefault();
        new CommonAdaptorTypeCoercions(rawCoercer).registerAllAdapters();
        Optional<TypeCoercer> coercer = Optional.<TypeCoercer>of(rawCoercer);
        
        Method m1Short = CI1.class.getMethod("m1", String.class, int.class);
        Method m1Long = CI1.class.getMethod("m1", String.class, int.class, int.class, int[].class);
        Assert.assertEquals(m1Short.invoke(null, "hello", 1), "hello1");
        Assert.assertEquals(m1Long.invoke(null, "hello", 1, 2, new int[] { 3, 4}), "hello10");
        
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList('a', "2"), false, coercer).get(), "a2");
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList('a', "3", "4", "5"), false, coercer).get(), "a12");
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList('a', (byte)3, (byte)4, (byte)5), false, coercer).get(), "a12");
    }
    
    @Test
    public void testMethodInvocation() throws Exception {
        Method m1Short = CI1.class.getMethod("m1", String.class, int.class);
        Method m1Long = CI1.class.getMethod("m1", String.class, int.class, int.class, int[].class);
        
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, m1Short, Arrays.<Object>asList("hello", 3)), "hello3");
        Assert.assertEquals(Reflections.invokeMethodFromArgs(CI1.class, m1Long, Arrays.<Object>asList("hello", 3, 4, 5)), "hello12");
    }
    
    @Test
    public void testGetMethod() throws Exception {
        Method m1Short = CI1.class.getMethod("m1", String.class, int.class);
        Method m1Long = CI1.class.getMethod("m1", String.class, int.class, int.class, int[].class);
        
        Assert.assertEquals(Reflections.getMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList("hello", 3)).get(), m1Short);
        Assert.assertEquals(Reflections.getMethodFromArgs(CI1.class, "m1", Arrays.<Object>asList("hello", 3, 4, 5)).get(), m1Long);
    }
    
    @Test
    public void testConstruction() throws Exception {
        Assert.assertEquals(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"hello", 3}).get().constructorArgs, ImmutableList.of("hello", 3));
        Assert.assertEquals(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"hello", 3, 4, 5}).get().constructorArgs, ImmutableList.of("hello", 3, 4, 5));
        Assert.assertFalse(Reflections.invokeConstructorFromArgs(CI1.class, new Object[] {"wrong", "args"}).isPresent());
    }

    @Test
    public void testArrayToList() throws Exception {
        assertEquals(Reflections.arrayToList(new String[] {"a", "b"}), ImmutableList.of("a", "b"));
        assertEquals(Reflections.arrayToList((Object) new String[] {"a", "b"}), ImmutableList.of("a", "b"));
    }
    
    @Test
    public void testFindAccessibleMethodFromSuperType() throws Exception {
        Method objectHashCode = Object.class.getMethod("hashCode", new Class[0]);
        Method methodOnSuperClass = PublicSuperClass.class.getMethod("methodOnSuperClass", new Class[0]);
        Method subMethodOnSuperClass = PrivateClass.class.getMethod("methodOnSuperClass", new Class[0]);
        Method methodOnInterface = PublicInterface.class.getMethod("methodOnInterface", new Class[0]);
        Method subMethodOnInterface = PrivateClass.class.getMethod("methodOnInterface", new Class[0]);
        
        assertEquals(Reflections.findAccessibleMethod(objectHashCode), objectHashCode);
        assertEquals(Reflections.findAccessibleMethod(methodOnSuperClass), methodOnSuperClass);
        assertEquals(Reflections.findAccessibleMethod(methodOnInterface), methodOnInterface);
        assertEquals(Reflections.findAccessibleMethod(subMethodOnSuperClass), methodOnSuperClass);
        assertEquals(Reflections.findAccessibleMethod(subMethodOnInterface), methodOnInterface);
    }
    
    @Test
    public void testFindAccessibleMethodCallsSetAccessible() throws Exception {
        Method inaccessibleOtherMethod = PrivateClass.class.getMethod("otherMethod", new Class[0]);
        
        assertFalse(inaccessibleOtherMethod.isAccessible());
        assertEquals(Reflections.findAccessibleMethod(inaccessibleOtherMethod), inaccessibleOtherMethod);
        assertTrue(inaccessibleOtherMethod.isAccessible());
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

    public static abstract class PublicSuperClass {
        public abstract void methodOnSuperClass();
    }
        
    public static interface PublicInterface {
        public void methodOnInterface();
    }
        
    static class PrivateClass extends PublicSuperClass implements PublicInterface {
        public PrivateClass() {}
        
        @Override
        public void methodOnSuperClass() {}
        
        @Override
        public void methodOnInterface() {}
        
        public void otherMethod() {}
    }
}
