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
package org.apache.brooklyn.camp.yoml;

import java.util.Arrays;

import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer.JavaClassNameTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsAtTopLevel;
import org.testng.Assert;
import org.testng.annotations.Test;

public class YomlTypeRegistryTest extends BrooklynMgmtUnitTestSupport {

    private BasicBrooklynTypeRegistry registry() {
        return (BasicBrooklynTypeRegistry) mgmt.getTypeRegistry();
    }
    
    private void add(RegisteredType type) {
        add(type, false);
    }
    private void add(RegisteredType type, boolean canForce) {
        registry().addToLocalUnpersistedTypeRegistry(type, canForce);
    }
    
    public static class ItemA {
        String name;
    }
    
    private final static RegisteredType SAMPLE_TYPE_JAVA = RegisteredTypes.bean("java.A", "1", 
        new JavaClassNameTypeImplementationPlan(ItemA.class.getName()), ItemA.class);

    @Test
    public void testInstantiateYomlPlan() {
        add(SAMPLE_TYPE_JAVA);
        Object x = registry().createBeanFromPlan("yoml", "{ type: java.A }", null, null);
        Assert.assertTrue(x instanceof ItemA);
    }

    @Test
    public void testInstantiateYomlPlanExplicitField() {
        add(SAMPLE_TYPE_JAVA);
        Object x = registry().createBeanFromPlan("yoml", "{ type: java.A, fields: { name: Bob } }", null, null);
        Assert.assertTrue(x instanceof ItemA);
        Assert.assertEquals( ((ItemA)x).name, "Bob" );
    }

    private static RegisteredType sampleTypeYoml(String typeName, String typeDefName) {
        return BrooklynYomlTypeRegistry.newYomlRegisteredType(RegisteredTypeKind.BEAN, 
            // symbolicName, version, 
            typeName==null ? "yoml.A" : typeName, "1", 
            // planData, 
            "{ type: "+ (typeDefName==null ? ItemA.class.getName() : typeDefName) +" }", 
            // javaConcreteType, superTypesAsClassOrRegisteredType, serializers)
            ItemA.class, Arrays.asList(ItemA.class), null);
    }
    private final static RegisteredType SAMPLE_TYPE_YOML = sampleTypeYoml(null, null);

    @Test
    public void testInstantiateYomlBaseType() {
        add(SAMPLE_TYPE_YOML);
        Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.A }", null, null);
        Assert.assertTrue(x instanceof ItemA);
    }
    
    @Test
    public void testInstantiateYomlBaseTypeJavaPrefix() {
        add(sampleTypeYoml(null, "'java:"+ItemA.class.getName()+"'"));
        Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.A }", null, null);
        Assert.assertTrue(x instanceof ItemA);
    }
    
    @Test
    public void testInstantiateYomlBaseTypeSameName() {
        add(sampleTypeYoml(ItemA.class.getName(), null));
        Object x = registry().createBeanFromPlan("yoml", "{ type: "+ItemA.class.getName()+" }", null, null);
        Assert.assertTrue(x instanceof ItemA);
    }
    
    @Test
    public void testInstantiateYomlBaseTypeExplicitField() {
        add(SAMPLE_TYPE_YOML);
        Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.A, fields: { name: Bob } }", null, null);
        Assert.assertTrue(x instanceof ItemA);
        Assert.assertEquals( ((ItemA)x).name, "Bob" );
    }
    
    @Test
    public void testYomlTypeMissingGiveGoodError() {
        try {
            Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.A, fields: { name: Bob } }", null, null);
            Asserts.shouldHaveFailedPreviously("Expected type resolution failure; instead it loaded "+x);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "yoml.A", "neither", "registry", "classpath");
            Asserts.expectedFailureDoesNotContain(e, JavaClassNames.simpleClassName(ClassNotFoundException.class));
        }
    }

    @Test
    public void testYomlTypeMissingGiveGoodErrorNested() {
        add(sampleTypeYoml("yoml.B", "yoml.A"));
        try {
            Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.B, fields: { name: Bob } }", null, null);
            Asserts.shouldHaveFailedPreviously("Expected type resolution failure; instead it loaded "+x);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "yoml.B", "yoml.A", "neither", "registry", "classpath");
            Asserts.expectedFailureDoesNotContain(e, JavaClassNames.simpleClassName(ClassNotFoundException.class));
        }
    }

    @YomlAllFieldsAtTopLevel
    @Alias("item-annotated")
    public static class ItemAn {
        final static RegisteredType YOML = BrooklynYomlTypeRegistry.newYomlRegisteredType(RegisteredTypeKind.BEAN, 
            null, "1", ItemAn.class);
        
        String name;
    }

    @Test
    public void testInstantiateAnnotatedYoml() {
        add(ItemAn.YOML);
        Object x = registry().createBeanFromPlan("yoml", "{ type: item-annotated, name: bob }", null, null);
        Assert.assertTrue(x instanceof ItemAn);
        Assert.assertEquals( ((ItemAn)x).name, "bob" );
    }

}
