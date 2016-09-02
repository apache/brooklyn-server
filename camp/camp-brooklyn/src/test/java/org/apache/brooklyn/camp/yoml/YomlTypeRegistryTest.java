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

import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.yoml.YomlTypePlanTransformer.YomlTypeImplementationPlan;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer.JavaClassNameTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.javalang.JavaClassNames;
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

    private final static RegisteredType SAMPLE_TYPE_YOML = RegisteredTypes.bean("yoml.A", "1", 
        new YomlTypeImplementationPlan("{ type: "+ItemA.class.getName() +" }"), ItemA.class);

    @Test
    public void testInstantiateYomlBaseType() {
        add(SAMPLE_TYPE_YOML);
        Object x = registry().createBeanFromPlan("yoml", "{ type: yoml.A }", null, null);
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

}
