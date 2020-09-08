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
package org.apache.brooklyn.camp.brooklyn;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

@Test
public class CustomTypeConfigYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(CustomTypeConfigYamlTest.class);

    protected Entity setupAndCheckTestEntityInBasicYamlWith(String ...extras) throws Exception {
        Entity app = createAndStartApplication(loadYaml("test-entity-basic-template.yaml", extras));
        waitForApplicationTasks(app);

        Dumper.dumpInfo(app);
        
        Assert.assertEquals(app.getDisplayName(), "test-entity-basic-template");

        Assert.assertTrue(app.getChildren().iterator().hasNext(), "Expected app to have child entity");
        Entity entity = app.getChildren().iterator().next();
        Assert.assertTrue(entity instanceof TestEntity, "Expected TestEntity, found " + entity.getClass());
        
        return entity;
    }

    public static class TestingCustomType {
        String x;
        String y;
    }
    
    protected Entity deployWithTestingCustomTypeObjectConfig(String type, ConfigKey<?> key) throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith( 
            "  brooklyn.config:",
            "    "+key.getName()+":",
            "      type: "+type,
            "      x: foo");
    }

    protected void assertObjectIsOurCustomTypeWithFieldValues(Object customObj, String x, String y) {
        Assert.assertNotNull(customObj);

        Asserts.assertInstanceOf(customObj, TestingCustomType.class);
        Asserts.assertEquals(((TestingCustomType)customObj).x, x);
        Asserts.assertEquals(((TestingCustomType)customObj).y, y);
    }

    protected Entity deployWithTestingCustomTypeObjectConfigAndAssert(String type, ConfigKey<?> key, String x, String y) throws Exception {
        Entity testEntity = deployWithTestingCustomTypeObjectConfig(type, key);
        Object customObj = testEntity.getConfig(key);
        assertObjectIsOurCustomTypeWithFieldValues(customObj, x, y);
        return testEntity;
    }

    @Test
    public void testCustomTypeInObjectConfigKeyReturnsMap() throws Exception {
        // baseline behaviour - if the config is of type 'object' there is no conversion; you get raw json map

        Entity testEntity = deployWithTestingCustomTypeObjectConfig(TestingCustomType.class.getName(), TestEntity.CONF_OBJECT);
        Object customObj = testEntity.getConfig(TestEntity.CONF_OBJECT);

        Assert.assertNotNull(customObj);

        Asserts.assertInstanceOf(customObj, Map.class);
        Asserts.assertEquals(((Map<?,?>)customObj).get("x"), "foo");
    }

    public static final ConfigKey<TestingCustomType> CONF_OBJECT_TYPED = ConfigKeys.newConfigKey(TestingCustomType.class, 
        "test.confTyped", "Configuration key that's our custom type");
    
    @Test
    public void testCustomTypeInTypedConfigKeyJavaType() throws Exception {
        // if the config key is typed, coercion returns the strongly typed value, correctly deserializing the java type;
        // but types used in config must be registered types
        Asserts.assertFailsWith(() -> deployWithTestingCustomTypeObjectConfigAndAssert(TestingCustomType.class.getName(), CONF_OBJECT_TYPED, "foo", null),
                e -> Asserts.expectedFailureContains(e, "TestingCustomType", "map", "test.confTyped"));
    }
    
    @Test
    public void testCustomTypeInTypedConfigKeyRegisteredType() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeConfigYamlTest.TestingCustomType.class.getName())), false);
        // if the config key is typed, coercion returns the strongly typed value, correctly deserializing the brooklyn registered type
        Entity testEntity = deployWithTestingCustomTypeObjectConfigAndAssert("custom-type", CONF_OBJECT_TYPED,
                "foo", null);
    }

    @Test
    public void testCustomTypeInTypedConfigKeyRegisteredTypeWithBeanWithTypeFields() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeConfigYamlTest.TestingCustomType.class.getName()+"\n" +
                        "x: unfoo\n"+
                        "y: bar")), false);
        // if the config key is typed, coercion returns the strongly typed value, correctly deserializing the brooklyn registered type
        Entity testEntity = deployWithTestingCustomTypeObjectConfigAndAssert("custom-type", CONF_OBJECT_TYPED,
                "foo", "bar");
    }

}
