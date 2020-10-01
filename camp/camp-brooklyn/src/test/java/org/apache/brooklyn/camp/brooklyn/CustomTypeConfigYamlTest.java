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
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.catalog.internal.BasicBrooklynCatalog;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerializationTest.SampleBean;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.text.Strings;
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
        return deployWithTestingCustomTypeObjectConfig(false, type, key);
    }
    protected Entity deployWithTestingCustomTypeObjectConfig(boolean useParameter, String type, ConfigKey<?> key) throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                useParameter ? Strings.lines(
                        "  brooklyn.parameters:",
                        "  - name: "+key.getName(),
                        "    type: "+type) : "",
                "  brooklyn.config:",
                "    "+key.getName()+":",
                !useParameter ?
                        "      type: "+type : "",
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

    public static final ConfigKey<Object> CONF_ANONYMOUS_OBJECT = ConfigKeys.newConfigKey(Object.class,
            "test.confAnonymous", "Configuration key that's declared as an Object, but not defined on the Entity, and should be our custom type; was it coerced when created?");
    public static final ConfigKey<TestingCustomType> CONF_ANONYMOUS_OBJECT_TYPED = ConfigKeys.newConfigKey(TestingCustomType.class,
            "test.confAnonymous", "Configuration key that's declared as our custom type, matching the key name as the Object, and also not defined on the Entity, and should be our custom type; is it coercible on read with this key (or already coerced)?");

    // old behaviour; previously java wouldn't be deserialized, but now if we are in the context of an entity,
    // we use its classpath when deserializing
    // TODO ideally restrict this behaviour to the outermost layer, where the type is defined (and so we have to use java),
    // but make any type: blocks within only valid for registered types
//    @Test
//    public void testJavaTypeDeclaredInValueOfAnonymousConfigKey_IgnoresType_ReturnsMap() throws Exception {
//        // java types are not permitted as the type of a value of a config key - it gets deserialized as a map
//        Entity testEntity = deployWithTestingCustomTypeObjectConfig(false, TestingCustomType.class.getName(), CONF_ANONYMOUS_OBJECT);
//        Object customObj = testEntity.getConfig(CONF_ANONYMOUS_OBJECT);
//
//        Assert.assertNotNull(customObj);
//
//        Asserts.assertInstanceOf(customObj, Map.class);
//        Asserts.assertEquals(((Map<?,?>)customObj).get("x"), "foo");
//    }
//    @Test
//    public void testJavaTypeDeclaredInValueOfAnonymousConfigKey_IgnoresType_FailsCoercionToCustomType() throws Exception {
//        // and if we try to access it with a typed key it fails
//        Asserts.assertFailsWith(() -> deployWithTestingCustomTypeObjectConfigAndAssert(TestingCustomType.class.getName(), CONF_ANONYMOUS_OBJECT_TYPED, "foo", null),
//                e -> Asserts.expectedFailureContains(e, "TestingCustomType", "map", "test.confAnonymous"));
//    }
    // new behaviour, cf above
    @Test
    public void testJavaTypeDeclaredInValueOfAnonymousConfigKey_TypeInferred() throws Exception {
        // java types now set on read, so okay as value for config
        deployWithTestingCustomTypeObjectConfigAndAssert(TestingCustomType.class.getName(), CONF_ANONYMOUS_OBJECT_TYPED, "foo", null);
    }
    @Test
    public void testJavaTypeDeclaredInValueOfAnonymousConfigKey_TypeMatched() throws Exception {
        // java types now set on read, so okay as value for config
        deployWithTestingCustomTypeObjectConfigAndAssert(TestingCustomType.class.getName(), CONF_ANONYMOUS_OBJECT_TYPED, "foo", null);
    }

    @Test
    public void testRegisteredTypeDeclaredInValueOfAnonymousConfigKey_CoercedOnCreation_ReturnsCustomType() throws Exception {
        // however if the value states its type as a registered type (and of course bean with type allows java type in definitions)
        // then it _is_ read and accessed as the custom type
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeConfigYamlTest.TestingCustomType.class.getName())), false);
        Entity testEntity = deployWithTestingCustomTypeObjectConfigAndAssert("custom-type", CONF_ANONYMOUS_OBJECT,
                "foo", null);
        // and of course now accessing it with the typed key works
        Asserts.assertInstanceOf(testEntity.getConfig(CONF_ANONYMOUS_OBJECT_TYPED), TestingCustomType.class);
    }
    @Test
    public void testRegisteredTypeDeclaredInValueOfAnonymousConfigKey_CoercedOnCreation_InheritedFieldsWork() throws Exception {
        // in the above case, fields are correctly inherited from ancestors and overridden
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeConfigYamlTest.TestingCustomType.class.getName()+"\n" +
                                "x: unfoo\n"+
                                "y: bar")), false);
        deployWithTestingCustomTypeObjectConfigAndAssert("custom-type", CONF_ANONYMOUS_OBJECT,
                "foo", "bar");
    }
    @Test
    public void testRegisteredTypeDeclaredInValueOfAnonymousConfigKey_CoercedOnCreation_InheritedFieldsWork_BeanAddCatalogSyntax() throws Exception {
        // in the above case, fields are correctly inherited from ancestors and overridden
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: "+TEST_VERSION,
                "  items:",
                "  - id: custom-type",
//                "    itemType: bean",             // optional
//                "    format: bean-with-type",     // optional
                "    item:",
                "      type: "+CustomTypeConfigYamlTest.TestingCustomType.class.getName(),
                "      x: unfoo",
                "      y: bar");

        RegisteredType item = mgmt().getTypeRegistry().get("custom-type", TEST_VERSION);
        Assert.assertNotNull(item);
        Assert.assertEquals(item.getKind(), RegisteredTypeKind.BEAN);

        deployWithTestingCustomTypeObjectConfigAndAssert("custom-type", CONF_ANONYMOUS_OBJECT,
                "foo", "bar");
    }

    // TODO - implicit typing - deserialization base type implied by key type.  on creation?  or on access?
    // probably on access, by config lookup.  though that will need custom logic for initializers and maybe others,
    // in addition to the obvious place(s) where it is done for entities
    //
    // test:  type declared on a parameter; get as object it recasts it as official type, and coerces
    // testRegisteredTypeImplicitInValueReadByTypedConfigKey_CoercedOnCreation
    // testRegisteredTypeImplicitInValueReadByObjectConfigKey_ReturnsMap
    // (and change testJavaTypeDeclaredInValueOfAnonymousConfigKey_IgnoresType_FailsCoercionToCustomType,
    //      if the java type exactly matches the expected type)

    // TODO DSL expressions inside these types might mess things up
    // they might just work, or maybe just when wrapped in ValueSupplier, or maybe they break horribly ...
    // can make jackson serialize and deserialize them specially, either pass-through or as strings TBD
    // see reference to DslSerializationAsToString in BeanWithTypeUtils


    @Test
    public void testRegisteredTypeMalformed_GoodError() throws Exception {
        // in the above case, fields are correctly inherited from ancestors and overridden
        Asserts.assertFailsWith(() -> {
                    addCatalogItems(
                            "brooklyn.catalog:",
                            "  version: " + TEST_VERSION,
                            "  items:",
                            "  - id: custom-type",
                            "    itemType: bean",         // optional - but force it here
                            "    format: bean-with-type", // optional - but force it here
                            "    item:",
                            "      type: " + CustomTypeConfigYamlTest.TestingCustomType.class.getName(),
                            "      x: {}");
                }, e -> {
                    Asserts.expectedFailureContainsIgnoreCase(e, "bean", "custom-type", "cannot deserialize", "string", "\"x\"");
                    return true;
                });
    }
}
