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

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.reflect.TypeToken;
import java.util.List;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.typereg.BrooklynTypeRegistry.RegisteredTypeKind;
import org.apache.brooklyn.api.typereg.RegisteredType;
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
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

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

    public interface Marker {}
    public static class TestingCustomType implements Marker {
        String x;
        String y;
    }
    
    protected Entity deployWithTestingCustomTypeObjectConfig(boolean declareParameter, boolean declareType, boolean isList, String type, ConfigKey<?> key) throws Exception {
        return deployWithTestingCustomTypeObjectConfig(declareParameter, declareType, isList, type, key, true);
    }
    protected Entity deployWithTestingCustomTypeObjectConfig(boolean declareParameter, boolean declareType, boolean isList, String type, ConfigKey<?> key, boolean includeValue) throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(
                declareParameter ? Strings.lines(
                        "  brooklyn.parameters:",
                        "  - name: "+key.getName(),
                        "    type: "+type) : "",
                "  brooklyn.config:",
                "    "+key.getName()+":",
                isList ? "    - " : "",
                declareType ?
                        "      type: "+(isList ? Strings.removeAllFromEnd(Strings.removeAllFromStart(type, "list", "<"), ">") : type) : "",
                includeValue ? "      x: foo" : "");
    }

    protected void assertObjectIsOurCustomTypeWithFieldValues(Object customObj, String x, String y) {
        Assert.assertNotNull(customObj);

        Asserts.assertInstanceOf(customObj, TestingCustomType.class);
        Asserts.assertEquals(((TestingCustomType)customObj).x, x);
        Asserts.assertEquals(((TestingCustomType)customObj).y, y);
    }

    protected void assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(ConfigKey<?> key, String x, String y) {
        Object customObj = lastDeployedEntity.getConfig(key);
        if (customObj instanceof List) {
            customObj = Iterables.getOnlyElement((List)customObj);
        }
        assertObjectIsOurCustomTypeWithFieldValues(customObj, x, y);
    }
    protected Entity assertLastDeployedKeysValueIs_NOT_OurCustomTypeWithFieldValues(ConfigKey<?> key, String x, String y) {
        try {
            assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(key, x, y);
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable expected) {
            Asserts.expectedFailure(expected);
            lastThrowable = expected;
        }
        return lastDeployedEntity;
    }

    Entity lastDeployedEntity;
    Throwable lastThrowable;

    protected Entity deployWithTestingCustomTypeObjectConfigAndAssert(boolean declareParameter, boolean declareType, boolean isList, String type, ConfigKey<?> key, String x, String y) throws Exception {
        lastDeployedEntity = deployWithTestingCustomTypeObjectConfig(declareParameter, declareType, isList, type, key);
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(key, x, y);
        return lastDeployedEntity;
    }
    protected Entity deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(boolean declareParameter, boolean declareType, boolean isList, String type, ConfigKey<?> key, String x, String y) throws Exception {
        try {
            deployWithTestingCustomTypeObjectConfigAndAssert(declareParameter, declareType, isList, type, key, x, y);
            Asserts.shouldHaveFailedPreviously();
        } catch (Throwable expected) {
            Asserts.expectedFailure(expected);
            lastThrowable = expected;
        }
        return lastDeployedEntity;
    }

    protected void assertLastThrowableContainsIgnoreCase(String phrase1, String ...morePhrases) {
        Asserts.expectedFailureContainsIgnoreCase(lastThrowable, phrase1, morePhrases);
    }

    public static final ConfigKey<Object> CONF1_ANONYMOUS = ConfigKeys.newConfigKey(Object.class, "test.conf1");
    public static final ConfigKey<TestingCustomType> CONF1_TYPED = ConfigKeys.newConfigKey(TestingCustomType.class, "test.conf1");
    public static final ConfigKey<Marker> CONF1_MARKER = ConfigKeys.newConfigKey(Marker.class, "test.conf1");
    public static final ConfigKey<List<TestingCustomType>> CONF1_LIST_TYPED = ConfigKeys.newConfigKey(new TypeToken<List<TestingCustomType>>() {}, "test.conf1");

    @Test
    public void testJavaTypeDeclaredWithoutTypeInTypedParameter_TypeRetrievedWithAnonymousKey() throws Exception {
        // java type now not set when spec is analysed by camp,
        // but declared type of parameter used with bean with type on access
        // so when we get the object using an untyped key, and it is correctly typed
        deployWithTestingCustomTypeObjectConfigAndAssert(true, false, false, TestingCustomType.class.getName(), CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testJavaTypeDeclaredWithTypeInAnonymousParameter_TypeNotRetrievedWithAnonymousKey() throws Exception {
        // here the _parameter_ is not declared, but there is a 'type:' entry in the map; that is not sufficient,
        // because we don't convert when setting, and we don't need to convert to coerce to an object
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, true, false, TestingCustomType.class.getName(), CONF1_ANONYMOUS, "foo", null);
        assertLastThrowableContainsIgnoreCase("expected class " + TestingCustomType.class.getName().toLowerCase(), "but found", "map");

        // but access with the strongly typed key does work
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_TYPED, "foo", null);

//        // access with a marker-typed key also fail - coercion detects Map is not of the Marker interface, so tries bean-with-type conversion, but can't convert pure java type
//        // (the registered type variant does work)
//        assertLastDeployedKeysValueIs_NOT_OurCustomTypeWithFieldValues(CONF1_MARKER, "foo", null);
//        assertLastThrowableContainsIgnoreCase("cannot resolve", "beanwithtype", "marker");
        // if tryConvertOrAbsentUsingContext doesn't allow java types this fails per above; however if we do allow java types, which we now do, it works
        // due to the type above; however of course WithoutType (below) fails
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_MARKER, "foo", null);
    }
    @Test
    public void testJavaTypeDeclaredWithTypeInTypedParameter_TypeRetrievedOfCourse() throws Exception {
        // having _both_ a declared type on the parameter / config key name, and a type: line, that also works as expected
        deployWithTestingCustomTypeObjectConfigAndAssert(true, true, false, TestingCustomType.class.getName(), CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testJavaTypeDeclaredWithoutTypeInAnonymousParameter_TypeNotRetrievedOfCourse() throws Exception {
        // if there is no type info at all, the parameter not declared AND type: not in the map, of course there's no way it gets the right type
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, false, false, TestingCustomType.class.getName(), CONF1_ANONYMOUS, "foo", null);
        assertLastThrowableContainsIgnoreCase("expected class " + TestingCustomType.class.getName().toLowerCase(), "but found", "map");

        // typed-key access works
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_TYPED, "foo", null);
        // however marker-typed-key access doesn't work, not enough type info
        assertLastDeployedKeysValueIs_NOT_OurCustomTypeWithFieldValues(CONF1_MARKER, "foo", null);
        assertLastThrowableContainsIgnoreCase("cannot resolve", "beanwithtype", "marker");
    }

    @Test
    public void testRegisteredTypeDeclaredWithoutTypeInTypedParameter_TypeRetrievedWithAnonymousKey() throws Exception {
        registerCustomType();
        deployWithTestingCustomTypeObjectConfigAndAssert(true, false, false, "custom-type", CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testRegisteredTypeDeclaredWithTypeInAnonymousParameter_TypeNotRetrievedWithAnonymousKey() throws Exception {
        registerCustomType();
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, true, false, "custom-type", CONF1_ANONYMOUS, "foo", null);
        assertLastThrowableContainsIgnoreCase("expected class " + TestingCustomType.class.getName().toLowerCase(), "but found", "map");

        // but typed-key access works because now we coerce
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_TYPED, "foo", null);
        // and here marker-typed-key access works, because it's a registered type, it's allowed to be coerced
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_MARKER, "foo", null);
    }
    @Test
    public void testRegisteredTypeDeclaredWithTypeInTypedParameter_TypeRetrievedOfCourse() throws Exception {
        registerCustomType();
        deployWithTestingCustomTypeObjectConfigAndAssert(true, true, false, "custom-type", CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testRegisteredTypeDeclaredWithoutTypeInAnonymousParameter_TypeNotRetrievedOfCourse() throws Exception {
        registerCustomType();
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, false, false, "custom-type", CONF1_ANONYMOUS, "foo", null);
        assertLastThrowableContainsIgnoreCase("expected class " + TestingCustomType.class.getName().toLowerCase(), "but found", "map");

        // typed-key access works because the type is explicit
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_TYPED, "foo", null);
        // however marker-typed-key access doesn't work, there is not enough type info supplied anywhere to go from Market to TestingCustomType
        assertLastDeployedKeysValueIs_NOT_OurCustomTypeWithFieldValues(CONF1_MARKER, "foo", null);
        assertLastThrowableContainsIgnoreCase("cannot resolve", "beanwithtype", "marker");
    }

    private void registerCustomType() {
        RegisteredType bean = RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, TestingCustomType.class.getName()));
        bean = RegisteredTypes.addSuperType(bean, TestingCustomType.class);
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(bean, false);
    }

    @Test
    public void testRegisteredType_InheritedFieldsWork_WhenTypeIsDeclared() throws Exception {
        // in the above case, fields are correctly inherited from ancestors and overridden
        RegisteredType bean = RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: " + TestingCustomType.class.getName() + "\n" +
                                "x: unfoo\n" +
                                "y: bar"));
        RegisteredTypes.addSuperType(bean, TestingCustomType.class);
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(bean, false);

        deployWithTestingCustomTypeObjectConfigAndAssert(true, true, false, "custom-type", CONF1_ANONYMOUS, "foo", "bar");
    }

    @Test
    public void testRegisteredType_InheritedFieldsWork_BeanAddCatalogSyntax() throws Exception {
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

        deployWithTestingCustomTypeObjectConfigAndAssert(true, true, false, "custom-type", CONF1_ANONYMOUS,
                "foo", "bar");
    }

    @Test
    public void testRegisteredType_InheritedFieldsNotSupported_WhenTypeIsInferredFromDeclaredParameterType() throws Exception {
        // in the above case, fields are correctly inherited from ancestors and overridden
        RegisteredType bean = RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: " + TestingCustomType.class.getName() + "\n" +
                                "x: unfoo\n" +
                                "y: bar"));
        RegisteredTypes.addSuperType(bean, TestingCustomType.class);
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(bean, false);

        deployWithTestingCustomTypeObjectConfigAndAssert(true, false, false, "custom-type", CONF1_ANONYMOUS,
                "foo",
                // bar now available because the registered type is known on the config key
                "bar");
    }

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

    // as above but when wrapped in a list: behaviour _mostly_ the same, except if type declared on items in the list

    @Test
    public void testJavaTypeInListDeclaredWithoutTypeInTypedParameter_TypeSetInEntityConfig() throws Exception {
        deployWithTestingCustomTypeObjectConfigAndAssert(true, false, true, "list<"+TestingCustomType.class.getName()+">", CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testJavaTypeInListDeclaredWithTypeInAnonymousParameter_TypeNotSetBecauseNotTopLevel() throws Exception {
        // here, we expect it to fail
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, true, true, "list<"+TestingCustomType.class.getName()+">", CONF1_ANONYMOUS, "foo", null);

        // but typed-key access works because now we coerce
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_LIST_TYPED, "foo", null);
    }

    @Test
    public void testJavaTypeInListDeclaredWithTypeInTypedParameter_TypeSetOfCourse() throws Exception {
        // having _both_ a declared type on the parameter / config key name, and a type: line, that also works as expected
        deployWithTestingCustomTypeObjectConfigAndAssert(true, true, true, "list<"+TestingCustomType.class.getName()+">", CONF1_ANONYMOUS, "foo", null);
    }
    @Test
    public void testJavaTypeInListDeclaredWithoutTypeInAnonymousParameter_TypeNotSetOfCourse() throws Exception {
        // if there is no type info at all, the parameter not declared AND type: not in the map, of course there's no way it gets the right type
        deployWithTestingCustomTypeObjectConfigAnd_FAIL_Assert(false, false, true, "list<"+TestingCustomType.class.getName()+">", CONF1_ANONYMOUS, "foo", null);

        // but typed-key access works because now we coerce
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_LIST_TYPED, "foo", null);
    }

    @Test
    public void TestRegisteredType_InheritFromPeer_nowWorksInPojoAndOsgi() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: custom-type-0",
                "    item:",
                "      type: " + CustomTypeConfigYamlTest.TestingCustomType.class.getName(),
                "      x: foo2",
                "  - id: custom-type",
                "    item:",
                "      type: custom-type-0",
                "      y: bar");
        lastDeployedEntity = deployWithTestingCustomTypeObjectConfig(true, true, false, "custom-type", CONF1_ANONYMOUS, false);
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_ANONYMOUS, "foo2", "bar");
    }

    @Test
    public void testRegisteredType_InheritedTwoStep() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: custom-type-0",
                "    item:",
                "      type: " + CustomTypeConfigYamlTest.TestingCustomType.class.getName(),
                "      x: foo2");
        addCatalogItems(
                "brooklyn.catalog:",
                "  version: " + TEST_VERSION,
                "  items:",
                "  - id: custom-type",
                "    item:",
                "      type: custom-type-0",
                "      y: bar");
        lastDeployedEntity = deployWithTestingCustomTypeObjectConfig(true, true, false, "custom-type", CONF1_ANONYMOUS, false);
        assertLastDeployedKeysValueIsOurCustomTypeWithFieldValues(CONF1_ANONYMOUS, "foo2", "bar");
    }
}
