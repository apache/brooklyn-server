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

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityInitializers.AddTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerializationTest.SampleBean;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;

@Test
public class CustomTypeInitializerYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(CustomTypeInitializerYamlTest.class);

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

    public static class TestingCustomInitializerType implements EntityInitializer {
        static Entity lastEntity;
        static String lastX;
        static String lastY;

        String x;
        String y;

        @Override
        public void apply(EntityLocal entity) {
            lastEntity = entity;
            lastX = x;
            lastY = y;
        }
    }

    public static class TestingCustomInitializerTypeAcceptingConfigBag extends TestingCustomInitializerType {
        // no-arg constructor needed for bean-with-type instantiation; it can be private
        private TestingCustomInitializerTypeAcceptingConfigBag() {}

        // old-style constructor if config bag is injected
        public TestingCustomInitializerTypeAcceptingConfigBag(ConfigBag config) {
            if (config!=null) setConfig(config.getAllConfig());
            throw new IllegalStateException("This test should not use the config-bag constructor");
        }

        // this pattern is needed to support nested brooklyn config on initializers (and other beans)
        @JsonProperty("brooklyn.config")
        void setConfig(Map<String,Object> config) {
            if (this.x==null) this.x = (String) config.get("x");
            if (this.y==null) this.y = (String) config.get("y");
        }

        // as an alternative to the above, the follow sort-of works
        // but NOT if brooklyn.config is defined at two levels (ie a bean and then extended);
        // we need the annotated setter for that.
        // and even then it doesn't support config inheritance; so only use brooklyn.config for spec objects.
//        @JsonCreator(mode= Mode.PROPERTIES)
//        public TestingCustomInitializerTypeAcceptingConfigBag(@JsonProperty("brooklyn.config") Map<String,Object> config) {
//            if (config!=null) setConfig(config);
//        }
    }
    
    protected Entity deployWithTestingCustomInitializerType(String type, String ...extras) throws Exception {
        return setupAndCheckTestEntityInBasicYamlWith(Strings.lines(MutableList.of(
            "  brooklyn.initializers:",
            "  - "+"type: "+type).appendAll(Arrays.asList(extras))));
    }

    protected void assertInitializerRanAndXY(Entity e, String xValue, String yValue) {
        Asserts.assertEquals(TestingCustomInitializerType.lastEntity, e);
        Asserts.assertEquals(TestingCustomInitializerType.lastX, xValue);
        Asserts.assertEquals(TestingCustomInitializerType.lastY, yValue);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsJavaType() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerType.class.getName());
        assertInitializerRanAndXY(testEntity, null, null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsJavaTypeWithArgUsingConfigBag() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerTypeAcceptingConfigBag.class.getName(),
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsJavaTypeWithArgUsingField() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerType.class.getName(), "    x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsJavaTypeWithArgUsingFieldAndConfigBag() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerTypeAcceptingConfigBag.class.getName(),
                "    y: bar",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredType() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerType.class.getName()));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type");
        assertInitializerRanAndXY(testEntity, null, null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingField() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerType.class.getName()));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingConfigBag() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeAcceptingConfigBag.class.getName()));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingConfigBagAndField() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeAcceptingConfigBag.class.getName()));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    y: bar",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingInheritedFieldAndLocalConfigBag() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeAcceptingConfigBag.class.getName()+"\n" +
                        "y: bar"));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingInheritedConfigBagAndLocalField() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeAcceptingConfigBag.class.getName()+"\n" +
                        "brooklyn.config:\n" +
                        "  x: foo"));

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    y: bar");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }


    public static class TestingCustomInitializerTypeOldStyleAcceptingConfigBagOnly extends TestingCustomInitializerType {
        // old-style constructor; with just this, it will be forced to use the fallback InstantiatorFromKey.newInstance construction mechanism
        // fields are not supported. see above for discussion why config bag is not great here.
        public TestingCustomInitializerTypeOldStyleAcceptingConfigBagOnly(ConfigBag config) {
            setConfig(config.getAllConfig());
        }

        void setConfig(Map<String,Object> config) {
            if (this.x==null) this.x = (String) config.get("x");
            if (this.y==null) this.y = (String) config.get("y");
        }
    }

    @Test
    public void testOldStyleCustomInitializerTypeSpecifiedAsRegisteredTypeFails() throws Exception {
        Asserts.assertFailsWith(() -> {
                    addBean("custom-type", "1",
                            new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                                    "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeOldStyleAcceptingConfigBagOnly.class.getName()));
                    deployWithTestingCustomInitializerType("custom-type");
                    return null;
                },
                e -> Asserts.expectedFailureContainsIgnoreCase(e, TestingCustomInitializerTypeOldStyleAcceptingConfigBagOnly.class.getName(), "no creators", "cannot construct"));
    }

    @Test
    public void testOldStyleCustomInitializerTypeSpecifiedAsJavaTypeSupportsBrooklynConfigButNotFields() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerTypeOldStyleAcceptingConfigBagOnly.class.getName(),
                "    brooklyn.config:\n" +
                "      x: foo\n" +
                "    y: bar");

        // NB: the field 'y' is ignored
        assertInitializerRanAndXY(testEntity, "foo", null);
    }


    public static class TestingCustomInitializerTypeFinal implements EntityInitializer {
        static String lastZ;
//        @JsonProperty
        final String z;

        // for use deserializing
        private TestingCustomInitializerTypeFinal() {
            z=null;
        }

        // old-style constructor; with just this, it will be forced to use the fallback InstantiatorFromKey.newInstance construction mechanism
        // fields are not supported. see above for discussion why config bag is not great here.
        public TestingCustomInitializerTypeFinal(ConfigBag config) {
            this.z = (String) config.getStringKey("z");
        }

        @Override
        public void apply(EntityLocal entity) {
            lastZ = z;
        }

    }

    @Test
    public void testCustomInitializerFinalPatternTakingFields() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeFinal.class.getName()));

        Entity entity = deployWithTestingCustomInitializerType("custom-type",
                "    z: hi");
        Assert.assertEquals(TestingCustomInitializerTypeFinal.lastZ, "hi");
    }

    @Test
    public void testCustomInitializerFinalPatternTakingConfigBag() throws Exception {
        Entity entity = deployWithTestingCustomInitializerType(TestingCustomInitializerTypeFinal.class.getName(),
                "    brooklyn.config:",
                "      z: hi0");
        Assert.assertEquals(TestingCustomInitializerTypeFinal.lastZ, "hi0");
    }

    @Test
    public void testCustomInitializerFinalPatternOverridingFields() throws Exception {
        addBean("custom-type", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeFinal.class.getName()+"\n" +
                                "z: hi1"));

        Entity entity = deployWithTestingCustomInitializerType("custom-type",
                "    z: hi2");
        Assert.assertEquals(TestingCustomInitializerTypeFinal.lastZ, "hi2");
    }

    @Test
    public void testCustomInitializerFinalPatternUsingConfigBagWithTypeNotSupported() throws Exception {
        // but deployment fails as we don't have a json setter; cannot use final var pattern with bean-with-type
        Asserts.assertFailsWith(() -> {
                    // we're allowed to add
                    addBean("custom-type", "1",
                            new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                                    "type: "+CustomTypeInitializerYamlTest.TestingCustomInitializerTypeFinal.class.getName()+"\n" +
                                            "brooklyn.config:\n" +
                                            "  z: hi1"));
                    deployWithTestingCustomInitializerType("custom-type");
                    return null;
                },
            e -> Asserts.expectedFailureContainsIgnoreCase(e, "unrecognized field", "brooklyn.config"));
    }

    // also test the stock initializer

    @Test
    public void testAddTags() throws Exception {
        addBean("add-tags", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+ AddTags.class.getName()));
        addBean("sample-bean", "1",
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT,
                        "type: "+ SampleBean.class.getName()));
        Entity ent = deployWithTestingCustomInitializerType("add-tags",
                "    tags:",
                "    - t1",
                "    - 4",
                "    - type: sample-bean",
                "      x: 4");
        Set<Object> tags = ent.tags().getTags();
        Assert.assertTrue(tags.contains("t1"));
        Assert.assertTrue(tags.contains(4));
        Assert.assertFalse(tags.contains("4"));

        // should this be converted to the type?  here it is iff we add using bean-with-type
        // (but not if using the legacy ConfigBag constructor), and if the bean declares the List<Object> as a field.
        // that seems about the right trade-off, convert to types if known, and through one layer of indirection
        // (eg List<Object>) but not through two (for ConfigKeys the deserializer doesn't know their types)
        Assert.assertEquals( ((SampleBean)(tags.stream().filter(t -> t instanceof SampleBean).findFirst().get())).x, "4" );
//        Assert.assertEquals( ((Map)(tags.stream().filter(t -> t instanceof Map).findFirst().get())).get("x"), 4 );
    }

}
