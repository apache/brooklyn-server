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
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;

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
        }
    }

    public static class TestingCustomInitializerTypeAcceptingConfigBag extends TestingCustomInitializerType {
        public TestingCustomInitializerTypeAcceptingConfigBag(ConfigBag keys) {
            if (this.x==null) this.x = (String) keys.getStringKey("x");
            if (this.y==null) this.y = (String) keys.getStringKey("y");
        }
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

    @Test(groups="WIP")  // TODO support brooklyn.config to set config keys on initializers; and/or allow java instantiator to read registered types
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingConfigBag() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerType.class.getName())), false);

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsJavaTypeWithArgUsingField() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerType.class.getName(), "    x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test(groups="WIP")  // TODO support brooklyn.config to set config keys on initializers
    public void testCustomInitializerTypeSpecifiedAsJavaTypeWithArgUsingFieldAndConfigBag() throws Exception {
        Entity testEntity = deployWithTestingCustomInitializerType(TestingCustomInitializerTypeAcceptingConfigBag.class.getName(),
                "    y: bar",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredType() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerType.class.getName())), false);

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type");
        assertInitializerRanAndXY(testEntity, null, null);
    }

    @Test
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingField() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerType.class.getName())), false);

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    x: foo");
        assertInitializerRanAndXY(testEntity, "foo", null);
    }

    @Test(groups="WIP")  // TODO support brooklyn.config to set config keys on initializers
    public void testCustomInitializerTypeSpecifiedAsRegisteredTypeWithArgUsingConfigBagAndField() throws Exception {
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(RegisteredTypes.bean("custom-type", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, CustomTypeInitializerYamlTest.TestingCustomInitializerTypeAcceptingConfigBag.class.getName())), false);

        Entity testEntity = deployWithTestingCustomInitializerType("custom-type",
                "    y: bar",
                "    brooklyn.config:",
                "      x: foo");
        assertInitializerRanAndXY(testEntity, "foo", "bar");
    }

    // TODO try using BeanWithTypePlanTransformer, where value specified in _definition_, here and in CustomTypeConfigYamlTest

}
