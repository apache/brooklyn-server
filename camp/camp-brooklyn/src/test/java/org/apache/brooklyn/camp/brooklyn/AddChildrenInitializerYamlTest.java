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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.AddChildrenInitializer;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

public class AddChildrenInitializerYamlTest extends AbstractYamlTest {

    private static final Logger LOG = LoggerFactory.getLogger(AddChildrenInitializerYamlTest.class);

    @Override
    protected Logger getLogger() {
        return LOG;
    }

    protected Entity makeAppAndAddChild(String... lines) {
        try {
            Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicApplication.class.getName(),
                    "  brooklyn.config:",
                    "    p.parent: parent",
                    "    p.child: parent",
                    "  brooklyn.initializers:",
                    "  - type: " + AddChildrenInitializer.class.getName(),
                    Strings.lines(indent("    ", lines))
            );
            waitForApplicationTasks(app);
            Asserts.assertThat(app.getChildren(), CollectionFunctionals.sizeEquals(1));
            return Iterables.getOnlyElement(app.getChildren());

        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    private String[] indent(String prefix, String... lines) {
        String[] result = new String[lines.length];
        for (int i = 0; i < lines.length; i++) {
            result[i] = prefix + lines[i];
        }
        return result;
    }

    @Test
    public void testAddChildrenWithServicesBlock() {
        Entity child = makeAppAndAddChild(
                "blueprint_yaml: |",
                "  services:",
                "  - type: " + BasicEntity.class.getName()
        );
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "parent");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
    }

    @Test
    public void testAddChildrenFailsWithoutServicesBlock() throws Exception {
        try {
            Entity child = makeAppAndAddChild(
                    "blueprint_yaml: |",
                    "  type: " + BasicEntity.class.getName()
            );

            // fine if implementation is improved to accept this format;
            // just change semantics of this test (and ensure comments on blueprint_yaml are changed!)
            Asserts.shouldHaveFailedPreviously("Didn't think we supported calls without 'services', but instantiation gave " + child);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "Invalid plan");
        }
    }

    @Test
    public void testAddChildrenAcceptsJson() {
        Entity child = makeAppAndAddChild(
                // note no '|' indicator
                "blueprint_yaml:",
                "  services:",
                "  - type: " + BasicEntity.class.getName(),
                "    brooklyn.config:",
                "      p.child: child"
        );
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "child");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
    }

    @Test
    public void testAddChildrenWithConfig() {
        Entity child = makeAppAndAddChild(
                "blueprint_yaml: |",
                "  services:",
                "  - type: " + BasicEntity.class.getName(),
                "    brooklyn.config:",
                "      p.child: $brooklyn:config(\"p.parent\")");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "parent");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
    }

    @Test
    public void testAddChildrenDslInJson() {
        Entity child = makeAppAndAddChild(
                // note no '|' indicator
                "blueprint_yaml:",
                "  services:",
                "  - type: " + BasicEntity.class.getName(),
                "    brooklyn.config:",
                "      p.child: $brooklyn:config(\"p.parent\")");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "parent");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
    }
}
