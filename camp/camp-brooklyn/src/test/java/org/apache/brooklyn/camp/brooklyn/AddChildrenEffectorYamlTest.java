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

import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.effector.AddChildrenEffector;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.CollectionFunctionals;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class AddChildrenEffectorYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(AddChildrenEffectorYamlTest.class);

    protected Entity makeAppAndAddChild(boolean includeDeclaredParameters, MutableMap<String,?> effectorInvocationParameters, String ...lines) throws Exception {
        Entity app = createAndStartApplication(
            "services:",
            "- type: " + BasicApplication.class.getName(),
            "  brooklyn.config:",
            "    p.parent: parent",
            "    p.child: parent",
            "    p.param1: parent",
            "  brooklyn.initializers:",
            "  - type: "+AddChildrenEffector.class.getName(),
            "    brooklyn.config:",
            "      name: add",
            (includeDeclaredParameters ? Strings.lines(indent("      ",
                "parameters:",
                "  p.param1:",
                "    defaultValue: default",
                "  p.param2:",
                "    defaultValue: default")) : ""),
            Strings.lines(indent("      ", lines))
            );
        waitForApplicationTasks(app);
        
        Asserts.assertThat(app.getChildren(), CollectionFunctionals.empty());
        
        Object result = app.invoke(Effectors.effector(Object.class, "add").buildAbstract(), effectorInvocationParameters).get();
        Asserts.assertThat((Iterable<?>)result, CollectionFunctionals.sizeEquals(1));
        Asserts.assertThat(app.getChildren(), CollectionFunctionals.sizeEquals(1));
        Entity child = Iterables.getOnlyElement(app.getChildren());
        Assert.assertEquals(child.getId(), Iterables.getOnlyElement((Iterable<?>)result));
        
        return child;
    }
    
    private String[] indent(String prefix, String ...lines) {
        String[] result = new String[lines.length];
        for (int i=0; i<lines.length; i++) {
            result[i] = prefix + lines[i];
        }
        return result;
    }

    @Test
    public void testAddChildrenWithServicesBlock() throws Exception {
        Entity child = makeAppAndAddChild(true, MutableMap.of("p.param1", "effector_param"),
            "blueprint_yaml: |",
            "  services:", 
            "  - type: "+BasicEntity.class.getName()
            );
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param2")), "default");
        
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param1")), "effector_param");
    }
    
    @Test
    public void testAddChildrenFailsWithoutServicesBlock() throws Exception {
        try {
            Entity child = makeAppAndAddChild(true, MutableMap.of("p.param1", "effector_param"),
                "blueprint_yaml: |",
                "  type: "+BasicEntity.class.getName()
                );
            
            // fine if implementation is improved to accept this format;
            // just change semantics of this test (and ensure comments on blueprint_yaml are changed!)
            Asserts.shouldHaveFailedPreviously("Didn't think we supported calls without 'services', but instantiation gave "+child);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "basic", "error", "invalid");
        }
    }
    
    
    @Test
    public void testAddChildrenAcceptsJson() throws Exception {
        Entity child = makeAppAndAddChild(false, MutableMap.<String,String>of(),
            // note no '|' indicator
            "blueprint_yaml:",  
            "  services:",
            "  - type: "+BasicEntity.class.getName(),
            "    brooklyn.config:",
            "      p.child: child"
            );
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "child");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.parent")), "parent");
        // param1 from parent
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param1")), "parent");
        // param2 not set
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param2")), null);
    }
    
    @Test
    public void testAddChildrenWithConfig() throws Exception {
        Entity child = makeAppAndAddChild(true, MutableMap.<String,Object>of(),
            "blueprint_yaml: |",
            "  services:", 
            "  - type: "+BasicEntity.class.getName(),
            "    brooklyn.config:",
            "      p.child: $brooklyn:config(\"p.parent\")");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param1")), "default");
        Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "parent");
    }
    
    @Test
    // TODO this "test" passes, but it's asserting behaviour we don't want
    public void testAddChildrenDslBrokenInJson() throws Exception {
        Entity child = makeAppAndAddChild(false, MutableMap.<String,String>of(),
            // note no '|' indicator
            "blueprint_yaml:",  
            "  services:", 
            "  - type: "+BasicEntity.class.getName(),
            "    brooklyn.config:",
            "      p.child: $brooklyn:config(\"p.parent\")");
        
        // this is probably what we want:
        // Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.child")), "parent");
        
        // but this is what we get:
        Object childValue = child.getConfig(ConfigKeys.newConfigKey(Object.class, "p.child"));
        Assert.assertTrue(childValue instanceof Map);
        Assert.assertEquals(((Map<?,?>)childValue).get("keyName"), "p.parent");
    }
    
    @Test
    // TODO this "test" passes, but it's asserting behaviour we don't want
    public void testAddChildrenParametersBrokenInJson() throws Exception {
        try {
            Entity child = makeAppAndAddChild(true, MutableMap.<String,String>of(),
                // note no '|' indicator, but there are declared parameters with defaults
                "blueprint_yaml:",  
                "  services:", 
                "  - type: "+BasicEntity.class.getName());
            // fine if implementation is improved to accept this format;
            // just change semantics of this test (and ensure comments on blueprint_yaml are changed!)
            Asserts.shouldHaveFailedPreviously("Didn't think we supported JSON with parameters, but instantiation gave "+child);
            // if it did work this value should be included
            Assert.assertEquals(child.getConfig(ConfigKeys.newStringConfigKey("p.param1")), "default");
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "basic", "error", "invalid");
        }
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
    
}
