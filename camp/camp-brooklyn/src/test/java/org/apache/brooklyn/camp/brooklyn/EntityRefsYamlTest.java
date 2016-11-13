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

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

@Test
public class EntityRefsYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(EntityRefsYamlTest.class);

    @Test
    public void testRefToSelf() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  test.confObject: $brooklyn:self()",
                "  test.confName: $brooklyn:self().attributeWhenReady(\"mysensor\")");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEquals(entity.getConfig(TestEntity.CONF_OBJECT), entity);
        
        entity.sensors().set(Sensors.newStringSensor("mysensor"), "myval");
        assertEquals(entity.getConfig(TestEntity.CONF_NAME), "myval");
    }

    @Test
    public void testRefToParent() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  name: entity1",
                "  brooklyn.children:",
                "  - type: " + TestEntity.class.getName(),
                "    brooklyn.config:",
                "      test.confObject: $brooklyn:parent()");
        Entity parent = Iterables.getOnlyElement(app.getChildren());
        Entity child = Iterables.getOnlyElement(parent.getChildren());
        
        assertEquals(child.getConfig(TestEntity.CONF_OBJECT), parent);
    }

    @Test
    public void testRefToRoot() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  brooklyn.config:",
                "    test.confObject: $brooklyn:root()");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        
        assertEquals(entity.getConfig(TestEntity.CONF_OBJECT), app);
    }

    @Test
    public void testRefToScopeRoot() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: myCatalogItem",
                "  item:",
                "    type: "+ TestEntity.class.getName(),
                "    brooklyn.children:",
                "    - type: " + TestEntity.class.getName(),
                "      brooklyn.config:",
                "        test.confObject: $brooklyn:scopeRoot()");

        Entity app = createAndStartApplication(
                "services:",
                "- type: myCatalogItem");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        
        assertEquals(child.getConfig(TestEntity.CONF_OBJECT), entity);
    }

    @Test
    public void testRefToEntityById() throws Exception {
        // Tests for sibling, descendant and ancestor
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  brooklyn.config:",
                "    conf.entity: $brooklyn:entity(\"childid\")",
                "    conf.component: $brooklyn:component(\"childid\")",
                "    conf.component.global: $brooklyn:component(\"global\", \"childid\")",
                "  brooklyn.children:",
                "  - type: " + TestEntity.class.getName(),
                "    id: childid");
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Entity child = Iterables.getOnlyElement(entity.getChildren());
        
        assertEquals(entity.getConfig(newConfigKey("conf.entity")), child);
        assertEquals(entity.getConfig(newConfigKey("conf.component")), child);
        assertEquals(entity.getConfig(newConfigKey("conf.component.global")), child);
    }

    @Test
    public void testRefToRelative() throws Exception {
        // Tests for sibling, descendant and ancestor
        String duplicatedId = "myDuplicatedId";
        
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + TestEntity.class.getName(),
                "  id: "+duplicatedId,
                "  name: entity1",
                "  brooklyn.config:",
                "    conf1.sibling: $brooklyn:sibling(\""+duplicatedId+"\")",
                "    conf1.descendant: $brooklyn:descendant(\""+duplicatedId+"\")",
                "    conf1.sibling2: $brooklyn:component(\"sibling\", \""+duplicatedId+"\")",
                "    conf1.descendant2: $brooklyn:component(\"descendant\", \""+duplicatedId+"\")",
                "  brooklyn.children:",
                "  - type: " + TestEntity.class.getName(),
                "    id: "+duplicatedId,
                "    name: entity1.1",
                "- type: " + TestEntity.class.getName(),
                "  id: "+duplicatedId,
                "  name: entity2",
                "  brooklyn.children:",
                "  - type: " + TestEntity.class.getName(),
                "    id: "+duplicatedId,
                "    name: entity2.1",
                "    brooklyn.config:",
                "      conf2.1.sibling: $brooklyn:sibling(\""+duplicatedId+"\")",
                "      conf2.1.ancestor: $brooklyn:ancestor(\""+duplicatedId+"\")",
                "      conf2.1.sibling2: $brooklyn:component(\"sibling\", \""+duplicatedId+"\")",
                "      conf2.1.ancestor2: $brooklyn:component(\"ancestor\", \""+duplicatedId+"\")",
                "  - type: " + TestEntity.class.getName(),
                "    id: "+duplicatedId,
                "    name: entity2.2");
        
        Entities.dumpInfo(app);
        
        Entity entity1 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("entity1"));
        Entity entity1_1 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("entity1.1"));
        Entity entity2 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("entity2"));
        Entity entity2_1 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("entity2.1"));
        Entity entity2_2 = Iterables.find(Entities.descendantsAndSelf(app), EntityPredicates.displayNameEqualTo("entity2.2"));
        
        assertEquals(entity1.getConfig(newConfigKey("conf1.sibling")), entity2);
        assertEquals(entity1.getConfig(newConfigKey("conf1.sibling2")), entity2);
        assertEquals(entity1.getConfig(newConfigKey("conf1.descendant")), entity1_1);
        assertEquals(entity1.getConfig(newConfigKey("conf1.descendant2")), entity1_1);
        assertEquals(entity2_1.getConfig(newConfigKey("conf2.1.sibling")), entity2_2);
        assertEquals(entity2_1.getConfig(newConfigKey("conf2.1.sibling2")), entity2_2);
        assertEquals(entity2_1.getConfig(newConfigKey("conf2.1.ancestor")), entity2);
        assertEquals(entity2_1.getConfig(newConfigKey("conf2.1.ancestor2")), entity2);
    }

    protected ConfigKey<Object> newConfigKey(String name) {
        return ConfigKeys.newConfigKey(Object.class, name);
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
