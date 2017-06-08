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
import static org.testng.Assert.assertNotNull;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;

public class ConfigParametersDeprecationYamlTest extends AbstractYamlRebindTest {
	
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParametersDeprecationYamlTest.class);

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingSshTool.clear();
        }
    }
    
    @Test
    public void testParameterDefinesDeprecatedNames() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+BasicEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: key1",
                "        deprecatedNames:",
                "        - oldKey1",
                "        - oldKey1b",
                "        description: myDescription",
                "        type: String",
                "        default: myDefaultVal");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());

        ConfigKey<?> key = findKey(entity, "key1");
        assertEquals(key.getDeprecatedNames(), ImmutableList.of("oldKey1", "oldKey1b"));

        // Rebind, and then check again that the config key is listed
        Entity newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        ConfigKey<?> newKey = findKey(newEntity, "key1");
        assertEquals(newKey.getDeprecatedNames(), ImmutableList.of("oldKey1", "oldKey1b"));
    }
    
    @Test
    public void testUsingDeprecatedNameInCatalog() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+BasicEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: key1",
                "        deprecatedNames:",
                "        - oldKey1",
                "        description: myDescription",
                "        type: String",
                "      brooklyn.config:",
                "        oldKey1: myval");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(findKey(entity, "key1")), "myval");

        // Rebind, and then check again the config key value
        Entity newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        assertEquals(newEntity.config().get(findKey(entity, "key1")), "myval");
    }
    
    @Test
    public void testUsingDeprecatedNameInUsage() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+BasicEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: key1",
                "        deprecatedNames:",
                "        - oldKey1",
                "        description: myDescription",
                "        type: String");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys",
                "  brooklyn.config:",
                "    oldKey1: myval");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(findKey(entity, "key1")), "myval");

        // Rebind, and then check again the config key value
        Entity newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        assertEquals(newEntity.config().get(findKey(entity, "key1")), "myval");
    }

    @Test
    public void testUsingDeprecatedNameInSubtype() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: "+BasicEntity.class.getName(),
                "      brooklyn.parameters:",
                "      - name: key1",
                "        deprecatedNames:",
                "        - oldKey1",
                "        description: myDescription",
                "        type: String");
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: sub-entity",
                "    item:",
                "      type: entity-with-keys",
                "      brooklyn.config:",
                "        oldKey1: myval");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: sub-entity");
        
        Entity app = createStartWaitAndLogApplication(yaml);
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        assertEquals(entity.config().get(findKey(entity, "key1")), "myval");

        // Rebind, and then check again the config key value
        Entity newApp = rebind();
        Entity newEntity = Iterables.getOnlyElement(newApp.getChildren());
        assertEquals(newEntity.config().get(findKey(entity, "key1")), "myval");
    }

    protected ConfigKey<?> findKey(Entity entity, String keyName) {
        ConfigKey<?> key = entity.getEntityType().getConfigKey(keyName);
        assertNotNull(key, "No key '"+keyName+"'; keys="+entity.getEntityType().getConfigKeys());
        return key;
    }
}
