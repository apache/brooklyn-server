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

import java.io.StringReader;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.api.client.repackaged.com.google.common.base.Joiner;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class ConfigParametersYamlTest extends AbstractYamlTest {
    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(ConfigParametersYamlTest.class);

    @Test
    public void testConfigParametersListedInType() throws Exception {
        addCatalogItems(
                "brooklyn.catalog:",
                "  itemType: entity",
                "  items:",
                "  - id: entity-with-keys",
                "    item:",
                "      type: org.apache.brooklyn.core.test.entity.TestEntity",
                "      brooklyn.parameters:",
                "      - name: testConfigParametersListedInType.mykey",
                "        description: myDescription",
                "        type: java.util.Map",
                "        inheritance.type: deep_merge",
                "        default: {myDefaultKey: myDefaultVal}");
        
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: entity-with-keys");
        
        Entity app = createStartWaitAndLogApplication(new StringReader(yaml));
        TestEntity entity = (TestEntity) Iterables.getOnlyElement(app.getChildren());

        // Check config key is listed
        ConfigKey<?> key = entity.getEntityType().getConfigKey("testConfigParametersListedInType.mykey");
        assertNotNull(key);
        assertEquals(key.getName(), "testConfigParametersListedInType.mykey");
        assertEquals(key.getDescription(), "myDescription");
        assertEquals(key.getType(), Map.class);
        assertEquals(key.getDefaultValue(), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
        
        // Check get default value
        assertEquals(entity.config().get(key), ImmutableMap.of("myDefaultKey", "myDefaultVal"));
    }
}
