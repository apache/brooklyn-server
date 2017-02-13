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
package org.apache.brooklyn.camp.brooklyn.catalog;

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

public class CatalogYamlPolicyTest extends AbstractYamlTest {
    private static final String POLICY_TYPE = TestPolicy.class.getName();

    @Test
    public void testAddCatalogItem() throws Exception {
        assertEquals(countCatalogPolicies(), 0);

        String symbolicName = "my.catalog.policy.id.load";
        addCatalogPolicy(symbolicName, POLICY_TYPE);

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);
        assertEquals(countCatalogPolicies(), 1);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testAddCatalogItemTopLevelLegacySyntax() throws Exception {
        assertEquals(countCatalogPolicies(), 0);

        String symbolicName = "my.catalog.policy.id.load";
        addCatalogPolicyLegacySyntax(symbolicName, POLICY_TYPE);

        RegisteredType item = mgmt().getTypeRegistry().get(symbolicName, TEST_VERSION);
        assertEquals(item.getSymbolicName(), symbolicName);
        assertEquals(countCatalogPolicies(), 1);

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingPolicy() throws Exception {
        String symbolicName = "my.catalog.policy.id.launch";
        addCatalogPolicy(symbolicName, POLICY_TYPE);
        Entity app = createAndStartApplication(
            "services: ",
            "  - type: " + BasicEntity.class.getName(),
            "    brooklyn.policies:",
            "    - type: " + ver(symbolicName),
            "      brooklyn.config:",
            "        config2: config2 override",
            "        config3: config3");

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        Policy policy = Iterables.getOnlyElement(simpleEntity.policies());
        assertEquals(policy.getPolicyType().getName(), POLICY_TYPE);
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config1")), "config1");
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config2")), "config2 override");
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config3")), "config3");

        deleteCatalogEntity(symbolicName);
    }

    @Test
    public void testLaunchApplicationReferencingPolicyTopLevelSyntax() throws Exception {
        String symbolicName = "my.catalog.policy.id.launch";
        addCatalogPolicyLegacySyntax(symbolicName, POLICY_TYPE);
        Entity app = createAndStartApplication(
            "services: ",
            "  - type: " + BasicEntity.class.getName(), 
            "    brooklyn.policies:",
            "    - type: " + ver(symbolicName),
            "      brooklyn.config:",
            "        config2: config2 override",
            "        config3: config3");

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        Policy policy = Iterables.getOnlyElement(simpleEntity.policies());
        assertEquals(policy.getPolicyType().getName(), POLICY_TYPE);
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config1")), "config1");
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config2")), "config2 override");
        assertEquals(policy.getConfig(new BasicConfigKey<String>(String.class, "config3")), "config3");

        deleteCatalogEntity(symbolicName);
    }
    
    @Test
    public void testLaunchApplicationWithCatalogReferencingOtherCatalog() throws Exception {
        String referencedSymbolicName = "my.catalog.policy.id.referenced";
        String referrerSymbolicName = "my.catalog.policy.id.referring";
        addCatalogPolicy(referencedSymbolicName, POLICY_TYPE);

        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + referrerSymbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: entity",
            "  name: My Catalog App",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  item:",
            "    type: " + BasicEntity.class.getName(),
            "    brooklyn.policies:",
            "    - type: " + ver(referencedSymbolicName));

        Entity app = createAndStartApplication(
                "services:",
                "- type: "+ ver(referrerSymbolicName));

        Entity simpleEntity = Iterables.getOnlyElement(app.getChildren());
        Policy policy = Iterables.getOnlyElement(simpleEntity.policies());
        assertEquals(policy.getPolicyType().getName(), POLICY_TYPE);

        deleteCatalogEntity(referencedSymbolicName);
    }

    private void addCatalogPolicy(String symbolicName, String policyType) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  version: " + TEST_VERSION,
            "  itemType: policy",
            "  name: My Catalog Policy",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  item:",
            "    type: " + policyType,
            "    brooklyn.config:",
            "      config1: config1",
            "      config2: config2");
    }

    private void addCatalogPolicyLegacySyntax(String symbolicName, String policyType) {
        addCatalogItems(
            "brooklyn.catalog:",
            "  id: " + symbolicName,
            "  name: My Catalog Policy",
            "  description: My description",
            "  icon_url: classpath://path/to/myicon.jpg",
            "  version: " + TEST_VERSION,
            "",
            "brooklyn.policies:",
            "- type: " + policyType,
            "  brooklyn.config:",
            "    config1: config1",
            "    config2: config2");
    }
}
