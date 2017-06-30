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

import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.test.entity.TestEntityImpl;
import org.apache.brooklyn.core.test.policy.TestEnricher;
import org.apache.brooklyn.core.test.policy.TestPolicy;
import org.apache.brooklyn.entity.group.DynamicCluster;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.text.Identifiers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

@Test
public class ValidationMissingTypeYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(ValidationMissingTypeYamlTest.class);

    
    @Test
    public void testNoEntityTypeSpecifiedInTopLevelService() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- foo: " + TestEntityImpl.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "must declare a type");
        }
    }

    @Test
    public void testNoEntityTypeInTopLevelCatalogEntity() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: entity",
                    "  item:",
                    "    foo: " + TestEntity.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "must declare a type");
        }
    }

    @Test
    public void testNoEntityTypeInTopLevelCatalogApp() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: template",
                    "  item:",
                    "    services:",
                    "    - foo: " + TestEntity.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "must declare a type");
        }
    }

    @Test
    public void testNoEntityTypeSpecifiedInChildService() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- type: " + TestApplication.class.getName(),
                    "  brooklyn.children:",
                    "  - foo: " + TestEntityImpl.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }
    
    @Test
    public void testNoEntityTypeInChildCatalogEntity() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: entity",
                    "  item:",
                    "    type: " + TestApplication.class.getName(),
                    "    brooklyn.children:",
                    "    - foo: " + TestEntityImpl.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }

    @Test
    public void testNoEntityTypeInChildCatalogApp() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: template",
                    "  item:",
                    "    services:",
                    "    - type: " + TestApplication.class.getName(),
                    "      brooklyn.children:",
                    "      - foo: " + TestEntityImpl.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }

    @Test
    public void testNoEntityTypeSpecifiedInEntitySpec() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- type: " + DynamicCluster.class.getName(),
                    "  brooklyn.config:",
                    "    initialSize: 0",
                    "    memberSpec: ",
                    "      $brooklyn:entitySpec:",
                    "        foo: " + TestEntityImpl.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }
    
    @Test
    public void testNoEntityTypeInEntitySpecInCatalogEntity() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: entity",
                    "  item:",
                    "    type: " + DynamicCluster.class.getName(),
                    "    brooklyn.config:",
                    "      initialSize: 0",
                    "      memberSpec: ",
                    "        $brooklyn:entitySpec:",
                    "          foo: " + TestEntityImpl.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }

    @Test
    public void testNoEntityTypeInEntitySpecInCatalogApp() throws Exception {
        try {
            addCatalogItems(
                    "brooklyn.catalog:",
                    "  id: " + Identifiers.makeRandomId(8),
                    "  version: 1.0.0",
                    "  itemType: template",
                    "  item:",
                    "    services:",
                    "    - type: " + DynamicCluster.class.getName(),
                    "      brooklyn.config:",
                    "        initialSize: 0",
                    "        memberSpec: ",
                    "          $brooklyn:entitySpec:",
                    "            foo: " + TestEntityImpl.class.getName());
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "No type defined");
        }
    }

    // TODO Preferred name should not be 'policy_type'; it should be 'type'!
    @Test
    public void testNoPolicyTypeSpecified() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- type: " + TestApplication.class.getName(),
                    "  brooklyn.policies:",
                    "  - foo: " + TestPolicy.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Missing key 'policy_type'");
        }
    }
    
    // TODO Preferred name should not be 'enricher_type'; it should be 'type'!
    @Test
    public void testNoEnricherTypeSpecified() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- type: " + TestApplication.class.getName(),
                    "  brooklyn.enrichers:",
                    "  - foo: " + TestEnricher.class.getName());
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Missing key 'enricher_type'");
        }
    }
    
    @Override
    protected Logger getLogger() {
        return log;
    }
}
