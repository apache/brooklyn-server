/*
uniqueSshConnection * Licensed to the Apache Software Foundation (ASF) under one
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.collect.Iterables;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.test.entity.TestEntity;

@Test
public class IdentityYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(IdentityYamlTest.class);

    private static final ConfigKey<String> TEST_ENTITY_ONE_ID = ConfigKeys.newStringConfigKey("testentityone.id");
    private static final ConfigKey<String> TEST_ENTITY_TWO_ID = ConfigKeys.newStringConfigKey("testentitytwo.id");

    protected Iterable<? extends Entity> setupAndCheckTestEntityInBasicYamlWith() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "  - type: " + TestEntity.class.getName(),
                "    id: testentityone",
                "    name: \"Test Entity One\"",
                "    brooklyn.config:",
                "      testentityone.id: $brooklyn:entityId()",
                "      testentitytwo.id: $brooklyn:entity(\"testentitytwo\").entityId()",
                "  - type: " + TestEntity.class.getName(),
                "    id: testentitytwo",
                "    name: \"Test Entity Two\"");
        waitForApplicationTasks(app);
        return Iterables.filter(app.getChildren(), TestEntity.class);
    }

    @Test
    public void testYamlParsing() throws Exception {
        Iterable<? extends Entity> testEntities = setupAndCheckTestEntityInBasicYamlWith();

        Assert.assertEquals(Iterables.size(testEntities), 2, "Should be two entities");
    }

    @Test
    public void testBrooklynIdentityFunction() throws Exception {
        Iterable<? extends Entity> testEntities = setupAndCheckTestEntityInBasicYamlWith();
        Entity entityOne = Iterables.find(testEntities, EntityPredicates.displayNameEqualTo("Test Entity One"));
        Entity entityTwo = Iterables.find(testEntities, EntityPredicates.displayNameEqualTo("Test Entity Two"));

        Assert.assertNotNull(entityOne, "Test entity one should be present");
        Assert.assertNotNull(entityTwo, "Test entity two should be present");

        Assert.assertEquals(entityOne.config().get(TEST_ENTITY_ONE_ID), entityOne.getId(), "Entity one IDs should match");
        Assert.assertEquals(entityOne.config().get(TEST_ENTITY_TWO_ID), entityTwo.getId(), "Entity two IDs should match");
    }

    @Override
    protected Logger getLogger() {
        return log;
    }

}
