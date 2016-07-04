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
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

@Test
public class EntityNameYamlTest extends AbstractYamlTest {
    private static final Logger log = LoggerFactory.getLogger(EntityNameYamlTest.class);

    @Test
    public void testExplicitDisplayName() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  name: myDisplayName");
        deployAndAssertDisplayName(yaml, "myDisplayName");
    }

    @Test
    public void testExplicitDefaultDisplayName() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    defaultDisplayName: myDefaultName");
        deployAndAssertDisplayName(yaml, "myDefaultName");
    }

    @Test
    public void testExplicitDefaultDisplayNameReferrencingConfig() throws Exception {
        String yaml = Joiner.on("\n").join(
                "services:",
                "- type: org.apache.brooklyn.core.test.entity.TestEntity",
                "  brooklyn.config:",
                "    myconf: myval",
                "    defaultDisplayName:",
                "      $brooklyn:formatString:",
                "      - \"PREFIX%sSUFFIX\"",
                "      - $brooklyn:config(\"myconf\")");
        deployAndAssertDisplayName(yaml, "PREFIXmyvalSUFFIX");
    }

    protected void deployAndAssertDisplayName(String yaml, String expectedName) throws Exception {
        Entity app = createAndStartApplication(yaml);
        Entity entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, TestEntity.class));
        assertEquals(entity.getDisplayName(), expectedName);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
