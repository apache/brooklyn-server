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
package org.apache.brooklyn.entity.group;

import static org.testng.Assert.assertEquals;

import java.util.Collection;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableSet;

public class AbstractDynamicClusterOrFabricTest extends BrooklynAppUnitTestSupport {
    void assertFirstAndNonFirstCounts(Collection<Entity> members, int expectedFirstCount, int expectedNonFirstCount) {
        Set<Entity> found = MutableSet.of();
        for (Entity e: members) {
            if ("first".equals(e.getConfig(TestEntity.CONF_NAME))) found.add(e);
        }
        assertEquals(found.size(), expectedFirstCount, "when counting 'first' nodes");

        found.clear();
        for (Entity e: members) {
            if ("non-first".equals(e.getConfig(TestEntity.CONF_NAME))) found.add(e);
        }
        assertEquals(found.size(), expectedNonFirstCount, "when counting 'non-first' nodes");
    }
}
