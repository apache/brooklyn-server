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
package org.apache.brooklyn.rest.resources;

import com.google.common.collect.Iterables;
import org.apache.brooklyn.rest.domain.CatalogEntitySummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.testng.annotations.Test;

import javax.ws.rs.core.GenericType;
import java.util.List;

import static org.testng.Assert.assertTrue;

@Test(suiteName = "CatalogResourceRebindTest")
public class CatalogResourceRebindTest extends BrooklynRestResourceTest {
    @Override
    protected boolean useLocalScannedCatalog() {
        return true;
    }

    @Test
    public void testEntityWithConfigReturnsBasicSpecParameters() {
        List<CatalogEntitySummary> entities = client().path("/catalog/entities")
                .get(new GenericType<List<CatalogEntitySummary>>() {});
        assertTrue(entities.size() > 5, "Entities catalog should have items.");
        assertTrue(Iterables.all(entities, c -> c.getConfig().size() > 1), "Entity spec should have all configs initialized. If not check spec deserialization restores config is empty. BasicSpecParameter#initializeSpecWithExplicitParameters.");
    }

    // TODO make a rebind test and assert `c -> c.getConfig().size() > 1'
}
