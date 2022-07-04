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

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.EntityRelations;
import org.apache.brooklyn.rest.domain.ApplicationSpec;
import org.apache.brooklyn.rest.domain.EntitySpec;
import org.apache.brooklyn.rest.domain.RelationSummary;
import org.apache.brooklyn.rest.testing.BrooklynRestResourceTest;
import org.apache.brooklyn.rest.testing.mocks.NameMatcherGroup;
import org.apache.brooklyn.rest.testing.mocks.RestMockSimpleEntity;
import org.apache.brooklyn.test.Asserts;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import java.net.URI;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.testng.Assert.*;

@Test(singleThreaded = true,
        // by using a different suite name we disallow interleaving other tests between the methods of this test class, which wrecks the test fixtures
        suiteName = "EntityRelationsResourceTest")
public class EntityRelationsResourceTest extends BrooklynRestResourceTest {

    @BeforeClass(alwaysRun = true)
    private void setUp() throws Exception {
        // Deploy an application that we'll use to read the relations of.
        startServer();
        final ApplicationSpec applicationSpec = ApplicationSpec.builder().name("simple-app")
                .entities(ImmutableSet.of(
                        new EntitySpec("simple-ent", RestMockSimpleEntity.class.getName()),
                        new EntitySpec("simple-group", NameMatcherGroup.class.getName(), ImmutableMap.of("namematchergroup.regex", "simple-ent"))
                ))
                .locations(ImmutableSet.of("localhost"))
                .build();
        Response response = clientDeploy(applicationSpec);
        int status = response.getStatus();
        assertTrue(status >= 200 && status <= 299, "expected HTTP Response of 2xx but got " + status);
        URI applicationUri = response.getLocation();
        waitForApplicationToBeRunning(applicationUri);
    }

    @Test
    public void testCustomRelationship() {

        // Expect no initial relationship.
        List<RelationSummary> simpleEntRelations = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/relations"))
                .get(new GenericType<List<RelationSummary>>() {});
        List<RelationSummary> simpleGroupRelations = client().path(
                URI.create("/applications/simple-app/entities/simple-group/relations"))
                .get(new GenericType<List<RelationSummary>>() {});
        assertTrue(simpleEntRelations.isEmpty());
        assertTrue(simpleGroupRelations.isEmpty());

        // Add custom relationship between 'simple-ent' and 'simple-group'.
        Collection<Entity> entities = manager.getEntityManager().getEntities();
        Entity simpleEnt = entities.stream().filter(e -> "simple-ent".equals(e.getDisplayName())).findFirst().orElse(null);
        Entity simpleGroup = entities.stream().filter(e -> "simple-group".equals(e.getDisplayName())).findFirst().orElse(null);
        assertNotNull(simpleEnt, "Did not find 'simple-ent'");
        assertNotNull(simpleGroup, "Did not find 'simple-group'");
        simpleGroup.relations().add(EntityRelations.HAS_TARGET, simpleEnt);

        // Verify simple-ent relationship.
        simpleEntRelations = client().path(
                URI.create("/applications/simple-app/entities/simple-ent/relations"))
                .get(new GenericType<List<RelationSummary>>() {});
        assertEquals(simpleEntRelations.size(), 1, "'simple-ent' must have 1 relation only");
        RelationSummary simpleEntRelationSummary = simpleEntRelations.get(0);
        assertEquals(simpleEntRelationSummary.getType().getName(), "targetted_by");
        assertEquals(simpleEntRelationSummary.getType().getTarget(), "targetter");
        assertEquals(simpleEntRelationSummary.getType().getSource(), "target");
        assertEquals(simpleEntRelationSummary.getTargets().size(), 1, "'simple-ent' must have 1 target only");
        assertTrue(simpleEntRelationSummary.getTargets().contains(simpleGroup.getId()), "'simple-ent' must target id of 'simple-group'");

        // Verify simple-group relationship.
        simpleGroupRelations = client().path(
                URI.create("/applications/simple-app/entities/simple-group/relations"))
                .get(new GenericType<List<RelationSummary>>() {});
        assertEquals(simpleGroupRelations.size(), 1, "'simple-group' must have 1 relation only");
        RelationSummary simpleGroupRelationSummary = simpleGroupRelations.get(0);
        assertEquals(simpleGroupRelationSummary.getType().getName(), "has_target");
        assertEquals(simpleGroupRelationSummary.getType().getTarget(), "target");
        assertEquals(simpleGroupRelationSummary.getType().getSource(), "targetter");
        assertEquals(simpleGroupRelationSummary.getTargets().size(), 1, "'simple-group' must have 1 target only");
        assertTrue(simpleGroupRelationSummary.getTargets().contains(simpleEnt.getId()), "'simple-group' must target id of 'simple-ent'");
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Test
    public void testCustomRelationshipInFetch() {

        // Add custom relationship between 'simple-ent' and 'simple-group'.
        Collection<Entity> entities = manager.getEntityManager().getEntities();
        Entity simpleEnt = entities.stream().filter(e -> "simple-ent".equals(e.getDisplayName())).findFirst().orElse(null);
        Entity simpleGroup = entities.stream().filter(e -> "simple-group".equals(e.getDisplayName())).findFirst().orElse(null);
        assertNotNull(simpleEnt, "Did not find 'simple-ent'");
        assertNotNull(simpleGroup, "Did not find 'simple-group'");
        simpleGroup.relations().add(EntityRelations.HAS_TARGET, simpleEnt);

        // Get relationship via 'fetch' request.
        Collection apps = client().path("/applications/fetch").get(Collection.class);
        Map app = ((Collection<Map>)apps).stream().filter(m -> "simple-app".equals(m.get("name"))).findFirst().orElse(null);
        Assert.assertNotNull(app, "Did not find 'simple-app'");
        Collection children = (Collection) app.get("children");
        Asserts.assertSize(children, 2);
        Map entitySummary = (Map) Iterables.find(children, withValueForKey("name", "simple-ent"), null);
        Map groupSummary = (Map) Iterables.find(children, withValueForKey("name", "simple-group"), null);
        Assert.assertNotNull(entitySummary, "Did not find 'simple-ent'");
        Assert.assertNotNull(groupSummary,"Did not find 'simple-group'");
        Collection simpleEntRelations = (Collection) entitySummary.get("relations");
        Collection simpleGroupRelations = (Collection) groupSummary.get("relations");

        // Verify simple-ent relationship.
        assertEquals(simpleEntRelations.size(), 1, "'simple-ent' must have 1 relation only");
        Map simpleEntRelationSummary = (Map) simpleEntRelations.toArray()[0];
        Map simpleEntRelationSummaryType = (Map) simpleEntRelationSummary.get("type");
        assertEquals(simpleEntRelationSummaryType.get("name"), "targetted_by");
        assertEquals(simpleEntRelationSummaryType.get("target"), "targetter");
        assertEquals(simpleEntRelationSummaryType.get("source"), "target");
        Collection simpleEntRelationSummaryTargets = (Collection) simpleEntRelationSummary.get("targets");
        assertEquals(simpleEntRelationSummaryTargets.size(), 1, "'simple-ent' must have 1 target only");
        assertTrue(simpleEntRelationSummaryTargets.contains(simpleGroup.getId()), "'simple-ent' must target id of 'simple-group'");

        // Verify simple-group relationship.
        assertEquals(simpleGroupRelations.size(), 1, "'simple-group' must have 1 relation only");
        Map simpleGroupRelationSummary = (Map) simpleGroupRelations.toArray()[0];
        Map simpleGroupRelationSummaryType = (Map) simpleGroupRelationSummary.get("type");
        assertEquals(simpleGroupRelationSummaryType.get("name"), "has_target");
        assertEquals(simpleGroupRelationSummaryType.get("target"), "target");
        assertEquals(simpleGroupRelationSummaryType.get("source"), "targetter");
        Collection simpleGroupRelationSummaryTargets = (Collection) simpleEntRelationSummary.get("targets");
        assertEquals(simpleGroupRelationSummaryTargets.size(), 1, "'simple-ent' must have 1 target only");
        assertTrue(simpleGroupRelationSummaryTargets.contains(simpleGroup.getId()), "'simple-ent' must target id of 'simple-group'");
    }
}
