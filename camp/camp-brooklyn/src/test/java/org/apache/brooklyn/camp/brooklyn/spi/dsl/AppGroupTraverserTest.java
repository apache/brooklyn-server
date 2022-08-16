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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampConstants;
import org.apache.brooklyn.core.entity.EntityPredicates;
import org.apache.brooklyn.core.mgmt.internal.AppGroupTraverser;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import org.testng.annotations.Test;

public class AppGroupTraverserTest extends AbstractYamlTest {

    @Test
    public void testComplexTreeTraversal() throws Exception {
        Application app = createAndStartApplication("\n" +
                "#- APP1\n" +
                "#   - ENT1\n" +
                "#   - APP2\n" +
                "#     - ENT2\n" +
                "#     - ENT3\n" +
                "#   - APP3\n" +
                "#     - ENT4\n" +
                "#       - ENT5\n" +
                "#       - APP4\n" +
                "#          - ENT6\n" +
                "#       - ENT7\n" +
                "#     - APP5\n" +
                "#       - ENT8\n" +
                "\n" +
                "id: APP1\n" +
                "services:\n" +
                "  - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "    id: ENT1\n" +
                "  - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
                "    id: APP2\n" +
                "    brooklyn.children:\n" +
                "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "        id: ENT2\n" +
                "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "        id: ENT3\n" +
                "  - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
                "    id: APP3\n" +
                "    brooklyn.children:\n" +
                "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "        id: ENT4\n" +
                "        brooklyn.children:\n" +
                "          - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "            id: ENT5\n" +
                "          - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
                "            id: APP4\n" +
                "            brooklyn.children:\n" +
                "              - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "                id: ENT6\n" +
                "          - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "            id: ENT7\n" +
                "\n" +
                "      - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
                "        id: APP5\n" +
                "        brooklyn.children:\n" +
                "          - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
                "            id: ENT8\n");

        String[] IDs = {"ENT1", "ENT2", "ENT3", "ENT4", "ENT5", "ENT6",
                "ENT7", "ENT8", "APP1", "APP2", "APP3", "APP4",
                "APP5"};

        Stream.of(IDs).forEach(s -> {
            Entity entity = findEntity(app, s);
            assertCanTraverseToEachEntityFromHere(entity, IDs);
        });
    }

    static final String APP_findIDISameApp = "id: APP1\n" +
            "services:\n" +
            "  - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
            "    id: APP2\n" +
            "    brooklyn.children:\n" +
            "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
            "        id: ENT1\n" +
            "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
            "        id: ENT2\n" +
            "        name: SAME_APP\n" +
            "  - type: org.apache.brooklyn.entity.stock.BasicApplication\n" +
            "    id: APP3\n" +
            "    brooklyn.children:\n" +
            "      - type: org.apache.brooklyn.entity.stock.BasicEntity\n" +
            "        id: ENT2\n" +
            "        name: DIFFERENT_APP\n";

    @Test void testFindIDISameApp__SharedParent() throws Exception {
        Application app = createAndStartApplication(APP_findIDISameApp);

        Entity ent1 = findEntity(app, "ENT1");
        Entity ent2 = AppGroupTraverser.findFirstGroupOfMatches(ent1, EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, "ENT2")::apply).get(0);

        assertEquals(ent2.getDisplayName(), "SAME_APP");

    }

    @Test void testFindIDISameApp__Child() throws Exception {
        Application app = createAndStartApplication(APP_findIDISameApp);

        Entity ent1 = findEntity(app, "APP2");
        Entity ent2 = AppGroupTraverser.findFirstGroupOfMatches(ent1, EntityPredicates.configEqualTo(BrooklynCampConstants.PLAN_ID, "ENT2")::apply).get(0);

        assertEquals(ent2.getDisplayName(), "SAME_APP");

    }

    private void assertCanTraverseToEachEntityFromHere(Entity startEntity, String[] allNodes) {
        Set<String> visitedNodes = traverseFullTreeRecordingNodes(startEntity);

        assertEquals(visitedNodes.size(), allNodes.length);
        Stream.of(allNodes).forEach(s -> assertTrue(visitedNodes.contains(s), s + " was not found in traversal"));
    }

    private Set<String> traverseFullTreeRecordingNodes(Entity startEntity) {
        Set<String> myIDs = new HashSet<>();
        AppGroupTraverser.findFirstGroupOfMatches(startEntity, entity -> {
            myIDs.add(entity.getConfig(BrooklynCampConstants.PLAN_ID));
            return false; //Traverse all nodes
        });
        return myIDs;
    }

    private Entity findEntity(Application app, String s) {
        return AppGroupTraverser.findFirstGroupOfMatches(app, entity -> {
            String campId = entity.getConfig(BrooklynCampConstants.PLAN_ID);
            return s.equals(campId);
        }).get(0);
    }
}