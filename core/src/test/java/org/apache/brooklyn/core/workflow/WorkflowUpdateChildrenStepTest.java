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
package org.apache.brooklyn.core.workflow;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class WorkflowUpdateChildrenStepTest extends BrooklynMgmtUnitTestSupport {

    private BasicApplication app;

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loadTypes();
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
    }

    @Test
    public void testSimpleCrud() {
        WorkflowExecutionContext execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "  - x_id: two",
                        "- update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}"),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        Set<String> childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two"));

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "  - x_id: two",
                        "- update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}"),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two"));

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let list items",
                        "  value: []",
                        "- update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}"),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertSize(childrenIds, 0);
    }

    @Test
    public void testCreateWithBlueprint() {
        WorkflowExecutionContext execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name2",
                        "- step: update-children id ${item.x_id} from ${items}",
                        "  blueprint:",
                        "    type: "+BasicEntity.class.getName(),
                        "    name: ${item.x_name}",
                        ""),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        Set<String> childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two"));

        Set<String> childrenNames = app.getChildren().stream().map(c -> c.getDisplayName()).collect(Collectors.toSet());
        Asserts.assertEquals(childrenNames, MutableSet.of("name1", "name2"));
    }

    @Test
    public void testCustomMatch() {
        WorkflowExecutionContext execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name2",
                        "- step: update-children type " + BasicEntity.class.getName() + " from ${items}",
                        "  match_check:",
                        "  - step: return ONE",
                        "    condition:",
                        "      target: ${item.x_id}",
                        "      equals: one",
                        "  - return ${item.x_id}",
                        ""),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        Set<String> childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("ONE", "two"));
    }

    @Test
    public void testStaticIdentifierGivesError() {
        WorkflowExecutionContext execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- let list items = [ ignored ]",
                        "- update-children type " + BasicEntity.class.getName() + " id item.x_id from ${items}"),
                "first run at children");
        Asserts.assertFailsWith(() -> execution.getTask(false).get().getUnchecked(),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "non-static", "item"));
    }


    @Test
    public void testCrudWithHandlers() {
        WorkflowExecutionContext execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name2",
                        "- step: update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}",
                        "  on_create:",
                        "  - step: set-entity-name ${item.x_name}",
                        "    entity: ${child}"),
                "create run");
        execution.getTask(false).get().getUnchecked();

        Set<String> childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two"));
        Set<String> childrenNames = app.getChildren().stream().map(c -> c.getDisplayName()).collect(Collectors.toSet());
        Asserts.assertEquals(childrenNames, MutableSet.of("name1", "name2"));
        Collection<Entity> oldChildren = app.getChildren();

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name-too",
                        "  - x_id: three",
                        "    x_name: name3",
                        "- step: update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}",
                        "  on_update:",
                        "  - step: set-entity-name ${item.x_name}",
                        "    entity: ${child}"),
                "update run");
        execution.getTask(false).get().getUnchecked();
        childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two", "three"));
        childrenNames = app.getChildren().stream().map(c -> c.getDisplayName()).collect(Collectors.toSet());
        Asserts.assertEquals(childrenNames, MutableSet.of("name1", "name-too", "name3"));
        Asserts.assertThat(app.getChildren(), x -> x.containsAll(oldChildren));  // didn't replace children

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let list items",
                        "  value: []",
                        "- step: update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}",
                        "  deletion_check:",
                        "  - condition:",
                        "      target: ${child.config['" + BrooklynConfigKeys.PLAN_ID.getName() + "']}",
                        "      equals: one",
                        "    step: return true",
                        "  - return false",
                        "  on_update:",
                        "  - set-sensor update_should_not_be_called = but was",
                        "  on_delete:",
                        "  - set-sensor deleted = ${child.name}"),
                "delete run");
        execution.getTask(false).get().getUnchecked();

        childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("two", "three"));
        EntityAsserts.assertAttributeEquals(app, Sensors.newStringSensor("deleted"), "name1");
        EntityAsserts.assertAttributeEquals(app, Sensors.newStringSensor("update_should_not_be_called"), null);  // not called

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let list items",
                        "  value: []",
                        "- step: update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}",
                        "  on_delete:",
                        "  - set-sensor deleted = ${child.name}"),
                "delete run");
        execution.getTask(false).get().getUnchecked();

        childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertSize(childrenIds, 0);
        EntityAsserts.assertAttributeEquals(app, Sensors.newStringSensor("deleted"), "name3");
    }

}
