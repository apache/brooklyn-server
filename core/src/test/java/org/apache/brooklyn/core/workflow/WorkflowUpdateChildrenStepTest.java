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

import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

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
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name2",
                        "- update-children type " + BasicEntity.class.getName() + " id ${item.x_id} from ${items}"),
                "first run at children");
        execution.getTask(false).get().getUnchecked();

        Set<String> childrenIds = app.getChildren().stream().map(c -> c.config().get(BrooklynConfigKeys.PLAN_ID)).collect(Collectors.toSet());
        Asserts.assertEquals(childrenIds, MutableSet.of("one", "two"));

        execution = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                        "- step: let items",
                        "  value:",
                        "  - x_id: one",
                        "    x_name: name1",
                        "  - x_id: two",
                        "    x_name: name-too",
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
}
