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

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.Test;

import java.util.List;

public class WorkflowOperandsTest extends BrooklynMgmtUnitTestSupport {

    private BasicApplication app;

    Object runSteps(List<?> steps) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);

        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, (List) steps)
        );
        eff.apply((EntityLocal)app);
        return app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).getUnchecked();
    }

    @Test
    public void testValueTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean foo = true",
                "return ${foo}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testValueFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean foo = false",
                "return ${foo}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanAndTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = true && true",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanAndFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = false && true",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanOrTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = false || true",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanOrFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = false || false",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanGreaterThanTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 > 3",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanGreaterThanFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 > 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanGreaterThanOrEqualTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 >= 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanGreaterThanOrEqualFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 3 >= 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanLessThanTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 3 < 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanLessThanFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 < 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }

    @Test
    public void testBooleanLessThanOrEqualTrue() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 <= 4",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, true);
    }

    @Test
    public void testBooleanLessThanOrEqualFalse() {
        Object result = runSteps(MutableList.of(
                "let boolean pass = 4 <= 3",
                "return ${pass}"
        ));
        Asserts.assertEquals(result, false);
    }
    // other boolean operations?
    // precedence?
}
