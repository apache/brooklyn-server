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

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.PropagatedRuntimeException;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.List;

public class WorkflowMapAndListTest  extends BrooklynMgmtUnitTestSupport {

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
    public void testMapDirect() {
        Object result = runSteps(MutableList.of(
                "let map myMap = {}",
                "let myMap.a = 1",
                "return ${myMap.a}"
        ));
        Asserts.assertEquals(result, "1");
    }

    @Test
    public void testSetSensorMap() {
        Object result = runSteps(MutableList.of(
                "set-sensor service.problems[latency] = \"too slow\"",
                "let x = ${entity.sensor['service.problems']}",
                "return ${x}"
        ));
        Asserts.assertEquals(result, ImmutableMap.of("latency", "too slow"));
    }

    @Test
    public void testListIndex() {
        Object result = runSteps(MutableList.of(
                "let list mylist = [1, 2, 3]",
                "let mylist[1] = 4",
                "return ${mylist[1]}"
        ));
        Asserts.assertEquals(result, "4");
    }

    @Test
    public void testUndefinedList() {
        Object result = null;
        try {
            result = runSteps(MutableList.of(
                    "let list mylist = [1, 2, 3]",
                    "let anotherList[1] = 4",
                    "return ${anotherList[1]}"
            ));
        } catch (Exception e) {
            // We can't use expectedExceptionsMessageRegExp as the error message is in the `Cause` exception
            if (e.getCause() == null || !e.getCause().getMessage().contains("Cannot set anotherList[1] because anotherList is unset")) {
                Assert.fail("Expected cause exception to contain 'Cannot set anotherList[1] because anotherList is unset'");
            }
            return;
        }
        Assert.fail("Expected IllegalArgumentException");
    }

    @Test
    public void testInvalidListSpecifier() {
        Object result = null;
        try {
            result = runSteps(MutableList.of(
                    "let list mylist = [1, 2, 3]",
                    "let mylist[1 = 4",
                    "return ${mylist[1]}"
            ));
        } catch (Exception e) {
            // We can't use expectedExceptionsMessageRegExp as the error message is in the `Cause` exception
            if (e.getCause() == null || !e.getCause().getMessage().contains("Invalid list index specifier mylist[1")) {
                Assert.fail("Expected cause exception to contain 'Invalid list index specifier mylist[1'");
            }
            return;
        }
        Assert.fail("Expected IllegalArgumentException");
    }
}
