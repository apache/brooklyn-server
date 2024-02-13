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

import java.io.Serializable;
import java.util.List;

import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowRetentionAndExpiration;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class WorkflowSubAndCustomExtensionEdgeTest extends RebindTestFixture<TestApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSubAndCustomExtensionEdgeTest.class);

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        app = null; // clear this
        mgmt.getBrooklynProperties().put(WorkflowRetentionAndExpiration.WORKFLOW_RETENTION_DEFAULT, "forever");
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override
    protected TestApplication createApp() {
        return null;
    }

    @Override protected TestApplication rebind() throws Exception {
        return rebind(RebindOptions.create().terminateOrigManagementContext(true));
    }

    public RegisteredType addBeanWithType(String typeName, String version, String plan) {
        return BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt(), typeName, version,
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, plan));
    }

    TestApplication app;
    Task<?> lastInvocation;

    Object runWorkflow(List<Object> steps) throws Exception {
        return runWorkflow(steps, null);
    }
    Object runWorkflow(List<Object> steps, ConfigBag extraEffectorConfig) throws Exception {
        if (app==null) app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
                .putAll(extraEffectorConfig));
        eff.apply((EntityLocal)app);

        lastInvocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        return lastInvocation.getUnchecked();
    }

    @Test
    public void testVisibilityOfReducingWhenDefiningCustomWorkflowStep() throws Exception {
        // test the fixture is right
        MutableMap<String, Serializable> basicReducingStep = MutableMap.of(
                "type", "workflow",
                "reducing", MutableMap.of("hello_word", "${hello_word}"),
                "steps", MutableList.of("set-sensor hi = ${hello_word} world"));
        runWorkflow(MutableList.of("let hello_word = HI", basicReducingStep));
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(String.class, "hi"), "HI world");

        addBeanWithType("set-sensor-hi-reducing", "1-SNAPSHOT", Strings.lines(
                "type: workflow",
                "parameters:",
                "  value: {}",
                "reducing:",
                "  hello_word: ${hello_word}",
                "steps:",
                "  - let hi_word = ${hello_word} ?? hi",
                "  - set-sensor hi = ${hi_word} ${value}",
                "  - let hello_word = bye"
        ));

        if (CustomWorkflowStep.CUSTOM_WORKFLOW_STEP_REGISTERED_TYPE_EXTENSIONS_CAN_REDUCE) {
            runWorkflow(MutableList.of(
                    "let hello_word = HELLO",
                    MutableMap.of("type", "set-sensor-hi-reducing", "input", MutableMap.of("value", "bob")),
                    "return ${hello_word}"));
            Asserts.assertEquals(lastInvocation.getUnchecked(), "bye");
            EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(String.class, "hi"), "HELLO bob");
        } else {
            Asserts.assertFailsWith(() -> runWorkflow(MutableList.of(
                    "let hello_word = HELLO",
                    MutableMap.of("type", "set-sensor-hi-reducing", "input", MutableMap.of("value", "bob")),
                    "return ${hello_word}")),
                Asserts.expectedFailureContainsIgnoreCase("not permitted", "reducing"));
        }

        addBeanWithType("set-sensor-hi", "1-SNAPSHOT", Strings.lines(
                "type: workflow",
                "parameters:",
                "  value: {}",
                "steps:",
                "  - set-sensor hi = hi ${value}"
        ));
        // value is a poor choice with set-sensor, because set-sensor tries to evaluate the input; but let's make sure the message is not too confusing
        Asserts.assertFailsWith(() -> runWorkflow(MutableList.of(
                "let hello_word = HI",
                "set-sensor-hi")),
                Asserts.expectedFailureContainsIgnoreCase("recursive or missing reference","value")
        );
    }

    @Test
    public void testSubWorkflowStep() throws Exception {
        MutableList<String> steps = MutableList.of(
                "let v0 = ${v0}B",
                "let v1 = ${v1}B",
                "let v3 = V3B",
                "return done");
        runWorkflow(MutableList.of(
                "let v1 = V1",
                "let v2 = V2",
                MutableMap.of(
                        "step", "subworkflow",
                    "steps", steps),
                "return ${v0}-${v1}-${v2}-${v3}-${output}"),
                ConfigBag.newInstance().configure(WorkflowCommonConfig.INPUT, MutableMap.of("v0", "V0")) );
        Asserts.assertEquals(lastInvocation.getUnchecked(), "V0B-V1B-V2-V3B-done");

        // subworkflow is chosen implicitly if step is omitted
        runWorkflow(MutableList.of(
                        "let v1 = V1",
                        "let v2 = V2",
                        MutableMap.of(
                                //"step", "subworkflow",
                                "steps", steps),
                        "return ${v0}-${v1}-${v2}-${v3}-${output}"),
                ConfigBag.newInstance().configure(WorkflowCommonConfig.INPUT, MutableMap.of("v0", "V0")) );
        Asserts.assertEquals(lastInvocation.getUnchecked(), "V0B-V1B-V2-V3B-done");
    }

}
