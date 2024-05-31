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
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Function;

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

public class WorkflowSubIfAndCustomExtensionEdgeTest extends RebindTestFixture<TestApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowSubIfAndCustomExtensionEdgeTest.class);

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

    Object runWorkflow(List<Object> steps) {
        return runWorkflow(steps, null);
    }
    Object runWorkflow(List<Object> steps, ConfigBag extraEffectorConfig) {
        if (app==null) app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
                .putAll(extraEffectorConfig));
        eff.apply(app);

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
        Function<Boolean, Object> test = (explicitSubworkflow) ->
                runWorkflow(MutableList.of(
                                "let v1 = V1",
                                "let v2 = V2",
                                MutableMap.<String, Object>of("steps", MutableList.of(
                                                "let v0 = ${v0}B",
                                                "let v1 = ${v1}B",
                                                "let v3 = V3B"))
                                        .add(explicitSubworkflow ? MutableMap.of("step", "subworkflow") : null),
                                "return ${v0}-${v1}-${v2}-${v3}"),
                        ConfigBag.newInstance().configure(WorkflowCommonConfig.INPUT, MutableMap.of("v0", "V0")));
        Asserts.assertEquals(test.apply(true), "V0B-V1B-V2-V3B");

        // subworkflow is chosen implicitly if step is omitted
        Asserts.assertEquals(test.apply(false), "V0B-V1B-V2-V3B");

        runWorkflow(MutableList.of(
                MutableMap.of("steps", MutableList.of(
                        "let v1 = V1",
                        "goto marker",  // prefers inner id 'marker'
                        "let v1 = NOT_V1_1",
                        MutableMap.of("id", "marker", "step", "goto " + WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END), // goes to end of this subworkflow
                        "let v1 = NOT_V1_2")),
                "let v2 = V2",
                MutableMap.of("steps", MutableList.of(
                        "let v3 = V3",
                        "goto marker", // goes to outer id 'marker' because nothing local matching
                        "let v3 = NOT_V3")),
                MutableMap.of("id", "marker", "step", "let v4 = V4"),
                MutableMap.of("steps", MutableList.of(
                        "return ${v1}-${v2}-${v3}-${v4}")), // returns from outer workflow
                "let v4 = NOT_V4"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "V1-V2-V3-V4");
    }
    @Test
    public void testSubworkflowReturnsAndGotoEndsAndLabel() {
        runWorkflow(MutableList.of(
                "let x = 1",
                "if ${x} == 1 then return yes_if_returns",
                "return no_if_exited with ${yes_if_returns}"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "yes_if_returns");

        runWorkflow(MutableList.of(
                "let y = 0",
                "let x = 1",
                MutableMap.of("steps", MutableList.of(
                    "let x = ${x} + 1",
                    "goto "+WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END,  // goes to end of this subworkflow, but not outer
                    "let x = 3"  // shouldn't run
                )),
                "let y = ${x}_1",
                "if ${x} == 2 then let y = ${x}_2",
                "let output = ${y}",
                "goto "+WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END,
                "return should_skip_this"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "2_2");

        // return ends all local workflows, but not nested
        addBeanWithType("step-with-return", "1-SNAPSHOT", Strings.lines(
                "type: workflow",
                "steps:",
                "  - return inner"
        ));
        runWorkflow(MutableList.of(
                MutableMap.of("steps", MutableList.of(
                        MutableMap.of("steps", MutableList.of(
                                "step-with-return",
                                "return ${output}-then-1"
                        )),
                        "return no-2")),
                "return no-3"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "inner-then-1");

        // end means local workflow
        runWorkflow(MutableList.of(
                "let x = 1",
                "let output = not_expected",
                MutableMap.of("step", "if ${x} == 1", "steps", MutableList.of("goto "+WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END)),
                "return last_step_should_run"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "last_step_should_run");
        // but if inline, local workflow is the parent
        runWorkflow(MutableList.of(
                "let x = 1",
                "let output = expected",
                "if ${x} == 1 then goto "+WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END,
                "return last_step_should_not_run_when_inline"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "expected");

        // explicit label available from local subworkflow
        runWorkflow(MutableList.of(
                "let x = A",
                MutableMap.of(
                        "steps", MutableList.of(
                                "goto l1",  // better explicit flow
                                "let x = ${x}_no1"
                        )),
                "let x = ${x}_no2",
                "label l1",
                "return ${x}_B"));
        Asserts.assertEquals(lastInvocation.getUnchecked(), "A_B");
    }

    @Test
    public void testIfWorkflowStep() throws Exception {
        BiFunction<String,String,Object> run = (preface,cond) ->
                runWorkflow(MutableList.<Object>of(preface==null ? "let x = hi" : preface,
                    "let y = no",
                    "if "+(cond==null ? "${x}" : cond)+" then let y = yes",
                    "return ${y}"));

        Asserts.assertEquals(run.apply(null, null), "yes");

        Asserts.assertEquals(run.apply("let boolean x = true", null), "yes");
        Asserts.assertEquals(run.apply("let boolean x = false", null), "no");

        Asserts.assertEquals(run.apply(null, "${x} == hi"), "yes");
        Asserts.assertEquals(run.apply(null, "${x} == ho"), "no");
        Asserts.assertEquals(run.apply(null, "hi == ${x}"), "yes");
        Asserts.assertEquals(run.apply(null, "${x} == ${x}"), "yes");
        Asserts.assertEquals(run.apply("let boolean x = true", "${x} == true"), "yes");

        Asserts.assertEquals(run.apply("set-sensor xy = yes", "{ sensor: xy, equals: yes }"), "yes");
        Asserts.assertEquals(run.apply(null, "{ sensor: xn, equals: yes }"), "no");
        Asserts.assertFailsWith(() -> run.apply(null, "{ unknown_field: xn }"),
                Asserts.expectedFailureContainsIgnoreCase("unknown_field", "predicate"));

        // unresolvable things -- allowed iff no == block
        Asserts.assertEquals(run.apply(null, "${unresolvable_without_equals}"), "no");
        Asserts.assertFailsWith(() -> run.apply(null, "${x} == ${unresolvable_on_rhs}"),
                Asserts.expectedFailureContainsIgnoreCase("unresolvable_on_rhs", "missing"));
        Asserts.assertFailsWith(() -> run.apply(null, "${unresolvable_on_lhs} == ${x}"),
                Asserts.expectedFailureContainsIgnoreCase("unresolvable_on_lhs", "missing"));

        // flow - returns directly
        Asserts.assertEquals(runWorkflow(MutableList.of("let boolean x = true",
                "if ${x} then return yes",
                "return no")), "yes");
        Asserts.assertEquals(runWorkflow(MutableList.of("let integer x = 1",
                MutableMap.of("id", "loop", "step", "let x = ${x} + 1"),
                "if ${x} == 2 then goto loop",
                "return ${x}")), 3);
    }

    @Test
    public void testIfWorkflowWithSteps() throws Exception {
        BiFunction<String, String, Object> run = (preface, cond) ->
                runWorkflow(MutableList.<Object>of(preface==null ? "let x = hi" : preface,
                        MutableMap.of("step", "if "+(cond==null ? "${x}" : cond),
                            "steps", MutableList.of("let y = yes", "return ${y}")),
                        "return no"));

        Asserts.assertEquals(run.apply(null, null), "yes");
        Asserts.assertEquals(run.apply("let boolean x = false", null), "no");
    }

}
