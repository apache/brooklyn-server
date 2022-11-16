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

import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.utils.WorkflowConcurrency;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class WorkflowNestedAndCustomExtensionTest extends RebindTestFixture<TestApplication> {

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        app = null; // clear this
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override
    protected TestApplication createApp() {
        return null;
    }

    public RegisteredType addBeanWithType(String typeName, String version, String plan) {
        return BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt(), typeName, version,
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, plan));
    }

    ClassLogWatcher lastLogWatcher;
    TestApplication app;

    Object invokeWorkflowStepsWithLogging(List<Object> steps) throws Exception {
        return invokeWorkflowStepsWithLogging(steps, null);
    }
    Object invokeWorkflowStepsWithLogging(List<Object> steps, ConfigBag extraEffectorConfig) throws Exception {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {
            lastLogWatcher = logWatcher;

            if (app==null) app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));

            WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                    .configure(WorkflowEffector.STEPS, steps)
                    .putAll(extraEffectorConfig));
            eff.apply((EntityLocal)app);

            Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
            return invocation.getUnchecked();
        }
    }

    void assertLogStepMessages(String ...lines) {
        Assert.assertEquals(lastLogWatcher.getMessages(),
                Arrays.asList(lines));
    }

    @Test
    public void testNestedWorkflowBasic() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("type", "workflow",
                        "steps", MutableList.of("return done"))));
        Asserts.assertEquals(output, "done");
    }

    @Test
    public void testNestedWorkflowParametersForbiddenWhenUsedDirectly() throws Exception {
        Asserts.assertFailsWith(() -> invokeWorkflowStepsWithLogging(MutableList.of(
                        MutableMap.of("type", "workflow",
                                "parameters", MutableMap.of(),
                                "steps", MutableList.of("return done")))),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "parameters"));
    }

    @Test
    public void testExtendingAStepWhichWorksButIsMessyAroundParameters() throws Exception {
        /*
         * extending any step type is supported, but discouraged because of confusion and no parameter definitions;
         * the preferred way is to use the method in the following test
         */
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: log",
                "message: hi ${name}",
                "input:",
                "  name: you"
        ));

        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");

        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi")));
        assertLogStepMessages("hi you");
    }

    @Test
    public void testDefiningCustomWorkflowStep() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}"
        ));
        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");

        Asserts.assertFailsWith(() -> invokeWorkflowStepsWithLogging(MutableList.of(
                        MutableMap.of("type", "log-hi",
                                "steps", MutableList.of("return not allowed to override")))),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "steps"));
    }

    @Test
    public void testDefiningCustomWorkflowStepWithShorthand() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "shorthand: ${name}",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}"
        ));
        invokeWorkflowStepsWithLogging(MutableList.of("log-hi bob"));
        assertLogStepMessages("hi bob");
    }

    @Test
    public void testDefiningCustomWorkflowStepWithOutput() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}",
                "output:",
                "  message: hi ${name}"
        ));
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");
        Asserts.assertEquals(output, MutableMap.of("message", "hi bob"));

        // output can be overridden
        output = invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"), "output", "I said ${message}")));
        assertLogStepMessages("hi bob");
        Asserts.assertEquals(output, "I said hi bob");
    }

    @Test
    public void testTargetExplicitList() throws Exception {
        Object output;
        output = invokeWorkflowStepsWithLogging(MutableList.of(Iterables.getOnlyElement(Yamls.parseAll(Strings.lines(
                "type: workflow",
                "steps:",
                "  - type: workflow",
                "    target: 1..5",
                "    steps:",
                "    - let integer r = ${target} * 5 - ${target} * ${target}",
                "    - return ${r}",
                "    output: ${output}",
                "  - transform max = ${output} | max",
                "  - return ${max}",
                ""
        )))));
        Asserts.assertEquals(output, 6);
    }

    @Test
    public void testTargetChildren() throws Exception {
        Object output;
        output = invokeWorkflowStepsWithLogging(MutableList.of(Iterables.getOnlyElement(Yamls.parseAll(Strings.lines(
                "type: workflow",
                "steps:",
                "  - type: workflow",
                "    target: children",
                "    steps:",
                "    - return ${entity.id}",
                ""
        )))));
        Asserts.assertEquals(output, MutableList.of());

        TestEntity child1 = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        TestEntity child2 = app.createAndManageChild(EntitySpec.create(TestEntity.class));

        output = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).getUnchecked();
        Asserts.assertEquals(output, MutableList.of(child1.getId(), child2.getId()));
    }

    @Test
    public void testWorkflowConcurrencyComputation() throws Exception {
        Asserts.assertEquals(WorkflowConcurrency.parse("3").apply(2d), 3d);
        Asserts.assertEquals(WorkflowConcurrency.parse("all").apply(2d), 2d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(1,all)").apply(2d), 2d);
        Asserts.assertEquals(WorkflowConcurrency.parse("50%").apply(10d), 5d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(50%,30%+1)").apply(10d), 5d);
        Asserts.assertEquals(WorkflowConcurrency.parse("min(50%,30%+1)").apply(10d), 4d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(1,min(-10,30%+1))").apply(10d), 1d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(1,min(-10,30%+1))").apply(20d), 7d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(1,min(-10,30%+1))").apply(15d), 5d);
        Asserts.assertEquals(WorkflowConcurrency.parse("max(1,min(-10,30%-2))").apply(15d), 2.5d);
    }

    static AttributeSensor<Integer> INVOCATIONS = Sensors.newSensor(Integer.class, "invocations");
    static AttributeSensor<Integer> COUNT = Sensors.newSensor(Integer.class, "count");
    static AttributeSensor<String> GO = Sensors.newSensor(String.class, "go");

    @Test
    public void testTargetManyChildrenConcurrently() throws Exception {
        Object output = addTargetManyChildrenWorkflow(false, false, false, "children", "max(1,50%)");
        Asserts.assertEquals(output, MutableList.of());

        app.sensors().set(COUNT, 0);
        List<Entity> children = MutableList.of();

//        // to test just one
//        app.sensors().set(GO, "now!");
//        children.add(app.createAndManageChild(EntitySpec.create(TestEntity.class)));
//        app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).get();
//        app.sensors().set(COUNT, 0);
//        app.sensors().remove(GO);

        for (int i=children.size(); i<10; i++) children.add(app.createAndManageChild(EntitySpec.create(TestEntity.class)));

        Task<?> call = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        EntityAsserts.assertAttributeEqualsEventually(app, COUNT, 5);

//        // for extra check
//        Time.sleep(Duration.millis(100));

        // should only be allowed to run 5
        EntityAsserts.assertAttributeEquals(app, COUNT, 5);
        Asserts.assertFalse(call.isDone());

        app.sensors().set(GO, "now!");

        Asserts.assertEquals(call.getUnchecked(), children.stream().map(Entity::getId).collect(Collectors.toList()));
    }

    private Object addTargetManyChildrenWorkflow(boolean replayRoot, boolean replayAtNested, boolean replayInNested, String target, String concurrency) throws Exception {
        return invokeWorkflowStepsWithLogging((List<Object>) Yamls.parseAll(Strings.lines(
                // count outermost invocations (to see where replay started replays)
                "  - let invocations = ${entity.sensor.invocations} ?? 0",
                "  - let invocations = ${invocations} + 1",
                "  - set-sensor invocations = ${invocations}",
                "  - type: workflow",
                "    target: "+target,
                "    " + (replayAtNested ? "replayable: yes" : ""),
                "    concurrency: "+concurrency,
                "    steps:",
                // count subworkflow invocations for concurrency and for replays
                "    - let count = ${entity.parent.sensor.count}",
                "    - let inc = ${count} + 1",
                "    - step: set-sensor count = ${inc}",
                "      require: ${count}",
                "      sensor:",
                "        entity: ${entity.parent}",
                "      on-error:",
                "        - retry from start limit 20 backoff 1ms jitter -1",  // repeat until count is ours to increment
                "    - step: transform go = ${entity.parent.attributeWhenReady.go} | wait",
                "      " + (replayInNested ? "replayable: yes" : ""),
                "    - return ${entity.id}",
                ""
        )).iterator().next(), ConfigBag.newInstance()
                .configure(WorkflowCommonConfig.ON_ERROR, MutableList.of(MutableMap.of("condition", MutableMap.of("error-cause", MutableMap.of("glob", "*Dangling*")),
                        "step", "retry replay")))
                .configure(WorkflowCommonConfig.REPLAYABLE, replayRoot ? WorkflowReplayUtils.ReplayableOption.YES : WorkflowReplayUtils.ReplayableOption.NO));
    }

    protected Task<?> doTestTargetManyChildrenConcurrentlyWithReplay(boolean replayRoot, boolean replayAtNested, boolean replayInNested, String target, int numChildren, String concurrency, int numExpectedWhenWaiting) throws Exception {
        Object output = addTargetManyChildrenWorkflow(replayRoot, replayAtNested, replayInNested, target, concurrency);
        if ("children".equals(target)) Asserts.assertEquals(output, MutableList.of());

        app.sensors().set(COUNT, 0);
        app.sensors().set(INVOCATIONS, 0);
        app.sensors().remove(GO);

        for (int i=app.getChildren().size(); i<numChildren; i++) app.createAndManageChild(EntitySpec.create(TestEntity.class));

        Task<?> call = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        EntityAsserts.assertAttributeEqualsEventually(app, COUNT, numExpectedWhenWaiting);
        Asserts.assertFalse(call.isDone());

        app = rebind();

        EntityAsserts.assertAttributeEquals(app, COUNT, numExpectedWhenWaiting);

        app.sensors().set(GO, "now!");

        WorkflowExecutionContext lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(call.getId());
        return mgmt().getExecutionManager().getTask(Iterables.getLast(lastWorkflowContext.getReplays()).taskId);
    }

    @Test
    void testReplayInNestedWithOuterReplayingToo() throws Exception {
        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(true, true, true, "children", 10, "max(1,50%)", 5).get();
        Asserts.assertEquals(result, app.getChildren().stream().map(Entity::getId).collect(Collectors.toList()));
        EntityAsserts.assertAttributeEquals(app, COUNT, 10);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 1);
    }

    @Test
    void testReplayInNestedWithOuterReplayingTooNonList() throws Exception {
        // check if given a non-list target, it doesn't return a list

        // need app with child for initial workflow run
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
        TestEntity child = app.createAndManageChild(EntitySpec.create(TestEntity.class));
        // and need these initialized for initial workflow to work, since now it has a child
        app.sensors().set(COUNT, 0);
        app.sensors().set(INVOCATIONS, 0);
        app.sensors().set(GO, "now!");

        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(true, true, true, "${entity.children[0]}", 1, "max(1,50%)", 1).get();
        Asserts.assertEquals(result, child.getId());
        EntityAsserts.assertAttributeEquals(app, COUNT, 1);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 1);
    }

    @Test
    void testReplayAtNotInNested() throws Exception {
        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(false, true, false, "children", 10, "max(1,50%)", 5).get();
        Asserts.assertEquals(result, app.getChildren().stream().map(Entity::getId).collect(Collectors.toList()));
        EntityAsserts.assertAttributeEquals(app, COUNT, 15);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 1);
    }

    @Test
    void testReplayAtRoot() throws Exception {
        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(true, false, false, "children", 10, "max(1,50%)", 5).get();
        Asserts.assertEquals(result, app.getChildren().stream().map(Entity::getId).collect(Collectors.toList()));
        EntityAsserts.assertAttributeEquals(app, COUNT, 15);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 2);
    }

    @Test
    void testReplayInNestedOnly() throws Exception {
        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(false, false, true, "children", 10, "max(1,50%)", 5).get();
        Asserts.assertEquals(result, app.getChildren().stream().map(Entity::getId).collect(Collectors.toList()));
        EntityAsserts.assertAttributeEquals(app, COUNT, 10);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 1);
    }

}
