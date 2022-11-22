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
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
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
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.CountdownTimer;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class WorkflowNestedAndCustomExtensionTest extends RebindTestFixture<TestApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowNestedAndCustomExtensionTest.class);

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

    @Override protected TestApplication rebind() throws Exception {
        return rebind(RebindOptions.create().terminateOrigManagementContext(true));
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
        return addTargetManyChildrenWorkflow(replayRoot ? "from start" : null, replayAtNested, replayInNested, target, concurrency);
    }
    private Object addTargetManyChildrenWorkflow(String replayRoot, boolean replayAtNested, boolean replayInNested, String target, String concurrency) throws Exception {
        return invokeWorkflowStepsWithLogging((List<Object>) Yamls.parseAll(Strings.lines(
                // count outermost invocations (to see where replay started replays)
                "  - let invocations = ${entity.sensor.invocations} ?? 0",
                "  - let invocations = ${invocations} + 1",
                "  - set-sensor invocations = ${invocations}",
                "  - type: workflow",
                "    target: "+target,
                "    " + (replayAtNested ? "replayable: from here" : ""),
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
                "    - workflow replayable from here",
                "    - step: transform go = ${entity.parent.attributeWhenReady.go} | wait",
                "      idempotent: false",
                "      " + (replayInNested ? "replayable: from here" : ""),
                "    - return ${entity.id}",
                ""
        )).iterator().next(), ConfigBag.newInstance()
                .configure(WorkflowCommonConfig.ON_ERROR,
                        "automatically".equals(replayRoot) ? null
                            : MutableList.of(
                            MutableMap.of("condition", MutableMap.of("error-cause", MutableMap.of("glob", "*Dangling*")),
                            "step", "retry",
                            WorkflowCommonConfig.ON_ERROR.getName(), MutableList.of("log non-replay retry for ${workflow.id} due to ${workflow.error}", "retry from start"))))
                .configure(WorkflowCommonConfig.REPLAYABLE, replayRoot));
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

    @Test(groups="Integration", invocationCount = 100)
    public void testReplayInNestedOnlyManyTimes() throws Exception {
        testReplayInNestedOnly();
    }

    @Test
    void testReplayWithAutomaticRecovery() throws Exception {
        Object result = doTestTargetManyChildrenConcurrentlyWithReplay(false, false, true, "children", 10, "max(1,50%)", 5).get();
        Asserts.assertEquals(result, app.getChildren().stream().map(Entity::getId).collect(Collectors.toList()));
        EntityAsserts.assertAttributeEquals(app, COUNT, 10);
        EntityAsserts.assertAttributeEquals(app, INVOCATIONS, 1);
    }

    @Test
    public void testCustomWorkflowLock() {
        // based on WorkflowInputOutputTest.testSetSensorAtomicRequire

        app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        MutableMap.of(
                                "type", "workflow",
                                "lock", "incrementor",
                                "steps", MutableList.of(
                                        "let x = ${entity.sensor.x} ?? 0",
                                        "let x = ${x} + 1",
                                        "set-sensor x = ${x}",
                                        "return ${x}"
                                )
                ))));
        eff.apply((EntityLocal)app);

        // 100 parallel runs all get a lock and increment nicely
        List<Task<?>> tasks = MutableList.of();
        int NUM = 10;
        for (int i=0; i<NUM; i++) tasks.add(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null));
        List<?> result = tasks.stream().map(t -> t.getUnchecked()).collect(Collectors.toList());
        Asserts.assertSize(result, NUM);
        Asserts.assertEquals(MutableSet.copyOf(result).size(), NUM, "Some entries duplicated: "+result);
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("x"), NUM);
        EntityAsserts.assertAttributeEquals(app, Sensors.newStringSensor("lock-for-incrementor"), null);
    }

    @Test // a bit slow, but a good test of many things
    public void testCustomWorkflowLockInterrupted() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.run();
    }

    @Test(groups="Integration", invocationCount = 50)
    public void testCustomWorkflowLockInterruptedGateOpenEarly() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.OPEN_GATE_EARLY = true;
        fixture.run();
    }

    @Test(groups="Integration", invocationCount = 100)  // catch races
    public void testCustomWorkflowLockInterruptedManyTimes() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.NUM = 4;
        fixture.run();
    }

    @Test(groups="Integration") // very slow, but catches races at startup due to lots of tasks
    public void testCustomWorkflowLockInterruptedBigger() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.MAX_ALLOWED_BEFORE_GATE = 20;
        fixture.MIN_REQUIRED_BEFORE_REBIND = 10;
        fixture.NUM = 100;
        fixture.COMPLETION_TIMEOUT = Duration.seconds(60);
        fixture.run();
    }

    @Test
    public void testCustomWorkflowLockInterruptedNoAutoReplay() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.REPLAYABLE_INNER = false;
        fixture.REPLAYABLE_OUTER = false;
        fixture.run();
    }

    @Test(groups="Integration", invocationCount = 10)
    public void testCustomWorkflowLockInterruptedNoAutoReplayGateOpenEarly() throws Exception {
        CustomWorkflowLockInterruptedFixture fixture = new CustomWorkflowLockInterruptedFixture();
        fixture.REPLAYABLE_INNER = false;
        fixture.REPLAYABLE_OUTER = false;
        fixture.OPEN_GATE_EARLY = true;
        fixture.run();
    }

    class CustomWorkflowLockInterruptedFixture {

        int NUM = 10;
        int MAX_ALLOWED_BEFORE_GATE = 2;
        int MIN_REQUIRED_BEFORE_REBIND = 1;
        boolean REPLAYABLE_OUTER = true;
        boolean REPLAYABLE_INNER = true;
        boolean OPEN_GATE_EARLY = false;
        Consumer<String> waitABit = (phase) -> Time.sleep((long) (10 * Math.random()));
        Duration COMPLETION_TIMEOUT = Duration.seconds(15);

        public void run() throws Exception {
            // based on WorkflowInputOutputTest.testSetSensorAtomicRequire

            app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class));
            app.sensors().set(Sensors.newIntegerSensor("x"), 0);

            WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                    .configure(WorkflowCommonConfig.REPLAYABLE, "from start")
                    .configure(WorkflowCommonConfig.ON_ERROR, REPLAYABLE_OUTER ? MutableList.of("retry replay limit 10") : null)
                    .configure(WorkflowEffector.STEPS, MutableList.of(
                                    MutableMap.of(
                                            "type", "workflow",
                                            "lock", "incrementor",
                                            "replayable", "from start",
                                            "steps", MutableList.of(
                                                    "let x = ${entity.sensor.x}",
                                                    MutableMap.of(
                                                            "step", "log ${workflow.id} possibly replaying local ${x} actual ${entity.sensor.x}",
                                                            "replayable", "from here"),
                                                    MutableMap.of(
                                                            "step", "goto already-run", // already run
                                                            "condition", MutableMap.of("target", "${entity.sensor.x}", "not", MutableMap.of("equals", "${x}"))),
                                                    "let x = ${x} + 1",
                                                    MutableMap.of(
                                                            "step", "wait ${entity.attributeWhenReady.gate}",
                                                            // make it block after 1 run
                                                            "condition", MutableMap.of("target", "${entity.sensor.x}", "greater-than-or-equal-to", MAX_ALLOWED_BEFORE_GATE)
                                                    ),
                                                    "set-sensor x = ${x}",
                                                    "return ${x}",
                                                    MutableMap.of("id", "already-run", "step", "log ${workflow.id} already set sensor, or error or other mismatch, not re-setting"),
                                                    "return ${entity.sensor.x}"
                                            ),
                                            "on-error", REPLAYABLE_INNER ? MutableList.of("retry replay limit 10") : null)
                            )
                    ));
            eff.apply((EntityLocal) app);

            List<String> workflowIds = MutableList.of();
            for (int i = 0; i < NUM; i++) {
                Task<?> t = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
                workflowIds.add(t.getId());
            }
            EntityAsserts.assertAttributeEventually(app, Sensors.newIntegerSensor("x"), x -> x >= MIN_REQUIRED_BEFORE_REBIND);
            waitABit.accept("after min complete reached");

            if (OPEN_GATE_EARLY) {
                app.sensors().set(Sensors.newStringSensor("gate"), "open");
                waitABit.accept("after opening gate early");
            }
            //        Dumper.dumpInfo(app);

            log.info("Rebind starting, from mgmt "+mgmt());
            app = rebind();
            log.info("Rebind completed, mgmt now "+mgmt());

            Integer numCompleted = app.sensors().get(Sensors.newIntegerSensor("x"));
            log.info("Rebinding of lock test had " + numCompleted + " completed, of " + NUM);

            List<Task> autoRecoveredTasks = MutableList.of();
            List<Integer> result = MutableList.of();
            workflowIds.stream().forEach(wf -> {
                // wait on this dangling to complete
                WorkflowExecutionContext w = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(wf);
                if (w == null) {
                    Asserts.fail("Did not find workflow " + wf + " (from " + workflowIds + ")");
                }
                if (w.getOutput() != null) {
                    // already finished
                    result.add((Integer) w.getOutput());

                } else {
                    Maybe<Task<Object>> tm = w.getTask(false);
                    if (tm.isAbsent()) {
                        log.error("Workflow does not have task (yet?) - " + wf + " (from " + workflowIds + ")");
                        Asserts.fail("Workflow does not have task (yet?) - " + wf + " (see log)");
                    }
                    autoRecoveredTasks.add(tm.get());
                }
            });

            boolean expectAllComplete = false;
            boolean expectRightAnswer = false;
            if (REPLAYABLE_OUTER && REPLAYABLE_INNER) {
                if (!OPEN_GATE_EARLY) {
                    // should NOT finish until gate is set
                    waitABit.accept("after rebind, waiting on gate");
                    long stillRunningCount = autoRecoveredTasks.stream().filter(t -> !t.isDone()).count();
                    //                autoRecoveredTasks.forEach(Task::blockUntilEnded);
                    Asserts.assertTrue(stillRunningCount >= NUM - MAX_ALLOWED_BEFORE_GATE, "Only " + stillRunningCount + " waiting");

                    // now open the gate to let them through
                    app.sensors().set(Sensors.newStringSensor("gate"), "open");
                } else {
                    // can't say anything, except we expect the right answer
                }

                expectRightAnswer = true;
                expectAllComplete = true;

            } else if (!REPLAYABLE_OUTER && !REPLAYABLE_INNER) {
                // if replayable not correclty set, there is no guarantee the answer will be right, but all should complete
                expectAllComplete = true;
            }

            if (expectAllComplete) {
                CountdownTimer timer = CountdownTimer.newInstanceStarted(COMPLETION_TIMEOUT);
                for (Task t : autoRecoveredTasks) {
                    t.blockUntilEnded(timer.getDurationRemaining());
                    if (!t.isDone()) {
                        Asserts.fail("Workflow task should have finished: " + t.getStatusDetail(true));
                    }
                    if (!t.isError() || expectRightAnswer) result.add((Integer) t.getUnchecked());
                }
            }

            // lock should now be cleared
            EntityAsserts.assertAttributeEquals(app, Sensors.newStringSensor("lock-for-incrementor"), null);

            if (expectRightAnswer) {
                Asserts.assertSize(result, NUM);
                Asserts.assertEquals(MutableSet.copyOf(result).size(), NUM, "Some entries duplicated: " + result);
                Asserts.assertEquals(result.stream().max(Integer::compare).orElse(null), (Integer) NUM, "Wrong max returned: " + result);
                EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("x"), NUM);
            }

            if (!REPLAYABLE_OUTER && !REPLAYABLE_INNER) {
                // do a manual replay

                List<Task<Object>> newReplays = workflowIds.stream()
                        .map(wf -> new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(wf))
                        .filter(wf -> wf.getStatus().error)
                        .map(wf -> DynamicTasks.submit(wf.createTaskReplaying(wf.makeInstructionsForReplayingFromLastReplayable("test", false)), app))
                        .collect(Collectors.toList());

                if (!OPEN_GATE_EARLY) {
                    // should NOT finish until gate is set
                    waitABit.accept("after rebind, waiting on gate");
                    long stillRunningCount = newReplays.stream().filter(t -> !t.isDone()).count();
                    //                autoRecoveredTasks.forEach(Task::blockUntilEnded);
                    Asserts.assertTrue(stillRunningCount >= NUM - MAX_ALLOWED_BEFORE_GATE, "Only " + stillRunningCount + " waiting");

                    // now open the gate to let them through
                    app.sensors().set(Sensors.newStringSensor("gate"), "open");
                }

                CountdownTimer timer = CountdownTimer.newInstanceStarted(COMPLETION_TIMEOUT);
                newReplays.forEach(t -> result.add((Integer) t.getUnchecked(timer.getDurationRemaining())));

                log.info("Results: "+result);
                // not guaranteed to get right answer because lock will be lost and cannot know if sensor was set or not, but at most 1 case
                Asserts.assertSize(result, NUM);  // should run exactly the right number
                // allowed to have 1 duplicate
                Asserts.assertTrue(MutableSet.copyOf(result).size() >= NUM-1, "Too many duplicates: " + result);
                // and/or for max to be 1 more or less (more if task with lock incremented sensor twice)
                Asserts.assertTrue(result.stream().max(Integer::compare).orElse(null) <= NUM+1, "Wrong max returned, too high: " + result);
                Asserts.assertTrue(result.stream().max(Integer::compare).orElse(null) >= NUM-1, "Wrong max returned, too low: " + result);
                // sensor value should be one of the results
                EntityAsserts.assertAttribute(app, Sensors.newIntegerSensor("x"), v -> result.contains(v));
            }

        }
    }

}
