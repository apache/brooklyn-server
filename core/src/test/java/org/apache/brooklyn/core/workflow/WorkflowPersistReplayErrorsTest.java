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

import com.google.common.base.Stopwatch;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.EntityManagementSupport;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class WorkflowPersistReplayErrorsTest extends RebindTestFixture<BasicApplication> {

    private static final Logger log = LoggerFactory.getLogger(WorkflowPersistReplayErrorsTest.class);

    private BasicApplication app;

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    @Override
    protected BasicApplication createApp() {
        return null;
    }

    @Override protected BasicApplication rebind() throws Exception {
        return rebind(RebindOptions.create().terminateOrigManagementContext(true));
    }

    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        return runSteps(steps, appFunction, null);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction, ConfigBag initialEffectorConfig) {
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        return runStepsOnExistingApp("myWorkflow", steps, appFunction, initialEffectorConfig);
    }
    Task<?> runStepsOnExistingApp(String effectorName, List<Object> steps, Consumer<BasicApplication> appFunction, ConfigBag initialEffectorConfig) {
        addEffector(effectorName, steps, appFunction, initialEffectorConfig);

        return app.invoke(app.getEntityType().getEffectorByName(effectorName).get(), null);
    }

    private WorkflowEffector addEffector(String effectorName, List<Object> steps, Consumer<BasicApplication> appFunction, ConfigBag initialEffectorConfig) {
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, effectorName)
                .configure(WorkflowEffector.STEPS, steps)
                .copy(initialEffectorConfig)
        );
        if (appFunction !=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);
        return eff;
    }

    Task<?> lastInvocation;
    WorkflowExecutionContext lastWorkflowContext;

    public final static List<Object> INCREMENTING_X_STEPS = MutableList.<Object>of(
            "let integer x = ${entity.sensor.x} ?? 0",
            "let x = ${x} + 1",
            "set-sensor x = ${x}",
            "wait ${entity.attributeWhenReady.gate}",
            "let x = ${entity.sensor.x} + 10",
            "set-sensor x = ${x}",
            "return ${x}").asUnmodifiable();

    private void runIncrementingX() {
        lastInvocation = runSteps(INCREMENTING_X_STEPS, null);

        // ensure workflow sensor set immediately (this is done synchronously when effector invocation task is created, to ensure it can be resumed)
        findSingleLastWorkflow();
    }

    private void runIncrementingXAwaitingGate() {
        runIncrementingX();

        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);
        // refresh this for good measure (it won't have changed but feels like bad practice to rely on that)
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
    }

    @Test
    public void testWorkflowSensorValuesWhenPausedAndCanReplay() throws IOException {
        runIncrementingXAwaitingGate();

        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);

        Integer index = lastWorkflowContext.getCurrentStepIndex();
        Asserts.assertTrue(index >= 2 && index <= 3, "Index is "+index);
        Asserts.assertEquals(lastWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.RUNNING);
        Asserts.assertFalse(lastInvocation.isDone());

        app.sensors().set(Sensors.newBooleanSensor("gate"), true);

        lastInvocation.blockUntilEnded(Duration.seconds(2));
        Asserts.assertEquals(lastInvocation.getUnchecked(), 11);
        Asserts.assertEquals(lastWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.SUCCESS);

        app.sensors().set(Sensors.newBooleanSensor("gate"), false);
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.factory(false).createTaskReplaying(lastWorkflowContext.factory(false).makeInstructionsForReplayingFromStep(1, "Test", true)), app);
        // sensor should go back to 1 because workflow vars are stored per-state
        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);
        Time.sleep(10);
        Asserts.assertFalse(invocation2.isDone());
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        invocation2.blockUntilEnded(Duration.seconds(2));
        Asserts.assertEquals(invocation2.getUnchecked(), 11);
    }

    @Test
    public void testWorkflowInterruptedAndCanReplay() throws IOException {
        runIncrementingXAwaitingGate();

        // cancel the workflow
        lastWorkflowContext.getTask(true).get().cancel(true);

        // workflow should no longer proceed even if gate is set
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);

        lastInvocation.blockUntilEnded(Duration.seconds(2));
        Asserts.assertTrue(lastInvocation.isError());

        // sensor should not increment to 2
        // workflow should go to error (before invocation ended)
        // TODO should show error when persisted
        lastWorkflowContext.persist();
        Asserts.assertThat(lastWorkflowContext.status, status -> status.error);
        // and sensor will not be set to 2
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 1);

        Integer index = lastWorkflowContext.getCurrentStepIndex();
        Asserts.assertTrue(index >= 2 && index <= 3, "Index is "+index);

        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.factory(false).createTaskReplaying(lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", true)), app);
        // the gate is set so this will finish soon
        Asserts.assertEquals(invocation2.getUnchecked(), 11);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 11);
    }

    public static <T> T possiblyWithAutoFailAndReplay(boolean autoFailAndResume, Callable<T> callable) throws Exception {
        Boolean old = null;
        try {
            if (!autoFailAndResume) {
                old = EntityManagementSupport.AUTO_FAIL_AND_RESUME_WORKFLOWS;
                EntityManagementSupport.AUTO_FAIL_AND_RESUME_WORKFLOWS = false;
            }
            return callable.call();
        } finally {
            if (old!=null) {
                EntityManagementSupport.AUTO_FAIL_AND_RESUME_WORKFLOWS = old;
            }
        }
    }

    public static void possiblyWithAutoFailAndReplay(boolean autoFailAndResume, Runnable runnable) {
        try {
            possiblyWithAutoFailAndReplay(autoFailAndResume, () -> {
                runnable.run();
                return null;
            });
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    @Test(groups="Integration", invocationCount = 10)  // because slow, and tests variety of interruption points with randomised delay
    public void testWorkflowShutdownAndCanReplay() throws Exception {
        doTestWorkflowShutdownAndCanReplay(false);
    }

    @Test(groups="Integration", invocationCount = 10)  // because slow, and tests variety of interruption points with randomised delay
    public void testWorkflowShutdownAndAutomaticReplay() throws Exception {
        doTestWorkflowShutdownAndCanReplay(true);
    }

    public void doTestWorkflowShutdownAndCanReplay(boolean autoFailAndReplay) throws Exception {
        runIncrementingX();

        // variable delay, shouldn't matter when it is interrupted
        Time.sleep((long) (Math.random()*Math.random()*200));

        possiblyWithAutoFailAndReplay(autoFailAndReplay, () -> {
            // do rebind
            ManagementContext oldMgmt = mgmt();
            app = rebind();

            // shutdown flags should get set on old object when destroyed
            ((ManagementContextInternal) oldMgmt).terminate();
            WorkflowExecutionContext prevWorkflowContext = lastWorkflowContext;
            Asserts.eventually(() -> prevWorkflowContext.status, status -> status.error);
            Asserts.assertEquals(prevWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN);

            // new object is present
            lastWorkflowContext = findSingleLastWorkflow();

            if (autoFailAndReplay) {
                assertReplayedAfterRebindAndEventuallyThrowsDangling(lastWorkflowContext);

            } else {
                // status should have been persisted as not ended
                Asserts.assertEquals(lastWorkflowContext.status.ended, false);
            }

            Asserts.assertThat(lastWorkflowContext.currentStepIndex, x -> x == null || x <= 3);

            // sensor might be one or might be null
            Asserts.assertThat(app.sensors().get(Sensors.newSensor(Integer.class, "x")), x -> x == null || x == 1);

            // now we can tell it to resume from where it crashed
            lastInvocation = Entities.submit(app, lastWorkflowContext.factory(false).createTaskReplaying(lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", true)));

            // will wait on gate, ie not finish
            Time.sleep((long) (Math.random() * Math.random() * 200));
            Asserts.assertFalse(lastInvocation.isDone());

            // workflow should now complete when gate is set
            app.sensors().set(Sensors.newBooleanSensor("gate"), true);
            Asserts.assertEquals(lastInvocation.get(), 11);
            EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 11);
            return null;
        });
    }

    private WorkflowExecutionContext findSingleLastWorkflow() {
        Map<String, WorkflowExecutionContext> workflow = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app);
        Asserts.assertSize(workflow, 1);
        lastWorkflowContext = workflow.values().iterator().next();
        return lastWorkflowContext;
    }

    private WorkflowExecutionContext findLastWorkflow(String workflowId) {
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(workflowId);
        return Asserts.assertNotNull(lastWorkflowContext, "Cannot find workflow for "+workflowId);
    }

    private void assertReplayedAfterRebindAndEventuallyThrowsDangling(WorkflowExecutionContext context) {
        lastInvocation = mgmt().getExecutionManager().getTask(context.getTaskId());
        Asserts.assertNotNull(lastInvocation);
        lastInvocation.blockUntilEnded();

        // should get the dangling error
        Asserts.assertEquals(context.status, WorkflowExecutionContext.WorkflowStatus.ERROR);
        try {
            lastInvocation.get();
            Asserts.shouldHaveFailedPreviously("Expected to throw "+DanglingWorkflowException.class);
        } catch (Exception ex) {
            // expected!
            if (Exceptions.getFirstThrowableOfType(ex, DanglingWorkflowException.class)==null) {
                throw new AssertionError("Wrong exception: "+ex, ex);
            }
        }
    }

    @Test
    public void testShutdownNotedIfManagementStopped() throws IOException {
        runIncrementingXAwaitingGate();

        Entities.destroyAll(mgmt());

        lastInvocation.blockUntilEnded(Duration.seconds(5));
        Asserts.assertTrue(lastInvocation.isError());

        // error is set, although it will not usually be persisted
        Asserts.eventually(() -> lastWorkflowContext.status, status -> status.error);
        if (lastWorkflowContext.status == WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN || lastWorkflowContext.status == WorkflowExecutionContext.WorkflowStatus.ERROR_ENTITY_DESTROYED) {
            // as expected
        } else if (lastWorkflowContext.status == WorkflowExecutionContext.WorkflowStatus.ERROR) {
            // sometimes happens; to be investigated
            log.warn("Workflow ended with error, not error shutdown; value:\n"+lastInvocation.getStatusDetail(true));
        } else {
            log.error("Workflow ended with wrong error status: "+lastWorkflowContext.status);
            Asserts.fail("Workflow ended with wrong error status: "+lastWorkflowContext.status+ " / value:\n"+lastInvocation.getStatusDetail(true));
        }
    }

    @Test(groups="Integration", invocationCount = 10)  // because a bit slow and non-deterministic
    public void testNestedEffectorShutdownAndReplayedAutomatically() throws Exception {
        doTestNestedWorkflowShutdownAndReplayed(true, "invoke-effector incrementXWithGate", app->{
            new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "incrementXWithGate")
                    .configure(WorkflowEffector.STEPS, INCREMENTING_X_STEPS)
            ).apply((EntityLocal)app);
        });
    }

    @Test(groups="Integration", invocationCount = 10)  // because a bit slow and non-deterministic
    public void testNestedWorkflowShutdownAndReplayedAutomatically() throws Exception {
        doTestNestedWorkflowShutdownAndReplayed(true, MutableMap.of("type", "workflow", "steps", INCREMENTING_X_STEPS), null);
    }

    @Test(groups="Integration", invocationCount = 10)  // because a bit slow and non-deterministic
    public void testNestedEffectorShutdownAndReplayedManually() throws Exception {
        doTestNestedWorkflowShutdownAndReplayed(false, "invoke-effector incrementXWithGate", app->{
            new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "incrementXWithGate")
                    .configure(WorkflowEffector.STEPS, MutableList.<Object>of("log running nested incrementX in ${workflow.name}").appendAll(INCREMENTING_X_STEPS))
            ).apply((EntityLocal)app);
        });
    }

    @Test(groups="Integration", invocationCount = 10)  // because a bit slow and non-deterministic
    public void testNestedWorkflowShutdownAndReplayedManually() throws Exception {
        doTestNestedWorkflowShutdownAndReplayed(false, MutableMap.of("type", "workflow", "steps", INCREMENTING_X_STEPS), null);
    }

    void doTestNestedWorkflowShutdownAndReplayed(boolean autoFailAndReplay, Object call, Consumer<BasicApplication> initializer) throws Exception {
        lastInvocation = runSteps(MutableList.<Object>of(
                "let y = ${entity.sensor.y} ?? 0",
                "let y = ${y} + 1",
                "set-sensor y = ${y}",
                call,
                "let x = ${workflow.previous_step.output}",
                "let y = ${y} + 10",
                "set-sensor y = ${y}",
                "let z = ${y} * 100 + ${x}",
                "return ${z}"
        ), initializer);

        // run once with no interruption, make sure fine
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(lastInvocation.get(), 1111);
        app.sensors().set(Sensors.newBooleanSensor("gate"), false);

        // now invoke again
        lastInvocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        String workflowId = BrooklynTaskTags.getWorkflowTaskTag(lastInvocation, false).getWorkflowId();

        // interrupt at any point, probably when gated
        Time.sleep((long) (Math.random()*Math.random()*200));
        ManagementContext oldMgmt = mgmt();

        possiblyWithAutoFailAndReplay(autoFailAndReplay, () -> {
            app = rebind();
            ((ManagementContextInternal) oldMgmt).terminate();

            lastWorkflowContext = findLastWorkflow(lastInvocation.getId());

            if (autoFailAndReplay) {
                assertReplayedAfterRebindAndEventuallyThrowsDangling(lastWorkflowContext);

            } else {
                // status should have been persisted as not ended
                Asserts.assertEquals(lastWorkflowContext.status.ended, false);
            }

            lastInvocation = Entities.submit(app, lastWorkflowContext.factory(false).createTaskReplaying(lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", true)));
            if (lastInvocation.blockUntilEnded(Duration.millis(20))) {
                Asserts.fail("Invocation ended when it shouldn't have, with "+lastInvocation.get());
            }

            app.sensors().set(Sensors.newBooleanSensor("gate"), true);
            Asserts.assertEquals(lastInvocation.get(), 2222);

            // wait for effector to be invoked
            EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 22);
            EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "y"), 22);
            return null;
        });
    }

    public static List<Object> INCREMENTING_X_STEPS_SETTING_REPLAYABLE(boolean replayableOnBuggyStep1, boolean replayableOnGoodStep2) {
        return MutableList.<Object>of(
            MutableMap.of("s", "let integer x = ${entity.sensor.x} ?? 0", "replayable", replayableOnBuggyStep1 ? "from here" : null),
            MutableMap.of("s", "let x = ${x} + 1", "replayable", replayableOnGoodStep2 ? "from here" : null),
            "set-sensor x = ${x}",
            "wait ${entity.attributeWhenReady.gate}",
            "return ${x}").asUnmodifiable();
    }

    private void runIncrementingXSettingReplayable(boolean replayableOnBuggyStep1, boolean replayableOnGoodStep2) {
        lastInvocation = runSteps(INCREMENTING_X_STEPS_SETTING_REPLAYABLE(replayableOnBuggyStep1, replayableOnGoodStep2), null);

        // ensure workflow sensor set immediately (this is done synchronously when effector invocation task is created, to ensure it can be resumed)
        findSingleLastWorkflow();
    }

    private void runIncrementingXAwaitingGateSettingReplayable(boolean replayableOnBuggyStep1, boolean replayableOnGoodStep2) {
        runIncrementingXSettingReplayable(replayableOnBuggyStep1, replayableOnGoodStep2);

        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);
        // refresh this for good measure (it won't have changed but feels like bad practice to rely on that)
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableBuggy() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(true, false, false, true);
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableSensibleReplayPoint() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(false, true, false, false);
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableSensibleResuming() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(false, false, true, false);
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableSensibleResumingEvenIfBuggyReplayPoint() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(true, false, true, false);
    }

    void doTestWorkflowInterruptedAndCanReplaySettingReplayable(boolean replayableOnBuggyStep1, boolean replayableOnGoodStep2, boolean isResuming, boolean expectBuggy) throws IOException {
        runIncrementingXAwaitingGateSettingReplayable(replayableOnBuggyStep1, replayableOnGoodStep2);

        // cancel the workflow
        lastWorkflowContext.getTask(true).get().cancel(true);
        lastInvocation.blockUntilEnded(Duration.seconds(2));

        // in buggy mode, should want to continue from 0
        int expected = -1;
        if (expectBuggy) {
            Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> step == 0);
            expected = 2;
        } else {
            if (!isResuming) {
                Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> step == 1);
            }
            expected = 1;
        }

        // replay
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.factory(false).createTaskReplaying(
                isResuming
                    ? lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", false)
                    : lastWorkflowContext.factory(false).makeInstructionsForReplayingFromLastReplayable("test", false)
                ), app);

        // should get 2 because it replays from 0
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(invocation2.getUnchecked(), expected);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), expected);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayable_BuggyNestedReplay() throws IOException {
        doTestNestedEffectorWorkflowInterruptedAndCanReplaySettingReplayable(true, true, true, false, false,
                1, 0, true);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayable_BuggyTopLevelReplay() throws IOException {
        doTestNestedEffectorWorkflowInterruptedAndCanReplaySettingReplayable(true, false,false, false, false,
                -1, null, true);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayable_AllPointsAreBuggy() throws IOException {
        doTestNestedEffectorWorkflowInterruptedAndCanReplaySettingReplayable(true, true, true, true, false,
                1, 1, true);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayable_SensibleResuming() throws IOException {
        doTestNestedEffectorWorkflowInterruptedAndCanReplaySettingReplayable(true, true, true, false, true,
                1, 0, false);
    }

    void doTestNestedEffectorWorkflowInterruptedAndCanReplaySettingReplayable(boolean replayableFromStartOuter, boolean replayableOnEffector,
                                                                              boolean replayableOnBuggyStep1, boolean replayableOnGoodStep2, boolean isResuming,
                                                                              Integer expectedOuterStep, Integer expectedNestedStep, boolean expectBuggy) throws IOException {
        // replay is only "good" if we set a replay point _after_ the sensor was read;
        // otherwise it increments x twice
        lastInvocation = runSteps(
                MutableList.of(
                    "sleep 20ms",
                    MutableMap.of("s", "invoke-effector nestedWorkflow", "replayable", replayableOnEffector ? "from here" : null)),
                app -> {
                    List<Object> steps = INCREMENTING_X_STEPS_SETTING_REPLAYABLE(replayableOnBuggyStep1, replayableOnGoodStep2);

                    WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                            .configure(WorkflowEffector.EFFECTOR_NAME, "nestedWorkflow")
                            .configure(WorkflowEffector.STEPS, steps)
                    );
                    eff.apply((EntityLocal)app);
                },
                ConfigBag.newInstance().configure(WorkflowEffector.REPLAYABLE, replayableFromStartOuter ? "from start" : null));

        // ensure workflow sensor set immediately (this is done synchronously when effector invocation task is created, to ensure it can be resumed)
        findSingleLastWorkflow();

        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);
        Time.sleep(Duration.millis(50));  // give it 50ms to make sure it gets to the waiting step
        // refresh this for good measure (it won't have changed but feels like bad practice to rely on that)
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();

        // cancel the workflow
        lastWorkflowContext.getTask(true).get().cancel(true);
        lastInvocation.blockUntilEnded(Duration.seconds(2));

        // in buggy mode, should want to continue from 0
        int expected = -1;
        Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> Objects.equals(step, expectedOuterStep));

        Map<String, WorkflowExecutionContext> workflows = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app);
        Asserts.assertSize(workflows, 2);
        WorkflowExecutionContext nestedWorkflow = workflows.values().stream().filter(w -> w.getParentTag() != null).findAny().get();

        Asserts.assertThat(nestedWorkflow.replayableLastStep, step -> Objects.equals(step, expectedNestedStep));

        if (expectBuggy) {
            expected = 2;
        } else {
            expected = 1;
        }

        // replay
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.factory(false).createTaskReplaying(
                isResuming
                        ? lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", false)
                        : lastWorkflowContext.factory(false).makeInstructionsForReplayingFromLastReplayable("test", false)
        ), app);

        // should get 2 because it replays from 0
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(invocation2.getUnchecked(), expected);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), expected);
    }

    @Test
    public void testSimpleErrorHandlerOnStep() throws IOException {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(getClass().getPackage().getName())) {
            lastInvocation = runSteps(MutableList.of(
                            MutableMap.of("s", "invoke-effector does-not-exist",
                                    "output", "should have failed",
                                    "on-error", MutableList.of(
                                            MutableMap.of("type", "no-op",
                                                    "output", "error-handler worked!")))),
                    null);
            Asserts.assertEquals(lastInvocation.getUnchecked(), "error-handler worked!");

            List<String> msgs = logWatcher.getMessages();
            log.info("Error handler output:\n"+msgs.stream().collect(Collectors.joining("\n")));
            Asserts.assertEntriesSatisfy(msgs, MutableList.of(
                m -> m.matches("Starting workflow 'myWorkflow .workflow effector.', moving to first step .*-1"),
                m -> m.matches("Starting step .*-1 in task .*"),
                m -> m.matches("Encountered error in step .*-1 '1 - invoke-effector does-not-exist' .handler present.: No effector matching 'does-not-exist'"),
                m -> m.matches("Starting .*-1-error-handler with 1 step in task .*"),
                m -> m.matches("Starting .*-1-error-handler-1 in task .*"),
                m -> m.matches("Completed handler .*-1-error-handler; no next step indicated so proceeding to default next step"),
                m -> m.matches("Completed step .*-1; no further steps: Workflow completed"),
                m -> m.matches("Completed workflow .* successfully; step count: 1 considered, 1 executed") ));

            lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
            Asserts.assertEquals(lastWorkflowContext.currentStepIndex, (Integer) 1);
        }
    }

    @Test
    public void testSimpleErrorHandlerOnWorkflow() throws IOException {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(getClass().getPackage().getName())) {
            lastInvocation = runSteps(MutableList.of(
                            MutableMap.of("s", "invoke-effector does-not-exist",
                                    "output", "should have failed"),
                            "log should not run"),
                    null,
                    ConfigBag.newInstance().configure(
                            WorkflowEffector.ON_ERROR, MutableList.of(
                                    MutableMap.of("type", "no-op",
                                            "output", "error-handler worked!"))));
            Asserts.assertEquals(lastInvocation.getUnchecked(), "error-handler worked!");

            List<String> msgs = logWatcher.getMessages();
            log.info("Error handler output:\n"+msgs.stream().collect(Collectors.joining("\n")));
            Asserts.assertEntriesSatisfy(msgs, MutableList.of(
                    m -> m.matches("Starting workflow 'myWorkflow .workflow effector.', moving to first step .*-1"),
                    m -> m.matches("Starting step .*-1 in task .*"),
                    m -> m.matches("myWorkflow .*: Error in step '1 - invoke-effector does-not-exist'; rethrowing: No effector matching 'does-not-exist'"),
                    m -> m.matches("Error in workflow 'myWorkflow .workflow effector.' around step .*-1, running error handler"),
                    m -> m.matches("Encountered error in workflow .*/.* 'myWorkflow' .handler present.: No effector matching 'does-not-exist'"),
                    m -> m.matches("Starting .*-error-handler with 1 step in task .*"),
                    m -> m.matches("Starting .*-error-handler-1 in task .*"),
                    m -> m.matches("Completed handler .*-error-handler; no next step indicated so proceeding to default next step"),
                    m -> m.matches("Handled error in workflow around step .*-1; inferred next step 'end': Workflow completed")));
        }
    }

    @Test
    public void testSimpleErrorHandlerOnWorkflowFailing() throws IOException {
        lastInvocation = runSteps(MutableList.of("invoke-effector does-not-exist"),
                null,
                ConfigBag.newInstance().configure(
                        WorkflowEffector.ON_ERROR, MutableList.of("set-sensor had_error = yes", "fail rethrow message rethrown")) );
        Asserts.assertFailsWith(() -> lastInvocation.getUnchecked(), e -> {
            Asserts.expectedFailureContains(e, "rethrown");
            Asserts.assertThat(Exceptions.getCausalChain(e), ee -> ee.stream().filter(e2 -> e2.toString().contains("does-not-exist")).findAny().isPresent());
            return true;
        });
        Asserts.assertEquals(app.sensors().get(Sensors.newSensor(Object.class, "had_error")), "yes");
        findSingleLastWorkflow();
        Asserts.assertEquals(lastWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.ERROR);
    }

    @Test
    public void testErrorHandlerListWithGotoExit() throws IOException {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(getClass().getPackage().getName())) {
            lastInvocation = runSteps(MutableList.of(
                            MutableMap.of("s", "invoke-effector does-not-exist",
                                    "output", "NOT returned",
                                    "on-error", MutableList.of(
                                            "log Error handler 1-1",
                                            MutableMap.of("step", "log Error handler 1-2",
                                                    "output", "from 1-2"),
                                            MutableMap.of("step", "log NOT shown because of condition",
                                                    "condition", MutableMap.of("target", "${output}", "equals", "NOT matched")),
                                            MutableMap.of("step", "log Error handler 1-4 has output ${output}",
                                                    "condition", MutableMap.of("target", "${output}", "glob", "from *")),
                                            MutableMap.of("step", "log Step created-but-not-logged because of bad variable ${not_available}",
                                                    "on-error", MutableList.of(
                                                            MutableMap.of("step", "log Error handler 1-5-1", "output", "from 1-5-1"),
                                                            "goto exit",
                                                            "log NOT shown after inner exit")
                                                    ),
                                            "log Error handler 1-6",
                                            "goto exit",
                                            "log NOT shown because of earlier exit")
                                    ),
                            "log Step 2 has output ${output}"
                            ),
                    null);
            Asserts.assertEquals(lastInvocation.getUnchecked(), "from 1-5-1");

            List<String> msgs = logWatcher.getMessages();
            log.info("Error handler output:\n"+msgs.stream().collect(Collectors.joining("\n")));
            Asserts.assertEquals(msgs.stream().filter(s -> s.contains("NOT")).findAny().orElse(null), null);
            Asserts.assertEquals(msgs.stream().filter(s -> s.contains("created-but-not-logged") && !s.contains("Creating handler")).findAny().orElse(null), null);
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("1-1")).findAny().orElse(null));
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("1-2")).findAny().orElse(null));
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("1-4")).findAny().orElse(null));
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("1-5-1")).findAny().orElse(null));
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("1-6")).findAny().orElse(null));
            Asserts.assertNotNull(msgs.stream().filter(s -> s.contains("Step 2 has output from 1-5-1")).findAny().orElse(null));
        }
    }

    @Test
    public void testErrorHandlerRethrows() throws IOException {
        lastInvocation = runSteps(MutableList.of(
                        MutableMap.of("step", "fail message expected exception",
                                "output", "should have failed",
                                "on-error", MutableList.of(
                                        MutableMap.of("step", "return not applicable",
                                                "condition", "not matched")))),
                null);
        Asserts.assertFailsWith(() -> Asserts.fail("Did not fail, returned: "+lastInvocation.getUnchecked()),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "expected exception"));
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
        Asserts.assertEquals(lastWorkflowContext.currentStepIndex, (Integer) 0);
    }

    @Test
    public void testMultilineErrorRegex() throws IOException {
        lastInvocation = runSteps(MutableList.of(
                        MutableMap.of("step", "log ${var_does_not_exist}",
                                "output", "should have failed",
                                "on-error", MutableList.of(
                                        MutableMap.of("step", "return error handled",
                                                "condition", MutableMap.of("regex", ".*InvalidReference.*var_does_not_exist.*"))))),
                null);
        Asserts.assertEquals(lastInvocation.getUnchecked(), "error handled");
    }

    @Test
    public void testTimeoutOnStep() throws Exception {
        doTestTimeout(false, true);
    }

    @Test
    public void testTimeoutOnWorkflow() throws Exception {
        doTestTimeout(false, false);
    }

    @Test
    public void testTimeoutNotExceededOnStep() throws Exception {
        doTestTimeout(true, true);
    }

    @Test
    public void testTimeoutNotExceededOnWorkflow() throws Exception {
        doTestTimeout(true, false);
    }

    public void doTestTimeout(boolean finishesBeforeTimeout, boolean onStep) throws Exception {
        Stopwatch sw = Stopwatch.createStarted();
        Duration sleepTime, timeout;
        ConfigBag effConfig = ConfigBag.newInstance();
        if (finishesBeforeTimeout) {
            sleepTime = Duration.seconds(1);
            timeout = Duration.seconds(10);
        } else {
            sleepTime = Duration.seconds(10);
            timeout = Duration.seconds(1);
        }

        Map<String,Object> step = MutableMap.of("s", "sleep "+sleepTime);
        if (!onStep) effConfig.configure(WorkflowEffector.TIMEOUT, timeout);
        else step.put("timeout", ""+timeout);

        try {
            lastInvocation = runSteps(MutableList.of(step), null, effConfig);
            Object result = lastInvocation.getUnchecked();

            if (finishesBeforeTimeout) {
                // expected
            } else {
                throw Asserts.fail("Should have timed out, instead gave: " + result);
            }

        } catch (Exception e) {
            if (!finishesBeforeTimeout && Exceptions.getFirstThrowableOfType(e, onStep ? (Class)TimeoutException.class : CancellationException.class)!=null) {
                // expected; for step, the workflow should always capture it as a timeout exception;
                // for workflow-level timeouts, per comments in WorkflowExecutionContext where it throws TimeoutException,
                // we cannot easily prevent a CancellationException from being reported
            } else {
                throw Asserts.fail(e);
            }
        }

        if (finishesBeforeTimeout) {
            if (Duration.of(sw).isShorterThan(sleepTime))
                Asserts.fail("Finished too quick: " + Duration.of(sw));
            if (Duration.of(sw).isLongerThan(timeout)) Asserts.fail("Took too long: " + Duration.of(sw));
        } else {
            if (Duration.of(sw).isShorterThan(timeout)) Asserts.fail("Finished too quick: " + Duration.of(sw));
            if (Duration.of(sw).isLongerThan(sleepTime)) Asserts.fail("Took too long: " + Duration.of(sw));
        }

        //app.getManagementContext().getExecutionManager().getTasksWithAllTags(
        Asserts.eventually(() -> ((EntityInternal)app).getExecutionContext().getTasks().stream().filter(t -> !t.isDone()).collect(Collectors.toList()),
            unfinishedTasks -> {
                System.out.println("TASKS: "+unfinishedTasks);
                System.out.println(lastInvocation);
                return unfinishedTasks.isEmpty();
            }, Duration.FIVE_SECONDS
        );
    }

    @Test
    public void testFailAndErrorHandlerAsListOrMapOrString() {
        MutableList<MutableMap<String, Serializable>> errorHandler = MutableList.of(MutableMap.of("step", "return Yay WTF",
                "condition", MutableMap.of("error-cause", MutableMap.of("regex", ".*Fail.*wtf.*"))));
        doTestFail(errorHandler);
        doTestFail(errorHandler.get(0));
        doTestFail(errorHandler.get(0).get("step"));
    }

    void doTestFail(Object errorHandler) {
        Task<?> out = runSteps(
                MutableList.of("fail message wtf"),
                null,
                ConfigBag.newInstance().configure(WorkflowCommonConfig.ON_ERROR, errorHandler)
        );
        Asserts.assertEquals(out.getUnchecked(), "Yay WTF");
    }

    @Test
    public void testReplayableDisabled() {
        Task<?> out = runSteps(
                MutableList.of("workflow replayable from here", "let x = ${entity.sensor.count} + 1 ?? 1", "set-sensor count = ${x}", "fail message wtf"),
                null,
                ConfigBag.newInstance().configure(WorkflowCommonConfig.REPLAYABLE, "disabled"));
        out.blockUntilEnded();
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("count"), 1);

        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();

        WorkflowExecutionContext.Factory wff = lastWorkflowContext.factory(true);
        DynamicTasks.submit(wff.createTaskReplaying(wff.makeInstructionsForReplayingFromLastReplayable("testing", false)), app).blockUntilEnded();
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("count"), 2);

        WorkflowExecutionContext.Factory wfd = lastWorkflowContext.factory(false);
        Asserts.assertFailsWith(() -> wfd.createTaskReplaying(wfd.makeInstructionsForReplayingFromLastReplayable("testing", false)),
                e -> {
                    Asserts.expectedFailureContainsIgnoreCase(e, "disabled");
                    return true;
                });
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("count"), 2);

        DynamicTasks.submit(wfd.createTaskReplaying(wfd.makeInstructionsForReplayingFromLastReplayable("testing", true)), app).blockUntilEnded();
        EntityAsserts.assertAttributeEquals(app, Sensors.newIntegerSensor("count"), 3);
    }

    @Test
    public void testRetentionQuickly() throws Exception {
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        // always use the latest mgmt context!
        Supplier<WorkflowStatePersistenceViaSensors> wp = () -> new WorkflowStatePersistenceViaSensors(mgmt());
        BrooklynTaskTags.WorkflowTaskTag w1, w2, w3, w4;

        w1 = doTestRetentionDisabled("context", "min(1,2) hash my-fixed-hash", false, false, false);
        Asserts.assertEquals(lastWorkflowContext.getRetentionSettings().expiryResolved, "min(1,2)");
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId()));

        w1 = doTestRetentionDisabled(2, "hash my-fixed-hash min(1,context)", false, false, false);
        Asserts.assertEquals(lastWorkflowContext.getRetentionSettings().expiryResolved, "min(1,2)");

        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId()));

        // invoking our test gives a new workflow hash because the effector name is different
        w2 = doTestRetentionDisabled(2, "1", false, false, false);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId()));

        // reinvoking that effector still gives 2
        Task<?> t = app.invoke(app.getEntityType().getEffectorByName("myWorkflow" + effNameCount).get(), null);
        t.blockUntilEnded();
        w2 = BrooklynTaskTags.getWorkflowTaskTag(t, false);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId()));

        // hash accepts variables
        app.config().set(ConfigKeys.newStringConfigKey("hash"), "my-fixed-hash");

        // this hash replaces old w1
        w1 = doTestRetentionDisabled("context", "min(1,2) hash ${entity.config.hash}", false, false, false);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId()));  // should replace the one above

        // workflow.id as the hash variable means each invocations has its own retention
        w3 = doTestRetentionDisabled("context", "1 hash ${workflow.id}", false, false, false);

        t = app.invoke(app.getEntityType().getEffectorByName("myWorkflow" + effNameCount).get(), null);
        t.blockUntilEnded();
        w4 = BrooklynTaskTags.getWorkflowTaskTag(t, false);

        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId(), w3.getWorkflowId(), w4.getWorkflowId()));  // should replace the one above
    }

    @Test(groups="Integration")  // very slow
    public void testRetentionManyWaysIncludingDisabled() throws Exception {
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        // always use the latest mgmt context!
        Supplier<WorkflowStatePersistenceViaSensors> wp = () -> new WorkflowStatePersistenceViaSensors(mgmt());
        BrooklynTaskTags.WorkflowTaskTag w1, w2, w3;

        doTestRetentionDisabled("disabled", "ignored", true, false, true);
        doTestRetentionDisabled("1", "disabled", true, false, false);
        doTestRetentionDisabled("1", "disabled", false, true, true);
        w1 = doTestRetentionDisabled("1", "min(1,5s)", true, false, false);

        // only w1 should be persisted
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId()));

        // run something else within 5s, should now be persisting 2

        w2 = doTestRetentionDisabled("1", "min(1,5s)", true, false, false);

        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId()));

        // wait 5s and run something, it should cause everything else to expire
        Time.sleep(Duration.FIVE_SECONDS);
        wp.get().expireOldWorkflows(app, null);
        // should now be empty
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of());

        String longWait = "10s";

        w1 = doTestRetentionDisabled("1", "hash my-fixed-hash max(context,"+longWait+")", false, true, false);
        Asserts.assertEquals(lastWorkflowContext.getRetentionSettings().expiryResolved, "max(1,"+Duration.of(longWait)+")");

        w2 = doTestRetentionDisabled("disabled", "max(1,"+longWait+") hash my-fixed-hash", false, true, false);
        Time.sleep(Duration.seconds(5));
        w3 = doTestRetentionDisabled("hash my-fixed-hash max(1,"+longWait+")", "context", false, true, false);
        // should now have all 3
        wp.get().expireOldWorkflows(app, null);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId(), w3.getWorkflowId()));

        Time.sleep(Duration.seconds(5));
        // now just the last 1 (only 1 in 10s)
        wp.get().expireOldWorkflows(app, null);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w3.getWorkflowId()));

        Time.sleep(Duration.seconds(5));
        // still have last 1 (even after 10s)
        wp.get().expireOldWorkflows(app, null);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w3.getWorkflowId()));

        // run two more, that's all we should have
        w1 = doTestRetentionDisabled("1", "hash my-fixed-hash", false, true, false);
        w2 = doTestRetentionDisabled("1", "context", false, true, false);
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of(w1.getWorkflowId(), w2.getWorkflowId()));
    }

    int effNameCount = 0;
    BrooklynTaskTags.WorkflowTaskTag doTestRetentionDisabled(Object retentionOnWorkflow, String retentionOnStep, boolean rebindBeforeRetentionChangedOnStep, boolean rebindAfterRetentionChangedOnStep, boolean shouldBeDisabled) throws Exception {
        effNameCount++;
        AttributeSensor<Integer> a = Sensors.newIntegerSensor("a");
        AttributeSensor<Integer> b = Sensors.newIntegerSensor("b");
        ((EntityInternal.SensorSupportInternal)app.sensors()).remove(a);
        ((EntityInternal.SensorSupportInternal)app.sensors()).remove(b);

        Task<?> out = runStepsOnExistingApp("myWorkflow"+effNameCount,
                MutableList.of(
                        "log wait for a in ${workflow.id} ${workflow.name}",
                        "wait ${entity.attributeWhenReady.a}",
                        "log got a",
                        "let result = ${entity.sensor.a}",
                        "workflow retention "+retentionOnStep,
                        "log retention step",
                        "wait ${entity.attributeWhenReady.b}",
                        "let result = ${result} + ${entity.sensor.b}",
                        "return ${result}"),
                null,
                ConfigBag.newInstance().configure((ConfigKey)WorkflowCommonConfig.RETENTION, retentionOnWorkflow));

        BrooklynTaskTags.WorkflowTaskTag wt = BrooklynTaskTags.getWorkflowTaskTag(out, false);

        if (rebindBeforeRetentionChangedOnStep) {
            Time.sleep(Duration.millis(500));
            app = rebind();
            switchOriginalToNewManagementContext();
        }
        app.sensors().set(a, 10);
        if (rebindAfterRetentionChangedOnStep) {
            Time.sleep(Duration.millis(500));
            app = rebind();
            switchOriginalToNewManagementContext();
        }
        app.sensors().set(b, 1);

        Maybe<WorkflowExecutionContext> wf = new WorkflowStatePersistenceViaSensors(mgmt()).getFromTag(wt);

        if (shouldBeDisabled) {
            Time.sleep(Duration.millis(500));
            if (wf.isPresent()) Asserts.fail("Workflow persistence should be disabled, instead found: "+wf.get());

        } else {
            if (wf.isAbsent()) Asserts.fail("Workflow persistence should be enabled, instead did not find it");
            lastWorkflowContext = wf.get();
            lastInvocation = lastWorkflowContext.getTask(false).get();
            if (rebindAfterRetentionChangedOnStep || rebindBeforeRetentionChangedOnStep) {
                // was interrupted, make sure the dangling handler finishes then replay
                lastInvocation.blockUntilEnded(Duration.FIVE_SECONDS);
                lastInvocation = Entities.submit(app, lastWorkflowContext.factory(false).createTaskReplaying(lastWorkflowContext.factory(false).makeInstructionsForReplayResuming("test", true)));
            }

            Asserts.assertEquals(lastInvocation.getUnchecked(Duration.seconds(5)), 11);
        }
        return wt;
    }

    @Test
    public void testParseErrorStatusAndRetention() throws Exception {
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Supplier<WorkflowStatePersistenceViaSensors> wp = () -> new WorkflowStatePersistenceViaSensors(mgmt());

        // non-parseable step fails at add time, and is not persisted
        try {
            WorkflowEffector eff = addEffector("e1", MutableList.of(
                            "non-parseable-step"),
                    null,
                    ConfigBag.newInstance().configure((ConfigKey) WorkflowCommonConfig.RETENTION, 1));
            Asserts.shouldHaveFailedPreviously("Instead had: "+eff);
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "non-parseable-step");
        }
        // nothing persisted
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of());

        // with condition, it does not parse until actual call
        addEffector("e1", MutableList.of(
                        "non-parseable-step"),
                null,
                ConfigBag.newInstance()
                        .configure((ConfigKey) WorkflowCommonConfig.CONDITION, "any: []")
                        .configure((ConfigKey) WorkflowCommonConfig.RETENTION, 1)
                );
        // but fails on invocation, before task created
        try {
            Task<?> task = app.invoke(app.getEntityType().getEffectorByName("e1").get(), null);
            Asserts.shouldHaveFailedPreviously("Instead had: " + task.getStatusDetail(true));
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "non-parseable-step");
        }
        // nothing persisted
        Asserts.assertEquals(wp.get().getWorkflows(app).keySet(), MutableSet.of());

        // invalid replayable step also fails before persisted
        try {
            WorkflowBasicTest.runWorkflow(app,
                    Strings.lines(
                            "steps:",
                            "- workflow replayable unknown-replayable-mode"),
                    "e2");
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "unknown-replayable-mode");
        }
        Asserts.assertSize(wp.get().getWorkflows(app).keySet(), 0);

        // (above is the result after fixing bug where some parses happened in the task, and again in the error handler,
        // and didn't properly handle errors; now all parse errors should prevent workflow creation;
        // there might be other edge cases where workflows remain in "staged" state, but they should be few if any,
        // and worst case they can be manually deleted)
    }

    @Test
    public void testErrorInSubWorkflowCaughtUpdatesContextAndStep() throws Exception {
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext run = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                "- log 1",
                "- type: workflow",
                "  steps:",
                "  - log 2-1",
                "  - step: fail message 2-2",
                "    on-error:",
                "    - log 2-2-error",
                "    - fail message 2-2-done",
                "  - log 2-3",
                "  on-error: ",
                "  - return 2-done",
                "- log 3"
        ), "test-error-in-subworkflow");
        Asserts.assertEquals(run.getTask(true).get().getUnchecked(), "2-done");

        WorkflowExecutionContext.OldStepRecord step2 = run.oldStepInfo.get(1);
        Asserts.assertNotNull(step2);
        Asserts.assertNotNull(step2.context);
        Asserts.assertNull(step2.context.error);  // should be null because handled
        Asserts.assertNull(step2.context.errorHandlerTaskId);  // should be null because not treated as a step handler, but handler for the workflow - step2sub.errorHandlerTaskId

        BrooklynTaskTags.WorkflowTaskTag step2subTag = Iterables.getOnlyElement(step2.context.getSubWorkflows());

        WorkflowExecutionContext step2sub = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(step2subTag.getWorkflowId());
        Asserts.assertEquals(step2sub.getStatus(), WorkflowExecutionContext.WorkflowStatus.SUCCESS);
        Asserts.assertNotNull(step2sub.errorHandlerTaskId);

        WorkflowExecutionContext.OldStepRecord step22 = step2sub.oldStepInfo.get(1);
        Asserts.assertNotNull(step22);

        Asserts.assertNotNull(step22);
        Asserts.assertNotNull(step22.context);
        Asserts.assertNotNull(step22.context.error);   // not null because not handled here
        Asserts.assertNotNull(step22.context.errorHandlerTaskId);
    }

    @Test
    public void testErrorInSubWorkflowUncaughtUpdatesContextAndStep() throws Exception {
        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext run = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                "- log 1",
                "- type: workflow",
                "  steps:",
                "  - log 2-1",
                "  - step: fail message 2-2",
                "    on-error:",
                "    - log 2-2-error",
                "    - fail message 2-2-done",
                "  - log 2-3",
                "- log 3"
        ), "test-error-in-subworkflow");
        run.getTask(true).get().blockUntilEnded();
        Asserts.assertEquals(run.getStatus(), WorkflowExecutionContext.WorkflowStatus.ERROR);

        WorkflowExecutionContext.OldStepRecord step2 = run.oldStepInfo.get(1);
        BrooklynTaskTags.WorkflowTaskTag step2subTag = Iterables.getOnlyElement(step2.context.getSubWorkflows());

        WorkflowExecutionContext step2sub = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(step2subTag.getWorkflowId());
        Asserts.assertEquals(step2sub.getStatus(), WorkflowExecutionContext.WorkflowStatus.ERROR);

        WorkflowExecutionContext.OldStepRecord step22 = step2sub.oldStepInfo.get(1);
        Asserts.assertNotNull(step22);
        Asserts.assertNotNull(step22.context);
        Asserts.assertNotNull(step22.context.error);
        Asserts.assertNotNull(step22.context.errorHandlerTaskId);
    }
}
