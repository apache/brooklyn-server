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
import com.google.common.base.Throwables;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.EntityManagementSupport;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
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

    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        return runSteps(steps, appFunction, null);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction, ConfigBag initialEffectorConfig) {
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
                .copy(initialEffectorConfig)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        return app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
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
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingFromStep(1, "test", true)), app);
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

        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingLast("test", true)), app);
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
            lastInvocation = Entities.submit(app, lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingLast("test", true)));

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
        } else {
            log.error("Workflow ended with wrong error status: "+lastWorkflowContext.status);
            Asserts.fail("Workflow ended with wrong error status: "+lastWorkflowContext.status+ " / value "+lastInvocation.getUnchecked());
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
//                MutableMap.of("s", "let y = ${entity.sensor.y} ?? 0", "replayable", "yes"),
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

            lastInvocation = Entities.submit(app, lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingLast("test", true)));
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

    public static List<Object> INCREMENTING_X_STEPS_SETTING_REPLAYABLE(String replayable) {
        return MutableList.<Object>of(
            MutableMap.of("s", "let integer x = ${entity.sensor.x} ?? 0", "replayable", replayable),
            "let x = ${x} + 1",
            "set-sensor x = ${x}",
            "wait ${entity.attributeWhenReady.gate}",
            "return ${x}").asUnmodifiable();
    }

    private void runIncrementingXSettingReplayable(boolean isBuggy) {
        lastInvocation = runSteps(INCREMENTING_X_STEPS_SETTING_REPLAYABLE(isBuggy ? "yes" : "on"), null);

        // ensure workflow sensor set immediately (this is done synchronously when effector invocation task is created, to ensure it can be resumed)
        findSingleLastWorkflow();
    }

    private void runIncrementingXAwaitingGateSettingReplayable(boolean isBuggy) {
        runIncrementingXSettingReplayable(isBuggy);

        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 1);
        // refresh this for good measure (it won't have changed but feels like bad practice to rely on that)
        lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableBuggy() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(true);
    }

    @Test(groups="Integration")  //because slow
    public void testWorkflowInterruptedAndCanReplaySettingReplayableSensible() throws IOException {
        doTestWorkflowInterruptedAndCanReplaySettingReplayable(false);
    }

    void doTestWorkflowInterruptedAndCanReplaySettingReplayable(boolean isBuggy) throws IOException {
        runIncrementingXAwaitingGateSettingReplayable(isBuggy);

        // cancel the workflow
        lastWorkflowContext.getTask(true).get().cancel(true);
        lastInvocation.blockUntilEnded(Duration.seconds(2));

        // in buggy mode, should want to continue from 0
        int expected = -1;
        if (isBuggy) {
            Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> step == 0);
            expected = 2;
        } else {
            Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> step == 2 || step == 3);
            expected = 1;
        }

        // replay
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingLast("test", false)), app);

        // should get 2 because it replays from 0
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(invocation2.getUnchecked(), expected);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), expected);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayableBuggyNestedReplay() throws IOException {
        doTestNestedWorkflowInterruptedAndCanReplaySettingReplayable("yes", 0, true);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayableBuggyTopLevelReplay() throws IOException {
        doTestNestedWorkflowInterruptedAndCanReplaySettingReplayable("off", null, true);
    }

    @Test(groups="Integration")  //because slow
    public void testNestedWorkflowInterruptedAndCanReplaySettingReplayableSensible() throws IOException {
        doTestNestedWorkflowInterruptedAndCanReplaySettingReplayable("on", 3, false);
    }

    void doTestNestedWorkflowInterruptedAndCanReplaySettingReplayable(String replayable, Integer expectedNestedStep, boolean isBuggy) throws IOException {
        // replay is only "good" if we set a replay point _after_ the sensor was read;
        // otherwise it increments x twice
        lastInvocation = runSteps(
                MutableList.of(
                    MutableMap.of("s", "sleep 20ms", "replayable", "on"), "invoke-effector nestedWorkflow"),
                app -> {
                    List<Object> steps = INCREMENTING_X_STEPS_SETTING_REPLAYABLE(replayable);

                    WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                            .configure(WorkflowEffector.EFFECTOR_NAME, "nestedWorkflow")
                            .configure(WorkflowEffector.STEPS, steps)
                    );
                    eff.apply((EntityLocal)app);
                });

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
        Asserts.assertThat(lastWorkflowContext.replayableLastStep, step -> step == 1);

        Map<String, WorkflowExecutionContext> workflows = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app);
        Asserts.assertSize(workflows, 2);
        WorkflowExecutionContext nestedWorkflow = workflows.values().stream().filter(w -> w.parentId != null).findAny().get();

        Asserts.assertThat(nestedWorkflow.replayableLastStep, step -> Objects.equals(step, expectedNestedStep));

        if (isBuggy) {
            expected = 2;
        } else {
            expected = 1;
        }

        // replay
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.createTaskReplaying(lastWorkflowContext.makeInstructionsForReplayingLast("test", false)), app);

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
                m -> m.matches("Starting workflow 'Workflow for effector myWorkflow', moving to first step .*-1"),
                m -> m.matches("Starting step .*-1 in task .*"),
                m -> m.matches("Encountered error in step .*-1 '1 - invoke-effector does-not-exist' .handler present.: No effector matching 'does-not-exist'"),
                m -> m.matches("Creating step .*-1 error handler .*-1-error-handler in task .*"),
                m -> m.matches("Starting .*-1-error-handler with 1 handler in task .*"),
                m -> m.matches("Creating handler .*-1-error-handler-1 'NoOp' in task .*"),
                m -> m.matches("Starting .*-1-error-handler-1 in task .*"),
                m -> m.matches("Completed handler .*-1-error-handler-1; proceeding to default next step"),
                m -> m.matches("Completed step .*-1; no further steps: Workflow completed")));
        }
    }

    @Test
    public void testSimpleErrorHandlerOnWorkflow() throws IOException {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(getClass().getPackage().getName())) {
            lastInvocation = runSteps(MutableList.of(
                            MutableMap.of("s", "invoke-effector does-not-exist",
                                    "output", "should have failed")),
                    null,
                    ConfigBag.newInstance().configure(
                            WorkflowEffector.ON_ERROR, MutableList.of(
                                    MutableMap.of("type", "no-op",
                                            "output", "error-handler worked!"))));
            Asserts.assertEquals(lastInvocation.getUnchecked(), "error-handler worked!");

            List<String> msgs = logWatcher.getMessages();
            log.info("Error handler output:\n"+msgs.stream().collect(Collectors.joining("\n")));
            Asserts.assertEntriesSatisfy(msgs, MutableList.of(
                    m -> m.matches("Starting workflow 'Workflow for effector myWorkflow', moving to first step .*-1"),
                    m -> m.matches("Starting step .*-1 in task .*"),
                    m -> m.matches("Error in step '1 - invoke-effector does-not-exist', no error handler so rethrowing: No effector matching 'does-not-exist'"),
                    m -> m.matches("Error in workflow 'Workflow for effector myWorkflow' around step .*-1 'myWorkflow', running error handler"),
                    m -> m.matches("Encountered error in workflow .*/.* 'myWorkflow' .handler present.: No effector matching 'does-not-exist'"),
                    m -> m.matches("Creating workflow .* error handler .*-error-handler in task .*"),
                    m -> m.matches("Starting .*-error-handler with 1 handler in task .*"),
                    m -> m.matches("Creating handler .*-error-handler-1 'NoOp' in task .*"),
                    m -> m.matches("Starting .*-error-handler-1 in task .*"),
                    m -> m.matches("Completed handler .*-error-handler-1; proceeding to default next step"),
                    m -> m.matches("Handled error in workflow around step .*-1; explicit next 'end': Workflow completed")));
        }
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
    public void testTimeoutNotApplyingOnStep() throws Exception {
        doTestTimeout(true, true);
    }

    @Test
    public void testTimeoutNotApplyingOnWorkflow() throws Exception {
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

}
