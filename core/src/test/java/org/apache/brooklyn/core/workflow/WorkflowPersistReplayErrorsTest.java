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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.internal.ManagementContextInternal;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixture;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class WorkflowPersistReplayErrorsTest extends RebindTestFixture<BasicApplication> {

    private BasicApplication app;

    @Override
    protected LocalManagementContext decorateOrigOrNewManagementContext(LocalManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        return super.decorateOrigOrNewManagementContext(mgmt);
    }

    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.app = app;
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, steps)
        );
        if (appFunction!=null) appFunction.accept(app);
        eff.apply((EntityLocal)app);

        return app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
    }

    Task<?> lastInvocation;
    WorkflowExecutionContext lastWorkflowContext;

    final static List<Object> INCREMENTING_X_STEPS = MutableList.<Object>of(
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
        Map<String, WorkflowExecutionContext> workflow = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app);
        Asserts.assertSize(workflow, 1);
        lastWorkflowContext = workflow.values().iterator().next();
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
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.getTaskReplayingFromStep(1), app);
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
        lastWorkflowContext.getOrCreateTask().get().cancel(true);

        // workflow should no longer proceed even if gate is set
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);

        lastInvocation.blockUntilEnded(Duration.seconds(2));
        Asserts.assertTrue(lastInvocation.isError());

        // sensor should not increment to 2
        // workflow should go to error
        Asserts.eventually(() -> lastWorkflowContext.status, status -> status.error);
        // and sensor will not be set to 2
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 1);

        Integer index = lastWorkflowContext.getCurrentStepIndex();
        Asserts.assertTrue(index >= 2 && index <= 3, "Index is "+index);

        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.getTaskReplayingCurrentStep(false), app);
        // the gate is set so this will finish soon
        Asserts.assertEquals(invocation2.getUnchecked(), 11);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 11);
    }

    @Test(groups="Integration", invocationCount = 100)  // because slow, and tests variety of interruption points with randomised delay
    public void testWorkflowShutdownAndCanReplay() throws Exception {
        runIncrementingX();

        // variable delay, shouldn't matter when it is interrupted
        Time.sleep((long) (Math.random()*Math.random()*200));

        // do rebind
        ManagementContext oldMgmt = mgmt();
        app = rebind();

        // shutdown flags should get set on old object when destroyed
        ((ManagementContextInternal)oldMgmt).terminate();
        WorkflowExecutionContext prevWorkflowContext = lastWorkflowContext;
        Asserts.eventually(() -> prevWorkflowContext.status, status -> status.error);
        Asserts.assertEquals(prevWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN);

        // new object is present
        Map<String, WorkflowExecutionContext> workflow = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app);
        Asserts.assertSize(workflow, 1);
        lastWorkflowContext = workflow.values().iterator().next();

        // status should not be ended
        Asserts.assertEquals(lastWorkflowContext.status.ended, false);
//        // though possibly could be shutdown if persistence ran again
//        Asserts.assertThat(lastWorkflowContext.status, x -> !x.ended || x == WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN);
        Asserts.assertThat(lastWorkflowContext.currentStepIndex, x -> x==null || x <=3);

        // sensor might be one or might be null
        Asserts.assertThat(app.sensors().get(Sensors.newSensor(Integer.class, "x")), x -> x==null || x==1);

        // we can tell it to resume from where it crashed
        lastInvocation = Entities.submit(app, lastWorkflowContext.getTaskReplayingCurrentStep(false));

        // will wait on gate, ie not finish
        Time.sleep((long) (Math.random()*Math.random()*200));
        Asserts.assertFalse(lastInvocation.isDone());

        // workflow should now complete when gate is set
        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(lastInvocation.get(), 11);
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), 11);
    }

    @Test
    public void testShutdownNotedIfManagementStopped() throws IOException {
        runIncrementingXAwaitingGate();

        Entities.destroyAll(mgmt());

        lastInvocation.blockUntilEnded(Duration.seconds(5));
        Asserts.assertTrue(lastInvocation.isError());

        Asserts.eventually(() -> lastWorkflowContext.status, status -> status.error);
        Asserts.eventually(() -> lastWorkflowContext.status, status -> status == WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN);
        // above is set, although it will not normally be persisted
    }

    @Test(groups="Integration", invocationCount = 100)  // because a bit slow and non-deterministic
    public void testNestedEffectorShutdownAndResumed() throws Exception {
        doTestNestedWorkflowShutdownAndResumed("invoke-effector incrementXWithGate", app->{
            new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "incrementXWithGate")
                    .configure(WorkflowEffector.STEPS, INCREMENTING_X_STEPS)
            ).apply((EntityLocal)app);
        });
    }

    @Test(groups="Integration", invocationCount = 100)  // because a bit slow and non-deterministic
    public void testNestedWorkflowShutdownAndResumed() throws Exception {
        doTestNestedWorkflowShutdownAndResumed(MutableMap.of("type", "workflow", "steps", INCREMENTING_X_STEPS), null);
    }

    void doTestNestedWorkflowShutdownAndResumed(Object call, Consumer<BasicApplication> initializer) throws Exception {
        lastInvocation = runSteps(MutableList.of(
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
        // interrupt at any point, probably when gated
        Time.sleep((long) (Math.random()*Math.random()*200));
        ManagementContext oldMgmt = mgmt();
        app = rebind();
        ((ManagementContextInternal)oldMgmt).terminate();

        String workflowId = BrooklynTaskTags.getWorkflowTaskTag(lastInvocation, false).getWorkflowId();
        WorkflowExecutionContext lastInvocationWorkflow = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).get(workflowId);
        Asserts.assertNotNull(lastInvocationWorkflow);

        lastInvocation = Entities.submit(app, lastInvocationWorkflow.getTaskReplayingCurrentStep(false));
        Asserts.assertFalse(lastInvocation.blockUntilEnded(Duration.millis(20)));

        app.sensors().set(Sensors.newBooleanSensor("gate"), true);
        Asserts.assertEquals(lastInvocation.get(), 2222);

        // wait for effector to be invoked
        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "x"), 22);
        EntityAsserts.assertAttributeEqualsEventually(app, Sensors.newSensor(Object.class, "y"), 22);
    }

    @Override
    protected BasicApplication createApp() {
        return null;
    }

    /*
     * TODO
     *  DONE - replay after task interruption
     *  DONE - replay after completion
     *  DONE - replay after mgmt stopped / rebind
     *  DONE - replay nested workflow / effector
     *
     *  auto throw error on mgmt stopped / rebind
     *
     * then UI ???
     */
}
