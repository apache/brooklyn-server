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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.DynamicTasks;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

public class WorkflowPersistReplayErrorsTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    BasicApplication lastApp;
    Task<?> runStep(Object step, Consumer<BasicApplication> appFunction) {
        return runSteps(MutableList.<Object>of(step), appFunction);
    }
    Task<?> runSteps(List<Object> steps, Consumer<BasicApplication> appFunction) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        this.lastApp = app;
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

    private void runIncrementingXAwaitingGate() {
        lastInvocation = runSteps(MutableList.of(
                "let integer x = 1",
                "set-sensor x = ${x}",
                "wait ${entity.attributeWhenReady.gate}",
                "let x = ${x} + 1",
                "set-sensor x = ${x}",
                "return ${x}"), null);

        // TODO would be nice if workflow effector invocation task creates the workflow sensor synchronously;
        // probably essential to ensure call does not return until there is a record it has been submitted
        EntityAsserts.assertAttributeEventuallyNonNull(lastApp, WorkflowStatePersistenceViaSensors.INTERNAL_WORKFLOWS);
        Map<String, WorkflowExecutionContext> workflow = new WorkflowStatePersistenceViaSensors(mgmt).getWorkflows(lastApp);

        EntityAsserts.assertAttributeEqualsEventually(lastApp, Sensors.newSensor(Object.class, "x"), 1);

        workflow = new WorkflowStatePersistenceViaSensors(mgmt).getWorkflows(lastApp);
        Asserts.assertSize(workflow, 1);
        lastWorkflowContext = workflow.values().iterator().next();
    }

    @Test
    public void testWorkflowSensorValuesWhenPausedAndCanReplay() throws IOException {
        runIncrementingXAwaitingGate();

        EntityAsserts.assertAttributeEqualsEventually(lastApp, Sensors.newSensor(Object.class, "x"), 1);

        Integer index = lastWorkflowContext.getCurrentStepIndex();
        Asserts.assertTrue(index >= 1 && index <= 2, "Index is "+index);
        Asserts.assertEquals(lastWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.RUNNING);
        Asserts.assertFalse(lastInvocation.isDone());

        lastApp.sensors().set(Sensors.newBooleanSensor("gate"), true);

        lastInvocation.blockUntilEnded(Duration.seconds(2));
        Asserts.assertEquals(lastInvocation.getUnchecked(), 2);
        Asserts.assertEquals(lastWorkflowContext.status, WorkflowExecutionContext.WorkflowStatus.SUCCESS);

        lastApp.sensors().set(Sensors.newBooleanSensor("gate"), false);
        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.getTaskReplayingFromStep(1), lastApp);
        // sensor should go back to 1 because workflow vars are stored per-state
        EntityAsserts.assertAttributeEqualsEventually(lastApp, Sensors.newSensor(Object.class, "x"), 1);
        Time.sleep(10);
        Asserts.assertFalse(invocation2.isDone());
        lastApp.sensors().set(Sensors.newBooleanSensor("gate"), true);
        invocation2.blockUntilEnded(Duration.seconds(2));
        Asserts.assertEquals(invocation2.getUnchecked(), 2);
    }

    @Test
    public void testWorkflowInterruptedAndCanReplay() throws IOException {
        runIncrementingXAwaitingGate();

        // cancel the workflow
        lastWorkflowContext.getOrCreateTask().get().cancel(true);

        // workflow should no longer proceed even if gate is set
        lastApp.sensors().set(Sensors.newBooleanSensor("gate"), true);

        lastInvocation.blockUntilEnded(Duration.seconds(2));
        Asserts.assertTrue(lastInvocation.isError());

        // sensor should not increment to 2
        // workflow should go to error
        Asserts.eventually(() -> lastWorkflowContext.status, status -> status.error);
        // and sensor will not be set to 2
        EntityAsserts.assertAttributeEquals(lastApp, Sensors.newSensor(Object.class, "x"), 1);


        Integer index = lastWorkflowContext.getCurrentStepIndex();
        Asserts.assertTrue(index >= 1 && index <= 2, "Index is "+index);

        Task<Object> invocation2 = DynamicTasks.submit(lastWorkflowContext.getTaskReplayingCurrentStep(false), lastApp);
        // the gate is set so this will finish soon
        Asserts.assertEquals(invocation2.getUnchecked(), 2);
        EntityAsserts.assertAttributeEquals(lastApp, Sensors.newSensor(Object.class, "x"), 2);
    }

    @Test
    public void testShutdownNotedIfManagementStopped() throws IOException {
        runIncrementingXAwaitingGate();

        Entities.destroyAll(mgmt);

        lastInvocation.blockUntilEnded(Duration.seconds(5));
        Asserts.assertTrue(lastInvocation.isError());

        Asserts.eventually(() -> lastWorkflowContext.status, status -> status.error);
        Asserts.eventually(() -> lastWorkflowContext.status, status -> status == WorkflowExecutionContext.WorkflowStatus.ERROR_SHUTDOWN);
        // above is set, although it will not normally be persisted
    }

    /* TODO would be nice if workflow effector invocation task creates the workflow sensor synchronously;
     *
     * TODO
     *  DONE - replay after task interruption
     *  DONE - replay after completion
     *  replay after mgmt stopped
     *  replay nested workflow
     *
     * then UI ???
     */
}
