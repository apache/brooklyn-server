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
package org.apache.brooklyn.camp.brooklyn;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.NoOpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SetSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SleepWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.internal.thread.ThreadTimeoutException;

public class WorkflowYamlTest extends AbstractYamlTest {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        RegisteredType rt = RegisteredTypes.bean(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
        return rt;
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        addRegisteredTypeBean(mgmt(), "log", LogWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "set-sensor", SetSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt(), "workflow-effector", WorkflowEffector.class);
    }

    @Test
    public void testWorkflowEffector() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        step1:",
                "          type: no-op",
                "        step2:",
                "          type: set-sensor",
                "          sensor: foo",
                "          value: bar",
                "        step3: set-sensor integer bar = 1");
        waitForApplicationTasks(app);

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();

        Task<?> invocation = app.invoke(effector, null);
        Object result = invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "bar"), 1);
    }

    @Test
    public void testWorkflowEffectorLogStep() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher = getLogWatcher(LogWorkflowStep.class);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        step1:", // this step runs 3rd... so confused :-(
                "          type: log",
                "          message: test message 1",
                "        step2: log test message 2",
                "        step3: no-op",
                "        6: log ??", // this step runs 2nd...
                "        step4: log test message 3",
                "        5: log test message N"); // this step runs 1st !...
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // Verify expected log messages.
        Assert.assertEquals(5, logWatcher.list.size());
        Assert.assertEquals(logWatcher.list.get(0).getFormattedMessage(), "5: test message N");
        Assert.assertEquals(logWatcher.list.get(1).getFormattedMessage(), "6: ??");
        Assert.assertEquals(logWatcher.list.get(2).getFormattedMessage(), "step1: test message 1");
        Assert.assertEquals(logWatcher.list.get(3).getFormattedMessage(), "step2: test message 2");
        Assert.assertEquals(logWatcher.list.get(4).getFormattedMessage(), "step4: test message 3");
    }

    @Test
    public void testWorkflowPropertyNext() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher = getLogWatcher(LogWorkflowStep.class);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        the-end: log bye",
                "        step-B:",
                "          type: log",
                "          message: test message 3",
                "          next: the-end",
                "        step-A:", // <-- this is the 1st step as per numeric-alpha order.
                "          type: log",
                "          message: test message 1",
                "          next: step-C",
                "        step-C:",
                "          type: log",
                "          message: test message 2",
                "          next: step-B",
                "        the-check-point: log check point");
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // Verify expected log messages.
        Assert.assertEquals(logWatcher.list.size(), 4);
        Assert.assertEquals(logWatcher.list.get(0).getFormattedMessage(), "step-A: test message 1");
        Assert.assertEquals(logWatcher.list.get(1).getFormattedMessage(), "step-C: test message 2");
        Assert.assertEquals(logWatcher.list.get(2).getFormattedMessage(), "step-B: test message 3");
        // 'the-check-point' step is never reached here.
        Assert.assertEquals(logWatcher.list.get(3).getFormattedMessage(), "the-end: bye");
    }

    @Test(timeOut = 1000L, expectedExceptions = ThreadTimeoutException.class)
    public void testWorkflowPropertyNext_InfiniteLoop() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher = getLogWatcher(LogWorkflowStep.class);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        the-end: log bye",
                "        step-A:", // <-- This is the 1st step as per numeric-alpha order.
                "          type: log",
                "          message: test message 1",
                "          next: step-C",
                "        step-B:",
                "          type: log",
                "          message: test message 3",
                "          # next: the-end", // <-- Omit the 'next', rely on the default order from here.
                "        step-C:",
                "          type: log",
                "          message: test message 2",
                "          next: step-B",
                "        the-check-point: log check point");
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // This is expected stuck in the infinite loop between 'step-B' and 'step-C'...
    }

    @Test
    public void testWorkflowPropertyNext_DefaultOrder() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher = getLogWatcher(LogWorkflowStep.class);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        the-end: log bye",
                "        step-B:",
                "          type: log",
                "          message: test message 3",
                "          next: the-end",
                "        step-A:", // <-- This is the 1st step as per numeric-alpha order.
                "          type: log",
                "          message: test message 1",
                "          next: step-C",
                "        step-C:",
                "          type: log",
                "          message: test message 2",
                "          # next: step-B", // <-- Omit the 'next', rely on the default order from here.
                "        the-check-point: log check point");
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // Verify expected log messages
        Assert.assertEquals(logWatcher.list.size(), 4);
        Assert.assertEquals(logWatcher.list.get(0).getFormattedMessage(), "step-A: test message 1");
        Assert.assertEquals(logWatcher.list.get(1).getFormattedMessage(), "step-C: test message 2");
        // 'test-B' is not reached, default order must jump to 'the-check-point' and 'the-end' step.
        Assert.assertEquals(logWatcher.list.get(2).getFormattedMessage(), "the-check-point: check point");
        Assert.assertEquals(logWatcher.list.get(3).getFormattedMessage(), "the-end: bye");
    }

    @Test
    public void testWorkflowPropertyNext_SetSensor() throws Exception {

        // Prepare log watcher.
        ListAppender<ILoggingEvent> logWatcher = getLogWatcher(LogWorkflowStep.class);

        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  id: my-entity",
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      steps:",
                "        the-end: log bye",
                "        step-B:",
                "          type: log",
                "          message: test message 2",
                "          next: the-end",
                "        step-A:", // <-- This is the 1st step as per numeric-alpha order.
                "          type: log",
                "          message: test message 1",
                "          next: step-C",
                "        step-C:",
                "          type: set-sensor", // set sensor
                "          sensor: foo",
                "          value: bar",
                "        the-check-point: log check point");
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
        Task<?> invocation = app.invoke(effector, null);
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        // Verify expected log messages
        Assert.assertEquals(logWatcher.list.size(), 3);
        Assert.assertEquals(logWatcher.list.get(0).getFormattedMessage(), "step-A: test message 1");
        // 'test-B' is not reached.
        Assert.assertEquals(logWatcher.list.get(1).getFormattedMessage(), "the-check-point: check point");
        Assert.assertEquals(logWatcher.list.get(2).getFormattedMessage(), "the-end: bye");

        // Verify expected sensor
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
    }

    private ListAppender<ILoggingEvent> getLogWatcher(Class<?> clazz) {
        ListAppender<ILoggingEvent> logWatcher = new ListAppender<>();
        logWatcher.start();
        ((Logger) LoggerFactory.getLogger(clazz)).addAppender(logWatcher);
        return logWatcher;
    }
}
