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
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.steps.*;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.util.collections.MutableList;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;
import org.testng.internal.thread.ThreadTimeoutException;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;

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
        WorkflowBasicTest.addWorkflowStepTypes(mgmt());
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
                "          input:",
                "            sensor: foo",
                "            value: bar",
                "        step3: set-sensor integer bar = 1",
                "        step4: set-config integer foo = 2",
                "");
        waitForApplicationTasks(app);

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();

        Task<?> invocation = app.invoke(effector, null);
        Object result = invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "bar"), 1);
        EntityAsserts.assertConfigEquals(app, ConfigKeys.newConfigKey(Object.class, "foo"), 2);
    }

    @Test
    public void testWorkflowEffectorLogStep() throws Exception {

        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {

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
            Assert.assertEquals(logWatcher.getMessages(),
                MutableList.of(
                    "5: test message N",
                    "6: ??",
                    "step1: test message 1",
                    "step2: test message 2",
                    "step4: test message 3"));
        }
    }

    @Test
    public void testWorkflowPropertyNext() throws Exception {

        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {

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
            Assert.assertEquals(logWatcher.getMessages(),
                MutableList.of(
                    "step-A: test message 1",
                    "step-C: test message 2",
                    "step-B: test message 3",
                    // 'the-check-point' step is never reached here.
                    "the-end: bye"));
        }
    }

//    // TODO test timeout
//    @Test
//    public void testTimeoutWithInfiniteLoop() throws Exception {
//
//        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {
//
//        // Declare workflow in a blueprint, add various log steps.
//        Entity app = createAndStartApplication(
//                "services:",
//                "- type: " + BasicEntity.class.getName(),
//                "  brooklyn.initializers:",
//                "  - type: workflow-effector",
//                "    brooklyn.config:",
//                "      name: myWorkflow",
//                "      timeout: 100ms",
//                "      steps:",
//                "        the-end: log bye",
//                "        step-A:", // <-- This is the 1st step as per numeric-alpha order.
//                "          type: log",
//                "          message: test message 1",
//                "          next: step-C",
//                "        step-B:",
//                "          type: log",
//                "          message: test message 3",
//                "          # next: the-end", // <-- Omit the 'next', rely on the default order from here.
//                "        step-C:",
//                "          type: log",
//                "          message: test message 2",
//                "          next: step-B",
//                "        the-check-point: log check point");
//        waitForApplicationTasks(app);
//
//        // Deploy the blueprint.
//        Entity entity = Iterables.getOnlyElement(app.getChildren());
//        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
//        Task<?> invocation = app.invoke(effector, null);
//        invocation.getUnchecked();
//
//        // TODO assert it takes at least 100ms, but less than 2s
//
//        Dumper.dumpInfo(invocation);
//        }
//    }

    @Test
    public void testWorkflowPropertyNext_DefaultOrder() throws Exception {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {

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
            Assert.assertEquals(logWatcher.getMessages(),
                MutableList.of(
                    "step-A: test message 1",
                    "step-C: test message 2",
                    // 'test-B' is not reached, default order must jump to 'the-check-point' and 'the-end' step.
                    "the-check-point: check point",
                    "the-end: bye"));
        }
    }

    @Test
    public void testWorkflowPropertyNext_SetSensor() throws Exception {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {

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
            Assert.assertEquals(logWatcher.getMessages(),
                    MutableList.of(
                        "step-A: test message 1",
                        // 'test-B' is not reached.
                        "the-check-point: check point",
                        "the-end: bye"));

            // Verify expected sensor
            EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        }
    }

    public static class ClassLogWatcher extends ListAppender<ILoggingEvent> implements AutoCloseable {
        private final Class<?> clazz;

        public ClassLogWatcher(Class<?> clazz) {
            this.clazz = clazz;
            startAutomatically();
        }

        protected void startAutomatically() {
            super.start();
            ((Logger) LoggerFactory.getLogger(clazz)).addAppender(this);
        }

        @Override
        public void start() {
            throw new IllegalStateException("This should not be started externally.");
        }

        @Override
        public void close() throws IOException {
            ((Logger) LoggerFactory.getLogger(clazz)).detachAppender(this);
            stop();
        }

        public List<String> getMessages() {
            return this.list.stream().map(s -> s.getFormattedMessage()).collect(Collectors.toList());
        }
    }

    void doTestWorkflowCondition(String setCommand, String logAccess, String conditionAccess) throws Exception {
        // Prepare log watcher.
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {

            // Declare workflow in a blueprint, add various log steps.
            Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicEntity.class.getName(),
                    "  brooklyn.initializers:",
                    "  - type: workflow-effector",
                    "    brooklyn.config:",
                    "      name: myWorkflow",
                    "      steps:",
                    "        1: log start",
                    "        2: " + setCommand + " color = blue",
                    "        3: log color " + logAccess,
                    "        4:",
                    "          s: log not blue",
                    "          condition:",
                    "            " + conditionAccess,
                    "            assert: { when: present, java-instance-of: string }",
                    "            not: { equals: blue }",
                    "        5:",
                    "          type: no-op",
                    "          next: 7",
                    "          condition:",
                    "            " + conditionAccess,
                    "            equals: blue",
                    "        6:",
                    "          type: no-op",
                    "          next: 9",
                    "        7:",
                    "           s: " + setCommand + " color = red",
                    "           next: 3",
                    "        9: log end",
                    "");
            waitForApplicationTasks(app);

            // Deploy the blueprint.
            Entity entity = Iterables.getOnlyElement(app.getChildren());
            Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
            Task<?> invocation = app.invoke(effector, null);
            invocation.getUnchecked();
            Dumper.dumpInfo(invocation);

            // Verify expected log messages
            Assert.assertEquals(logWatcher.getMessages(), MutableList.of(
                    "1: start", "3: color blue", "3: color red", "4: not blue", "9: end"));
        }
    }

    @Test
    public void testWorkflowSensorCondition() throws Exception {
        // TODO how for conditions to access entity for a workflow context
        doTestWorkflowCondition("set-sensor", "${entity.sensor.color}", "sensor: color");
    }

    @Test
    public void testWorkflowVariableInCondition() throws Exception {
        doTestWorkflowCondition("let", "${color}", "target: ${color}");
    }

}
