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
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;

public class WorkflowYamlTest extends AbstractYamlTest {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        RegisteredType rt = RegisteredTypes.bean(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
        return rt;
    }

    public static void addWorkflowTypes(ManagementContext mgmt) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        addRegisteredTypeBean(mgmt, "workflow-effector", WorkflowEffector.class);
    }

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        addWorkflowTypes(mgmt());
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
                "        - type: no-op",
                "        - type: set-sensor",
                "          input:",
                "            sensor: foo",
                "            value: bar",
                "        - set-sensor integer bar = 1",
                "        - set-config integer foo = 2",
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

    ClassLogWatcher lastLogWatcher;

    Object invokeWorkflowStepsWithLogging(String ...stepLines) throws Exception {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {
            lastLogWatcher = logWatcher;

            // Declare workflow in a blueprint, add various log steps.
            Entity app = createAndStartApplication(
                    "services:",
                    "- type: " + BasicEntity.class.getName(),
                    "  brooklyn.initializers:",
                    "  - type: workflow-effector",
                    "    brooklyn.config:",
                    "      name: myWorkflow",
                    "      steps:",
                    Strings.indent(8, Strings.lines(stepLines)));
            waitForApplicationTasks(app);

            // Deploy the blueprint.
            Entity entity = Iterables.getOnlyElement(app.getChildren());
            Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
            Task<?> invocation = entity.invoke(effector, null);
            return invocation.getUnchecked();
        }
    }

    void assertLogStepMessages(String ...lines) {
        Assert.assertEquals(lastLogWatcher.getMessages(),
                Arrays.asList(lines));
    }

    @Test
    public void testWorkflowEffectorLogStep() throws Exception {
        invokeWorkflowStepsWithLogging(
                "- log test message 1",
                "- type: log",
                "  id: second",
                "  name: Second Step",
                "  message: test message 2, step '${workflow.current_step.name}' id ${workflow.current_step.step_id} in workflow '${workflow.name}'");

        assertLogStepMessages(
                "test message 1",
                "test message 2, step 'Second Step' id second in workflow 'Workflow for effector myWorkflow'");
    }

    @Test
    public void testWorkflowPropertyNext() throws Exception {
        invokeWorkflowStepsWithLogging(
                "- s: log going to A",
                "  next: A",
                "- s: log now at B",
                "  next: end",
                "  id: B",
                "- s: log now at A",
                "  id: A",
                "  next: B");
        assertLogStepMessages(
                    "going to A",
                    "now at A",
                    "now at B");
    }

//    // TODO test timeout
//    @Test
//    public void testTimeoutWithInfiniteLoop() throws Exception {
//        invokeWorkflowStepsWithLogging(
//                "        - s: log going to A",
//                        "          next: A",
//                        "        - s: log now at B",
//                        "          id: B",
//                        "        - s: sleep 100ms",
//                        "          next: B",
//                        "        - s: log now at A",
//                        "          id: A",
//                        "          next: B");
//        // TODO assert it takes at least 100ms, but less than 5s
//        assertLogStepMessages(
//                ...);
//    }

    void doTestWorkflowCondition(String setCommand, String logAccess, String conditionAccess) throws Exception {
        invokeWorkflowStepsWithLogging(
                    "- log start",
                    "- " + setCommand + " color = blue",
                    "- id: log-color",
                    "  s: log color " + logAccess,
                    "-",
                    "  s: log not blue",
                    "  condition:",
                    "    " + conditionAccess,
                    "    assert: { when: present, java-instance-of: string }",
                    "    not: { equals: blue }",
                    "-",
                    "  type: no-op",
                    "  next: make-red",
                    "  condition:",
                    "    " + conditionAccess,
                    "    equals: blue",
                    "-",
                    "  type: no-op",
                    "  next: log-end",
                    "- id: make-red",
                    "  s: " + setCommand + " color = red",
                    "  next: log-color",
                    "- id: log-end",
                    "  s: log end",
                    "");
        assertLogStepMessages(
                    "start", "color blue", "color red", "not blue", "end");
    }

    @Test
    public void testWorkflowSensorCondition() throws Exception {
        doTestWorkflowCondition("set-sensor", "${entity.sensor.color}", "sensor: color");
    }

    @Test
    public void testWorkflowVariableInCondition() throws Exception {
        doTestWorkflowCondition("let", "${color}", "target: ${color}");
    }

    @Test
    public void testEffectorToSetColorSensorConditionally() throws Exception {
        // Declare workflow in a blueprint, add various log steps.
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-effector",
                "    brooklyn.config:",
                "      name: myWorkflow",
                "      parameters:\n" +
                "        color:\n" +
                "          type: string\n" +
                "          description: What color do you want to set?\n" +
                "\n" +
                "      steps:\n" +
//                "        - let old_color = ${(entity.sensor.color)! \"unset\"}\n" +
                        // above does not work. but the below is recommended
                "        - let old_color = ${entity.sensor.color} ?? \"unset\"\n" +

                        // alternative (supported) if not using nullish operator
//                "        - let old_color = unset\n" +
//                "        - s: let old_color = ${entity.sensor.color}\n" +
//                "          condition:\n" +
//                "            sensor: color\n" +
//                "            when: present_non_null\n" +

                "        - log changing color sensor from ${old_color} to ${color}\n" +
                "        - set-sensor color = ${color}\n" +
                "        - s: set-sensor color_is_red = true\n" +
                "          condition:\n" +
                "            sensor: color\n" +
                "            equals: red\n" +
                "          next: end\n" +
                "        - set-sensor color_is_red = false");

        Entity entity = Iterables.getOnlyElement(app.getChildren());
        Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();

        entity.invoke(effector, MutableMap.of("color", "red")).get();
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color"), "red");
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color_is_red"), "true");

        entity.invoke(effector, MutableMap.of("color", "blue")).get();
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color"), "blue");
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color_is_red"), "false");

        entity.invoke(effector, MutableMap.of("color", "red")).get();
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color"), "red");
        EntityAsserts.assertAttributeEquals(entity, Sensors.newStringSensor("color_is_red"), "true");

    }

    @Test
    public void testInvalidStepsFailDeployment() throws Exception {
        try {
            createAndStartApplication(
                    "services:",
                    "- type: " + BasicEntity.class.getName(),
                    "  brooklyn.initializers:",
                    "  - type: workflow-effector",
                    "    brooklyn.config:",
                    "      name: myWorkflow",
                            "      steps:\n" +
                            "        - unsupported-type"
            );
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "resolve step", "unsupported-type");
        }
    }
}
