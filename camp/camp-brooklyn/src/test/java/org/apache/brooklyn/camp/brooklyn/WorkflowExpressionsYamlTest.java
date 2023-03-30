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
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.steps.flow.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;

public class WorkflowExpressionsYamlTest extends AbstractYamlTest {

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        WorkflowYamlTest.addWorkflowTypes(mgmt());
        lastEntity = null;
        lastLogWatcher = null;
    }

    ClassLogWatcher lastLogWatcher;
    Entity lastEntity;

    Object invokeWorkflowStepsWithLogging(String ...stepLines) {
        try {
            try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {
                lastLogWatcher = logWatcher;

                Entity entity = stepLines.length == 0 ? lastEntity : createEntityWithWorkflowEffector(stepLines);
                if (entity == null) throw new IllegalStateException("No last entity set and no steps provided");
                Effector<?> effector = entity.getEntityType().getEffectorByName("myWorkflow").get();
                Task<?> invocation = entity.invoke(effector, null);
                return invocation.getUnchecked();
            }
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
    }

    private Entity createEntityWithWorkflowEffector(String ...stepLines) throws Exception {
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
        Entity entity = lastEntity = Iterables.getOnlyElement(app.getChildren());
        synchronized (this) { this.notifyAll(); }
        return entity;
    }

    private WorkflowExecutionContext invocationWorkflowOnLastEntity(String ...workflowDefinition) throws Exception {
        return WorkflowBasicTest.runWorkflow(lastEntity, Strings.lines(workflowDefinition), "custom");
    }

    private Object invokeWorkflowOnLastEntity(String ...workflowDefinition) throws Exception {
        WorkflowExecutionContext context = invocationWorkflowOnLastEntity(workflowDefinition);
        return context.getTask(false).get().get(Duration.seconds(5));
    }

    Entity waitForLastEntity() {
        synchronized (this) {
            while (lastEntity==null) {
                try {
                    this.wait(100);
                } catch (InterruptedException e) {
                    throw Exceptions.propagate(e);
                }
            }
        }
        return lastEntity;
    }

    public static class AutoStartStopThread extends Thread implements AutoCloseable {
        public AutoStartStopThread() { start(); }
        public AutoStartStopThread(Runnable r) { super(r); start(); }

        boolean closed = false;

        @Override
        public void run() {
            try {
                super.run();
            } catch (Exception e) {
                if (closed && Exceptions.isCausedByInterruptInThisThread(e)) {
                    // silently close
                } else {
                    throw Exceptions.propagate(e);
                }
            }
        }

        @Override
        public void close() throws Exception {
            if (!closed) {
                this.closed = true;
                interrupt();
            }
        }
    }

    @Test
    public void testWorkflowExpressionSensor() throws Exception {
        createEntityWithWorkflowEffector("- s: let x = ${entity.sensor.foo}", "  output: \"${x}\"");
        lastEntity.sensors().set(Sensors.newStringSensor("foo"), "bar");
        Object x = invokeWorkflowStepsWithLogging();
        Asserts.assertEquals(x, "bar");
    }

    @Test
    public void testWorkflowExpressionSensorBlank() throws Exception {
        createEntityWithWorkflowEffector("- s: let x = ${entity.sensor.foo}", "  output: \"${x}\"");
        lastEntity.sensors().set(Sensors.newStringSensor("foo"), "");
        Object x = invokeWorkflowStepsWithLogging();
        Asserts.assertEquals(x, "");
    }

    @Test
    public void testWorkflowExpressionSensorUnavailable() throws Exception {
        try (AutoStartStopThread t = new AutoStartStopThread(() -> { Time.sleep(Duration.ONE_SECOND); waitForLastEntity().sensors().set(Sensors.newStringSensor("foo"), "bar"); })) {
            Asserts.assertFailsWith(() -> invokeWorkflowStepsWithLogging("- s: let x = ${entity.attributeWhenReady.foo}", "  output: \"${x}\""),
                    e -> Asserts.expectedFailureContainsIgnoreCase(e, "unavailable", "entity.attributeWhenReady.foo", "Error resolving attribute", "BasicEntity"));
        }
    }

    @Test
    public void testWorkflowExpressionSensor_FreemarkerDoesNotCatchExceptions() throws Exception {
        try (AutoStartStopThread t = new AutoStartStopThread(() -> { Time.sleep(Duration.ONE_SECOND); waitForLastEntity().sensors().set(Sensors.newStringSensor("foo"), "bar"); })) {
            Callable<Object> expressionUnderTest = () -> invokeWorkflowStepsWithLogging("- s: let x = ${(entity.attributeWhenReady.foo)!\"unset\"}", "  output: \"${x}\"");

//            Asserts.assertEquals(expressionUnderTest.call(), "unset");

            // Freemarker evaluation syntax does not allow models to throw exceptions that can be caught by the ! syntax, so can't easily support the above;
            // annnoying, but probably better not to rely on that weird syntax, but to handle with extensions to 'let' (nullish operator, below)
            Asserts.assertFailsWith(expressionUnderTest,
                    e -> Asserts.expectedFailureContainsIgnoreCase(e, "unavailable", "entity.attributeWhenReady.foo", "Error resolving attribute", "BasicEntity"));
        }
    }

    @Test
    public void testWorkflowExpressionSensor_LetDoesCatchExceptionsWithNullish() throws Exception {
        try (AutoStartStopThread t = new AutoStartStopThread(() -> { Time.sleep(Duration.ONE_SECOND); waitForLastEntity().sensors().set(Sensors.newStringSensor("foo"), "bar"); })) {
            Callable<Object> expressionUnderTest = () -> invokeWorkflowStepsWithLogging("- s: let x = ${entity.attributeWhenReady.foo} ?? unset", "  output: \"${x}\"");

            Asserts.assertEquals(expressionUnderTest.call(), "unset");

            // old behaviour; above is now better
//            Asserts.assertFailsWith(expressionUnderTest,
//                    e -> Asserts.expectedFailureContainsIgnoreCase(e, "unavailable", "entity.sensor.foo", "Error resolving attribute", "BasicEntity"));
        }
    }

    @Test
    public void testWorkflowExpressionWaitResolvesAfterDelay() throws Exception {
        try (AutoStartStopThread t = new AutoStartStopThread(() -> { Time.sleep(Duration.millis(200*Math.random())); waitForLastEntity().sensors().set(Sensors.newStringSensor("foo"), "bar"); })) {
            Object x = invokeWorkflowStepsWithLogging("- wait ${entity.attributeWhenReady.foo}");
            Asserts.assertEquals(x, "bar");
        }
    }

    @Test
    public void testWorkflowExpressionMixingBrooklynDslAndExpressions() throws Exception {
        createEntityWithWorkflowEffector("- s: let x = $brooklyn:self()", "  output: ${x}");

        Asserts.assertEquals(invokeWorkflowStepsWithLogging(), lastEntity);
        Asserts.assertEquals(invokeWorkflowOnLastEntity("steps:", "- return $brooklyn:entity(\""+lastEntity.getId()+"\")"),
                lastEntity);

        lastEntity.sensors().set(Sensors.newStringSensor("my_id"), lastEntity.getId());
        Asserts.assertEquals(invokeWorkflowOnLastEntity("steps:", "- let x = $brooklyn:entity(\"${entity.sensor.my_id}\")", "- return ${x}"),
                lastEntity);
        Asserts.assertEquals(invokeWorkflowOnLastEntity("steps:",
                        "- step: let x",
                        "  value:",
                        "    $brooklyn:entity:",
                        "      ${entity.sensor.my_id}",
                        "- return ${x}"),
                lastEntity);
    }

}
