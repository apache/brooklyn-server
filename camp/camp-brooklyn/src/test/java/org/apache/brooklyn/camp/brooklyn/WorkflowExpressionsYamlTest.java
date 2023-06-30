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
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.lifecycle.Lifecycle;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.steps.flow.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.entity.stock.BasicEntityImpl;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Secret;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

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

    private Entity createEntity() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName());
        waitForApplicationTasks(app);

        // Deploy the blueprint.
        Entity entity = lastEntity = Iterables.getOnlyElement(app.getChildren());
        synchronized (this) { this.notifyAll(); }
        return entity;
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

    private Object invokeWorkflowOnLastEntity(String ...workflowDefinition) {
        try {
            WorkflowExecutionContext context = invocationWorkflowOnLastEntity(workflowDefinition);
            return context.getTask(false).get().get(Duration.seconds(5));
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        }
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
    public void testWorkflowTemplateExpressionAllowsOnFire() throws Exception {
        Entity entity = createEntity();
        WorkflowExecutionContext workflow = invocationWorkflowOnLastEntity(
                "    - step: transform ${entity.attributeWhenReady.foo} | wait | set foo_in_workflow",
                "      timeout: 4s",
                "    - step: set-sensor new-foo = ${foo_in_workflow}"
                );

        Time.sleep(Duration.ONE_SECOND);
        Asserts.assertFalse(workflow.getTask(false).get().isDone());

        waitForLastEntity().sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        Time.sleep(Duration.of(2, TimeUnit.SECONDS));
        waitForLastEntity().sensors().set(Sensors.newIntegerSensor("foo"), 10);

        workflow.getTask(false).get().blockUntilEnded(Duration.TEN_SECONDS);
        Asserts.assertFalse(workflow.getTask(true).get().isError());
        Integer fooInWorkflow = entity.sensors().get(Sensors.newIntegerSensor("new-foo"));
        Asserts.assertEquals((Integer) 10, fooInWorkflow);
    }

    @Test
    public void testBrooklynDslExpressionAbortWhenOnFire() throws Exception {
        createEntity();
        WorkflowExecutionContext workflow = invocationWorkflowOnLastEntity(
                "    - step: transform $brooklyn:attributeWhenReady(\"foo\") | wait | set foo_in_workflow",
                "      timeout: 3s"
        );

        Time.sleep(Duration.ONE_SECOND);
        Asserts.assertFalse(workflow.getTask(false).get().isDone());

        waitForLastEntity().sensors().set(Attributes.SERVICE_STATE_ACTUAL, Lifecycle.ON_FIRE);
        Time.sleep(Duration.of(2, TimeUnit.SECONDS));
        waitForLastEntity().sensors().set(Sensors.newIntegerSensor("foo"), 10);

        workflow.getTask(false).get().blockUntilEnded(Duration.TEN_SECONDS);
        Asserts.assertTrue(workflow.getTask(true).get().isError());
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

    @Test
    public void testWorkflowSecretGet() throws Exception {
        createEntityWithWorkflowEffector("- s: transform x = ${entity.config.a_secret} | get", "  output: ${x}");
        lastEntity.config().set(ConfigKeys.newConfigKey(Secret.class, "a_secret"), new Secret("53cr37"));
        Asserts.assertEquals(invokeWorkflowStepsWithLogging(), "53cr37");
    }


    @Test
    public void testEntityConditionSucceeds() throws Exception {
        createEntityWithWorkflowEffector("- return ignored");
        Function<String,Object> test = equalsCheckTarget -> invokeWorkflowOnLastEntity(
                "steps:\n" +
                        "  - let ent = $brooklyn:self()\n" +
                        "  - let root = $brooklyn:root()\n" +
                        "  - transform checkTargetS = "+equalsCheckTarget+" | to_string\n" +
                        "  - log comparing ${ent.id} with ${checkTargetS}\n" +
                        "  - step: return condition met\n" +
                        "    condition:\n" +
                        "      target: ${ent}\n" +
                        "      equals: "+equalsCheckTarget+"\n" +
                        "  - step: return condition not met");

        Asserts.assertEquals(test.apply("xxx"), "condition not met");
        Asserts.assertEquals(test.apply("${root}"), "condition not met");
        Asserts.assertEquals(test.apply("${root.children[0]}"), "condition met");


        // checking equality to the ID does not work
        // it could, fairly easily -- the ID *is* coerced to an entity;
        // but it then fails because it is coerced to the _proxy_ which is *not* coerced to the real delegate
        Asserts.assertEquals(test.apply("${ent.id}"), "condition not met");

        // notes and minor tests on the above
        // coercion of ID
        Entity coercedFromId = Entities.submit(lastEntity, Tasks.of("test", () -> TypeCoercions.coerce(lastEntity.getId(), Entity.class))).get();
        Asserts.assertEquals(coercedFromId, lastEntity);
        Maybe<BasicEntityImpl> coercedFromIdProxyToConcreteFails = Entities.submit(lastEntity, Tasks.of("test", () -> TypeCoercions.tryCoerce(lastEntity.getId(), BasicEntityImpl.class))).get();
        Asserts.assertThat(coercedFromIdProxyToConcreteFails, Maybe::isAbsent);
        // under the covers above works using coercer 80-bean, which does
        Entity coercedFromIdEntity = BeanWithTypeUtils.convert(mgmt(), lastEntity.getId(), TypeToken.of(Entity.class), true, null, true);
        Asserts.assertEquals(coercedFromIdEntity, lastEntity);

        // some extra checks for coercion of unknown IDs -- conversion returns null, but tryConvert and coerce will not accept that
        Entity coercedFromMissingIdRaw = BeanWithTypeUtils.convert(mgmt(), "xxx", TypeToken.of(Entity.class), true, null, true);
        Asserts.assertNull(coercedFromMissingIdRaw);

        Maybe<Entity> coercedFromMissingId;
        coercedFromMissingId = BeanWithTypeUtils.tryConvertOrAbsent(mgmt(), Maybe.of("xxx"), TypeToken.of(Entity.class), true, null, true);
        Asserts.assertThat(coercedFromMissingId, Maybe::isAbsent);

        coercedFromMissingId = Entities.submit(lastEntity, Tasks.of("test", () -> TypeCoercions.tryCoerce("does_not_exist", Entity.class))).get();
        Asserts.assertThat(coercedFromMissingId, Maybe::isAbsent);
    }
}
