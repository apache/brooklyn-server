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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.*;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkflowBasicTest extends BrooklynMgmtUnitTestSupport {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        RegisteredType rt = RegisteredTypes.bean(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        ((BasicBrooklynTypeRegistry)mgmt.getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
        return rt;
    }

    protected void loadTypes() {
        addWorkflowStepTypes(mgmt);
    }

    public static void addWorkflowStepTypes(ManagementContext mgmt) {
        addRegisteredTypeBean(mgmt, "log", LogWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-config", SetConfigWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-config", ClearConfigWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-sensor", SetSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-sensor", ClearSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "let", SetVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-workflow-variable", SetVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-workflow-variable", ClearVariableWorkflowStep.class);
    }

    <T> T convert(Object input, Class<T> type) {
        return convert(input, TypeToken.of(type));
    }

    <T> T convert(Object input, TypeToken<T> type) {
        BrooklynClassLoadingContext loader = RegisteredTypes.getCurrentClassLoadingContextOrManagement(mgmt);
        try {
            return BeanWithTypeUtils.convert(mgmt, input, type, true, loader, false);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }
    }

    @Test
    public void testStepResolution() {
        loadTypes();
        Map<String,Object> input = MutableMap.of("type", "no-op");

        // jackson
        WorkflowStepDefinition s = convert(input, WorkflowStepDefinition.class);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);

        // util
        s = WorkflowStepResolution.resolveStep(mgmt, "s1", input);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);
    }

    @Test
    public void testShorthandStepResolution() {
        loadTypes();
        String input = "sleep 1s";

        // only util will work for shorthand
        WorkflowStepDefinition s = WorkflowStepResolution.resolveStep(mgmt, "s1", input);
        Asserts.assertInstanceOf(s, SleepWorkflowStep.class);
        Asserts.assertEquals( Duration.of(s.getInput().get(SleepWorkflowStep.DURATION.getName())), Duration.ONE_SECOND);
    }

    @Test
    public void testWorkflowDefinitionResolution() {
        loadTypes();

        Map<String,Object> input = MutableMap.of(
                "steps", MutableMap.of(
                        "step1", MutableMap.of("type", "no-op"),
                        "step2", MutableMap.of("type", "sleep", "duration", "1s"),
                        "step3", "sleep 1s",
                        "step4", "log test message"
                ));

        WorkflowDefinition w = convert(input, WorkflowDefinition.class);
        Asserts.assertNotNull(w);
        w.validate(mgmt);
        Map<String, WorkflowStepDefinition> steps = w.getStepsResolved(mgmt);
        Asserts.assertSize(steps, 4);
    }

    @Test
    public void testWorkflowEffector() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableMap.<String,Object>of()
                        .add("step1", MutableMap.of("type", "no-op"))
                        .add("step2", "log test message")

                        .add("step3", MutableMap.of("type", "set-sensor", "sensor", "foo", "value", "bar"))
                        .add("step4", "set-sensor integer bar = 1")

                        .add("step5", "set-config integer foo = 2")

                        .add("step6a", "set-config bad = will be removed")
                        .add("step6b", "clear-config bad")

                        .add("step7a", "set-sensor bad = will be removed")
                        .add("step7b", "clear-sensor bad")

                        .add("step8a-1", "let integer workflow_var = 3")
                        .add("step8a-2", WorkflowTestStep.of( (context) -> Asserts.assertEquals(context.getWorkflowExectionContext().getWorkflowScratchVariables().get("workflow_var"), 3 )))
                        .add("step8b-1", "set-workflow-variable bad = will be removed")
                        .add("step8b-2", WorkflowTestStep.of( (context) -> Asserts.assertEquals(context.getWorkflowExectionContext().getWorkflowScratchVariables().get("bad"), "will be removed") ))
                        .add("step8b-3", "clear-workflow-variable bad")
                        .add("step8b-4", WorkflowTestStep.of( (context) -> Asserts.assertThat(context.getWorkflowExectionContext().getWorkflowScratchVariables(), map -> !map.containsKey("bad")) ))
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();

        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "foo"), "bar");
        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "bar"), 1);
        // sensor IS not added dynamically
        AttributeSensor<?> goodSensor = (AttributeSensor<?>) app.getEntityType().getSensor("bar");
        Asserts.assertNotNull(goodSensor);
        Asserts.assertEquals(goodSensor.getType(), Integer.class);

        EntityAsserts.assertConfigEquals(app, ConfigKeys.newConfigKey(Object.class, "foo"), 2);
        // config is NOT added dynamically
        ConfigKey<?> goodConfig = app.getEntityType().getConfigKey("foo");
        Asserts.assertNull(goodConfig);
//        Asserts.assertEquals(goodConfig.getType(), Integer.class);

        // dynamic config key definition not available (never), and value also not available
        ConfigKey<?> badConfig = app.getEntityType().getConfigKey("bad");
        Asserts.assertNull(badConfig);
        Asserts.assertEquals(app.config().get(ConfigKeys.newConfigKey(Object.class, "bad")), null);
        Asserts.assertThat(app.config().findKeysPresent(k -> k.getName().equals("bad")), s -> s.isEmpty());

        // dynamic sensor type not available when cleared
        AttributeSensor<?> badSensor = (AttributeSensor<?>) app.getEntityType().getSensor("bad");
        Asserts.assertNull(badSensor);
        Asserts.assertEquals(app.sensors().get(Sensors.newSensor(Object.class, "bad")), null);
        Asserts.assertThat(app.sensors().getAll().keySet().stream().map(Sensor::getName).collect(Collectors.toSet()), s -> !s.contains("bad"));
    }

    public static class WorkflowTestStep extends WorkflowStepDefinition {
        Function<WorkflowStepInstanceExecutionContext, Object> task;

        WorkflowTestStep(Function<WorkflowStepInstanceExecutionContext, Object> task) { this.task = task; }

        static WorkflowTestStep ofFunction(Function<WorkflowStepInstanceExecutionContext, Object> task) { return new WorkflowTestStep(task); }
        static WorkflowTestStep of(Consumer<WorkflowStepInstanceExecutionContext> task) { return new WorkflowTestStep(context -> { task.accept(context); return null; }); }
        static WorkflowTestStep of(Runnable task) { return new WorkflowTestStep((context) -> { task.run(); return null; }); }

        @Override
        public void populateFromShorthand(String value) {
            throw new UnsupportedOperationException();
        }

        @Override
        protected Object doTaskBody(WorkflowStepInstanceExecutionContext context) {
            return task.apply(context);
        }
    }

    @Test
    public void testWorkflowResolutionScratchVariable() {
        doTestOfWorkflowVariable(context -> context.getWorkflowExectionContext().getWorkflowScratchVariables().put("foo", "bar"), "${foo}", "bar");
    }

    @Test
    public void testWorkflowResolutionScratchVariableCoerced() {
        doTestOfTypedWorkflowVariable(context -> context.getWorkflowExectionContext().getWorkflowScratchVariables().put("foo", "7"), "${foo}", "integer", 7);
    }

    @Test
    public void testWorkflowResolutionEntityConfig() {
        doTestOfWorkflowVariable(context -> context.getEntity().config().set(ConfigKeys.newStringConfigKey("foo"), "bar"), "${entity.config.foo}", "bar");
    }

    @Test
    public void testWorkflowResolutionMore() {
        doTestOfWorkflowVariable(context -> context.getWorkflowExectionContext().getWorkflowScratchVariables().put("foo", MutableList.of("baz", "bar")), "${foo[1]}", "bar");
        doTestOfWorkflowVariable(context -> context.getEntity().config().set(ConfigKeys.newConfigKey(Object.class, "foo"), MutableMap.of("bar", "baz")), "${entity.config.foo.bar}", "baz");
    }

    public void doTestOfWorkflowVariable(Consumer<WorkflowStepInstanceExecutionContext> setup, String expression, Object expected) {
        doTestOfTypedWorkflowVariable(setup, expression, null, expected);
    }
    public void doTestOfTypedWorkflowVariable(Consumer<WorkflowStepInstanceExecutionContext> setup, String expression, String type, Object expected) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableMap.of(
                        "step1", WorkflowTestStep.of( setup::accept ),
                        "step2", "set-sensor " + (type!=null ? type+" " : "") + "x = " + expression
                ))
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();

        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), expected);
    }

}
