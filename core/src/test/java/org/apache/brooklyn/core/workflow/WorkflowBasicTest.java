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

import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.mgmt.classloading.BrooklynClassLoadingContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.api.policy.Policy;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.api.sensor.Sensor;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.CustomWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.AddEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.AddPolicyWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ApplyInitializerWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ClearConfigWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ClearSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeleteEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeletePolicyWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.DeployApplicationWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.InvokeEffectorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.ReparentEntityWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetConfigWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetEntityNameWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.SetSensorWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.appmodel.UpdateChildrenWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.HttpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.ShellWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.external.SshWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.FailWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.ForeachWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.GotoWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.IfWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.LabelWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.NoOpWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.RetryWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.ReturnWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SleepWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SubWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.flow.SwitchWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.ClearVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.LoadWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.SetVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.TransformVariableWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.variables.WaitWorkflowStep;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.json.BrooklynObjectsJsonMapper;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;


public class WorkflowBasicTest extends BrooklynMgmtUnitTestSupport {

    static final String VERSION = "0.1.0-SNAPSHOT";

    @SuppressWarnings("deprecation")
    public static RegisteredType addRegisteredTypeBean(ManagementContext mgmt, String symName, Class<?> clazz) {
        return BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, symName, VERSION,
                new BasicTypeImplementationPlan(
                        BeanWithTypePlanTransformer.FORMAT, "type: "+
                        // above is better than below because it can access private constructors
//                        JavaClassNameTypePlanTransformer.FORMAT,
                            clazz.getName()));
    }

    public static RegisteredType addRegisteredTypeSpec(ManagementContext mgmt, String symName, Class<?> clazz, Class<? extends BrooklynObject> superClazz) {
        RegisteredType rt = RegisteredTypes.spec(symName, VERSION,
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, clazz.getName()));
        RegisteredTypes.addSuperType(rt, superClazz);
        mgmt.getCatalog().validateType(rt, null, false);
        return mgmt.getTypeRegistry().get(rt.getSymbolicName(), rt.getVersion());
    }

    protected void loadTypes() {
        addWorkflowStepTypes(mgmt);
    }

    public static void addWorkflowStepTypes(ManagementContext mgmt) {
        addRegisteredTypeBean(mgmt, "log", LogWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "sleep", SleepWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "no-op", NoOpWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-config", SetConfigWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-entity-name", SetEntityNameWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-config", ClearConfigWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-sensor", SetSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-sensor", ClearSensorWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "let", SetVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "transform", TransformVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "load", LoadWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "set-workflow-variable", SetVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "clear-workflow-variable", ClearVariableWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "wait", WaitWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "return", ReturnWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "label", LabelWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "if", IfWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "goto", GotoWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "switch", SwitchWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "fail", FailWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "invoke-effector", InvokeEffectorWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "deploy-application", DeployApplicationWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "add-entity", AddEntityWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "delete-entity", DeleteEntityWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "reparent-entity", ReparentEntityWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "add-policy", AddPolicyWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "delete-policy", DeletePolicyWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "apply-initializer", ApplyInitializerWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "update-children", UpdateChildrenWorkflowStep.class);

        addRegisteredTypeBean(mgmt, "retry", RetryWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "workflow", CustomWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "subworkflow", SubWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "foreach", ForeachWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "ssh", SshWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "shell", ShellWorkflowStep.class);
        addRegisteredTypeBean(mgmt, "http", HttpWorkflowStep.class);

        addRegisteredTypeBean(mgmt, "workflow-effector", WorkflowEffector.class);
        addRegisteredTypeBean(mgmt, "workflow-sensor", WorkflowSensor.class);
        addRegisteredTypeSpec(mgmt, "workflow-policy", WorkflowPolicy.class, Policy.class);
        addRegisteredTypeBean(mgmt, "workflow-initializer", WorkflowInitializer.class);
    }

    public static WorkflowExecutionContext runWorkflow(Entity target, String workflowYaml, String defaultName) {
        // mimic what EntityResource.runWorkflow does
        CustomWorkflowStep workflow;
        try {
            workflow = BeanWithTypeUtils.newYamlMapper(((EntityInternal)target).getManagementContext(), true, RegisteredTypes.getClassLoadingContext(target), true)
                    .readerFor(CustomWorkflowStep.class).readValue(workflowYaml);
        } catch (JsonProcessingException e) {
            throw Exceptions.propagate(e);
        }

        WorkflowExecutionContext execution = workflow.newWorkflowExecution(target,
                Strings.firstNonBlank(workflow.getName(), workflow.getId(), defaultName),
                null,
                MutableMap.of("tags", MutableList.of(MutableMap.of("workflow_yaml", workflowYaml))));

        Entities.submit(target, execution.getTask(true).get());
        return execution;
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

    static Pair<BasicApplication,Object> runStepsInNewApp(ManagementContext mgmt, List<?> steps) {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Task<?> invocation = runSteps(app, steps);
        return Pair.of(app, invocation.getUnchecked());
    }

    static Task<?> runSteps(BasicApplication app, List<?> steps) {
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, (List) steps)
        );
        eff.apply((EntityLocal) app);
        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        return invocation;
    }

    @Test
    public void testStepResolution() throws JsonProcessingException {
        loadTypes();
        Map<String,Object> input = MutableMap.of("type", "no-op");

        // jackson
        WorkflowStepDefinition s = convert(input, WorkflowStepDefinition.class);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);

        // util
        s = new WorkflowStepResolution(mgmt, null, null).resolveStep(input);
        Asserts.assertInstanceOf(s, NoOpWorkflowStep.class);

        String output1 = BrooklynObjectsJsonMapper.newDslToStringSerializingMapper(mgmt).writeValueAsString(s);
        String output2 = BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writerFor(Object.class).writeValueAsString(s);

        Asserts.assertStringContains(output1, "\"shorthandTypeName\":\"no-op\"");
        Asserts.assertStringContains(output2, "shorthandTypeName: no-op");
    }

    @Test
    public void testShorthandStepResolution() throws JsonProcessingException {
        loadTypes();
        String input = "sleep 1s";

        // jackson doesn't handle shorthand; our custom method does that
        WorkflowStepDefinition s = new WorkflowStepResolution(mgmt, null, null).resolveStep(input);
        Asserts.assertInstanceOf(s, SleepWorkflowStep.class);
        Asserts.assertEquals( Duration.of(s.getInput().get(SleepWorkflowStep.DURATION.getName())), Duration.ONE_SECOND);

        String output1 = BrooklynObjectsJsonMapper.newDslToStringSerializingMapper(mgmt).writeValueAsString(s);
        String output2 = BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writerFor(Object.class).writeValueAsString(s);

        Asserts.assertStringContains(output1, "\"shorthandTypeName\":\"sleep\"");
        Asserts.assertStringContains(output2, "shorthandTypeName: sleep");
    }

    @Test
    public void testWorkflowStepsResolution() {
        loadTypes();

        List<Object> stepsDefinition =
                MutableList.of(
                        MutableMap.of("type", "no-op"),
                        MutableMap.of("type", "sleep", "duration", "1s"),
                        "sleep 1s",
                        "log test message"
                );

        List<WorkflowStepDefinition> steps = WorkflowStepResolution.resolveSteps(mgmt, stepsDefinition, null);
        Asserts.assertSize(steps, 4);
    }

    @Test
    public void testWorkflowObjectResolution() throws JsonProcessingException {
        loadTypes();

        Consumer<Object> test = wf -> {
            Asserts.assertInstanceOf(wf, WorkflowStepDefinition.class);
            Asserts.assertInstanceOf(wf, CustomWorkflowStep.class);
            Asserts.assertSize(((CustomWorkflowStep) wf).peekSteps(), 1);
            Asserts.assertInstanceOf(
                    WorkflowStepResolution.resolveSteps( mgmt, ((CustomWorkflowStep) wf).peekSteps(), null ).get(0), LogWorkflowStep.class);
        };

        test.accept( BeanWithTypeUtils.convert(mgmt,
                MutableMap.of(
                        "type", "workflow",
                        "steps", MutableList.of("log hi: bob")),
                    TypeToken.of(Object.class), true, null, true) );

        test.accept( BeanWithTypeUtils.convert(mgmt,
                MutableMap.of(
                        "steps", MutableList.of("log hi: bob")),
                TypeToken.of(CustomWorkflowStep.class), true, null, true) );

        test.accept( BeanWithTypeUtils.convert(mgmt,
                MutableList.of("log hi: bob"),
                TypeToken.of(CustomWorkflowStep.class), true, null, true) );
    }

    @Test
    public void testCommonStepsInEffector() throws JsonProcessingException {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append(MutableMap.of("type", "no-op"))
                        .append("log test message")

                        .append(MutableMap.of("type", "set-sensor", "sensor", "foo", "value", "bar"))
                        .append("set-sensor integer bar = 1")

                        .append("set-config integer foo = 2")

                        .append("set-config bad = will be removed")
                        .append("clear-config bad")

                        .append("set-sensor bad = will be removed")
                        .append("clear-sensor bad")

                        .append("let integer workflow_var = \"3\"")   // strings should be stripped
                        .append(WorkflowTestStep.of( (context) -> Asserts.assertEquals(context.getWorkflowExectionContext().getWorkflowScratchVariables().get("workflow_var"), 3 )))
                        .append("set-workflow-variable bad = will be removed")
                        .append(WorkflowTestStep.of( (context) -> Asserts.assertEquals(context.getWorkflowExectionContext().getWorkflowScratchVariables().get("bad"), "will be removed") ))
                        .append("clear-workflow-variable bad")
                        .append(WorkflowTestStep.of( (context) -> Asserts.assertThat(context.getWorkflowExectionContext().getWorkflowScratchVariables(), map -> !map.containsKey("bad")) ))
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

        WorkflowExecutionContext lastWorkflowContext = new WorkflowStatePersistenceViaSensors(mgmt()).getWorkflows(app).values().iterator().next();
        String output1 = BrooklynObjectsJsonMapper.newDslToStringSerializingMapper(mgmt).writeValueAsString(lastWorkflowContext);
        String output2 = BeanWithTypeUtils.newYamlMapper(mgmt, false, null, false).writerFor(Object.class).writeValueAsString(lastWorkflowContext);

        Asserts.assertStringContains(output1, "\"type\":\"no-op\"");
        Asserts.assertStringContains(output2, "type: no-op");
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

        @Override
        protected Boolean isDefaultIdempotent() {
            return true;
        }
    }

    @Test
    public void testWorkflowResolutionScratchVariable() {
        doTestOfWorkflowVariable(context -> context.getWorkflowExectionContext().updateWorkflowScratchVariable("foo", "bar"), "${foo}", "bar");
    }

    @Test
    public void testWorkflowResolutionScratchVariableCoerced() {
        doTestOfTypedWorkflowVariable(context -> context.getWorkflowExectionContext().updateWorkflowScratchVariable("foo", "7"), "${foo}", "integer", 7);
    }

    @Test
    public void testWorkflowResolutionEntityConfig() {
        doTestOfWorkflowVariable(context -> context.getEntity().config().set(ConfigKeys.newStringConfigKey("foo"), "bar"), "${entity.config.foo}", "bar");
    }

    @Test
    public void testWorkflowResolutionMore() {
        doTestOfWorkflowVariable(context -> context.getWorkflowExectionContext().updateWorkflowScratchVariable("foo", MutableList.of("baz", "bar")), "${foo[1]}", "bar");
        doTestOfWorkflowVariable(context -> context.getEntity().config().set(ConfigKeys.newConfigKey(Object.class, "foo"), MutableMap.of("bar", "baz")), "${entity.config.foo.bar}", "baz");
    }

    public void doTestOfWorkflowVariable(Consumer<WorkflowStepInstanceExecutionContext> setup, String expression, Object expected) {
        doTestOfTypedWorkflowVariable(setup, expression, null, expected);
    }
    public void doTestOfTypedWorkflowVariable(Consumer<WorkflowStepInstanceExecutionContext> setup, String expression, String type, Object expected) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        Task<?> invocation = runSteps(app, MutableList.of(
                WorkflowTestStep.of(setup::accept),
                "set-sensor " + (type != null ? type + " " : "") + "x = " + expression));
        invocation.getUnchecked();
        Dumper.dumpInfo(invocation);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "x"), expected);
    }

    @Test
    public void testWorkflowLogging() throws Exception {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        "log one",
                        MutableMap.of("s", "log two", "id", "ii", "name", "Two",
                                "output", MutableMap.of(
                                        "tasks", MutableList.of("${workflow.previous_step.task_id}", "${workflow.current_step.task_id}"),
                                        "workflow", "${workflow.task_id}")
                        )))
                .configure(WorkflowEffector.OUTPUT, "${workflow.previous_step.output}")
        );
        eff.apply((EntityLocal)app);

        try (ClassLogWatcher logWatcher = new ClassLogWatcher(getClass().getPackage().getName())) {
            Map ids = (Map) app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).get();
            Object workflowId = ids.get("workflow");
            List tasksIds = (List) ids.get("tasks");

            List<String> msgs = logWatcher.getMessages().stream().filter(x -> !x.startsWith("Blocked by lock")).collect(Collectors.toList());
            // can have "Blocked by lock on lock-for-incrementor, currently held by JPuhvC9I" from a previous invocation?

            if (msgs.size()!=8) throw new IllegalStateException("Wrong number of messages found ("+msgs.size()+", not 8): "+msgs);

            Asserts.assertEquals(msgs, MutableList.of(
                    "Starting workflow 'myWorkflow (workflow effector)', moving to first step "+workflowId+"-1",
                    "Starting step "+workflowId+"-1 in task "+tasksIds.get(0),
                    "one",
                    "Completed step "+workflowId+"-1; moving to sequential next step "+workflowId+"-2-ii",
                    "Starting step "+workflowId+"-2-ii 'Two' in task "+tasksIds.get(1),
                    "two",
                    "Completed step "+workflowId+"-2-ii; no further steps: Workflow completed",
                    "Completed workflow "+workflowId+" successfully; step count: 2 considered, 2 executed"));
        }
    }

    @Test
    public void testWorkflowLoggingWithCategoryLevel() throws Exception {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        String category = "org.acme.audit.example";
        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        MutableMap.of("step", "log with category and level",
                                "level", "info",
                                "category", category
                        ),
                        MutableMap.of("step", "log with default info level",
                                "level", "incorrect",
                                "category", category
                        )))
        );
        eff.apply((EntityLocal)app);
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(category)) {
            app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).get();
            Asserts.assertEquals(logWatcher.getMessages(), MutableList.of(
                    "with category and level",
                    "with default info level"
                    ));
        }
    }

    @Test
    public void testConditionResolvesAndExactlyOnce() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "steps:",
                " - step: let a = b",
                " - step: let b = c",
                " - step: let list result = []",
                " - step: transform variable result | append a=b",
                "   condition:",
                "     target: ${a}",
                "     equals: b",
                " - step: transform variable result | append a=c",
                "   condition:",
                "     target: ${a}",
                "     equals: c",
                " - step: transform variable result | append b=c",
                "   condition:",
                "     target: ${b}",
                "     equals: c",
                " - return ${result}"
        ), null);
        Asserts.assertEquals(
                w1.getTask(false).get().getUnchecked(),
                MutableList.of("a=b", "b=c"));
    }

    @Test
    public void testOutputOnlyWorkflow() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "output: 42"
        ), null);
        Asserts.assertEquals(
                w1.getTask(false).get().getUnchecked(),
                42);
    }

    @Test
    public void testWorkflowStepWithCustomName() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        WorkflowExecutionContext w1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "- name: step on entity ${entity.id}",
                "  step: return ${entity.id}"
        ), null);
        Asserts.assertEquals(
                w1.getTask(false).get().getUnchecked(),
                app.getId());
        Asserts.assertEquals(w1.getStepsResolved().get(0).getName(), "step on entity "+app.getId());
    }
    
}
