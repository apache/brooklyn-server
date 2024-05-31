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

import java.io.File;
import java.util.Map;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.api.objs.EntityAdjunct;
import org.apache.brooklyn.api.sensor.Feed;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Dumper;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.entity.EntityInternal;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.core.workflow.WorkflowEffector;
import org.apache.brooklyn.core.workflow.WorkflowExecutionContext;
import org.apache.brooklyn.core.workflow.store.WorkflowStatePersistenceViaSensors;
import org.apache.brooklyn.entity.software.base.EmptySoftwareProcess;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.entity.stock.BasicEntity;
import org.apache.brooklyn.location.localhost.LocalhostMachineProvisioningLocation;
import org.apache.brooklyn.tasks.kubectl.ContainerEffectorTest;
import org.apache.brooklyn.tasks.kubectl.ContainerWorkflowStep;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class WorkflowYamlRebindTest extends AbstractYamlRebindTest {

    private static final Logger log = LoggerFactory.getLogger(WorkflowYamlRebindTest.class);

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        WorkflowYamlTest.addWorkflowTypes(mgmt());
    }

    @Override
    protected LocalManagementContext createNewManagementContext(File mementoDir, Map<?, ?> additionalProperties) {
        LocalManagementContext result = super.createNewManagementContext(mementoDir, additionalProperties);
        WorkflowYamlTest.addWorkflowTypes(result);
        return result;
    }

    @Test
    public void testEffectorArgDslInMap() throws Exception {
        BrooklynDslCommon.registerSerializationHooks();
        BasicApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class).configure("z", "Z"));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector3")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("x", MutableMap.of("type", "map")))
                .configure(WorkflowEffector.STEPS, MutableList.of("return ${x}")));
        eff.apply((EntityLocal)app);
        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector2")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("x", MutableMap.of()))
                .configure(WorkflowEffector.STEPS, MutableList.of(MutableMap.of("step", "invoke-effector myWorkflowEffector3",
                        "args", MutableMap.of("x", "${x}")))));
        eff.apply((EntityLocal)app);
        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector1")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("x", MutableMap.of()))
                .configure(WorkflowEffector.STEPS, MutableList.of(MutableMap.of("step", "invoke-effector myWorkflowEffector2",
                        "args", MutableMap.of("x", MutableMap.of("y", "$brooklyn:config(\"z\")"))))));
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflowEffector1").get(), MutableMap.of());
        Asserts.assertEquals(invocation.getUnchecked(), MutableMap.of("y","Z"));

        app = (BasicApplication) rebind();

        invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflowEffector1").get(), MutableMap.of());
        Asserts.assertEquals(invocation.getUnchecked(), MutableMap.of("y","Z"));
    }

    @Test(groups="Live")
    public void testEffectorSshEnvArgDslInMap() throws Exception {
        BrooklynDslCommon.registerSerializationHooks();
        TestApplication app = mgmt().getEntityManager().createEntity(EntitySpec.create(TestApplication.class).configure("z", "Z"));

        EmptySoftwareProcess child = app.createAndManageChild(EntitySpec.create(EmptySoftwareProcess.class).location(LocationSpec.create(LocalhostMachineProvisioningLocation.class)));
        app.start(ImmutableList.of());

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector3")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("script", MutableMap.of(), "env", MutableMap.of("defaultValue", MutableMap.of())))
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        MutableMap.of("type", "ssh",
                                "command", "bash -c \"${script}\"",
                                "env", "${env}"),
                        "return ${stdout}")));
        eff.apply((EntityLocal)child);
        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector2")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("script", MutableMap.of(), "env", MutableMap.of("defaultValue", MutableMap.of())))
                .configure(WorkflowEffector.STEPS, MutableList.of(MutableMap.of("step", "invoke-effector myWorkflowEffector3",
                        "args", MutableMap.of("script", "${script}", "env", "${env}")))));
        eff.apply((EntityLocal)child);
        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflowEffector1")
                .configure(WorkflowEffector.STEPS, MutableList.of(MutableMap.of("step", "invoke-effector myWorkflowEffector2",
                        "args", MutableMap.of("script", "echo Y is $Y", "env", MutableMap.of("Y", "$brooklyn:config(\"z\")"))))));
        eff.apply((EntityLocal)child);

        Task<?> invocation = child.invoke(child.getEntityType().getEffectorByName("myWorkflowEffector1").get(), MutableMap.of());
        Asserts.assertEquals(invocation.getUnchecked().toString().trim(), "Y is Z");

        app = (TestApplication) rebind();
        child = (EmptySoftwareProcess) Iterables.getLast(app.getChildren());

        invocation = child.invoke(child.getEntityType().getEffectorByName("myWorkflowEffector1").get(), MutableMap.of());
        Asserts.assertEquals(invocation.getUnchecked().toString().trim(), "Y is Z");
    }

    @Test(groups="Live")
    public void testContainerEchoBashCommandAsWorkflowEffectorWithVarFromConfig() throws Exception {
        WorkflowBasicTest.addRegisteredTypeBean(mgmt(), "container", ContainerWorkflowStep.class);
        BrooklynDslCommon.registerSerializationHooks();
        final String message = ("hello " + Strings.makeRandomId(10)).toLowerCase();

        EntitySpec<TestApplication> appSpec = EntitySpec.create(TestApplication.class);
        TestApplication app = mgmt().getEntityManager().createEntity(appSpec);

        Object output = ContainerEffectorTest.doTestEchoBashCommand(app, () -> {
            ConfigBag parameters = ConfigBag.newInstance(ImmutableMap.of(
                    WorkflowEffector.EFFECTOR_NAME, "test-container-effector",
                    WorkflowEffector.STEPS, MutableList.of(
                            MutableMap.<String, Object>of("step", "container " + ContainerEffectorTest.BASH_SCRIPT_CONTAINER + " echo " + message + " $VAR",
                                    "input",
                                    MutableMap.of("env", MutableMap.of("VAR", "$brooklyn:config(\"hello\")")),
                                    "output", "${stdout}"))));

            return new WorkflowEffector(parameters);
        }, entity -> entity.config().set(ConfigKeys.newStringConfigKey("hello"), "world"));
        Asserts.assertEquals(output.toString().trim(), message + " world");

        app = (TestApplication) rebind();

        TestEntity child = (TestEntity) Iterables.getLast(app.getChildren());

        output = Entities.invokeEffector(app, child, child.getEffector("test-container-effector")).getUnchecked(Duration.ONE_MINUTE);
        Asserts.assertEquals(output.toString().trim(), message + " world");
    }

    static class AdjunctHolder {
        String name;
        EntityAdjunct adjunct;

        public static AdjunctHolder of(String name, EntityAdjunct x) {
            AdjunctHolder result = new AdjunctHolder();
            result.name = name;
            result.adjunct = x;
            return result;
        }
    }

    @Test
    public void testWorkflowSensorRebind() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-sensor",
                "    brooklyn.config:",
                "      sensor: myWorkflowSensor",
                "      triggers:",
                "        - trig",
                "        - trig2",  // to make sure doesn't public too much
                "      steps:",
                "        - type: return",
                "          value:",
                "            n: ${entity.sensor.trig}",
                "");

        waitForApplicationTasks(app);
        Entity child = Iterables.getOnlyElement(app.getChildren());

        child.sensors().set(Sensors.newIntegerSensor("trig"), 1);
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "myWorkflowSensor"), MutableMap.of("n", 1));

        Set<Task<?>> tt = BrooklynTaskTags.getTasksInAdjunctContext(mgmt().getExecutionManager(), Iterables.getOnlyElement(((EntityInternal) child).feeds().getFeeds()));
        Asserts.assertThat(tt, ts -> ts.stream().anyMatch(ti -> ti.getDisplayName().contains("Workflow for sensor")));

        Feed f = ((EntityInternal) child).feeds().getFeeds().iterator().next();
        app.tags().addTag(AdjunctHolder.of("1", f));

        Dumper.dumpInfo(app);
        app = rebind();
        child = Iterables.getOnlyElement(app.getChildren());

        child.sensors().set(Sensors.newIntegerSensor("trig"), 2);
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "myWorkflowSensor"), MutableMap.of("n", 2));

        tt = BrooklynTaskTags.getTasksInAdjunctContext(mgmt().getExecutionManager(), Iterables.getOnlyElement(((EntityInternal) child).feeds().getFeeds()));
        Asserts.assertThat(tt, ts -> ts.stream().anyMatch(ti -> ti.getDisplayName().contains("Workflow for sensor")));

        f = ((EntityInternal) child).feeds().getFeeds().iterator().next();
        app.tags().addTag(AdjunctHolder.of("2", f));

        // assert adjuncts rebind and rebind quite happily
        switchOriginalToNewManagementContext();
        app = rebind();
    }

    @Test
    public void testWorkflowSensorWithMutexRebind() throws Exception {
        Entity app = createAndStartApplication(
                "services:",
                "- type: " + BasicEntity.class.getName(),
                "  brooklyn.initializers:",
                "  - type: workflow-sensor",
                "    brooklyn.config:",
                "      sensor: myWorkflowSensor",
                "      triggers:",
                "        - trig",
                "        - trig2",  // to make sure doesn't public too much
                "      steps:",
                "        - let trig = ${entity.sensor.trig} ?? 0",
                "        - step: workflow",
                "          lock: count",
                "          steps:",
                "           - let count = ${entity.sensor.count} ?? 0",
                "           - let count = ${count} + 1",
                "           - log count now ${count}",
                "           - step: set-sensor count = ${count}",
                "             replayable: from here",  // not needed for this test, but for good measure
                "        - type: return",
                "          value:",
                "            n: ${trig}",
                "");

        waitForApplicationTasks(app);
        Entity child = Iterables.getOnlyElement(app.getChildren());

        child.sensors().set(Sensors.newIntegerSensor("trig"), 1);
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "myWorkflowSensor"), MutableMap.of("n", 1));
        // should run once initially and once on trigger
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "count"), 2);

        Set<Task<?>> tt = BrooklynTaskTags.getTasksInAdjunctContext(mgmt().getExecutionManager(), Iterables.getOnlyElement(((EntityInternal) child).feeds().getFeeds()));
        Asserts.assertThat(tt, ts -> ts.stream().anyMatch(ti -> ti.getDisplayName().contains("Workflow for sensor")));

        Dumper.dumpInfo(app);
        app = rebind();
        child = Iterables.getOnlyElement(app.getChildren());

        // is run again when feed restarts (but could weaken)
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "count"), 3);

        log.info("About to emit trig 2");
        child.sensors().set(Sensors.newIntegerSensor("trig"), 2);
        EntityAsserts.assertAttributeEqualsEventually(child, Sensors.newSensor(Object.class, "myWorkflowSensor"), MutableMap.of("n", 2));
        EntityAsserts.assertAttributeEquals(child, Sensors.newSensor(Object.class, "count"), 4);

        tt = BrooklynTaskTags.getTasksInAdjunctContext(mgmt().getExecutionManager(), Iterables.getOnlyElement(((EntityInternal) child).feeds().getFeeds()));
        Asserts.assertThat(tt, ts -> ts.stream().anyMatch(ti -> ti.getDisplayName().contains("Workflow for sensor")));
    }

    @Test
    public void testRebindsHistoricNextIsReturnLocalWorkflow() throws Exception {
        Application app;
//        app = mgmt().getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
//        WorkflowExecutionContext wc1 = WorkflowBasicTest.runWorkflow(app, Strings.lines(
//                "steps:",
//                "- let x = 1",
//                "- steps:",
//                "  - if ${x} == 1 then goto " + WorkflowExecutionContext.STEP_TARGET_NAME_FOR_END,  // used to not go to end of workflow
//                "  - let x = 2",
//                "  - goto end",
//                "  - let x = no1",
//                "- steps:",
//                "  - return ${x}"), "test");
//        Asserts.assertEquals(wc1.getTask(true).get().getUnchecked(), "2");

        ResourceUtils resourceUtils = ResourceUtils.create(this);
        String persistedXmlForAbove = resourceUtils.getResourceAsString("classpath://org/apache/brooklyn/camp/brooklyn/persisted-entity-with-workflow-next-fields.xml");
        /* We used to store nextIsReturn and isLocalSubWorkflow; these are migrating, but we want to make sure persistence/rebind works.
         * It is mapped to the right place.
         */
        FileUtils.write(new File(mementoDir, "entities/zxg2xnpxur"), persistedXmlForAbove);

        app = rebind();
        WorkflowExecutionContext wc2 = WorkflowStatePersistenceViaSensors.get(mgmt()).getWorkflows(app).values().iterator().next();
        Asserts.assertEquals(wc2.getOutput(), "2");
    }

}