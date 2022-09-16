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
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypePlanTransformer;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;

public class WorkflowInputOutputExtensionTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    @Test
    public void testParameterReference() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableMap.<String,Object>of()
                        .add("1", "set-sensor p1 = ${p1}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertEquals(result, "p1v");

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "p1"), "p1v");
    }


    @Test
    public void testMapOutputAndInputFromLastStep() {
        doTestMapOutputAndInput(cfg -> {
            Map<String, Object> map = cfg.get(WorkflowEffector.STEPS);
            map.put("s5", "let map result = { c: ${c}, d: ${d}, e: ${e} }");
            cfg.put(WorkflowEffector.STEPS, map);
        });
    }

    @Test
    public void testMapOutputAndInputFromExplicitOutput() {
        doTestMapOutputAndInput(cfg -> cfg.put(WorkflowEffector.OUTPUT,
                MutableMap.of("c", "${c}", "d", "${d}", "e", "${e}") ));
    }

    public void doTestMapOutputAndInput(Consumer<ConfigBag> mod) {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        ConfigBag cfg = ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableMap.<String, Object>of()
                        .add("s1", "let map v = { x: { y: a }, z: b }")
                        .add("s2", "let c = ${x.y}")  // reference output of previous step
                        .add("s3", "let d = ${v.z}")  // reference workflow var
                        .add("s4", "let e = ${workflow.step.s1.output.z}")  // reference explicit output step
                );
        // mod says how results are returned
        mod.accept(cfg);

        WorkflowEffector eff = new WorkflowEffector(cfg);
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Map result = (Map) invocation.getUnchecked();
        Asserts.assertEquals(result.get("c"), "a");
        Asserts.assertEquals(result.get("d"), "b");
        Asserts.assertEquals(result.get("e"), "b");
    }

//    static class WorkflowTestEffector1 extends WorkflowEffector {
//        WorkflowTestEffector1() {
//            initParams().put(
//        }
//    }

    public RegisteredType addBeanWithType(String typeName, String version, String plan) {
        RegisteredType type = RegisteredTypes.bean(typeName, version, new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, plan));
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(type, false);
        return type;
    }

    ClassLogWatcher lastLogWatcher;

    Object invokeWorkflowStepsWithLogging(Map<String,Object> steps) throws Exception {
        try (ClassLogWatcher logWatcher = new ClassLogWatcher(LogWorkflowStep.class)) {
            lastLogWatcher = logWatcher;

            loadTypes();
            BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

            WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                    .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                    .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                    .configure(WorkflowEffector.STEPS, steps));
            eff.apply((EntityLocal)app);

            Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
            return invocation.getUnchecked();
        }
    }

    void assertLogStepMessages(String ...lines) {
        Assert.assertEquals(lastLogWatcher.getMessages(),
                Arrays.asList(lines));
    }

    @Test
    public void testExtendingAStepWhichWorksButIsMessyAroundParameters() throws Exception {
        // can't use "name" because that is understood by the step
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: log",
                "message: hi ${nom}",
                "input:",
                "  nom: you"
        ));

        invokeWorkflowStepsWithLogging(MutableMap.of("1", MutableMap.of("type", "log-hi", "nom", "bob")));
        assertLogStepMessages("1: hi bob");

        invokeWorkflowStepsWithLogging(MutableMap.of("1", MutableMap.of("type", "log-hi")));
        assertLogStepMessages("1: hi you");
    }

    // TODO this is probably cleaner for parameters
    @Test
    public void testCompoundStep() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: compound-step",
                "shorthand: ${name}",
                "parameters:",
                "  name: {}",
                "steps:",
                "  1: log hi ${name}"
        ));
        invokeWorkflowStepsWithLogging(MutableMap.of("1", "log-hi bob"));
        assertLogStepMessages("1: hi bob");
    }

    // TODO shorthand type / extension (in YAML)
    // TODO parameters to steps
    // TODO test complex object in an expression

}
