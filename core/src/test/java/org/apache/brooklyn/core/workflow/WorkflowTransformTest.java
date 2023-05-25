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

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.EntityAsserts;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.workflow.steps.flow.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class WorkflowTransformTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    @Test
    public void testTransformTrim() throws Exception {
        loadTypes();

        String untrimmed = "Hello, World!   ";
        String trimmed = untrimmed.trim();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("let mystring = '"+untrimmed+"'")
                        .append("transform mytrimmedstring = ${mystring} | trim")
                        .append("return ${mytrimmedstring}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, trimmed);
    }

    @Test
    public void testTransformRegex() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'silly world' | replace regex l. k")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "siky world");
    }

    @Test
    public void testTransformAllRegex() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'silly world' | replace all regex l. k")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "siky work");
    }

    @Test
    public void testTransformRegexWithBackslash() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'abc/def/ghi' | replace regex 'c/d' XXX")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "abXXXef/ghi");
    }

    @Test
    public void testTransformRegexWithSpace() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'abc def ghi' | replace regex 'c d' XXX")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "abXXXef ghi");
    }

    @Test
    public void testTransformLiteral() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'abc.*def ghi' | replace literal c.*d XXX")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "abXXXef ghi");
    }

    @Test
    public void testTransformGlob() throws Exception {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("transform x = 'abc.*def ghi' | replace glob c*e XXX")
                        .append("return ${x}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "abXXXf ghi");
    }

    @Test
    public void testMapDirect() {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("p1", MutableMap.of("defaultValue", "p1v")))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("let map myMap = {a: 1}")
                        .append("let myMap.a = 2")
                        .append("return ${myMap.a}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNotNull(result);
        Asserts.assertEquals(result, "2");
    }

    @Test
    public void testReturnTransformWithMapYaml() {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Object result = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "- step: let s",
                "  value: |",
                "    bogus",
                "    - not valid: yaml: here",
                "    that's: okay",
                "    ---",
                "     key: value",
                "- transform s | yaml | return",
                "- return should not come here",
        ""), "test").getTask(false).get().getUnchecked();
        Asserts.assertInstanceOf(result, Map.class);
        Asserts.assertEquals(result, MutableMap.of("key", "value"));
    }

    @Test
    public void testSetVarTransform() {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Object result = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "- step: let s",
                "  value: \"key: Value\"",
                "- transform s | yaml | set y",
                "- transform y.key2 = ${output.key} | to_upper_case",
                "- transform output.key | to_lower_case",  // output should still be the yaml map transformed from ${s}
                "- transform output | set y.key3",   // output passed in here will be 'value' from previous step
                "- transform value true | set y.key4",
                "- transform boolean value true | set y.key5",
                "- return ${y}",
                ""), "test").getTask(false).get().getUnchecked();
        Asserts.assertInstanceOf(result, Map.class);
        Asserts.assertEquals(result, MutableMap.of("key", "Value", "key2", "VALUE", "key3", "value", "key4", "true", "key5", true));
    }

    @Test
    public void testResolveTransform() {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        Object result = WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "- let a = b",
                "- let b = c",
                "- let x = \"${\" ${a} \"}\"",
                "- transform x | resolve_expression | return",
                ""), "test").getTask(false).get().getUnchecked();
        Asserts.assertEquals(result, "c");
    }

    @Test
    public void testSliceAndRemoveTransform() {
        loadTypes();

        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        Function<String,Object> transform = tx -> WorkflowBasicTest.runWorkflow(app, Strings.lines("- "+tx), "test").getTask(false).get().getUnchecked();

        // slice list
        Asserts.assertEquals(transform.apply("transform value ['a','bb','ccc'] | type list | slice 1"), MutableList.of("bb", "ccc"));
        Asserts.assertEquals(transform.apply("transform value ['a','bb','ccc'] | type list | slice 1 -1"), MutableList.of("bb"));
        Asserts.assertEquals(transform.apply("transform value ['a','bb','ccc'] | type list | slice -1"), MutableList.of("ccc"));

        // slice string
        Asserts.assertEquals(transform.apply("transform value abc | slice 1"), "bc");

        // remove list
        Asserts.assertEquals(transform.apply("transform value ['a','bb','ccc'] | type list | remove 1"), MutableList.of("a", "ccc"));

        // remove map
        Asserts.assertEquals(transform.apply("step: transform\n" +
                "  transform: type map | remove b\n" +
                "  value:\n" +
                "    a: 1\n" +
                "    b: 2"), MutableMap.of("a", 1));
    }

}
