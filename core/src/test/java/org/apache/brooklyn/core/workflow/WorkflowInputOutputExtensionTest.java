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
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
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
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append("set-sensor p1 = ${p1}")
                )
        );
        eff.apply((EntityLocal)app);

        Task<?> invocation = app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null);
        Object result = invocation.getUnchecked();
        Asserts.assertNull(result);

        EntityAsserts.assertAttributeEquals(app, Sensors.newSensor(Object.class, "p1"), "p1v");
    }

    @Test
    public void testMapOutputAndInputFromLastStep() {
        doTestMapOutputAndInput(cfg -> {
            List<Object> step = cfg.get(WorkflowEffector.STEPS);
            step.add("let map result = { c: ${c}, d: ${d}, e: ${e} }");
            cfg.put(WorkflowEffector.STEPS, step);
            cfg.put(WorkflowEffector.OUTPUT, "${result}");
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
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append(MutableMap.of("s", "let map v = { x: { y: a }, z: b }", "id", "s1", "output", "${v}"))
                        .append("let c = ${x.y}")  // reference output of previous step
                        .append("let d = ${v.z}")  // reference workflow var
                        .append("let e = ${workflow.step.s1.output.z}")  // reference explicit output step
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

    public RegisteredType addBeanWithType(String typeName, String version, String plan) {
        loadTypes();
        return BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, typeName, version,
                new BasicTypeImplementationPlan(BeanWithTypePlanTransformer.FORMAT, plan));
    }

    ClassLogWatcher lastLogWatcher;

    Object invokeWorkflowStepsWithLogging(List<Object> steps) throws Exception {
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
    public void testNestedWorkflowBasic() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("type", "workflow",
                        "steps", MutableList.of("return done"))));
        Asserts.assertEquals(output, "done");
    }

    @Test
    public void testNestedWorkflowParametersForbiddenWhenUsedDirectly() throws Exception {
        Asserts.assertFailsWith(() -> invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("type", "workflow",
                        "parameters", MutableMap.of(),
                        "steps", MutableList.of("return done")))),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "parameters"));
    }

    @Test
    public void testExtendingAStepWhichWorksButIsMessyAroundParameters() throws Exception {
        /*
         * extending any step type is supported, but discouraged because of confusion and no parameter definitions;
         * the preferred way is to use the method in the following test
         */
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: log",
                "message: hi ${name}",
                "input:",
                "  name: you"
        ));

        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");

        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi")));
        assertLogStepMessages("hi you");
    }

    @Test
    public void testDefiningCustomWorkflowStep() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}"
        ));
        invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");

        Asserts.assertFailsWith(() -> invokeWorkflowStepsWithLogging(MutableList.of(
                        MutableMap.of("type", "log-hi",
                                "steps", MutableList.of("return not allowed to override")))),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "steps"));
    }

    @Test
    public void testDefiningCustomWorkflowStepWithShorthand() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "shorthand: ${name}",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}"
        ));
        invokeWorkflowStepsWithLogging(MutableList.of("log-hi bob"));
        assertLogStepMessages("hi bob");
    }

    @Test
    public void testDefiningCustomWorkflowStepWithOutput() throws Exception {
        addBeanWithType("log-hi", "1", Strings.lines(
                "type: workflow",
                "parameters:",
                "  name: {}",
                "steps:",
                "  - log hi ${name}",
                "output:",
                "  message: hi ${name}"
        ));
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"))));
        assertLogStepMessages("hi bob");
        Asserts.assertEquals(output, MutableMap.of("message", "hi bob"));

        // output can be overridden
        output = invokeWorkflowStepsWithLogging(MutableList.of(MutableMap.of("type", "log-hi", "input", MutableMap.of("name", "bob"), "output", "I said ${message}")));
        assertLogStepMessages("hi bob");
        Asserts.assertEquals(output, "I said hi bob");
    }

    // test complex object in an expression
    @Test
    public void testMapOutputAsComplexFreemarkerVar() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map my_map = { x: 1 }",
                MutableMap.of("s", "let my_map_copy = ${my_map}", "output", "${my_map_copy}")));
        Asserts.assertEquals(output, MutableMap.of("x", 1));
    }

    @Test
    public void testLetNullishRhsThenLhs() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let integer x = ${x} ?? 1",
                MutableMap.of("s", "let x = ${x} ?? 2", "output", "${x}")));
        Asserts.assertEquals(output, 1);
    }

    @Test
    public void testLetMergeMaps() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map a = { i: 1, z: { a: 1 } }",
                "let map b = { ii: 2, z: { b: 2 } }",
                "let merge map x = ${a} ${b}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableMap.of("i", 1, "ii", 2, "z", MutableMap.of("b", 2)));
    }

    @Test
    public void testLetMergeMapsDeep() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map a = { i: 1, z: { a: 1 } }",
                "let map b = { ii: 2, z: { b: 2 } }",
                "let merge deep map x = ${a} ${b}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableMap.of("i", 1, "ii", 2, "z", MutableMap.of("a", 1, "b", 2)));
    }

    @Test
    public void testLetMergeLists() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1, 2 ]",
                MutableMap.of("step", "let b", "value", MutableList.of(2, 3)),
                "let merge list x = ${a} ${b}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableList.of(1, 2, 2, 3));
    }

    @Test
    public void testLetMergeSets() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1, 2 ]",
                MutableMap.of("step", "let b", "value", MutableList.of(2, 3)),
                "let merge set x = ${a} ${b}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableSet.of(1, 2, 3));
    }

    @Test
    public void testLetTrimMergeAllowsNull() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1 ]",
                "let merge trim list x = ${a} ${b}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableList.of(1));
    }

    @Test
    public void testLetMergeCleanRemovesNull() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("step", "let a", "value", MutableList.of(2, null)),
                "let merge trim list x = ${a}",
                "return ${x}"));
        Asserts.assertEquals(output, MutableList.of(2));
    }

    @Test
    public void testLetKey() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map a = { i: 1 }",
                "let integer a.ii = 2",
                "return ${a}"));
        Asserts.assertEquals(output, MutableMap.of("i", 1, "ii", 2));
    }

    @Test
    public void testLetMathOps() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let double x = 99 / 0 ?? ${x} ?? 4 * 4 - 5 * 3",
                "let integer x = ${x} * 8 / 2.0",
                MutableMap.of("s", "let x = ${x} / 5", "output", "${x}")));
        Asserts.assertEquals(output, 0.8);
    }

    @Test
    public void testLetMathMode() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("s", "let x = 11 % 7 % 4", "output", "${x}")));
        // should get 0, ie (11 % 7) % 4, not 11 % (7 % 4).
        Asserts.assertEquals(output, 0);
    }

    @Test
    public void testLetQuoteVar() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let person = Anna",
                "let note = \"${person}\" is ${person}",
                "log NOTE: ${note}"));
        Asserts.assertEquals(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith("NOTE:")).findAny().get(), "NOTE: ${person} is Anna");
    }

    @Test
    public void testLetTrimString() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let person_spaces = \" Anna \"",
                "let string person = ${person_spaces}",
                "let trim person_tidied = ${person_spaces}",
                "log PERSON: ${person}",
                "log PERSON_TIDIED: ${person_tidied}"));
        Asserts.assertEquals(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith("PERSON:")).findAny().get(), "PERSON:  Anna ");
        Asserts.assertEquals(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith("PERSON_TIDIED:")).findAny().get(), "PERSON_TIDIED: Anna");
    }

    private void assertLogMessageFor(String prefix, String value) {
        Asserts.assertEquals(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith(prefix+": ")).findAny().get(),
                prefix+": "+value);
    }

    private Object let(String prefix, Object expression) throws Exception {
        return invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.<String,Object>of("step", "let x1", "value",
                        expression instanceof String
                                ? StringEscapes.JavaStringEscapes.wrapJavaString((String)expression)
                                : expression instanceof Maybe
                                ? ((Maybe)expression).get()
                                : expression),
                "let "+prefix+" x2 = ${x1}", "return ${x2}"));
    }

    private void checkLet(String prefix, Object expression, Object expectedResult) throws Exception {
        Object out = let(prefix, expression);
        Asserts.assertEquals(out, expectedResult);
    }

    private void checkLetBidi(String prefix, Object expression, String expectedResult) throws Exception {
        Object out = let(prefix, expression);

        Object backIn = BeanWithTypeUtils.newYamlMapper(null, false, null, false).readValue((String) out, Object.class);
        Asserts.assertEquals(backIn, expression);

        Asserts.assertEquals(out, expectedResult, "YAML encoding (for whitespace) different to expected; interesting, but not a real fault");
    }

    @Test
    public void testLetYaml() throws Exception {
        // null not permitted as return type; would be nice to support but not vital
//        checkLetBidi("yaml string", null, "null");

        checkLet("yaml", 2, 2);
        checkLet("yaml", Maybe.of("2"), 2);
        checkLet("yaml", "2", 2);  //wrapped by our test, unwrapped once by shorthand, again by step
        checkLet("yaml", "\"2\"", "2");  //wrapped by our test, unwrapped once by shorthand, again by step
        checkLetBidi("yaml string", 2, "2");
        checkLetBidi("yaml string", "2", "\"2\"");
        // for these especially the exact yaml is not significant, but included for interest
        checkLetBidi("yaml string", "\"2\"", "'\"2\"'");
        checkLetBidi("yaml string", " ", "' '");
        checkLetBidi("yaml string", "\n", "|2+\n\n");  // 2 is totally unnecessary but returned
        checkLetBidi("yaml string", " \n ", "\" \\n \"");   // double quotes used if spaces and newlines significant

        checkLet("trim yaml", "ignore \n---\n x: 1 \n", MutableMap.of("x", 1));
        checkLetBidi("yaml string", "ignore \n---\n x: 1 \n", "\"ignore \\n---\\n x: 1 \\n\"");
        checkLet("trim yaml string", "ignore \n---\n x: 1 \n", "\"ignore \\n---\\n x: 1\"");
        checkLet("trim string", "ignore \n---\n x: 1 \n", "ignore \n---\n x: 1");

        checkLetBidi("yaml string", MutableMap.of("a", 1), "a: 1");
        checkLetBidi("yaml string", MutableMap.of("a", MutableList.of(null), "x", "1"), "a:\n- null\nx: \"1\"");
        checkLet("yaml", MutableMap.of("x", 1), MutableMap.of("x", 1));
    }

    @Test
    public void testLetJson() throws Exception {
        checkLet("json", 2, 2);
        checkLet("json", Maybe.of("2"), 2);
        checkLet("json", "2", 2);
        checkLet("json", "\"2\"", "2");
        checkLetBidi("json string", 2, "2");
        checkLetBidi("json string", "2", "\"2\"");
        checkLetBidi("json string", "\"2\"", "\"\\\"2\\\"\"");
        checkLetBidi("json string", " ", "\" \"");
        checkLetBidi("json string", "\n", "\"\\n\"");
        checkLetBidi("json string", " \n ", "\" \\n \"");

        checkLetBidi("json string", MutableMap.of("a", 1), "{\"a\":1}");
        checkLetBidi("json string", MutableMap.of("a", MutableList.of(null), "x", "1"), "{\"a\":[null],\"x\":\"1\"}");
        checkLet("json", MutableMap.of("x", 1), MutableMap.of("x", 1));
    }

    public static class MockObject {
        String id;
        String name;
    }

    @Test
    public void testLetYamlCoercion() throws Exception {
        addBeanWithType("mock-object", "1", "type: " + MockObject.class.getName());
        Object result = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.<String, Object>of("step", "let x1", "value",
                        "ignore\n" +
                        "---\n" +
                        "  id: x\n" +
                        "  name: foo"),
                "let yaml mock-object result = ${x1}",
                "return ${result}"));
        Asserts.assertThat(result, r -> r instanceof MockObject);
        Asserts.assertEquals(((MockObject)result).id, "x");
        Asserts.assertEquals(((MockObject)result).name, "foo");
    }

    @Test
    public void testOutputYamlAndJson() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map x = { i: [ 1, 'A' ] }",
                "let map result = {}",
                "let yaml string result.y = ${x}",
                "let json string result.j = ${x}",
                "return ${result}"));
        Asserts.assertEquals(output, MutableMap.of("y", "i:\n- 1\n- A\n", "j", "{\"i\":[1,\"A\"]}"));
    }

    // test complex object in an expression
    @Test
    public void testAccessLocalOutput() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("id" , "l1",
                        "step", "let x = ${x} + ${workflow.step.l1.output.prev} ?? 1",
                        "output", MutableMap.of("prev", "${x}")),
                "log ${x}",
                MutableMap.of(
                        "step", "let x = ${x} + 2 * ${prev}",
                        "output", MutableMap.of("prev", "${x}")),
                "log ${x}",
                MutableMap.of("step" , "goto l1",
                        "condition", MutableMap.of("target", "${x}", "less-than", 10)),
                "return ${workflow.step.l1.output.prev}"
        ));
        Asserts.assertEquals(output, 4);
        Asserts.assertEquals(lastLogWatcher.getMessages(), MutableList.of(
            "1", "3",  "4", "12"
        ));
    }

    @Test
    public void testLoadData() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "load x = classpath://hello-world.txt",
                "return ${x}"));
        Asserts.assertStringContains((String)output, "The file hello-world.war contains its source code.");
    }

}
