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
import org.apache.brooklyn.core.workflow.steps.LogWorkflowStep;
import org.apache.brooklyn.core.workflow.steps.SetSensorWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.test.ClassLogWatcher;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

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
    public void testLetTransformMaps() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map a = { i: 1, z: { a: 1 } }",
                "let map b = { ii: 2, z: { b: 2 } }",
                "transform map x = ${a} ${b} | merge",
                "return ${x}"));
        Asserts.assertEquals(output, MutableMap.of("i", 1, "ii", 2, "z", MutableMap.of("b", 2)));
    }

    @Test
    public void testLetTransformMapsDeep() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let map a = { i: 1, z: { a: 1 } }",
                "let map b = { ii: 2, z: { b: 2 } }",
                "transform map x = ${a} ${b} | merge deep",
                "return ${x}"));
        Asserts.assertEquals(output, MutableMap.of("i", 1, "ii", 2, "z", MutableMap.of("a", 1, "b", 2)));
    }

    @Test
    public void testTransformMergeLists() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1, 2 ]",
                MutableMap.of("step", "let b", "value", MutableList.of(2, 3)),
                "transform list x = ${a} ${b} | merge",
                "return ${x}"));
        Asserts.assertEquals(output, MutableList.of(1, 2, 2, 3));
    }

    @Test
    public void testTransformMergeSets() throws Exception {
        Object o1 = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1, 2 ]",
                MutableMap.of("step", "let b", "value", MutableList.of(2, 3)),
                "transform x = ${a} ${b} | merge set",
                "return ${x}"));
        Asserts.assertEquals(o1, MutableSet.of(1, 2, 3));
        o1 = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1, 2 ]",
                MutableMap.of("step", "let b", "value", MutableList.of(2, 3)),
                "transform set x = ${a} ${b} | merge",
                "return ${x}"));
        Asserts.assertEquals(o1, MutableSet.of(1, 2, 3));
    }

    @Test
    public void testTransformLaxMergeAllowsNull() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let list a = [ 1 ]",
                "transform list x = ${a} ${b} | merge lax",
                "return ${x}"));
        Asserts.assertEquals(output, MutableList.of(1));
    }

    @Test
    public void testSimpleTransforms() throws Exception {
        Function<String,Object> transform = filter -> {
            try {
                return invokeWorkflowStepsWithLogging(MutableList.of(
                        "let list a = [ 1, 2 ]",
                        "transform x = ${a} | "+filter,
                        "return ${x}"));
            } catch (Exception e) {
                throw Exceptions.propagate(e);
            }
        };
        Asserts.assertEquals(transform.apply("min"), 1);
        Asserts.assertEquals(transform.apply("max"), 2);
        Asserts.assertEquals(transform.apply("sum"), 3d);
        Asserts.assertEquals(transform.apply("size"), 2);
        Asserts.assertEquals(transform.apply("average"), 1.5d);
        Asserts.assertEquals(transform.apply("first"), 1);
        Asserts.assertEquals(transform.apply("last"), 2);
    }

    @Test
    public void testTransformMergeLaxTrimRemovesNull() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.of("step", "let a", "value", MutableList.of(2, null)),
                "transform list x = ${a} ${a[1]} | merge lax | trim",
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
    public void testLetQuoteVar() {
        Consumer<String> invoke = input -> { try {
            invokeWorkflowStepsWithLogging(MutableList.of(
                    "let person = Anna",
                    "let note = "+input,
                    "log NOTE: ${note}"));
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } };
        BiFunction<String,String,String> assertLetGives = (input,expected) -> {
            invoke.accept(input);
            String note = Strings.removeFromStart(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith("NOTE:")).findAny().get(), "NOTE: ");
            Asserts.assertEquals(note, expected);
            return note;
        };

        assertLetGives.apply("\"${person}\"", "${person}");
        assertLetGives.apply("\"${person}\" is ${person}", "${person} is Anna");
        assertLetGives.apply("\"${person} is\" ${person}", "${person} is Anna");
        assertLetGives.apply("\"${person}\" is  '${person}'", "${person} is  ${person}");
        assertLetGives.apply("\"${person}\" is '\"${person}\"'", "${person} is \"${person}\"");
        assertLetGives.apply("\"${person}\" is 'person'", "${person} is person");
        Asserts.assertFails(() -> invoke.accept("\"${person}\" is  ''person '"));
        assertLetGives.apply("\"${person} is  person \"", "${person} is  person ");
        Asserts.assertFails(() -> invoke.accept("\"\"${person}\" is  person \""));
        assertLetGives.apply("\"'${person}' is  person \"", "'${person}' is  person ");
        assertLetGives.apply("\"\\\"${person}\\\" is  person \"", "\"${person}\" is  person ");
        Asserts.assertFails(() -> invoke.accept("\"\\\"${person}\\\" is  \"person \""));
        assertLetGives.apply("\"\\\"${person}\" is  \"person \"", "\"${person} is  person ");
    }

    @Test
    public void testSetSensorQuoteVar() {
        Consumer<String> invoke = input -> { try {
            invokeWorkflowStepsWithLogging(MutableList.of(
                    "let person = Anna",
                    "set-sensor note = "+input,
                    "let note = ${entity.sensor.note}",
                    "log NOTE: ${note}"));
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } };
        BiFunction<String,String,String> assertSetSensorGives = (input,expected) -> {
            invoke.accept(input);
            String note = Strings.removeFromStart(lastLogWatcher.getMessages().stream().filter(s -> s.startsWith("NOTE:")).findAny().get(), "NOTE: ");
            Asserts.assertEquals(note, expected);
            return note;
        };

        assertSetSensorGives.apply("\"${person}\" is ${person}", "\"Anna\" is Anna");
        assertSetSensorGives.apply("\"${person}\" is  '${person}'", "\"Anna\" is  'Anna'");
        assertSetSensorGives.apply("\"${person} is\" ${person}", "\"Anna is\" Anna");
        assertSetSensorGives.apply("\"${person}\" is ''${person}''", "\"Anna\" is ''Anna''");
        assertSetSensorGives.apply("\"${person}\" is 'person'", "\"Anna\" is 'person'");
        assertSetSensorGives.apply("\"${person}\" is  ''person '", "\"Anna\" is  ''person '");  // doesn't fail because quotes preserved
        assertSetSensorGives.apply("\"${person} is  person \"", "Anna is  person ");
        Asserts.assertFails(() -> invoke.accept("\"\"${person}\" is  person \""));
        assertSetSensorGives.apply("\"'${person}' is  person \"", "'Anna' is  person ");
        assertSetSensorGives.apply("\"\\\"${person}\\\" is  person \"", "\"Anna\" is  person ");
        Asserts.assertFails(() -> invoke.accept("\"\\\"${person}\\\" is  \"person \""));
        assertSetSensorGives.apply("\"\\\"${person}\" is  \"person \"", "\"\\\"Anna\" is  \"person \"");
    }

    @Test
    public void testTransformTrimString() throws Exception {
        Object output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let person_spaces = \" Anna \"",
                "let string person = ${person_spaces}",
                "transform person_tidied = ${person_spaces} | trim",
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
                MutableMap.<String,Object>of("step", "let x1", "value", expression),
                "let "+prefix+" x2 = ${x1}", "return ${x2}"));
    }

    private Object transform(String filter, Object expression) throws Exception {
        return invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.<String,Object>of("step", "let x1", "value", expression),
                "transform x2 = ${x1} | "+filter, "return ${x2}"));
    }

    private void checkTransform(String prefix, Object expression, Object expectedResult) throws Exception {
        Object out = transform(prefix, expression);
        Asserts.assertEquals(out, expectedResult);
    }

    private void checkTransformBidi(String prefix, Object expression, String expectedResult) throws Exception {
        Object out = transform(prefix, expression);

        Object backIn = BeanWithTypeUtils.newYamlMapper(null, false, null, false).readValue((String) out, Object.class);
        Asserts.assertEquals(backIn, expression);

        Asserts.assertEquals(out, expectedResult, "YAML encoding (for whitespace) different to expected; interesting, but not a real fault");
    }

    static String q(String s) {
        return StringEscapes.JavaStringEscapes.wrapJavaString(s);
    }

    @Test
    public void testTransformYaml() throws Exception {
        // null not permitted as return type; would be nice to support but not vital
//        checkLetBidi("yaml string", null, "null");

        checkTransform("yaml", 2, 2);
        checkTransform("yaml", "2", 2);
        checkTransform("yaml", q("2"), 2);       //unwrapped once by shorthand, again by step
        checkTransform("yaml", q(q("2")), "2");  //finally we get the string

        checkTransform("yaml parse", 2, 2);
        checkTransform("yaml parse", "2", 2);
        checkTransform("yaml parse", q("2"), 2);
        checkTransform("yaml parse", q(q("2")), "2");

        checkTransform("yaml string", 2, "2");
        checkTransform("yaml string", "2", "2");
        checkTransform("yaml string", q("2"), "2");
        checkTransform("yaml string", q(q("2")), "\"2\"");

        checkTransform("yaml encode", 2, "2");
        checkTransform("yaml encode", "2", "\"2\"");
        checkTransform("yaml encode", q("2"), "\"2\"");
        // for these especially the exact yaml is not significant, but included for interest
        checkTransform("yaml encode", q(q("2")), "'\"2\"'");

        checkTransformBidi("yaml encode", " ", "' '");
        checkTransformBidi("yaml encode", "\n", "|2+\n\n");  // 2 is totally unnecessary but returned
        checkTransformBidi("yaml encode", " \n ", "\" \\n \"");   // double quotes used if spaces and newlines significant

        checkTransform("yaml", "ignore \n---\n x: 1 \n", MutableMap.of("x", 1));
        checkTransform("yaml parse", "ignore \n---\n x: 1 \n y: 2", MutableMap.of("x", 1, "y", 2));
        checkTransform("yaml string", "ignore \n---\n x: 1 \n", " x: 1 \n");
        checkTransformBidi("yaml encode", "ignore \n---\n x: 1 \n", "\"ignore \\n---\\n x: 1 \\n\"");
        checkTransform("trim | yaml encode", "ignore \n---\n x: 1 \n", "\"ignore \\n---\\n x: 1\"");
        checkTransform("yaml string | trim | yaml encode", "ignore \n---\n x: 1 \n", "\"x: 1\"");
        checkTransform("json string | trim", "ignore \n---\n x: 1 \n", "ignore \n---\n x: 1");

        checkTransformBidi("yaml string", MutableMap.of("a", 1), "a: 1");
        checkTransformBidi("yaml string", MutableMap.of("a", MutableList.of(null), "x", "1"), "a:\n- null\nx: \"1\"");
        checkTransform("yaml", MutableMap.of("x", 1), MutableMap.of("x", 1));
    }

    @Test
    public void testTransformJson() throws Exception {
        checkTransform("json", 2, 2);
        checkTransform("json", "2", 2);
        checkTransform("json", q("2"), 2);       //unwrapped once by shorthand, again by step
        checkTransform("json", q(q("2")), "2");  //finally we get the string

        checkTransform("json parse", 2, 2);
        checkTransform("json parse", "2", 2);
        checkTransform("json parse", q("2"), 2);
        checkTransform("json parse", q(q("2")), "2");

        checkTransform("json string", 2, "2");
        checkTransform("json string", "2", "2");
        checkTransform("json string", q("2"), "2");
        checkTransform("json string", q(q("2")), "\"2\"");

        checkTransform("json encode", 2, "2");
        checkTransform("json encode", "2", "\"2\"");
        checkTransform("json encode", q("2"), "\"2\"");
        // for these especially the exact yaml is not significant, but included for interest
        checkTransform("json encode", q(q("2")), "\"\\\"2\\\"\"");

        checkTransform("json string", " ", " ");
        checkTransform("json string", "\n", "\n");
        checkTransform("json string", " \n ", " \n ");

        checkTransformBidi("json encode", " ", "\" \"");
        checkTransformBidi("json encode", "\n", "\"\\n\"");
        checkTransformBidi("json encode", " \n ", "\" \\n \"");

        checkTransformBidi("json string", MutableMap.of("a", 1), "{\"a\":1}");
        checkTransform("json encode", MutableMap.of("a", 1), "{\"a\":1}");
        checkTransform("json string | json encode", MutableMap.of("a", 1), q("{\"a\":1}"));
        checkTransformBidi("json string", MutableMap.of("a", MutableList.of(null), "x", "1"), "{\"a\":[null],\"x\":\"1\"}");
        checkTransform("json", MutableMap.of("x", 1), MutableMap.of("x", 1));
    }

    @Test
    public void testTransformBash() throws Exception {
        checkTransform("bash", 2, q("2"));
        checkTransform("bash encode", 2, q("2"));
        checkTransform("bash", "2", q("2"));
        checkTransform("bash", q("2"), q("2"));       //unwrapped once by shorthand, again by step
        checkTransform("bash", q(q("2")), q("\"2\""));  //finally we get the string

        checkTransform("bash", "hello(n $o!)", "\"hello(n \\$o\"'!'\")\"");
        checkTransform("bash", "two\nlines", "\"two\nlines\"");
        checkTransform("bash", MutableMap.of("x", 1), q("{\"x\":1}"));
    }

    public static class MockObject {
        String id;
        String name;
    }

    @Test
    public void testTransformYamlCoercion() throws Exception {
        addBeanWithType("mock-object", "1", "type: " + MockObject.class.getName());
        Object result = invokeWorkflowStepsWithLogging(MutableList.of(
                MutableMap.<String, Object>of("step", "let x1", "value",
                        "ignore\n" +
                        "---\n" +
                        "  id: x\n" +
                        "  name: foo"),
                "transform mock-object result = ${x1} | yaml",
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
                "transform result.y = ${x} | yaml string",
                "transform result.j = ${x} | json string",           // will give the stringified map
                "transform result.j2 = ${result.j} | json string",   // no change to the string
                "transform result.e = ${x} | json encode",
                "transform result.es = ${x} | json string | json encode",
                "transform map result.m0 = ${x} | json",
                "transform map result.m1 = ${result.j} | json",
                "return ${result}"));
        String s = "{\"i\":[1,\"A\"]}";
        Object m = MutableMap.of("i", MutableList.of(1, "A"));
        Asserts.assertEquals(output, MutableMap.of("y", "i:\n- 1\n- A\n", "j", s,
                "j2", s, "e", s, "es", q(s),
                "m0", m, "m1", m));

        String s0 = "{ i: [ 1, 'A' ] }";
        output = invokeWorkflowStepsWithLogging(MutableList.of(
                "let string x = "+ q(s0),  // wrap to preserve the make the second thing in list be 'A' (including single quotes)
                "let map result = {}",
                "transform result.mp = ${x} | yaml",           // parsing a string gives a map
                "transform result.mp2 = ${result.mp} | yaml parse",  // parsing a map has no effect
                "transform result.mo = ${result.mp} | json",         // json referencing a map has no discernable effect (though it serializes and deserializes)
                "transform result.so = ${x} | json string",           // pass through
                "transform result.ss0 = ${result.so} | json encode",  // encoding a string gives the double-stringified map, you can parse to get the string back
                "transform result.ss1 = ${result.mp} | json encode", // stringifies
                "transform result.ss2 = ${result.mp} | json string | json encode", // specifying string on non-string gives single encoding (again)
                "return ${result}"));
        Asserts.assertEquals(output, MutableMap.<String,Object>of("mp", m, "mp2", m, "mo", m,
                "so", s0, "ss0", q(s0), "ss1", s, "ss2", q(s)));
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

    @Test
    public void testSetSensorAtomicRequire() {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "init")
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        MutableMap.of(
                                "step", "set-sensor integer x = 0",
                                "require", MutableMap.of("when", "absent")
                        )
                )));
        eff.apply((EntityLocal)app);

        eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.STEPS, MutableList.of(
                        "let x = ${entity.sensor.x} ?? 0",
                        "let x2 = ${x} + 1",
                        MutableMap.of(
                                "step", "set-sensor x = ${x2}",
                                "require", "${x}"
                        )
                )));
        eff.apply((EntityLocal)app);

        // fails with Absent exception if value required
        Asserts.assertFailsWith(() -> app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null).getUnchecked(),
                e -> {
                    Asserts.expectedFailureContains(e, "x", "non-absent requirement");
                    SetSensorWorkflowStep.SensorRequirementFailedAbsent e0 = (SetSensorWorkflowStep.SensorRequirementFailedAbsent) Exceptions.getFirstThrowableOfType(e, SetSensorWorkflowStep.SensorRequirementFailed.class);
                    Asserts.assertNull(e0.getSensorValue());
                    return true;
                });

        // works if we require absent, and initializes it
        app.invoke(app.getEntityType().getEffectorByName("init").get(), null).getUnchecked();

        // fails with normal exception if we require absent when it isn't
        Asserts.assertFailsWith(() -> app.invoke(app.getEntityType().getEffectorByName("init").get(), null).getUnchecked(),
            e -> {
                Asserts.expectedFailureContains(e, "x");
                // value not in output, not any mention of absence
                Asserts.expectedFailureDoesNotContain(e, "0", "absent");
                // but value is in field
                SetSensorWorkflowStep.SensorRequirementFailed e0 = Exceptions.getFirstThrowableOfType(e, SetSensorWorkflowStep.SensorRequirementFailed.class);
                Asserts.assertEquals(e0.getSensorValue(), 0);
                return true;
            });

        // subsequently works at least once, but probably gets some errors due to concurrency
        List<Task<?>> tasks = MutableList.of();
        int NUM = 100;
        for (int i=0; i<NUM; i++) tasks.add(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), null));
        long numErrors = tasks.stream().filter(t -> {
            t.blockUntilEnded();
            return t.isError();
        }).count();
        Integer numSuccess = app.sensors().get(Sensors.newIntegerSensor("x"));
        if (numSuccess==0) tasks.iterator().next().getUnchecked();  // show failure if they all failed
        Asserts.assertEquals(numErrors + numSuccess, NUM, "Tally mismatch, had "+numErrors+" errors when sensor showed "+numSuccess);
    }

    @Test
    public void testSwitch() throws JsonProcessingException {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("x", null))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append(MutableMap.of("step", "switch ${x}", "cases",
                                MutableList.of(
                                        MutableMap.of("condition", "A", "step", "return Aaa"),
                                        MutableMap.of("condition", "B", "step", "return Bbb"),
                                        "return Zzz")))
                )
        );
        eff.apply((EntityLocal) app);

        Asserts.assertEquals(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("x", "A")).getUnchecked(), "Aaa");
        Asserts.assertEquals(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("x", "B")).getUnchecked(), "Bbb");
        Asserts.assertEquals(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("x", "C")).getUnchecked(), "Zzz");
    }

    @Test
    public void testSwitchNoValueSingleDefaultCase() throws JsonProcessingException {
        loadTypes();
        BasicApplication app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

        WorkflowEffector eff = new WorkflowEffector(ConfigBag.newInstance()
                .configure(WorkflowEffector.EFFECTOR_NAME, "myWorkflow")
                .configure(WorkflowEffector.EFFECTOR_PARAMETER_DEFS, MutableMap.of("x", null))
                .configure(WorkflowEffector.STEPS, MutableList.<Object>of()
                        .append(MutableMap.of("step", "switch", "cases",
                                MutableList.of(
                                        "return Zzz")))
                )
        );
        eff.apply((EntityLocal) app);

        Asserts.assertEquals(app.invoke(app.getEntityType().getEffectorByName("myWorkflow").get(), MutableMap.of("x", "A")).getUnchecked(), "Zzz");
    }

}
