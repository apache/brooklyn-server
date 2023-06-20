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
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.workflow.steps.variables.TransformVariableWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

public class WorkflowTransformTest extends BrooklynMgmtUnitTestSupport {

    protected void loadTypes() {
        WorkflowBasicTest.addWorkflowStepTypes(mgmt);
    }

    BasicApplication app;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        loadTypes();
        app = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
    }

    @AfterMethod(alwaysRun = true, timeOut = Asserts.THIRTY_SECONDS_TIMEOUT_MS)
    @Override
    public void tearDown() throws Exception {
        if (app!=null) Entities.unmanage(app);
        super.tearDown();
    }

    static Object runWorkflowSteps(Entity entity, String ...steps) {
        return WorkflowBasicTest.runWorkflow(entity, Arrays.asList(steps).stream().map(s -> "- "+Strings.indent(2, s).trim()).collect(Collectors.joining("\n")), "test").getTask(false).get().getUnchecked();
    }
    Object runWorkflowSteps(String ...steps) {
        return runWorkflowSteps(app, steps);
    }

    Object transform(String args) {
        return runWorkflowSteps(app, "transform "+args);
    }


    @Test
    public void testTransformShorthand() {
        Function<String,Map<String,Object>> parse = shorthand -> {
            TransformVariableWorkflowStep s = new TransformVariableWorkflowStep();
            s.populateFromShorthand(shorthand);
            return s.input;
        };

        Asserts.assertEquals(parse.apply("variable x"), MutableMap.of(
                "variable", MutableMap.of("name", "x")));

        Asserts.assertEquals(parse.apply("value ${x}"), MutableMap.of(
                "value", "${x}"));

        Asserts.assertEquals(parse.apply("x"), MutableMap.of(
                "vv_auto", "x"));

        Asserts.assertEquals(parse.apply("x = 1 | foo"), MutableMap.of(
                "vv_auto", "x",
                "value", "1",
                "transform", "foo"));
        Asserts.assertEquals(parse.apply("integer x = 1 | foo"), MutableMap.of(
                "variable", MutableMap.of("type", "integer"),
                "vv_auto", "x",
                "value", "1",
                "transform", "foo"));

        Asserts.assertEquals(parse.apply("integer variable x | foo"), MutableMap.of(
                "variable", MutableMap.of("name", "x", "type", "integer"),
                "transform", "foo"));
        Asserts.assertEquals(parse.apply("integer value ${x} | foo"), MutableMap.of(
                "variable", MutableMap.of("type", "integer"),
                "value", "${x}",
                "transform", "foo"));

        Asserts.assertFailsWith(() -> parse.apply("integer variable x = 99 | foo"),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "Invalid shorthand expression for transform"));
        Asserts.assertFailsWith(() -> parse.apply("integer value x = 99 | foo"),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "Invalid shorthand expression for transform"));
    }


    @Test
    public void testTransformTrim() throws Exception {
        String untrimmed = "Hello, World!   ";
        String trimmed = untrimmed.trim();

        Asserts.assertEquals(
                runWorkflowSteps(
                    "let mystring = '"+untrimmed+"'",
                    "transform mytrimmedstring = ${mystring} | trim",
                    "return ${mytrimmedstring}"),
                trimmed);
    }

    @Test
    public void testTransformRegex() throws Exception {
        Asserts.assertEquals(transform("value 'silly world' | replace regex l. k"), "siky world");
        Asserts.assertEquals(transform("value 'silly world' | replace all regex l. k"), "siky work");
        // with slash
        Asserts.assertEquals(transform("value 'abc/def/ghi' | replace regex 'c/d' XXX"), "abXXXef/ghi");
        // with space
        Asserts.assertEquals(transform("value 'abc def ghi' | replace regex 'c d' XXX"), "abXXXef ghi");

        // greedy
        Asserts.assertEquals(transform("value 'abc def ghi c2d' | replace regex 'c.*d' XXX"), "abXXX");
        Asserts.assertEquals(transform("value 'abc def ghi c2d' | replace all regex 'c.*d' XXX"), "abXXX");
        // non-greedy qualifier
        Asserts.assertEquals(transform("value 'abc def ghi c2d' | replace regex 'c.*?d' XXX"), "abXXXef ghi c2d");
        Asserts.assertEquals(transform("value 'abc def ghi c2d' | replace all regex 'c.*?d' XXX"), "abXXXef ghi XXX");

        Asserts.assertEquals(transform("value 'abc def ghi' | replace regex 'c d' ''"), "abef ghi");
    }

    @Test
    public void testTransformLiteral() throws Exception {
        Asserts.assertEquals(transform("value 'abc def ghi' | replace literal c.*d XXX"), "abc def ghi");
        Asserts.assertEquals(transform("value 'abc.*def ghi c.*d' | replace literal c.*d XXX"), "abXXXef ghi c.*d");
        Asserts.assertEquals(transform("value 'abc.*def ghi c.*d' | replace all literal c.*d XXX"), "abXXXef ghi XXX");
    }

    @Test
    public void testTransformGlob() throws Exception {
        Asserts.assertEquals(transform("value 'abc def ghi' | replace glob c*e XXX"), "abXXXf ghi");
        // glob is greedy, unless all is specified where it is not
        Asserts.assertEquals(transform("value 'abc def ghi c2e' | replace glob c*e XXX"), "abXXX");
        Asserts.assertEquals(transform("value 'abc def ghi c2e' | replace all glob c*e XXX"), "abXXXf ghi XXX");
    }

    @Test
    public void testMapDirect() {
        Asserts.assertEquals(runWorkflowSteps(
                "let map myMap = {a: 1}",
                "let myMap.a = ${myMap.a} + 2",
                "return ${myMap.a}"),
            3);
    }

    @Test
    public void testReturnTransformWithMapYaml() {
        Asserts.assertEquals(WorkflowBasicTest.runWorkflow(app, Strings.lines(
                    "- step: let s",
                    "  value: |",
                    "    bogus",
                    "    - not valid: yaml: here",
                    "    that's: okay",
                    "    ---",
                    "     key: value",
                    "- transform ${s} | yaml | return",
                    "- return should not come here",
                ""), "test").getTask(false).get().getUnchecked(),
                MutableMap.of("key", "value"));
    }

    @Test
    public void testSetVarTransform() {
        Asserts.assertEquals(WorkflowBasicTest.runWorkflow(app, Strings.lines(
                    "- step: let s",
                    "  value: \"key: Value\"",
                    "- transform ${s} | yaml | set y",
                    "- transform y.key2 = ${output.key} | to_upper_case",
                    "- transform ${output.key} | to_lower_case",  // output should still be the yaml map transformed from ${s}
                    "- transform ${output} | set y.key3",   // output passed in here will be 'value' from previous step
                    "- transform true | set y.key4a",
                    "- transform value true | set y.key4b",
                    "- transform boolean value true | set y.key4c",
                    "- return ${y}",
                ""), "test").getTask(true).get().getUnchecked(),
                MutableMap.of("key", "Value", "key2", "VALUE", "key3", "value", "key4a", "true", "key4b", "true", "key4c", true));
    }

    @Test
    public void testValueTransform() {
        Asserts.assertEquals(WorkflowBasicTest.runWorkflow(app, Strings.lines(
                "- let map m = { a: x }",
                "- transform value ${m} | size"
        ), "test").getTask(true).get().getUnchecked(), 1);

//                "- let x = hello",
//                "- transform value ${x} | append world"), "test").getTask(true).get().getUnchecked(), "helloworld");
//                "- let apply_result = hello",
//                "- transform value ${apply_result} | to_upper_case"
//                , "- return ${apply_result}"
//        ), "test").getTask(true).get().getUnchecked(), "HELLO");
    }

    @Test
    public void testResolveTransform() {
        Asserts.assertEquals(runWorkflowSteps(
                    "let a = b",
                    "let b = c",
                    "let x = \"${\" ${a} \"}\"",
                    "transform variable x | resolve_expression",
                    "return ${x}"
                ), "c");

        Asserts.assertEquals(runWorkflowSteps(
                    "let a = b",
                    "let b = c",
                    "transform value {${a}} | prepend $ | resolve_expression | return"
                ), "c");
    }

    @Test
    public void testSliceRemoveAndAppendTransform() {
        // slice list
        Asserts.assertEquals(transform("value ['a','bb','ccc'] | type list | slice 1"), MutableList.of("bb", "ccc"));
        Asserts.assertEquals(transform("value ['a','bb','ccc'] | type list | slice 1 -1"), MutableList.of("bb"));
        Asserts.assertEquals(transform("value ['a','bb','ccc'] | type list | slice -1"), MutableList.of("ccc"));

        // slice string
        Asserts.assertEquals(transform("value abc | slice 1"), "bc");

        // append and prepend list
        Asserts.assertEquals(transform("value ['a','bb'] | type list | append ccc"), MutableList.of("a", "bb", "ccc"));
        Asserts.assertEquals(runWorkflowSteps(
                "transform value ['ccc'] | type list | set c",
                "transform value ['a','bb'] | type list | append ${c}"
            ), MutableList.of("a", "bb", MutableList.of("ccc")));
        Asserts.assertEquals(transform("value ['a','bb'] | type list | prepend ccc"), MutableList.of("ccc", "a", "bb"));

        // append and prepend string
        Asserts.assertEquals(transform("value hello | append _world"), "hello_world");
        Asserts.assertEquals(transform("value hello | append \" world\""), "hello world");

        // remove list
        Asserts.assertEquals(transform("value ['a','bb','ccc'] | type list | remove 1"), MutableList.of("a", "ccc"));

        // remove map
        Asserts.assertEquals(
                runWorkflowSteps(
                    "step: transform\n" +
                    "transform: type map | remove b\n" +
                    "value:\n" +
                    "  a: 1\n" +
                    "  b: 2"),
                MutableMap.of("a", 1));
    }

}
