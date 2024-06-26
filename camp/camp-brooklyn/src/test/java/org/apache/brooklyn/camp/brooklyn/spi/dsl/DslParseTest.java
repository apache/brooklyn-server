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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.DslParser;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.FunctionWithArgs;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.PropertyAccess;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.parse.QuotedString;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Test
public class DslParseTest {

    public void testParseString() {
        assertEquals(new DslParser("\"hello world\"").parse(), new QuotedString(JavaStringEscapes.wrapJavaString("hello world")));
    }

    public void testParseNoArgFunction() {
        Object fx = new DslParser("f()").parse();
        fx = Iterables.getOnlyElement( (List<?>)fx );
        assertEquals( ((FunctionWithArgs)fx).getFunction(), "f" );
        assertEquals( ((FunctionWithArgs)fx).getArgs(), ImmutableList.of());
    }
    
    public void testParseOneArgFunction() {
        Object fx = new DslParser("f(\"x\")").parse();
        fx = Iterables.getOnlyElement( (List<?>)fx );
        assertEquals( ((FunctionWithArgs)fx).getFunction(), "f" );
        assertEquals( ((FunctionWithArgs)fx).getArgs(), Arrays.asList(new QuotedString("\"x\"")) );
    }
    
    public void testParseMultiArgMultiTypeFunction() {
        Object fx = new DslParser("f(\"x\", \"y\")").parse();
        fx = Iterables.getOnlyElement( (List<?>)fx );
        assertEquals( ((FunctionWithArgs)fx).getFunction(), "f" );
        assertEquals( ((FunctionWithArgs)fx).getArgs(), ImmutableList.of(new QuotedString("\"x\""), new QuotedString("\"y\"")));

        fx = new DslParser("f(\"x\", 1)").parse();
        fx = Iterables.getOnlyElement( (List<?>)fx );
        assertEquals( ((FunctionWithArgs)fx).getFunction(), "f" );
        assertEquals( ((FunctionWithArgs)fx).getArgs(), ImmutableList.of(new QuotedString("\"x\""), new PropertyAccess("1")));

        fx = new DslParser("$brooklyn:formatString(\"%s-%s\", parent().attributeWhenReady(\"host.address\"), $brooklyn:attributeWhenReady(\"host.address\"))").parse();
        fx = Iterables.getOnlyElement( (List<?>)fx );
        Asserts.assertInstanceOf(fx, FunctionWithArgs.class);
        Asserts.assertPasses((FunctionWithArgs)fx, fx0 -> Asserts.assertSize(fx0.getArgs(), 3));
    }

    
    public void testParseFunctionChain() {
        Object fx = new DslParser("f(\"x\").g()").parse();
        assertTrue(((List<?>)fx).size() == 2, ""+fx);
        Object fx1 = ((List<?>)fx).get(0);
        Object fx2 = ((List<?>)fx).get(1);
        assertEquals( ((FunctionWithArgs)fx1).getFunction(), "f" );
        assertEquals( ((FunctionWithArgs)fx1).getArgs(), ImmutableList.of(new QuotedString("\"x\"")) );
        assertEquals( ((FunctionWithArgs)fx2).getFunction(), "g" );
        assertTrue( ((FunctionWithArgs)fx2).getArgs().isEmpty() );
    }

    public void testParseAttributeProperty() {
        Object fx = new DslParser("$brooklyn:attributeWhenReady(\"input_credential\")[\"user\"]").parse();
        assertEquals(((List<?>) fx).size(), 2, "" + fx);

        Object fx1 = ((List<?>)fx).get(0);
        assertEquals( ((FunctionWithArgs)fx1).getFunction(), "$brooklyn:attributeWhenReady" );
        assertEquals( ((FunctionWithArgs)fx1).getArgs(), ImmutableList.of(new QuotedString("\"input_credential\"")) );

        Object fx2 = ((List<?>)fx).get(1);
        assertEquals( ((PropertyAccess)fx2).getSelector(), "user" );
    }

    public void testParseAttributePropertyOnComponent() {
        Object fx = new DslParser("$brooklyn:component(\"credential-node\").attributeWhenReady(\"input_credential\")[\"token\"]").parse();
        assertEquals(((List<?>) fx).size(), 3, "" + fx);

        Object fx1 = ((List<?>)fx).get(0);
        assertEquals( ((FunctionWithArgs)fx1).getFunction(), "$brooklyn:component" );
        assertEquals( ((FunctionWithArgs)fx1).getArgs(), ImmutableList.of(new QuotedString("\"credential-node\"")) );

        Object fx2 = ((List<?>)fx).get(1);
        assertEquals( ((FunctionWithArgs)fx2).getFunction(), "attributeWhenReady" );
        assertEquals( ((FunctionWithArgs)fx2).getArgs(), ImmutableList.of(new QuotedString("\"input_credential\"")) );

        Object fx3 = ((List<?>)fx).get(2);
        assertEquals( ((PropertyAccess)fx3).getSelector(), "token" );
    }

    public void testParseConfigProperty() {
        Object fx = new DslParser("$brooklyn:config(\"user_credentials\")[\"user\"]").parse();
        assertEquals(((List<?>) fx).size(), 2, "" + fx);

        Object fx1 = ((List<?>)fx).get(0);
        assertEquals( ((FunctionWithArgs)fx1).getFunction(), "$brooklyn:config" );
        assertEquals( ((FunctionWithArgs)fx1).getArgs(), ImmutableList.of(new QuotedString("\"user_credentials\"")) );

        Object fx2 = ((List<?>)fx).get(1);
        assertEquals( ((PropertyAccess)fx2).getSelector(), "user" );
    }

    @Test
    public void testParseObjectProperty() {
        Object fx = new DslParser("$brooklyn:object(\"[brooklyn.obj.TestObject,host]\").config(\"ips_container\")[\"ips\"][0]").parse();
        assertEquals(((List<?>) fx).size(), 4, "" + fx);

        Object fx1 = ((List<?>)fx).get(0);
        assertEquals( ((FunctionWithArgs)fx1).getFunction(), "$brooklyn:object" );
        assertEquals( ((FunctionWithArgs)fx1).getArgs(), ImmutableList.of(new QuotedString("\"[brooklyn.obj.TestObject,host]\"")) );

        Object fx2 = ((List<?>)fx).get(1);
        assertEquals( ((FunctionWithArgs)fx2).getFunction(), "config" );
        assertEquals( ((FunctionWithArgs)fx2).getArgs(), ImmutableList.of(new QuotedString("\"ips_container\"")) );

        Object fx3 = ((List<?>)fx).get(2);
        assertEquals( ((PropertyAccess)fx3).getSelector(), "ips" );

        Object fx4 = ((List<?>)fx).get(3);
        assertEquals( ((PropertyAccess)fx4).getSelector(), "0" );
    }

    @Test
    public void testParseObjectAttribute() {
        List fx = (List) new DslParser("$brooklyn:object(\"[brooklyn.obj.TestObject,host]\").attributeWhenReady(\"ips_container\")[\"ips\"][0]").parse();
        assertEquals(fx.size(), 4, "" + fx);

        assertEquals( ((FunctionWithArgs)fx.get(0)).getFunction(), "$brooklyn:object" );
        assertEquals( ((FunctionWithArgs)fx.get(0)).getArgs(), ImmutableList.of(new QuotedString("\"[brooklyn.obj.TestObject,host]\"")) );

        assertEquals( ((FunctionWithArgs)fx.get(1)).getFunction(), "attributeWhenReady" );
        assertEquals( ((FunctionWithArgs)fx.get(1)).getArgs(), ImmutableList.of(new QuotedString("\"ips_container\"")) );

        assertEquals( ((PropertyAccess)fx.get(2)).getSelector(), "ips" );
        assertEquals( ((PropertyAccess)fx.get(3)).getSelector(), "0" );

        fx = (List) new DslParser("$brooklyn:object(\"[brooklyn.obj.TestObject,host]\").attributeWhenReady(\"ips_container\").ips[0]").parse();
        assertEquals(fx.size(), 4, "" + fx);
        assertEquals( ((PropertyAccess)fx.get(2)).getSelector(), "ips" );

        fx = (List) new DslParser("$brooklyn:object(\"[brooklyn.obj.TestObject,host]\").attributeWhenReady(\"ips_container\").a.b[0].c.d[1]").parse();
        assertEquals(fx.size(), 8, "" + fx);
        assertEquals( ((PropertyAccess)fx.get(3)).getSelector(), "b" );
        assertEquals( ((PropertyAccess)fx.get(4)).getSelector(), "0" );
        assertEquals( ((PropertyAccess)fx.get(6)).getSelector(), "d" );
        assertEquals( ((PropertyAccess)fx.get(7)).getSelector(), "1" );
    }

    @Test
    public void testParseMapValueVariousWays() {
        Function<String,Object> accessor = suffix -> ((List) new DslParser("$brooklyn:literal(\"ignored\")"+suffix).parse()).get(1);
        Asserts.assertPasses(accessor.apply("[\"a-b\"]"), v -> Asserts.assertEquals( ((PropertyAccess)v).getSelector(), "a-b" ));
        Asserts.assertPasses(accessor.apply(".[\"a-b\"]"), v -> Asserts.assertEquals( ((PropertyAccess)v).getSelector(), "a-b" ));

        Asserts.assertPasses(accessor.apply(".a"), v -> Asserts.assertEquals( ((PropertyAccess)v).getSelector(), "a" ));
        Asserts.assertPasses(accessor.apply("[\"a\"]"), v -> Asserts.assertEquals( ((PropertyAccess)v).getSelector(), "a" ));

        Asserts.assertFailsWith(() -> accessor.apply("a"), Asserts.expectedFailureContainsIgnoreCase(
                "unexpected character", " 28 ", ")a"));
        Asserts.assertFailsWith(() -> accessor.apply(".a-b"), Asserts.expectedFailureContainsIgnoreCase(
                "unexpected character", " 30 ", "a-b"));
    }


        @Test
    public void testParseFunctionExplicit() {
        List fx = (List) new DslParser("$brooklyn:function.foo()").parse();
        assertEquals( ((FunctionWithArgs)fx.get(0)).getFunction(), "$brooklyn:function.foo" );
        assertEquals( ((FunctionWithArgs)fx.get(0)).getArgs(), ImmutableList.of() );
    }

}
