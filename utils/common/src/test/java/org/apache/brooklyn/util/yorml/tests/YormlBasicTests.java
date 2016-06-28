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
package org.apache.brooklyn.util.yorml.tests;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.yorml.Yorml;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Objects;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YormlBasicTests {

    private static final Logger log = LoggerFactory.getLogger(YormlBasicTests.class);
    
    static class Shape {
        String name;
        String color;
        
        @Override
        public boolean equals(Object xo) {
            if (xo==null || !Objects.equal(getClass(), xo.getClass())) return false;
            Shape x = (Shape) xo;
            return Objects.equal(name, x.name) && Objects.equal(color, x.color);
        }
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("name", name).add("color", color).omitNullValues().toString();
        }

        public Shape name(String name) { this.name = name; return this; }
        public Shape color(String color) { this.color = color; return this; }
    }
        
    // very basic tests illustrating read/write
    
    @Test
    public void testReadJavaType() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        tr.put("shape", Shape.class);
        Yorml y = Yorml.newInstance(tr);
        Object resultO = y.read("{ type: shape }", "object");
        
        Assert.assertNotNull(resultO);
        Assert.assertTrue(resultO instanceof Shape, "Wrong result type: "+resultO);
        Shape result = (Shape)resultO;
        Assert.assertNull(result.name);
        Assert.assertNull(result.color);
    }
    
    @Test
    public void testReadFieldInFields() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        tr.put("shape", Shape.class);
        Yorml y = Yorml.newInstance(tr);
        Object resultO = y.read("{ type: shape, fields: { color: red } }", "object");
        
        Assert.assertNotNull(resultO);
        Assert.assertTrue(resultO instanceof Shape, "Wrong result type: "+resultO);
        Shape result = (Shape)resultO;
        Assert.assertNull(result.name);
        Assert.assertEquals(result.color, "red");
    }

    @Test
    public void testWritePrimitiveFieldInFields() {
        MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
        Yorml y = Yorml.newInstance(tr);
        
        Shape s = new ShapeWithSize();
        s.color = "red";
        Object resultO = y.write(s);
        
        Assert.assertNotNull(resultO);
        String out = Jsonya.newInstance().add(resultO).toString();
        String expected = Jsonya.newInstance().add("type", "java"+":"+ShapeWithSize.class.getName())
                .at("fields").add("color", "red", "size", 0).root().toString();
        Assert.assertEquals(out, expected);
    }
    
    // now using the fixture
    
    @Test
    public void testFieldInFieldsUsingTestFixture() {
        YormlTestFixture.newInstance().writing( new Shape().color("red") ).
        reading(
            Jsonya.newInstance().add("type", "java"+":"+Shape.class.getName())
            .at("fields").add("color", "red").root().toString() ).
        doReadWriteAssertingJsonMatch();
    }
    
    @Test
    public void testStringPrimitiveWhereTypeKnown() {
        YormlTestFixture.newInstance().
        write("hello", "string").assertResult("hello").
        read("hello", "string").assertResult("hello");
    }

    @Test
    public void testStringPrimitiveWhereTypeUnknown() {
        YormlTestFixture.newInstance().
        write("hello").assertResult("{ type: string, value: hello }").
        read("{ type: string, value: hello }", null).assertResult("hello");
    }
    @Test
    public void testIntPrimitiveWhereTypeUnknown() {
        YormlTestFixture.newInstance().
        write(42).assertResult("{ type: int, value: 42 }").
        read("{ type: int, value: 42 }", null).assertResult(42);
    }

    @Test
    public void testRegisteredType() {
        YormlTestFixture.newInstance().
        addType("shape", Shape.class).
        writing(new Shape()).
        reading( "{ type: shape }" ).
        doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testExpectedType() {
        YormlTestFixture.newInstance().
        addType("shape", Shape.class).
        writing(new Shape().color("red"), "shape").
        reading( "{ fields: { color: red } }", "shape" ).
        doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testExtraFieldError() {
        // TODO see failures
        // TODO extra blackboard item of fields to handle
        // TODO instantiate expected type if none explicit
        try {
            YormlTestFixture.newInstance().
            addType("shape", Shape.class).
            read( "{ type: shape, fields: { size: 4 } }", "shape" );
            Asserts.shouldHaveFailedPreviously("should complain about fields still existing");
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "size");
        }
    }

    static class ShapeWithSize extends Shape {
        int size;
        
        @Override
        public boolean equals(Object xo) {
            return super.equals(xo) && Objects.equal(size, ((ShapeWithSize)xo).size);
        }

        public ShapeWithSize size(int size) { this.size = size; return this; }
        public ShapeWithSize name(String name) { return (ShapeWithSize)super.name(name); }
        public ShapeWithSize color(String color) { return (ShapeWithSize)super.color(color); }
        
        @Override
        public String toString() {
            return Objects.toStringHelper(this).add("name", name).add("color", color).add("size", size).omitNullValues().toString();
        }
    }

    @Test
    public void testFieldInExtendedClassInFields() {
        YormlTestFixture.newInstance().writing( new ShapeWithSize().size(4).color("red") ).
        reading(
            Jsonya.newInstance().add("type", "java"+":"+ShapeWithSize.class.getName())
            .at("fields").add("color", "red", "size", 4).root().toString() ).
        doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testFieldInExtendedClassInFieldsDefault() {
        // note we get 0 written
        YormlTestFixture.newInstance().writing( new ShapeWithSize().color("red") ).
        reading(
            Jsonya.newInstance().add("type", "java"+":"+ShapeWithSize.class.getName())
            .at("fields").add("color", "red", "size", 0).root().toString() ).
        doReadWriteAssertingJsonMatch();
    }

    
    @Test
    public void testFailOnUnknownType() {
        try {
            YormlTestFixture ytc = YormlTestFixture.newInstance().read("{ type: shape }", null);
            Asserts.shouldHaveFailedPreviously("Got "+ytc.lastReadResult+" when we should have failed due to unknown type shape");
        } catch (Exception e) {
            try {
                Asserts.expectedFailureContainsIgnoreCase(e, "shape", "unknown type");
            } catch (Throwable e2) {
                log.warn("Failure detail: "+e, e);
                throw Exceptions.propagate(e2);
            }
        }
    }

    static class ShapePair {
        String pairName;
        Shape shape1;
        Shape shape2;
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof ShapePair)) return false;
            ShapePair sp = (ShapePair)obj;
            return Objects.equal(pairName, sp.pairName) && Objects.equal(shape1, sp.shape1) && Objects.equal(shape2, sp.shape2);
        }
    }

    @Test
    public void testWriteComplexFieldInFields() {
        ShapePair pair = new ShapePair();
        pair.pairName = "red and blue";
        pair.shape1 = new Shape().color("red");
        pair.shape2 = new ShapeWithSize().size(8).color("blue");

        YormlTestFixture.newInstance().
        addType("shape", Shape.class).
        addType("shape-with-size", ShapeWithSize.class).
        addType("shape-pair", ShapePair.class).
        writing(pair).
        reading(
            Jsonya.newInstance().add("type", "shape-pair")
                .at("fields").add("pairName", pair.pairName)
                    .at("shape1")
                        // .add("type", "shape")  // default is suppressed
                        .at("fields").add("color", pair.shape1.color)
                .root()
                .at("fields", "shape2")
                    .add("type", "shape-with-size")
                    .at("fields").add("color", pair.shape2.color, "size", ((ShapeWithSize)pair.shape2).size)
                .root().toString()
        ).doReadWriteAssertingJsonMatch();
    }

    static class ShapeWithWeirdFields extends ShapeWithSize {
        static int aStatic = 1;
        transient int aTransient = 11;
        
        /** masks name in parent */
        String name;
        ShapeWithWeirdFields realName(String name) { this.name = name; return this; }
    }
    
    @Test
    public void testStaticNotWrittenButExtendedItemsAre() {
        ShapeWithWeirdFields shape = new ShapeWithWeirdFields();
        shape.size(4).name("weird-shape");
        shape.realName("normal-trust-me");
        ShapeWithWeirdFields.aStatic = 2;
        shape.aTransient = 12;
        
        YormlTestFixture.newInstance().
        addType("shape-weird", ShapeWithWeirdFields.class).
        writing(shape).reading("{ \"type\": \"shape-weird\", "
            + "\"fields\": { \"name\": \"normal-trust-me\", "
            + "\""+Shape.class.getCanonicalName()+"."+"name\": \"weird-shape\", "
            + "\"size\": 4 "
            + "} }").
        doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testStaticNotRead() {
        ShapeWithWeirdFields.aStatic = 3;
        try {
            YormlTestFixture.newInstance().
            addType("shape-weird", ShapeWithWeirdFields.class).
            read("{ \"type\": \"shape-weird\", "
                + "\"fields\": { \"aStatic\": 4 "
                + "} }", null);
            Assert.assertEquals(3, ShapeWithWeirdFields.aStatic);
            Asserts.shouldHaveFailedPreviously();
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "aStatic");
        }
        Assert.assertEquals(3, ShapeWithWeirdFields.aStatic);
    }


}
