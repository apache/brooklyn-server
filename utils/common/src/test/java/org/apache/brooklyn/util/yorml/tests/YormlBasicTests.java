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
import org.apache.brooklyn.util.yorml.Yorml;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Objects;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YormlBasicTests {

    static class Shape {
        String name;
        String color;
        int size;
        
        @Override
        public boolean equals(Object xo) {
            if (xo==null || !Objects.equal(getClass(), xo.getClass())) return false;
            Shape x = (Shape) xo;
            return Objects.equal(name, x.name) && Objects.equal(color, x.color) && Objects.equal(size, x.size);
        }
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
        
        Shape s = new Shape();
        s.color = "red";
        Object resultO = y.write(s);
        
        Assert.assertNotNull(resultO);
        String out = Jsonya.newInstance().add(resultO).toString();
        String expected = Jsonya.newInstance().add("type", "java"+":"+Shape.class.getName())
                .at("fields").add("color", "red", "size", 0).root().toString();
        Assert.assertEquals(out, expected);
    }
    
    // now using the fixture
    
    @Test
    public void testSimpleFieldInFieldsWithTestCase() {
        Shape s = new Shape();
        s.color = "red";

        YormlTestFixture.newInstance().writing(s).
        reading(
            Jsonya.newInstance().add("type", "java"+":"+Shape.class.getName())
            .at("fields").add("color", "red", "size", 0).root().toString()
        ).doReadWriteAssertingJsonMatch();
    }
    
    @Test
    public void testStringOnItsOwn() {
        YormlTestFixture.newInstance().
        write("hello").assertResult("hello").
        read("hello", "string").assertResult("hello");
    }

    @Test
    public void testFailOnUnknownType() {
        try {
            YormlTestFixture ytc = YormlTestFixture.newInstance().read("{ type: shape }", null);
            Asserts.shouldHaveFailedPreviously("Got "+ytc.lastReadResult+" when we should have failed due to unknown type shape");
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "shape", "unknown type");
        }
    }

    public static class ShapePair {
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
        pair.shape1 = new Shape();
        pair.shape1.color = "red";
        pair.shape2 = new Shape();
        pair.shape2.color = "blue";
        pair.shape2.size = 8;

        YormlTestFixture.newInstance().
        addType("shape", Shape.class).
        addType("shape-pair", ShapePair.class).
        writing(pair).
        reading(
            Jsonya.newInstance().add("type", "shape-pair")
                .at("fields", "shape1").add("type", "shape")
                    .at("fields").add("color", "red", "size", 0)
                .root()
                .at("fields", "shape2").add("type", "shape")
                    .at("fields").add("color", "blue", "size", 8)
                .root().toString()
        ).doReadWriteAssertingJsonMatch();
    }

}
