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
package org.apache.brooklyn.util.yoml.tests;

import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsAtTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlFieldAtTopLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Objects;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YomlAnnotationTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(YomlAnnotationTests.class);
    
    static class ExplicitFieldsAtTopLevelExamples {
        @Alias("shape")
        static class Shape {
            @YomlFieldAtTopLevel
            String name;
            @YomlFieldAtTopLevel
            @Alias(value={"kolor","couleur"}, preferred="colour")
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
            @Override
            public int hashCode() { return Objects.hashCode(name, color); }
    
            public Shape name(String name) { this.name = name; return this; }
            public Shape color(String color) { this.color = color; return this; }
        }
    }
        
    @Test
    public void testYomlFieldsAtTopLevel() {
        ExplicitFieldsAtTopLevelExamples.Shape shape = new ExplicitFieldsAtTopLevelExamples.Shape().name("nifty_shape").color("blue");
        YomlTestFixture.newInstance().
        addTypeWithAnnotations(ExplicitFieldsAtTopLevelExamples.Shape.class).
        read("{ name: nifty_shape, couleur: blue }", "shape").assertResult(shape).
        write(shape).assertResult("{ type: shape, colour: blue, name: nifty_shape }");
    }

    static class ExplicitFieldsAllExamples {
        @YomlAllFieldsAtTopLevel
        @Alias("shape")
        static class Shape {
            String name;
            @YomlFieldAtTopLevel
            @Alias(value={"kolor","couleur"}, preferred="colour")
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
            @Override
            public int hashCode() { return Objects.hashCode(name, color); }
    
            public Shape name(String name) { this.name = name; return this; }
            public Shape color(String color) { this.color = color; return this; }
        }
    }
        
    @Test
    public void testYomlAllFields() {
        ExplicitFieldsAllExamples.Shape shape = new ExplicitFieldsAllExamples.Shape().name("nifty_shape").color("blue");
        YomlTestFixture.newInstance().
        addTypeWithAnnotations(ExplicitFieldsAllExamples.Shape.class).
        read("{ name: nifty_shape, couleur: blue }", "shape").assertResult(shape).
        write(shape).assertResult("{ type: shape, colour: blue, name: nifty_shape }");
    }

}
