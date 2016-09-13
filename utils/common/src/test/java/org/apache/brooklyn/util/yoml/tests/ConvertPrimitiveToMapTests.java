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

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.DefaultKeyValue;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlFromPrimitive;
import org.apache.brooklyn.util.yoml.serializers.AllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.serializers.ConvertFromPrimitive;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.Shape;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.ShapeWithSize;
import org.testng.annotations.Test;

public class ConvertPrimitiveToMapTests {
    
    YomlTestFixture y = YomlTestFixture.newInstance().
        addType("shape", Shape.class, MutableList.of(
            new ConvertFromPrimitive("name", null),
            new AllFieldsTopLevel()));
    
    @Test
    public void testPrimitive() {
        y.reading("red-square", "shape").writing(new Shape().name("red-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }

    
    YomlTestFixture y2 = YomlTestFixture.newInstance().
        addType("shape", ShapeWithSize.class, MutableList.of(
            new ConvertFromPrimitive("name", MutableMap.of("size", 0)),
            new AllFieldsTopLevel()));
    
    @Test
    public void testWithDefaults() {
        y2.reading("red-square", "shape").writing(new ShapeWithSize().name("red-square").size(0), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    
    @YomlAllFieldsTopLevel
    @YomlFromPrimitive(keyToInsert="name", defaults={@DefaultKeyValue(key="size",val="0",valNeedsParsing=true)})
    static class ShapeAnn extends ShapeWithSize {
    }
    
    YomlTestFixture y3 = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnn.class);

    @Test 
    public void testFromAnnotation() {
        y2.reading("red-square", "shape").writing(new ShapeWithSize().name("red-square").size(0), "shape")
        .doReadWriteAssertingJsonMatch();
    }

}
