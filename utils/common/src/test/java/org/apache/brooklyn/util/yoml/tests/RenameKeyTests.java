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

import org.apache.brooklyn.util.yoml.annotations.DefaultKeyValue;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlFromPrimitive;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultKey;
import org.apache.brooklyn.util.yoml.annotations.YomlRenameKey.YomlRenameDefaultValue;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.Shape;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.ShapeWithSize;
import org.testng.annotations.Test;

public class RenameKeyTests {
    
    @YomlAllFieldsTopLevel
    @YomlRenameKey(oldKeyName="name-of-tiny-shape", newKeyName="name", defaults={@DefaultKeyValue(key="size",val="0",valNeedsParsing=true)})
    static class ShapeAnn extends ShapeWithSize {
    }
    
    YomlTestFixture y = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnn.class);

    @Test 
    public void testFromAnnotation() {
        y.reading("{ name-of-tiny-shape: red-square }", "shape")
            .writing(new ShapeAnn().name("red-square").size(0), "shape")
            .doReadWriteAssertingJsonMatch();
    }

    @YomlAllFieldsTopLevel
    @YomlFromPrimitive
    @YomlRenameDefaultValue("name")
    static class ShapeAnnUsingDotValue extends Shape {
    }
    
    YomlTestFixture y2 = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnnUsingDotValue.class);

    @Test 
    public void testWithDotValue() {
        y2.reading("red-square", "shape")
            .writing(new ShapeAnnUsingDotValue().name("red-square"), "shape")
            .doReadWriteAssertingJsonMatch();
    }


    @YomlAllFieldsTopLevel
    @YomlRenameDefaultKey("name")
    static class ShapeAnnUsingDotKey extends Shape {
    }
    
    YomlTestFixture y3 = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnnUsingDotKey.class);

    @Test 
    public void testWithDotKey() {
        y3.reading("{ .key: red-square }", "shape")
            .writing(new ShapeAnnUsingDotKey().name("red-square"), "shape")
            .doReadWriteAssertingJsonMatch();
    }

}
