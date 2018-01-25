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
import org.apache.brooklyn.util.yoml.annotations.YomlDefaultMapValues;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.ShapeWithSize;
import org.testng.annotations.Test;

public class DefaultMapValuesTests {
    
    @YomlAllFieldsTopLevel
    @YomlDefaultMapValues({@DefaultKeyValue(key="size",val="0",valNeedsParsing=true), 
        @DefaultKeyValue(key="name",val="anonymous")})
    static class ShapeAnn extends ShapeWithSize {
    }
    
    YomlTestFixture y = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnn.class);

    @Test 
    public void testEntirelyFromAnnotation() {
        y.reading("{ }", "shape").writing(new ShapeAnn().name("anonymous").size(0), "shape")
        .doReadWriteAssertingJsonMatch();
    }

    @Test 
    public void testOverwritingDefault() {
        y.reading("{ name: special }", "shape").writing(new ShapeAnn().name("special").size(0), "shape")
        .doReadWriteAssertingJsonMatch();
    }

}
