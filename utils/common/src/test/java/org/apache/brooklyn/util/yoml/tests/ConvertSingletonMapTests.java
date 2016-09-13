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

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.DefaultKeyValue;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlSingletonMap;
import org.apache.brooklyn.util.yoml.serializers.AllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.serializers.ConvertSingletonMap;
import org.apache.brooklyn.util.yoml.tests.YomlBasicTests.ShapeWithSize;
import org.testng.annotations.Test;

import com.google.common.base.Objects;

public class ConvertSingletonMapTests {

    static class ShapeWithTags extends ShapeWithSize {
        List<String> tags;
        Map<String,String> metadata;
        
        @Override
        public boolean equals(Object xo) {
            return super.equals(xo) && Objects.equal(tags, ((ShapeWithTags)xo).tags) && Objects.equal(metadata, ((ShapeWithTags)xo).metadata);
        }
        @Override
        public int hashCode() { return Objects.hashCode(super.hashCode(), tags, metadata); }

        public ShapeWithTags tags(String ...tags) { this.tags = Arrays.asList(tags); return this; }
        public ShapeWithTags metadata(Map<String,String> metadata) { this.metadata = metadata; return this; }
    }
    
    /* y does:
     * * key defaults to name
     * * primitive value defaults to color
     * * list value defaults to tags
     * and y2 adds:
     * * map defaults to metadata
     * * default for value is size (but won't be used unless one of the above is changed to null)
     */
    YomlTestFixture y = YomlTestFixture.newInstance().
        addType("shape", ShapeWithTags.class, MutableList.of(
            new AllFieldsTopLevel(),
            new ConvertSingletonMap("name", null, "color", "tags", null, null, MutableMap.of("size", 0))));
    
    YomlTestFixture y2 = YomlTestFixture.newInstance().
        addType("shape", ShapeWithTags.class, MutableList.of(
            new AllFieldsTopLevel(),
            new ConvertSingletonMap("name", "size", "color", "tags", "metadata", null, MutableMap.of("size", 42))));
    
    
    @Test public void testPrimitiveValue() {
        y.reading("{ red-square: red }", "shape").writing(new ShapeWithTags().name("red-square").color("red"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testListValue() {
        y.reading("{ good-square: [ good ] }", "shape").writing(new ShapeWithTags().tags("good").name("good-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testMapValueMerge() {
        y.reading("{ merge-square: { size: 12 } }", "shape").writing(new ShapeWithTags().size(12).name("merge-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }

    @Test public void testMapValueSetAndDefaults() {
        y2.reading("{ happy-square: { mood: happy } }", "shape").writing(new ShapeWithTags().metadata(MutableMap.of("mood", "happy")).size(42).name("happy-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testMapValueWontMergeIfWouldTreatAsMetadataAndDoesntApplyDefaults() {
        y2.reading("{ name: bad-square, size: 12 }", "shape").writing(new ShapeWithTags().size(12).name("bad-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testWontApplyIfTypeUnknown() {
        // size is needed without an extra defaults bit
        y.write(new ShapeWithTags().name("red-square").color("red"), null)
        .assertResult("{ type: shape, color: red, name: red-square, size: 0 }");
    }

    YomlTestFixture y3 = YomlTestFixture.newInstance().
        addTypeWithAnnotations("shape", ShapeAnn.class);

    @YomlAllFieldsTopLevel
    @YomlSingletonMap(keyForKey="name", keyForListValue="tags", keyForPrimitiveValue="color",
//        keyForAnyValue="",
        defaultValues={@DefaultKeyValue(key="size", val="0", valNeedsParsing=true)})
    static class ShapeAnn extends ShapeWithTags {}
    
    @Test public void testAnnPrimitiveValue() {
        y3.reading("{ red-square: red }", "shape").writing(new ShapeAnn().name("red-square").color("red"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testAnnListValue() {
        y3.reading("{ good-square: [ good ] }", "shape").writing(new ShapeAnn().tags("good").name("good-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testAnnMapValueMerge() {
        y3.reading("{ merge-square: { size: 12 } }", "shape").writing(new ShapeAnn().size(12).name("merge-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testAnnNothingExtra() {
        y3.reading("{ merge-square: { } }", "shape").writing(new ShapeAnn().name("merge-square"), "shape")
        .doReadWriteAssertingJsonMatch();
    }
    
    @Test public void testAnnList() {
        y3.reading("[ { one: { size: 1 } }, { two: { size: 2 } } ]", "list<shape>").writing(
            MutableList.of(new ShapeAnn().name("one").size(1), new ShapeAnn().name("two").size(2)), "list<shape>") 
        .doReadWriteAssertingJsonMatch();
    }
    
    // TODO
    @Test(enabled=false) public void testAnnListCompressed() {
        y3.reading("{ one: { size: 1 }, two: { size: 2 } }", "list<shape>").writing(
            MutableList.of(new ShapeAnn().name("one").size(1), new ShapeAnn().name("two").size(2)), "list<shape>") 
        .doReadWriteAssertingJsonMatch();
    }
    

}
