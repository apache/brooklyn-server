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

import java.math.RoundingMode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.yorml.serializers.AllFieldsExplicit;
import org.apache.brooklyn.util.yorml.tests.YormlBasicTests.Shape;
import org.testng.Assert;
import org.testng.annotations.Test;

/** Tests that explicit fields can be set at the outer level in yaml. */
public class MapListTests {

    YormlTestFixture y = YormlTestFixture.newInstance();
    
    String MAP1_JSON = "{ a: 1, b: bbb }";
    Map<String,Object> MAP1_OBJ = MutableMap.<String,Object>of("a", 1, "b", "bbb");
    
    @Test public void testReadMap() { y.read(MAP1_JSON, "json").assertResult(MAP1_OBJ); }
    @Test public void testWriteMap() { y.write(MAP1_OBJ, "json").assertResult(MAP1_JSON); }
    
    String MAP1_JSON_EXPLICIT_TYPE = "{ type: \"map<string,json>\", value: "+MAP1_JSON+" }";
    @Test public void testReadMapNoTypeExpected() { y.read(MAP1_JSON_EXPLICIT_TYPE, null).assertResult(MAP1_OBJ); }
    @Test public void testWriteMapNoTypeExpected() { y.write(MAP1_OBJ, null).assertResult(MAP1_JSON_EXPLICIT_TYPE); }

    String MAP1_JSON_OBJECT_OBJECT = "[ { key: { type: string, value: a }, value: { type: int, value: 1 } }, "+
        "{ key: { type: string, value: b }, value: { type: string, value: bbb } } ]";
    @Test public void testReadMapVerbose() { y.read(MAP1_JSON_OBJECT_OBJECT, "map<object,object>").assertResult(MAP1_OBJ); }

    String MAP1_JSON_STRING_OBJECT = "{ a: { type: int, value: 1 }, b: { type: string, value: bbb } }";
    @Test public void testReadMapVerboseStringKey() { y.read(MAP1_JSON_STRING_OBJECT, "map<string,object>").assertResult(MAP1_OBJ); }
    @Test public void testReadMapVerboseJsonKey() { y.read(MAP1_JSON_STRING_OBJECT, "map<json,object>").assertResult(MAP1_OBJ); }
    
    String LIST1_JSON = "[ a, 1, b ]";
    List<Object> LIST1_OBJ = MutableList.<Object>of("a", 1, "b");
    
    @Test public void testReadList() { y.read(LIST1_JSON, "json").assertResult(LIST1_OBJ); }
    @Test public void testWriteList() { y.write(LIST1_OBJ, "json").assertResult(LIST1_JSON); }
    @Test public void testReadListListJson() { y.read(LIST1_JSON, "list<json>").assertResult(LIST1_OBJ); }
    @Test public void testWriteListListJson() { y.write(LIST1_OBJ, "list<json>").assertResult(LIST1_JSON); }
    
    @Test public void testReadListNoTypeAnywhereIsError() { 
        try {
            y.read(LIST1_JSON, null);
            Asserts.shouldHaveFailedPreviously("Got "+y.lastResult+" when should have failed");
        } catch (Exception e) { Asserts.expectedFailureContainsIgnoreCase(e, "'a'", "unknown type"); }
    }

    String LIST1_JSON_LIST_TYPE = "{ type: json, value: "+LIST1_JSON+" }";
    String LIST1_JSON_LIST_JSON_TYPE = "{ type: list<json>, value: "+LIST1_JSON+" }";
    @Test public void testReadListNoTypeExpected() { y.read(LIST1_JSON_LIST_TYPE, null).assertResult(LIST1_OBJ); }
    @Test public void testReadListExplicitTypeNoTypeExpected() { y.read(LIST1_JSON_LIST_TYPE, null).assertResult(LIST1_OBJ); }
    @Test public void testWriteListNoTypeExpected() { y.write(LIST1_OBJ, null).assertResult(LIST1_JSON_LIST_JSON_TYPE); }
    // write prefers type: json syntax above if appropriate; otherwise will to LIST_OBJECT syntax below

    String LIST1_JSON_OBJECT = "[ { type: string, value: a }, { type: int, value: 1 }, { type: string, value: b } ]";
    @Test public void testReadListObject() { y.read(LIST1_JSON_OBJECT, "list<object>").assertResult(LIST1_OBJ); }
    @Test public void testWriteListObject() { y.write(LIST1_OBJ, "list<object>").assertResult(LIST1_JSON_OBJECT); }    
    @Test public void testReadListRawType() { y.read(LIST1_JSON_OBJECT, "list").assertResult(LIST1_OBJ); }
    @Test public void testWriteListRawType() { y.write(LIST1_OBJ, "list").assertResult(LIST1_JSON_LIST_JSON_TYPE); }   
    @Test public void testReadListJsonType() { y.read(LIST1_JSON_OBJECT, "list<json>").assertResult(LIST1_OBJ); }
    @Test public void testReadListJsonTypeInBody() { y.read(LIST1_JSON_LIST_JSON_TYPE, null).assertResult(LIST1_OBJ); }
    @Test public void testReadListJsonTypeDeclaredAndInBody() { y.read(LIST1_JSON_LIST_JSON_TYPE, "list<json>").assertResult(LIST1_OBJ); }

    @Test public void testArraysAsList() { 
        y.reading("[ a, b ]", "list<json>").writing(Arrays.asList("a", "b"), "list<json>")
        .doReadWriteAssertingJsonMatch();
    }
    
    Set<Object> SET1_OBJ = MutableSet.<Object>of("a", 1, "b");
    String SET1_JSON = "{ type: set<json>, value: [ a, 1, b ] }";
    @Test public void testWriteSet() { y.write(SET1_OBJ, "set<json>").assertResult(LIST1_JSON); }
    @Test public void testWriteSetNoType() { y.write(SET1_OBJ, null).assertResult(SET1_JSON); }
    @Test public void testReadSet() { y.read(SET1_JSON, "set<json>").assertResult(SET1_OBJ); }
    @Test public void testWriteSetJson() { y.write(SET1_OBJ, "json").assertResult(LIST1_JSON); }
    
    @Test public void testReadWithShape() {
        y.tr.put("shape", Shape.class);
        Shape shape = new Shape().name("my-shape");
        
        Map<String,Object> m1 = MutableMap.<String,Object>of("k1", shape);
        String MAP_W_SHAPE = "{ k1: { type: shape, fields: { name: my-shape } } }";
        y.read(MAP_W_SHAPE, "map").assertResult(m1); 
        y.write(m1, "map").assertResult(MAP_W_SHAPE); 
        y.write(m1, null).assertResult("{ type: map, value: " + MAP_W_SHAPE + " }");
        
        Map<Object,Object> m2 = MutableMap.<Object,Object>of(shape, "v1", "k2", 2);
        Map<Object,Object> m3 = MutableMap.<Object,Object>of(shape, "v1", "k2", 2);
        Assert.assertEquals(m2, m3);
        String MAP_W_SHAPE_KEY_JSON = "[ { key: { type: shape, fields: { name: my-shape } }, value: v1 }, { k2: 2 } ]";
        String MAP_W_SHAPE_KEY_NON_GENERIC = "[ { key: { type: shape, fields: { name: my-shape } }, value: { type: string, value: v1 } }, { k2: { type: int, value: 2 } } ]";
        y.read(MAP_W_SHAPE_KEY_JSON, "map<?,json>").assertResult(m2);
        y.write(m2, "map<?,json>").assertResult(MAP_W_SHAPE_KEY_JSON);
        y.write(m2, "map").assertResult(MAP_W_SHAPE_KEY_NON_GENERIC);
    }
    
    @Test public void testReadWithEnum() {
        y.tr.put("rounding-mode", RoundingMode.class);
        
        Map<String,Object> m1 = MutableMap.<String,Object>of("k1", RoundingMode.UP);
        String MAP_W_RM = "{ k1: { type: rounding-mode, value: UP } }";
        y.read(MAP_W_RM, "map").assertResult(m1); 
        y.write(m1, "map").assertResult(MAP_W_RM); 
        y.write(m1, null).assertResult("{ type: map, value: " + MAP_W_RM + " }");
        
        String MAP_W_RM_TYPE_KNOWN = "{ k1: UP }";
        y.read(MAP_W_RM_TYPE_KNOWN, "map<?,rounding-mode>").assertResult(m1); 
        y.write(m1, "map<?,rounding-mode>").assertResult(MAP_W_RM_TYPE_KNOWN); 
    }

    static class TestingGenericsOnFields {
        List<RoundingMode> list;
        Map<RoundingMode,RoundingMode> map;
        Set<RoundingMode> set;
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof TestingGenericsOnFields)) return false;
            TestingGenericsOnFields gf = (TestingGenericsOnFields)obj;
            return Objects.equals(list, gf.list) && Objects.equals(map, gf.map) && Objects.equals(set, gf.set);
        }
        @Override
        public int hashCode() {
            return Objects.hash(list, map, set);
        }
        @Override
        public String toString() {
            return com.google.common.base.Objects.toStringHelper(this).add("list", list).add("map", map).add("set", set).omitNullValues().toString();
        }
    }
    
    @Test public void testGenericList() {
        y.tr.put("gf", TestingGenericsOnFields.class, MutableList.of(new AllFieldsExplicit()));
        TestingGenericsOnFields gf;
        
        gf = new TestingGenericsOnFields();
        gf.list = MutableList.of(RoundingMode.UP);
        y.reading("{ list: [ UP ] }", "gf").writing(gf, "gf").doReadWriteAssertingJsonMatch();
    }
    @Test public void testGenericMap() {
        y.tr.put("gf", TestingGenericsOnFields.class, MutableList.of(new AllFieldsExplicit()));
        TestingGenericsOnFields gf;
        String json;
        gf = new TestingGenericsOnFields();
        gf.map = MutableMap.of(RoundingMode.UP, RoundingMode.DOWN);
        json = "{ map: [ { key: UP, value: DOWN } ] }";
        y.reading(json, "gf").writing(gf, "gf").doReadWriteAssertingJsonMatch();
        // TODO make it smart enough to realize all keys are strings and adjust, so (a) this works, and (b) we can swap json2 with json above
        // (but enum keys are not a high priority)
//        String json2 = "{ map: { UP: DOWN } }";
//        y.read(json2, "gf").assertResult(gf);
    }
    
    @Test public void testGenericListSet() {
        y.tr.put("gf", TestingGenericsOnFields.class, MutableList.of(new AllFieldsExplicit()));
        TestingGenericsOnFields gf;
        String json;
        gf = new TestingGenericsOnFields();
        gf.set = MutableSet.of(RoundingMode.UP);
        json = "{ set: [ UP ] }";
        String json2 = "{ set: { type: set, value: [ UP ] } }";
        y.read(json2, "gf").assertResult(gf);
        y.reading(json, "gf").writing(gf, "gf").doReadWriteAssertingJsonMatch();
    }

}
