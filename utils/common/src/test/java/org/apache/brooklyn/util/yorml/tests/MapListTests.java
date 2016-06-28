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

import java.util.List;
import java.util.Set;

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableSet;
import org.testng.annotations.Test;

/** Tests that explicit fields can be set at the outer level in yaml. */
public class MapListTests {

    YormlTestFixture y = YormlTestFixture.newInstance();
    
//    String MAP1_JSON = "{ a: 1, b: bbb }";
//    Map<String,Object> MAP1_OBJ = MutableMap.<String,Object>of("a", 1, "b", "bbb");
//    
//    @Test public void testReadMap() { y.read(MAP1_JSON, "json").assertResult(MAP1_OBJ); }
//    @Test public void testWriteMap() { y.write(MAP1_OBJ, "json").assertResult(MAP1_JSON); }
//    
//    String MAP1_JSON_EXPLICIT_TYPE = "{ type: json, value: "+MAP1_JSON+" }";
//    @Test public void testReadMapNoTypeExpected() { y.read(MAP1_JSON_EXPLICIT_TYPE, null).assertResult(MAP1_OBJ); }
//    @Test public void testWriteMapNoTypeExpected() { y.write(MAP1_OBJ, null).assertResult(MAP1_JSON_EXPLICIT_TYPE); }
//
//    String MAP1_JSON_OBJECT_OBJECT = "[ { key: { type: string, value: a }, value: { type: int, value: 1 }, "+
//        "{ key: { type: string, value: b }, value: { type: string, value: bbb } ]";
//    @Test public void testReadMapVerbose() { y.read(MAP1_JSON_OBJECT_OBJECT, "map<object,object>").assertResult(MAP1_OBJ); }
//    @Test public void testWriteMapVerbose() { y.write(MAP1_OBJ, "map<object,object>").assertResult(MAP1_JSON_OBJECT_OBJECT); }
//
//    String MAP1_JSON_STRING_OBJECT = "{ a: { type: int, value: 1 }, b: { type: string, value: bbb } ]";
//    @Test public void testReadMapVerboseStringKey() { y.read(MAP1_JSON_STRING_OBJECT, "map<string,object>").assertResult(MAP1_OBJ); }
//    @Test public void testWriteMapVerboseStringKey() { y.write(MAP1_OBJ, "map<string,object>").assertResult(MAP1_JSON_STRING_OBJECT); }
//    @Test public void testReadMapVerboseJsonKey() { y.read(MAP1_JSON_STRING_OBJECT, "map<json,object>").assertResult(MAP1_OBJ); }
//    @Test public void testWriteMapVerboseJsonKey() { y.write(MAP1_OBJ, "map<json,object>").assertResult(MAP1_JSON_STRING_OBJECT); }

    
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

    Set<Object> SET1_OBJ = MutableSet.<Object>of("a", 1, "b");
    String SET1_JSON = "{ type: set<json>, value: [ a, 1, b ] }";
    @Test public void testWriteSet() { y.write(SET1_OBJ, "set<json>").assertResult(LIST1_JSON); }
    @Test public void testWriteSetNoType() { y.write(SET1_OBJ, null).assertResult(SET1_JSON); }
    @Test public void testReadSet() { y.read(SET1_JSON, "set<json>").assertResult(SET1_OBJ); }
    @Test public void testWriteSetJson() { y.write(SET1_OBJ, "json").assertResult(LIST1_JSON); }
    
    
    // TODO
    // map tests
    // passing generics from fields
    //   poor man: if field is compatible to mutable list or mutable set then make list<..> or set<..>
    //   rich man: registry can handle generics
    // maps w generics

}
