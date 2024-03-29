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
package org.apache.brooklyn.feed.http;

import java.lang.reflect.Array;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.Jsonya.Navigator;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.guava.Functionals;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import com.jayway.jsonpath.PathNotFoundException;

public class JsonFunctionsTest {

    public static JsonElement europeMap() {
        Navigator<MutableMap<Object, Object>> europe = Jsonya.newInstance().at("europe", "uk", "edinburgh")
                .put("population", 500*1000)
                .put("weather", "wet", "lighting", "dark")
                .root().at("europe").at("france").put("population", 80*1000*1000)
                .root();
        return new JsonParser().parse( europe.toString() );
    }

    @Test
    public void testAsJson() {
        JsonElement jsonElement = europeMap();
        String jsonString = jsonElement.toString();
        JsonElement result = JsonFunctions.asJson().apply(jsonString);
        Assert.assertEquals(result.toString(), jsonString);
    }

    @Test
    public void testAsJsonWithNullReturnsNull() {
        Assert.assertNull(JsonFunctions.asJson().apply(null));
    }

    @Test
    public void testWalk1() {
        JsonElement pop = JsonFunctions.walk("europe", "france", "population").apply(europeMap());
        Assert.assertEquals( (int)JsonFunctions.cast(Integer.class).apply(pop), 80*1000*1000 );
    }

    @Test
    public void testWalk2() {
        String weather = Functionals.chain(
            JsonFunctions.walk("europe.uk.edinburgh.weather"),
            JsonFunctions.cast(String.class) ).apply(europeMap());
        Assert.assertEquals(weather, "wet");
    }

    @Test(expectedExceptions=NoSuchElementException.class)
    public void testWalkWrong() {
        Functionals.chain(
            JsonFunctions.walk("europe", "spain", "barcelona"),
            JsonFunctions.cast(String.class) ).apply(europeMap());
    }


    @Test
    public void testWalkM() {
        Maybe<JsonElement> pop = JsonFunctions.walkM("europe", "france", "population").apply( Maybe.of(europeMap()) );
        Assert.assertEquals( (int)JsonFunctions.castM(Integer.class).apply(pop), 80*1000*1000 );
    }

    @Test
    public void testWalkMWrong1() {
        Maybe<JsonElement> m = JsonFunctions.walkM("europe", "spain", "barcelona").apply( Maybe.of( europeMap()) );
        Assert.assertTrue(m.isAbsent());
    }

    @Test(expectedExceptions=IllegalStateException.class)
    public void testCastMWhenAbsent() {
//        Maybe<JsonElement> m = JsonFunctions.walkM("europe", "spain", "barcelona").apply( Maybe.of( europeMap()) );
        Maybe<JsonElement> m = Maybe.absent();
        JsonFunctions.castM(String.class).apply(m);
    }

    @Test(expectedExceptions=UnsupportedOperationException.class)
    public void testCastMWrong() {
        Maybe<JsonElement> m = JsonFunctions.walkM("europe", "france").apply( Maybe.of( europeMap()) );
        JsonFunctions.castM(String.class).apply(m);
    }

    @Test
    public void testCastCollectionsAndMaps() {
        Asserts.assertEquals(JsonFunctions.doCast(JsonParser.parseString("[1,2,3]"), (new Integer[]{}).getClass()), new Integer[] { 1, 2, 3 });
        Asserts.assertEquals(JsonFunctions.doCast(JsonParser.parseString("[1,2,3]"), new TypeToken<List<Integer>>() {}), MutableList.of(1, 2, 3));
        Asserts.assertEquals(JsonFunctions.doCast(JsonParser.parseString("[1,2,3]"), JsonElement.class), JsonParser.parseString("[1,2,3]"));
        Asserts.assertEquals(JsonFunctions.doCast(JsonParser.parseString("{ \"a\": 1 }"), new TypeToken<Map<String,Integer>>() {}), MutableMap.of("a", 1));
        Asserts.assertEquals(JsonFunctions.doCast(JsonParser.parseString("{ \"a\": 1 }"), Object.class), MutableMap.of("a", 1));
    }
    
    @Test
    public void testWalkN() {
        JsonElement pop = JsonFunctions.walkN("europe", "france", "population").apply( europeMap() );
        Assert.assertEquals( (int)JsonFunctions.cast(Integer.class).apply(pop), 80*1000*1000 );
    }

    @Test
    public void testWalkNWrong1() {
        JsonElement m = JsonFunctions.walkN("europe", "spain", "barcelona").apply( europeMap() );
        Assert.assertNull(m);
    }

    public void testWalkNWrong2() {
        JsonElement m = JsonFunctions.walkN("europe", "spain", "barcelona").apply( europeMap() );
        String n = JsonFunctions.cast(String.class).apply(m);
        Assert.assertNull(n);
    }

    @Test
    public void testGetPath1(){
        Integer obj = (Integer) JsonFunctions.getPath("$.europe.uk.edinburgh.population").apply(europeMap());
        Assert.assertEquals((int) obj, 500*1000);
    }

    @Test
    public void testGetPath2(){
        String obj = (String) JsonFunctions.getPath("$.europe.uk.edinburgh.lighting").apply(europeMap());
        Assert.assertEquals(obj, "dark");
    }

    @Test
    public void testGetPathWithNullReturnsNull(){
        Integer obj = (Integer) JsonFunctions.getPath("$.europe.uk.edinburgh.population").apply(null);
        Assert.assertNull(obj);
    }

    @Test
    public void testGetPathSizeOfMap(){
        JsonElement json = JsonFunctions.asJson().apply("{\"mymap\": {\"k1\": \"v1\", \"k2\": \"v2\"}}");
        Integer obj = (Integer) JsonFunctions.getPath("$.mymap.size()").apply(json);
        Assert.assertEquals(obj, (Integer)2);
    }

    @Test
    public void testGetPathSizeOfList(){
        JsonElement json = JsonFunctions.asJson().apply("{\"mylist\": [\"a\", \"b\", \"c\"]}");
        Integer obj = (Integer) JsonFunctions.getPath("$.mylist.size()").apply(json);
        Assert.assertEquals(obj, (Integer)3);
    }

    @Test
    public void testGetMissingPathIsNullOrThrows(){
        try {
            // TODO is there a way to force this to return null if not found?
            // for me (Alex) it throws but for others it seems to return null
            Object obj = JsonFunctions.getPath("$.europe.spain.malaga").apply(europeMap());
            Assert.assertNull(obj);
        } catch (PathNotFoundException e) {
            // not unexpected
        }
    }
    
}
