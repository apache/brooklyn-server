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
package org.apache.brooklyn.core.resolve.jackson;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Iterables;
import javassist.tools.rmi.Sample;
import org.apache.brooklyn.core.resolve.jackson.BrooklynRegisteredTypeJacksonSerializationTest.SampleBean;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class PerverseSerializationTest implements MapperTestFixture {

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(null, true, null, true);
    }

    private static class BeanWithFieldCalledType {
        // also see tests of SampleBeanWithType
        String type;
    }

    @Test
    public void testFieldCalledTypeMakesBetterJson() throws Exception {
        BeanWithFieldCalledType a = new BeanWithFieldCalledType();
        a.type = "not my type";
        Assert.assertEquals(ser(a),
                "{\"(type)\":\""+ BeanWithFieldCalledType.class.getName()+"\",\"type\":\"not my type\"}");
    }

    private final static class BeanWithFieldCalledTypeHolder {
        BeanWithFieldCalledType bean;
    }

    @Test
    public void testFieldCalledTypeOkayIfTypeConcrete() throws Exception {
        BeanWithFieldCalledType a = new BeanWithFieldCalledType();
        a.type = "not my type";
        BeanWithFieldCalledTypeHolder b = new BeanWithFieldCalledTypeHolder();
        b.bean = a;
        String expected = "{\"type\":\"" + BeanWithFieldCalledTypeHolder.class.getName() + "\"," +
                "\"bean\":{\"type\":\"not my type\"}" +
                "}";
        Assert.assertEquals(ser(b), expected);

        BeanWithFieldCalledTypeHolder b2 = deser(expected);
        Assert.assertEquals(b2.bean.type, "not my type");
    }

    /* lists with maps - some funny things */

    static final String LIST_MAP_TYPE_SAMPLE_BEAN = "[{\"type\":\"" + SampleBean.class.getName() + "\"}]";
    @Test
    public void testSerializeListMapWithTypeTwoWays() throws Exception {
        // these are the same, but that's normally okay, we should have context to help us out
        Assert.assertEquals(ser(new LinkedList<Object>(MutableList.of(new SampleBean()))),
                LIST_MAP_TYPE_SAMPLE_BEAN);
        Assert.assertEquals(ser(MutableList.of(MutableMap.of("type", SampleBean.class.getName()))),
                LIST_MAP_TYPE_SAMPLE_BEAN);

        // and same is true if we know it is a list
        Assert.assertEquals(ser(new LinkedList<Object>(MutableList.of(new SampleBean())), List.class),
                LIST_MAP_TYPE_SAMPLE_BEAN);
        Assert.assertEquals(ser(MutableList.of(MutableMap.of("type", SampleBean.class.getName())), List.class),
                LIST_MAP_TYPE_SAMPLE_BEAN);
    }

    // see AsPropertyIfAmbiguousTypeSerializer for discussion
    @Test
    public void testDeserializeListMapWithType() throws Exception {
        Object x;

        // here, if it's an object we are reading, we get a list containing a map
        x = deser(LIST_MAP_TYPE_SAMPLE_BEAN, Object.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement((List) x), Map.class);

        // but if we know it's a list then we are more aggressive about reading type information
        x = deser(LIST_MAP_TYPE_SAMPLE_BEAN, List.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement((List) x), SampleBean.class);
    }

    static class ListsGenericAndRaw {
        List<SampleBean> ls;
        List<Object> lo;
        List lr;
        Object o;
        List<Object> lo2;
    }
    @Test
    public void testDeserializeContextualListMapWithType() throws Exception {
        ListsGenericAndRaw o = deser("{\"ls\":"+LIST_MAP_TYPE_SAMPLE_BEAN+","+
                "\"lo\":"+LIST_MAP_TYPE_SAMPLE_BEAN+","+
                "\"lo2\":["+LIST_MAP_TYPE_SAMPLE_BEAN+"],"+
                "\"lr\":"+LIST_MAP_TYPE_SAMPLE_BEAN+","+
                "\"o\":"+LIST_MAP_TYPE_SAMPLE_BEAN+","+
                "\"lo2\":["+LIST_MAP_TYPE_SAMPLE_BEAN+"]"+
                "}", ListsGenericAndRaw.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement(o.ls), SampleBean.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement(o.lo), SampleBean.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement(o.lr), SampleBean.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement((List)o.o), Map.class);
        Asserts.assertInstanceOf(Iterables.getOnlyElement((List)Iterables.getOnlyElement(o.lo2)), Map.class);
    }

    /* arrays - no surprises - always read and written as lists, and coerced if context requires */

    static class ArrayWrapper {
        String[] o;
    }
    @Test
    public void testArray() throws Exception {
        ArrayWrapper o = new ArrayWrapper();
        o.o = new String[]{"a", "b"};
        String s = "[\"a\",\"b\"]";
        Assert.assertEquals(ser(o.o), s);
        Assert.assertEquals(deser(s), Arrays.asList("a", "b"));

        String aws = "{\"o\":" + s + "}";
        Assert.assertEquals(ser(o, ArrayWrapper.class), aws);
        ArrayWrapper x = deser(aws, ArrayWrapper.class);
        Assert.assertEquals(x.o, o.o);
    }

}
