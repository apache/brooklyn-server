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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yaml.Yamls;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class JacksonJsonShorthandDeserializerTest {

    private static final Logger LOG = LoggerFactory.getLogger(JacksonJsonShorthandDeserializerTest.class);

    static class X0 {
        @JsonDeserialize(using = JsonShorthandDeserializer.class)
        X x;
    }

    static class MapT<T> {
        @JsonDeserialize(contentUsing = JsonShorthandDeserializer.class)
        Map<String,T> map;
        void setMap(Map<String,T> map) { this.map = map; }
    }

    static class MapX extends MapT<X> {}

    static class X {

//        @JsonCreator
        X()          {        o = "noa"; }
//        @JsonCreator
//        X(String x)  { v = x; o = "str"; }
//        @JsonCreator
//        X(Integer x) { v = x; o = "int"; }

        Object v;
        String o = "orig";

        @JsonShorthandDeserializer.JsonShorthandInstantiator
        public static X fromShorthand(Object o) {
            X x = new X();
            x.v = o;
            x.o = "sho";
            return x;
        }
    }

    static class V {
        static V of(int v) {
            V result = new V();
            result.v = v;
            return result;
        }
        Object v;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            V v1 = (V) o;
            return Objects.equals(v, v1.v);
        }

        @Override
        public int hashCode() {
            return Objects.hash(v);
        }
    }

    @Test
    public void testX() throws JsonProcessingException {
        check("foo", "foo", "sho");
        check(""+1, 1, "sho");
        check("{ type: "+ V.class.getName()+", v: 2 }", V.of(2), "sho");  // or obj?
        check("{ v: 3 }", 3, "noa");
        check("{ u: 4 }", MutableMap.of("u", 4), "sho");
        check("{ type: "+X.class.getName()+", v: 5 }", 5, "noa");  // or obj?
    }

    private static void check(String xJson, Object v, String o) throws JsonProcessingException {
        X0 x0 = BeanWithTypeUtils.newMapper(null, false, null, true).readerFor(Object.class).readValue(
                Jsonya.newInstance().add(Yamls.parseAll(Strings.lines(
                        "type: "+X0.class.getName(),
                        "x: "+xJson
                )).iterator().next()).toString()
        );

        Assert.assertEquals(x0.x.v, v);
        Assert.assertEquals(x0.x.o, o);


        MapX xm = BeanWithTypeUtils.newMapper(null, false, null, true).readerFor(MapX.class).readValue(
                Jsonya.newInstance().add(Yamls.parseAll(Strings.lines(
                        "map:",
                        "  x: "+xJson
                )).iterator().next()).toString()
        );

        Assert.assertEquals(xm.map.get("x").v, v);
        Assert.assertEquals(xm.map.get("x").o, o);
    }

    public static class DS implements DeferredSupplier<String> {
        String s;
        public String get() { return s; }
    }

    public static class W {
        WrappedValue<String> w;

        @JsonShorthandDeserializer.JsonShorthandInstantiator
        public static W fromShorthand(WrappedValue<String> o) {
            W w = new W();
            w.w = o;
            return w;
        }
    }

    public static class MWH {
        MapT<W> holder;
    }

    @Test
    public void convertShallowOrDeep() throws JsonProcessingException {
        Consumer<MapX> check = r -> {
            Asserts.assertEquals(r.map.get("a").v, "vv");
            Asserts.assertEquals(r.map.get("a").o, "sho");
        };

        MapX r;

        r = BeanWithTypeUtils.convertDeeply(null, MutableMap.of("map", MutableMap.of("a", "vv")), TypeToken.of(MapX.class), false, null, false);
        check.accept(r);
        r = BeanWithTypeUtils.convertShallow(null, MutableMap.of("map", MutableMap.of("a", "vv")), TypeToken.of(MapX.class), false, null, false);
        check.accept(r);

        DS ds = new DS(); ds.s = "vv";
        ObjectMapper mapper = BeanWithTypeUtils.newMapper(null, false, null, true);
        MapT<W> mw = new MapT<>();
        W w = W.fromShorthand(WrappedValue.of(ds));
        mw.setMap(MutableMap.of("a", w));
        MWH h = new MWH();
        h.holder = mw;
        String hs = mapper.writeValueAsString(h);
        Asserts.assertEquals(hs, "{\"holder\":{\"map\":{\"a\":{\"w\":{\"type\":\"org.apache.brooklyn.core.resolve.jackson.JacksonJsonShorthandDeserializerTest$DS\",\"s\":\"vv\"}}}}}");
        MWH h2 = mapper.readValue(hs, MWH.class);
        Asserts.assertEquals(h2.holder.map.get("a").w.get(), "vv");

        LocalManagementContext mgmt = LocalManagementContextForTests.newInstance();
        MapT<W> r2;
        r2 = BeanWithTypeUtils.convertShallow(mgmt, MutableMap.of("map", MutableMap.of("a", ds)), new TypeToken<MapT<W>>() {}, true, null, true);
        Asserts.assertEquals(r2.map.get("a").w.get(), "vv");

        r2 = BeanWithTypeUtils.convertDeeply(mgmt, MutableMap.of("map", MutableMap.of("a", ds)), new TypeToken<MapT<W>>() {}, true, null, true);
        Asserts.assertEquals(r2.map.get("a").w.get(), "vv");
    }
}
