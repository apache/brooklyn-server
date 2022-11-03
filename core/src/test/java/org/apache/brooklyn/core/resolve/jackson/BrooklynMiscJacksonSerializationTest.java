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
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.units.ByteSize;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class BrooklynMiscJacksonSerializationTest implements MapperTestFixture {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynMiscJacksonSerializationTest.class);

    private ObjectMapper mapper;

    public ObjectMapper mapper() {
        if (mapper==null) mapper = BeanWithTypeUtils.newMapper(null, false, null, true);
        return mapper;
    }

    @BeforeMethod(alwaysRun = true)
    public void setUp() throws Exception {
        mapper = null;
    }

    // baseline

    static class EmptyObject {}

    @Test
    public void testMapperDoesntBreakBasicThings() throws Exception {
        Asserts.assertEquals(deser("\"hello\""), "hello");
        Asserts.assertInstanceOf(deser("{\"type\":\""+EmptyObject.class.getName()+"\"}"), EmptyObject.class);
    }

    @Test
    public void testMapperAllowsBrooklynTypeCoercionsOfStrings() throws Exception {
        Asserts.assertEquals(deser("\"1m\"", Duration.class), Duration.minutes(1));
    }

    static class ObjForSerializingAsReference {
        String foo;

        @Override
        public String toString() {
            return "Obj{" +
                    "foo='" + foo + '\'' +
                    "}@"+ System.identityHashCode(this);
        }
    }

    @Test
    public void testCustomHandlerForReferences() throws Exception {
        mapper = YAMLMapper.builder().build();
        mapper = BeanWithTypeUtils.applyCommonMapperConfig(mapper, null, false, null, true);
        mapper = new ObjectReferencingSerialization().useAndApplytoMapper(mapper);

        ObjForSerializingAsReference f1 = new ObjForSerializingAsReference(); f1.foo = "1";
        ObjForSerializingAsReference f2 = new ObjForSerializingAsReference(); f2.foo = "2";
        String out = ser(MutableMap.of("a", f1, "b", f2, "c", f1));
        LOG.info("Result of "+ JavaClassNames.niceClassAndMethod()+": "+out);

        Map in = deser(out,
                Map.class
//                new TypeToken<Map<String, ObjForSerializingAsReference>>() {}
        );
        ObjForSerializingAsReference a = (ObjForSerializingAsReference)in.get("a");
        ObjForSerializingAsReference b = (ObjForSerializingAsReference)in.get("b");
        ObjForSerializingAsReference c = (ObjForSerializingAsReference)in.get("c");
        Asserts.assertTrue(a.foo.equals(c.foo), "expected same foo value for a and c - "+a+" != "+c);
        Asserts.assertTrue(!b.foo.equals(c.foo), "expected different foo value for a and b");
        Asserts.assertTrue(a == c, "expected same instance for a and c - "+a+" != "+c);
        Asserts.assertTrue(a != b, "expected different instance for a and b");
    }

    @Test
    public void testObjectReferences() throws IOException {
        ObjForSerializingAsReference f1 = new ObjForSerializingAsReference(); f1.foo = "1";
        Object f2 = new ObjectReferencingSerialization().serializeAndDeserialize(f1);
        Asserts.assertEquals(f1, f2);
        Asserts.assertTrue(f1==f2, "different instances for "+f1+" and "+f2);
    }


    @Test
    public void testDurationCustomSerialization() throws Exception {
        mapper = BeanWithTypeUtils.newSimpleYamlMapper();

        // need these two to get the constructor stuff we want (but _not_ the default duration support)
        BrooklynRegisteredTypeJacksonSerialization.apply(mapper, null, false, null, true);
        WrappedValuesSerialization.apply(mapper, null);

        Assert.assertEquals(ser(Duration.FIVE_SECONDS, Duration.class), "nanos: 5000000000");
        Assert.assertEquals(deser("nanos: 5000000000", Duration.class), Duration.FIVE_SECONDS);

        Asserts.assertFailsWith(() -> deser("5s", Duration.class),
                e -> e.toString().contains("Duration"));


        // custom serializer added as part of standard mapper construction

        mapper = BeanWithTypeUtils.newYamlMapper(null, false, null, true);

        Assert.assertEquals(deser("5s", Duration.class), Duration.FIVE_SECONDS);
        Assert.assertEquals(deser("nanos: 5000000000", Duration.class), Duration.FIVE_SECONDS);

        Assert.assertEquals(ser(Duration.FIVE_SECONDS, Duration.class), "5s");
    }


    public static class DateTimeBean {
        String x;
        Date juDate;
//        LocalDateTime localDateTime;
        GregorianCalendar calendar;
        Instant instant;
    }

    @Test
    public void testDateTimeInRegisteredTypes() throws Exception {
        mapper = BeanWithTypeUtils.newYamlMapper(null, false, null, true);
//        mapper.findAndRegisterModules();

        DateTimeBean impl = new DateTimeBean();
        Asserts.assertEquals(ser(impl, DateTimeBean.class), "{}" );

        impl.x = "foo";

        impl.juDate = new Date(60*1000);
//        impl.localDateTime = LocalDateTime.of(2020, 1, 1, 12, 0, 0, 0);
        impl.calendar = new GregorianCalendar(TimeZone.getTimeZone("GMT"), Locale.ROOT);
        impl.calendar.set(2020, 0, 1, 12, 0, 0);
        impl.calendar.set(GregorianCalendar.MILLISECOND, 0);
        impl.instant = impl.calendar.toInstant();
        Asserts.assertEquals(ser(impl, DateTimeBean.class), Strings.lines(
                "x: foo",
                "juDate: 1970-01-01T00:01:00.000Z",
//                "localDateTime: 2020-01-01T12:00:00",
                "calendar: 2020-01-01T12:00:00.000+00:00",
                "instant: 2020-01-01T12:00:00.000Z"));

        // ones commented out cannot be parsed
        DateTimeBean impl2 = deser(Strings.lines(
                "x: foo",
                "juDate: 1970-01-01T00:01:00.000+00:00",
//                "localDateTime: \"2020-01-01T12:00:00\"",
//                "calendar: \"2020-01-01T12:00:00.000+00:00\"",
                "instant: 2020-01-01T12:00:00Z",
                ""
        ), DateTimeBean.class);
        Assert.assertEquals( impl2.x, impl.x );
        Assert.assertEquals( impl2.juDate, impl.juDate );
//        Assert.assertEquals( impl2.localDateTime, impl.localDateTime );
//        Assert.assertEquals( impl2.calendar, impl.calendar );
        Assert.assertEquals( impl2.instant, impl.instant );
    }

    @Test
    public void testFailsOnTrailing() throws Exception {
        try {
            Duration d = mapper().readValue("1 m", Duration.class);
            Asserts.shouldHaveFailedPreviously("Instead got: " + d);
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "Unrecognized token 'm'");
        }
    }

    @Test
    public void testStringBean() throws Exception {
        Duration d = mapper().readValue("\"1m\"", Duration.class);
        Asserts.assertEquals(d, Duration.ONE_MINUTE);
    }

    @Test
    public void testStringByteSize() throws Exception {
        ByteSize x = mapper().readValue("\"1b\"", ByteSize.class);
        Asserts.assertEquals(x, ByteSize.fromString("1b"));
    }

}
