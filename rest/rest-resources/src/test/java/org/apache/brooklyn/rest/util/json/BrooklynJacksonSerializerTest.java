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
package org.apache.brooklyn.rest.util.json;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Multimap;
import com.google.common.collect.MultimapBuilder;
import com.google.common.reflect.TypeToken;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.api.objs.BrooklynObject;
import org.apache.brooklyn.camp.brooklyn.BrooklynCampPlatform;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.BrooklynDslDeferredSupplier;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.DslUtils;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.camp.spi.PlatformRootSummary;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.resolve.jackson.*;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.core.workflow.WorkflowStepDefinition;
import org.apache.brooklyn.core.workflow.steps.flow.ReturnWorkflowStep;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.json.BidiSerialization;
import org.apache.brooklyn.util.core.units.ByteSize;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.javalang.Reflections;
import org.apache.brooklyn.util.net.UserAndHostAndPort;
import org.apache.brooklyn.util.stream.Streams;
import org.apache.brooklyn.util.text.StringEscapes;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.Time;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.NotSerializableException;
import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class BrooklynJacksonSerializerTest {

    private static final Logger log = LoggerFactory.getLogger(BrooklynJacksonSerializerTest.class);
    
    public static class SillyClassWithManagementContext {
        @JsonProperty
        ManagementContext mgmt;
        @JsonProperty
        String id;
        
        public SillyClassWithManagementContext() { }
        
        public SillyClassWithManagementContext(String id, ManagementContext mgmt) {
            this.id = id;
            this.mgmt = mgmt;
        }

        @Override
        public String toString() {
            return super.toString()+"[id="+id+";mgmt="+mgmt+"]";
        }
    }

    @Test
    public void testCustomSerializerWithSerializableSillyManagementExample() throws Exception {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {

            ObjectMapper mapper = BrooklynJacksonJsonProvider.newPrivateObjectMapper(mgmt);

            SillyClassWithManagementContext silly = new SillyClassWithManagementContext("123", mgmt);
            log.info("silly is: "+silly);

            String sillyS = mapper.writeValueAsString(silly);

            log.info("silly json is: "+sillyS);

            SillyClassWithManagementContext silly2 = mapper.readValue(sillyS, SillyClassWithManagementContext.class);
            log.info("silly2 is: "+silly2);

            Assert.assertEquals(silly.id, silly2.id);
            
        } finally {
            Entities.destroyAll(mgmt);
        }
    }
    
    public static class SelfRefNonSerializableClass {
        @JsonProperty
        Object bogus = this;
    }

    @Test
    public void testSelfReferenceFailsWhenStrict() {
        checkNonSerializableWhenStrict(new SelfRefNonSerializableClass());
    }
    @Test
    public void testSelfReferenceGeneratesErrorMapObject() throws Exception {
        checkSerializesAsMapWithErrorAndToString(new SelfRefNonSerializableClass());
    }
    @Test
    public void testNonSerializableInListIsShownInList() throws Exception {
        List<?> result = checkSerializesAs(MutableList.of(1, new SelfRefNonSerializableClass()), List.class);
        Assert.assertEquals( result.get(0), 1 );
        Assert.assertEquals( ((Map<?,?>)result.get(1)).get("errorType"), NotSerializableException.class.getName() );
    }
    @Test
    public void testNonSerializableInMapIsShownInMap() throws Exception {
        Map<?,?> result = checkSerializesAs(MutableMap.of("x", new SelfRefNonSerializableClass()), Map.class);
        Assert.assertEquals( ((Map<?,?>)result.get("x")).get("errorType"), NotSerializableException.class.getName() );
    }
    static class TupleWithNonSerializable {
        String good = "bon";
        SelfRefNonSerializableClass bad = new SelfRefNonSerializableClass();
    }
    @Test
    public void testNonSerializableInObjectIsShownInMap() throws Exception {
        String resultS = checkSerializesAs(new TupleWithNonSerializable(), null);
        log.info("nested non-serializable json is "+resultS);
        Assert.assertTrue(resultS.startsWith("{\"good\":\"bon\",\"bad\":{"), "expected a nested map for the error field, not "+resultS);
        
        Map<?,?> result = checkSerializesAs(new TupleWithNonSerializable(), Map.class);
        Assert.assertEquals( result.get("good"), "bon" );
        Assert.assertTrue( result.containsKey("bad"), "Should have had a key for field 'bad'" );
        Assert.assertEquals( ((Map<?,?>)result.get("bad")).get("errorType"), NotSerializableException.class.getName() );
    }
    
    public static class EmptyClass {
    }

    @Test
    public void testEmptySerializesAsEmpty() throws Exception {
        // deliberately, a class with no fields and no annotations serializes as an error,
        // because the alternative, {}, is useless.  however if it *is* annotated, as below, then it will serialize fine.
        checkSerializesAsMapWithErrorAndToString(new SelfRefNonSerializableClass());
    }
    @Test
    public void testEmptyNonSerializableFailsWhenStrict() {
        checkNonSerializableWhenStrict(new EmptyClass());
    }

    @JsonSerialize
    public static class EmptyClassWithSerialize {
    }

    @Test
    public void testEmptyAnnotatedSerializesAsEmptyEvenWhenStrict() throws Exception {
        try {
            BidiSerialization.setStrictSerialization(true);
            testEmptyAnnotatedSerializesAsEmpty();
        } finally {
            BidiSerialization.clearStrictSerialization();
        }
    }
    
    @Test
    public void testEmptyAnnotatedSerializesAsEmpty() throws Exception {
        Map<?, ?> map = checkSerializesAs( new EmptyClassWithSerialize(), Map.class );
        Assert.assertTrue(map.isEmpty(), "Expected an empty map; instead got: "+map);

        String result = checkSerializesAs( MutableList.of(new EmptyClassWithSerialize()), null );
        result = result.replaceAll(" ", "").trim();
        Assert.assertEquals(result, "[{}]");
    }

    @Test
    public void testInstantReadWrite() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        Instant now = Instant.now();

        String nowYaml = mapper.writerFor(Instant.class).writeValueAsString(now);
        Asserts.assertEquals(nowYaml.trim(), Time.makeIso8601DateStringZ(now));

        String nowS = Time.makeIso8601DateStringZ(now);
        Object now2 = mapper.readerFor(Instant.class).readValue(nowS);
        Asserts.assertEquals(now2, now, "Now deserialized differently, from '"+nowS+"' (from "+now+") to "+now2);

        final String asMap = "type: "+Instant.class.getName()+"\n"+
                "value: "+Time.makeIso8601DateStringZ(now);
        nowYaml = mapper.writerFor(Object.class).writeValueAsString(now);
        Asserts.assertEquals(nowYaml.trim(), asMap);

        now2 = mapper.readerFor(Object.class).readValue( asMap );
        Asserts.assertEquals(now2, now);
    }

    @Test
    public void testInstantReadWriteBuggyTime() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        // this evaluates as 32.3549999999999999 seconds due to floating point operations;
        // test ensures we round it appropriately
        String nowS = "2023-04-07T10:28:32.355Z";
        Asserts.assertEquals(nowS, Time.parseCalendarMaybe(nowS).get().toInstant().toString());

        Instant now2 = mapper.readerFor(Instant.class).readValue(nowS);
        Asserts.assertEquals(now2.toString(), nowS);
    }


    @Test
    public void testNumberReadWriteYaml() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        Map<String, ?> x = MutableMap.of("int", 1, "double", 1.0d);
        String xYaml = mapper.writer().writeValueAsString(x);
        Asserts.assertEquals(xYaml.trim(), "int: 1\ndouble: 1.0");

        Object x2 = mapper.readerFor(Object.class).readValue(xYaml);
        Asserts.assertInstanceOf(x2, Map.class);
        Asserts.assertSize( (Map)x2, 2 );
        Asserts.assertEquals( ((Map)x2).get("int"), 1 );
        Asserts.assertThat( ((Map)x2).get("double"), v -> v.equals(1.0d) || v.equals(BigDecimal.valueOf(1.0d)) );
    }

    @Test
    public void testNumberReadWriteJson() throws JsonProcessingException {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            ObjectMapper mapper = BrooklynJacksonJsonProvider.findAnyObjectMapper(mgmt);

            Map<String, ?> x = MutableMap.of("int", 1, "double", 1.0d);
            String xYaml = mapper.writer().writeValueAsString(x);
            Asserts.assertEquals(xYaml.trim(), "{\"int\":1,\"double\":1.0}");

            Object x2 = mapper.readerFor(Object.class).readValue(xYaml);
            Asserts.assertInstanceOf(x2, Map.class);
            Asserts.assertSize((Map) x2, 2);
            Asserts.assertEquals(((Map) x2).get("int"), 1);
            Asserts.assertThat(((Map) x2).get("double"), v -> v.equals(1.0d) || v.equals(BigDecimal.valueOf(1.0d)));

            Asserts.assertEquals(mapper.writer().writeValueAsString(new BigDecimal("1.01")), "1.01");

        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testDurationReadWrite() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        String vYaml = mapper.writerFor(Duration.class).writeValueAsString(Duration.minutes(90));
        Asserts.assertEquals(vYaml.trim(), "1h 30m");

        Object v2 = mapper.readerFor(Duration.class).readValue( "1h  30 m" );
        Asserts.assertEquals(v2, Duration.minutes(90));

        final String asMap = "type: "+Duration.class.getName()+"\n"+
                "value: 1h 30m";
        vYaml = mapper.writerFor(Object.class).writeValueAsString(Duration.minutes(90));
        Asserts.assertEquals(vYaml.trim(), asMap);

        v2 = mapper.readerFor(Object.class).readValue( asMap );
        Asserts.assertEquals(v2, Duration.minutes(90));
    }

    @Test
    public void testEmptyDurationCreation() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        Object v2 = mapper.readerFor(Object.class).readValue(  "type: "+StringEscapes.JavaStringEscapes.wrapJavaString(Duration.class.getName()) );
        Asserts.assertEquals(v2, Duration.of(0));
    }

    @Test
    public void testManagementContextReadWrite() throws JsonProcessingException {
        ManagementContext mgmt = null;
        try {
            mgmt = LocalManagementContextForTests.newInstance();
            ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true);

            Object mgmt2 = mapper.readerFor(ManagementContext.class).readValue(BrooklynJacksonSerializationUtils.DEFAULT);
            Asserts.assertEquals(mgmt2, mgmt);

            String mgmtYaml = mapper.writerFor(ManagementContext.class).writeValueAsString(mgmt);
            Asserts.assertEquals(mgmtYaml.trim(), BrooklynJacksonSerializationUtils.DEFAULT);

            final String asMap = "type: org.apache.brooklyn.api.mgmt.ManagementContext\n"+
                    "value: "+ BrooklynJacksonSerializationUtils.DEFAULT;
            mgmtYaml = mapper.writerFor(Object.class).writeValueAsString(mgmt);
            Asserts.assertEquals(mgmtYaml.trim(), asMap);

            mgmt2 = mapper.readerFor(Object.class).readValue(asMap);
            Asserts.assertEquals(mgmt2, mgmt);
        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testEntityReadWrite() throws JsonProcessingException {
        ManagementContext mgmt = null;
        try {
            mgmt = LocalManagementContextForTests.newInstance();
            ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true);

            BasicApplication e1 = mgmt.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));

            Object e2 = mapper.readerFor(BrooklynObject.class).readValue(e1.getId());
            Asserts.assertEquals(e2, e1);

            String eYaml = mapper.writerFor(Entity.class).writeValueAsString(e1);
            Asserts.assertEquals(eYaml.trim(), e1.getId());

            final String asMap = "type: "+Entity.class.getName()+"\n"+
                    "value: "+ e1.getId();
            eYaml = mapper.writerFor(Object.class).writeValueAsString(e1);
            Asserts.assertEquals(eYaml.trim(), asMap);

            e2 = mapper.readerFor(Object.class).readValue(asMap);
            Asserts.assertEquals(e2, e1);

            e2 = mapper.readerFor(Entity.class).readValue(e1.getId());
            Asserts.assertEquals(e2, e1);

        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testSensorFailsWhenStrict() {
        checkNonSerializableWhenStrict(MutableList.of(Attributes.HTTP_PORT));
    }
    @Test
    public void testSensorSensible() throws Exception {
        Map<?,?> result = checkSerializesAs(Attributes.HTTP_PORT, Map.class);
        log.info("SENSOR json is: "+result);
        Assert.assertFalse(result.toString().contains("error"), "Shouldn't have had an error, instead got: "+result);
    }

    @Test
    public void testLinkedListSerialization() throws Exception {
        LinkedList<Object> ll = new LinkedList<Object>();
        ll.add(1); ll.add("two");
        String result = checkSerializes(ll);
        log.info("LLIST json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "[1,\"two\"]");
    }

    @Test
    public void testInstantSerialization() throws Exception {
        Instant x = Instant.now().minusSeconds(5);
        String xs = checkSerializes(x);
        log.info("Instant json is: "+xs);
        Assert.assertFalse(xs.contains("error"), "Shouldn't have had an error, instead got: "+xs);
        Instant x2 = checkSerializesAs(x, Instant.class);
        // instants sometimes round by one ms, as noted elsewhere in other tests
        Asserts.assertThat(Math.abs(x2.toEpochMilli() - x.toEpochMilli()), diff -> diff < 2);
    }

    @Test
    public void testMultiMapSerialization() throws Exception {
        Multimap<String, Integer> m = MultimapBuilder.hashKeys().arrayListValues().build();
        m.put("bob", 24);
        m.put("bob", 25);
        String result = checkSerializes(m);
        log.info("multimap serialized as: " + result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "{\"bob\":[24,25]}");
    }

    @Test
    public void testUserHostAndPortSerialization() throws Exception {
        String result = checkSerializes(UserAndHostAndPort.fromParts("testHostUser", "1.2.3.4", 22));
        log.info("UserHostAndPort serialized as: " + result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "{\"user\":\"testHostUser\",\"hostAndPort\":{\"host\":\"1.2.3.4\",\"port\":22,\"hasBracketlessColons\":false}}");
    }

    @Test
    public void testSupplierSerialization() throws Exception {
        ByteArrayOutputStream x = Streams.byteArrayOfString("x");
        String result = checkSerializes(Strings.toStringSupplier(x));
        log.info("SUPPLIER json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
    }

    @Test
    public void testByteArrayOutputStreamSerialization() throws Exception {
        ByteArrayOutputStream x = Streams.byteArrayOfString("x");
        String result = checkSerializes(x);
        log.info("BAOS json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        ByteArrayOutputStream x2 = checkSerializesAs(x, ByteArrayOutputStream.class);
        Asserts.assertEquals(x2.toString(), "x");
    }

    @Test
    public void testWrappedStreamSerialization() throws Exception {
        String result = checkSerializes(BrooklynTaskTags.tagForStream("TEST", Streams.byteArrayOfString("x")));
        log.info("WRAPPED STREAM json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
    }

    @Test
    public void testGuavaTypeTokenSerialization() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        // fails with SO if we haven't intercepted
        String out = mapper.writerFor(Object.class).writeValueAsString(TypeToken.of(Object.class));

        Asserts.assertStringContains(out, Object.class.getName());

        Object tt2 = mapper.readerFor(Object.class).readValue(out);
        Asserts.assertInstanceOf(tt2, TypeToken.class);
        Asserts.assertEquals(((TypeToken)tt2).getRawType(), Object.class);
        Asserts.assertEquals(tt2.toString(), "java.lang.Object");
    }

    @Test
    public void testComplexGuavaTypeTokenSerialization() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        TypeToken<List<List<String>>> tt = new TypeToken<List<List<String>>>() {};
        // fails with SO if we haven't intercepted
        String out = mapper.writerFor(Object.class).writeValueAsString(tt);

        Asserts.assertStringContains(out, List.class.getName());
        Asserts.assertStringContains(out, String.class.getName());

        Object tt2 = mapper.readerFor(Object.class).readValue(out);
        Asserts.assertInstanceOf(tt2, TypeToken.class);
        Asserts.assertEquals(tt2.toString(), "java.util.List<java.util.List<java.lang.String>>");
    }

    // also see DslSerializationTest.ObjectWithWrappedValueSpec
    static class SpecHolder {
        String label;
        EntitySpec<?> spec;
        WrappedValue<EntitySpec<?>> specT;
        WrappedValue<Object> specU;
    }

    @Test
    public void testEntitySpecSerialization() throws JsonProcessingException {
        // also see DslSerializationTest.ObjectWithWrappedValueSpec
        ObjectMapper mapperY = BeanWithTypeUtils.newYamlMapper(null, true, null, true);
        ObjectMapper mapperJ = BeanWithTypeUtils.newMapper(null, true, null, true);

        SpecHolder in = mapperY.readerFor(SpecHolder.class).readValue(Strings.lines(
                "label: foo",
                "specT:",
                "  $brooklyn:entitySpec:",
                "    type: "+ TestEntity.class.getName()));

//        Asserts.assertInstanceOf(in.specT.get(), Map.class);
        // used to do above, create a map because the DSL isn't recognised with non-management mapper; now it sets it as a flag
        Asserts.assertInstanceOf(in.specT.get(), EntitySpec.class);
        Asserts.assertNull(in.specT.get().getType());
        Asserts.assertEquals(in.specT.get().getFlags().get("$brooklyn:entitySpec"), MutableMap.of("type", TestEntity.class.getName()));

        String out;
        SpecHolder in2;

        in.specT = WrappedValue.of( EntitySpec.create(TestEntity.class) );
        out = mapperJ.writerFor(Object.class).writeValueAsString(in);
        // strongly typed - not contained
        Asserts.assertStringDoesNotContain(out, EntitySpec.class.getName());
        in2 = mapperJ.readerFor(SpecHolder.class).readValue(out);
        Asserts.assertEquals(in2.specT.get().getType(), TestEntity.class);
        // and same with yaml
        out = mapperY.writerFor(Object.class).writeValueAsString(in);
        Asserts.assertStringDoesNotContain(out, EntitySpec.class.getName());
        in2 = mapperY.readerFor(SpecHolder.class).readValue(out);
        Asserts.assertEquals(in2.specT.get().getType(), TestEntity.class);

        in.specT = null;
        in.specU = WrappedValue.of( EntitySpec.create(TestEntity.class) );
        out = mapperJ.writerFor(Object.class).writeValueAsString(in);
        // not strongly typed typed - type is contained, but we get a map which still we need to coerce
        Asserts.assertStringContains(out, "\"(type)\":\""+EntitySpec.class.getName()+"\"");
        Asserts.assertStringContains(out, "\"type\":\""+TestEntity.class.getName()+"\"");

        in2 = mapperJ.readerFor(SpecHolder.class).readValue(out);
        // and we used to get a map
//        Map map = (Map) in2.specU.get();
//        Asserts.assertEquals( TypeCoercions.coerce(map, EntitySpec.class).getType(), TestEntity.class);
        // but now the type is populated
        Asserts.assertEquals( ((EntitySpec)in2.specU.get()).getType(), TestEntity.class);

        // and same with yaml
        out = mapperY.writerFor(Object.class).writeValueAsString(in);
//        Asserts.assertStringContains(out, "\"(type)\":\""+EntitySpec.class.getName()+"\"");
        Asserts.assertStringContains(out, EntitySpec.class.getName());

        // when we used tags this was done, but we don't use tags (this mapper supports it but snake yaml does not)
        // see org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils.newSimpleYamlMapper(boolean)
//        Asserts.assertStringContains(out, "!<"+EntitySpec.class.getName()+">");
        Asserts.assertStringContains(out, "type: "+TestEntity.class.getName());

        in2 = mapperY.readerFor(SpecHolder.class).readValue(out);
//        map = (Map) in2.specU.get();
//        Asserts.assertEquals( TypeCoercions.coerce(map, EntitySpec.class).getType(), TestEntity.class);
        Asserts.assertEquals( ((EntitySpec)in2.specU.get()).getType(), TestEntity.class);

        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            in.specT = WrappedValue.of( EntitySpec.create(TestEntity.class) );

            BiConsumer<List<SpecHolder>,Boolean> test = (inL, expectMap) -> {
                SpecHolder inX = inL.iterator().next();
                if (!expectMap) {
                    Asserts.assertEquals(inX.specT.get().getType(), TestEntity.class);
                }

                // management mappers don't have the map artifice (unless type is unknown and doing deep conversion)
                Object sp = inX.specU.get();
                if (Boolean.TRUE.equals(expectMap)) Asserts.assertInstanceOf(sp, Map.class);
                if (Boolean.FALSE.equals(expectMap)) Asserts.assertInstanceOf(sp, EntitySpec.class);
                Object v = null;
                if (sp instanceof Map) {
                    sp = TypeCoercions.coerce(sp, EntitySpec.class);
                }
                Asserts.assertEquals(((EntitySpec)sp).getType(), TestEntity.class);
            };

            // and in a list, deep and shallow conversion both work
            test.accept( BeanWithTypeUtils.convertDeeply(mgmt, Arrays.asList(in), new TypeToken<List<SpecHolder>>() {}, true, null, true),
//                    /* deep conversion serializes and for untyped key it doesn't know the type to deserialize, until it is explicitly coerced, so we get a map */ true
                    false /* now it does know the type */
                    );
            test.accept( BeanWithTypeUtils.convertShallow(mgmt, Arrays.asList(in), new TypeToken<List<SpecHolder>>() {}, true, null, true),
                    /* shallow conversion should preserve the object */ false );
            test.accept( BeanWithTypeUtils.convert(mgmt, Arrays.asList(in), new TypeToken<List<SpecHolder>>() {}, true, null, true),
//                    /* overall convert tries deep first */ true
                    false
                    );

            // but if it's a wrapped _object_ then we get a map back
            List<Map<String, Map<String, String>>> in3 = Arrays.asList(MutableMap.of("specU", MutableMap.of("type", TestEntity.class.getName())));
            test.accept( BeanWithTypeUtils.convertDeeply(mgmt, in3, new TypeToken<List<SpecHolder>>() {}, true, null, true),
                true);
            test.accept( BeanWithTypeUtils.convertShallow(mgmt, in3, new TypeToken<List<SpecHolder>>() {}, true, null, true),
                    true);
            test.accept( BeanWithTypeUtils.convert(mgmt, in3, new TypeToken<List<SpecHolder>>() {}, true, null, true),
                    true);
        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testEntitySpecNonDslDeserialization() throws JsonProcessingException {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            ObjectMapper mapperY = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true);
            // note, this does JSON deserialization of the entity spec, which is not the same as CAMP parsing, though not hugely far away;
            // use $brooklyn:entitySpec if the CAMP deserialization is needed
            SpecHolder in = mapperY.readerFor(SpecHolder.class).readValue(Strings.lines(
                "label: foo",
                "spec:",
                "  type: "+ TestEntity.class.getName(),
                "  name: TestEntity",
                "specT:",
                "  type: "+ TestEntity.class.getName(),
                "  name: TestEntity",
                "specU:",
                "  type: "+ TestEntity.class.getName(),
                "  name: TestEntity"));
            Asserts.assertInstanceOf(in.specT.get(), EntitySpec.class);
            Asserts.assertInstanceOf(in.spec, EntitySpec.class);
            Asserts.assertInstanceOf(in.specU.get(), Map.class);

        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testStringCoercedRegisteredType() throws Exception {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            mgmt.getCatalog().addTypesAndValidateAllowInconsistent(
                    "brooklyn.catalog:\n" +
                            "  id: test\n" +
                            "  version: 1.0.0-SNAPSHOT\n" +
                            "  items:\n" +
                            "  - id: byte-size\n" +
                            "    format: bean-with-type\n" +
                            "    item:\n" +
                            "      type: org.apache.brooklyn.util.core.units.ByteSize\n", null, false);
            BrooklynJacksonType bst = BrooklynJacksonType.of(mgmt.getTypeRegistry().get("byte-size"));

            Consumer<ObjectMapper> test = mapper -> {
                ByteSize bs = null;
                try {
                    bs = mapper.readValue("\"1k\"", bst);
                } catch (JsonProcessingException e) {
                    throw Exceptions.propagate(e);
                }
                Assert.assertEquals(bs.getBytes(), 1024);
            };

//            test.accept(BrooklynJacksonJsonProvider.newPrivateObjectMapper(mgmt));
//            test.accept(BeanWithTypeUtils.newYamlMapper(null, false, null, true));
//            // above work, but below fails as it tries to instantiate then populate as a bean
            test.accept(BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true));

        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    protected String checkSerializes(Object x) {
        return checkSerializesAs(x, null);
    }

    @SuppressWarnings("unchecked")
    protected <T> T checkSerializesAs(Object x, Class<T> type) {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            ObjectMapper mapper = BrooklynJacksonJsonProvider.newPrivateObjectMapper(mgmt);
            String tS = mapper.writeValueAsString(x);
            log.debug("serialized "+x+" as "+tS);
            Assert.assertTrue(tS.length() < 1000, "Data too long, size "+tS.length()+" for "+x);
            if (type==null) return (T) tS;
            return mapper.readValue(tS, type);
        } catch (Exception e) {
            throw Exceptions.propagate(e);
        } finally {
            Entities.destroyAll(mgmt);
        }
    }
    protected Map<?,?> checkSerializesAsMapWithErrorAndToString(Object x) {
        Map<?,?> rt = checkSerializesAs(x, Map.class);
        Assert.assertEquals(rt.get("toString"), x.toString());
        Assert.assertEquals(rt.get("error"), Boolean.TRUE);
        return rt;
    }
    protected void checkNonSerializableWhenStrict(Object x) {
        checkNonSerializable(x, true);
    }
    protected void checkNonSerializable(Object x, boolean strict) {
        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            ObjectMapper mapper = BrooklynJacksonJsonProvider.newPrivateObjectMapper(mgmt);
            if (strict)
                BidiSerialization.setStrictSerialization(true);
            
            String tS = mapper.writeValueAsString(x);
            Assert.fail("Should not have serialized "+x+"; instead gave: "+tS);
            
        } catch (Exception e) {
            Exceptions.propagateIfFatal(e);
            log.info("Got expected error, when serializing "+x+": "+e);
            
        } finally {
            if (strict)
                BidiSerialization.clearStrictSerialization();
            Entities.destroyAll(mgmt);
        }
    }

    public static class ObjectWithWrappedValueStringAndAnother extends WrappedValuesSerializationTest.ObjectWithWrappedValueString {
        public String y;
    }

    @Test
    public void testDslSupplier() throws Exception {
        LocalManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            BrooklynCampPlatform platform = new BrooklynCampPlatform(
                    PlatformRootSummary.builder().name("Brooklyn CAMP Platform").build(),
                    mgmt)
                    .setConfigKeyAtManagmentContext();
            TestEntity entity = mgmt.getEntityManager().createEntity(EntitySpec.create(TestEntity.class).configure("fk", "foo"));
            Object dsl = DslUtils.parseBrooklynDsl(mgmt, "$brooklyn:config(\"fk\")");
            ObjectWithWrappedValueStringAndAnother obj = new ObjectWithWrappedValueStringAndAnother();
            obj.x = WrappedValue.of(dsl);
            obj.y = "yyy";

            mgmt.getExecutionContext(entity).submit("resolve", () -> Asserts.assertEquals(obj.x.get(), "foo")).get();

            List result = BeanWithTypeUtils.convert(mgmt, MutableList.of(obj), TypeToken.of(List.class), true, null, true);

            mgmt.getExecutionContext(entity).submit("resolve", () -> Asserts.assertEquals(
                    ((WrappedValuesSerializationTest.ObjectWithWrappedValueString) result.get(0)).x.get(), "foo")).get();

            // if dsl field is cleared, we can still read it, this time using class instantiation
            Field dslField = Reflections.findField(dsl.getClass(), "dsl");
            dslField.setAccessible(true);
            dslField.set(dsl, null);

            dslField = Reflections.findField(((DslComponent.DslConfigSupplier)dsl).getComponent().getClass(), "dsl");
            dslField.setAccessible(true);
            dslField.set( ((DslComponent.DslConfigSupplier)dsl).getComponent() , null);

            List result2 = BeanWithTypeUtils.convert(mgmt, MutableList.of(obj), TypeToken.of(List.class), true, null, true);

            mgmt.getExecutionContext(entity).submit("resolve", () -> Asserts.assertEquals(
                    ((WrappedValuesSerializationTest.ObjectWithWrappedValueString) result2.get(0)).x.get(), "foo")).get();

            // (above test was to investigate a situation where ultimately a downstream "shorthand" serializer was using
            // findRootValueDeserializer rather than findContextualValueDeserializer, so not attending to our custom type info rules;
            // the test is useful however, so has been added
        } finally {
            Entities.destroyAll(mgmt);
        }
    }

    @Test
    public void testDslDeepConversionIntoMap() throws Exception {
        LocalManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            BrooklynCampPlatform platform = new BrooklynCampPlatform(
                    PlatformRootSummary.builder().name("Brooklyn CAMP Platform").build(),
                    mgmt)
                    .setConfigKeyAtManagmentContext();
            BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "return", "1", ReturnWorkflowStep.class);

            Object v = DslUtils.parseBrooklynDsl(mgmt, "$brooklyn:config(\"x\")");

            Object obj = MutableMap.of("type", "return", "value", MutableMap.of("n", v));
            WorkflowStepDefinition result = BeanWithTypeUtils.convert(mgmt, obj, TypeToken.of(WorkflowStepDefinition.class), true, null, true);
            Asserts.assertInstanceOf(result, ReturnWorkflowStep.class);
            Object n = ((Map) result.getInput().get("value")).get("n");
            Asserts.assertInstanceOf(n, BrooklynDslDeferredSupplier.class);

        } finally {
            Entities.destroyAll(mgmt);
        }
    }
}
