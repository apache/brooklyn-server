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
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.json.BidiSerialization;
import org.apache.brooklyn.util.exceptions.Exceptions;
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

import java.io.NotSerializableException;
import java.time.Instant;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

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
        Asserts.assertEquals(nowYaml.trim(), "--- " + StringEscapes.JavaStringEscapes.wrapJavaString(Time.makeIso8601DateStringZ(now)));

        Object now2 = mapper.readerFor(Instant.class).readValue( Time.makeIso8601DateStringZ(now) );
        Asserts.assertEquals(now2, now);

        final String asMap = "type: "+StringEscapes.JavaStringEscapes.wrapJavaString(Instant.class.getName())+"\n"+
                "value: "+StringEscapes.JavaStringEscapes.wrapJavaString(Time.makeIso8601DateStringZ(now));
        nowYaml = mapper.writerFor(Object.class).writeValueAsString(now);
        Asserts.assertEquals(nowYaml.trim(), "---\n" + asMap);

        now2 = mapper.readerFor(Object.class).readValue( asMap );
        Asserts.assertEquals(now2, now);
    }

    @Test
    public void testDurationReadWrite() throws JsonProcessingException {
        ObjectMapper mapper = BeanWithTypeUtils.newYamlMapper(null, true, null, true);

        String vYaml = mapper.writerFor(Duration.class).writeValueAsString(Duration.minutes(90));
        Asserts.assertEquals(vYaml.trim(), "--- " + StringEscapes.JavaStringEscapes.wrapJavaString("1h 30m"));

        Object v2 = mapper.readerFor(Duration.class).readValue( "1h  30 m" );
        Asserts.assertEquals(v2, Duration.minutes(90));

        final String asMap = "type: "+StringEscapes.JavaStringEscapes.wrapJavaString(Duration.class.getName())+"\n"+
                "value: \"1h 30m\"";
        vYaml = mapper.writerFor(Object.class).writeValueAsString(Duration.minutes(90));
        Asserts.assertEquals(vYaml.trim(), "---\n" + asMap);

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
            Asserts.assertEquals(mgmtYaml.trim(), "--- " + StringEscapes.JavaStringEscapes.wrapJavaString(BrooklynJacksonSerializationUtils.DEFAULT));

            final String asMap = "type: \"org.apache.brooklyn.api.mgmt.ManagementContext\"\n"+
                    "value: "+ StringEscapes.JavaStringEscapes.wrapJavaString(BrooklynJacksonSerializationUtils.DEFAULT);
            mgmtYaml = mapper.writerFor(Object.class).writeValueAsString(mgmt);
            Asserts.assertEquals(mgmtYaml.trim(), "---\n" +
                    asMap);

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
            Asserts.assertEquals(eYaml.trim(), "--- " + StringEscapes.JavaStringEscapes.wrapJavaString(e1.getId()));

            final String asMap = "type: "+StringEscapes.JavaStringEscapes.wrapJavaString(BrooklynObject.class.getName())+"\n"+
                    "value: "+ StringEscapes.JavaStringEscapes.wrapJavaString(e1.getId());
            eYaml = mapper.writerFor(Object.class).writeValueAsString(e1);
            Asserts.assertEquals(eYaml.trim(), "---\n" +
                    asMap);

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
        String result = checkSerializesAs(ll, null);
        log.info("LLIST json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "[1,\"two\"]");
    }

    @Test
    public void testMultiMapSerialization() throws Exception {
        Multimap<String, Integer> m = MultimapBuilder.hashKeys().arrayListValues().build();
        m.put("bob", 24);
        m.put("bob", 25);
        String result = checkSerializesAs(m, null);
        log.info("multimap serialized as: " + result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "{\"bob\":[24,25]}");
    }

    @Test
    public void testUserHostAndPortSerialization() throws Exception {
        String result = checkSerializesAs(UserAndHostAndPort.fromParts("testHostUser", "1.2.3.4", 22), null);
        log.info("UserHostAndPort serialized as: " + result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
        Assert.assertEquals(Strings.collapseWhitespace(result, ""), "{\"user\":\"testHostUser\",\"hostAndPort\":{\"host\":\"1.2.3.4\",\"port\":22,\"hasBracketlessColons\":false}}");
    }

    @Test
    public void testSupplierSerialization() throws Exception {
        String result = checkSerializesAs(Strings.toStringSupplier(Streams.byteArrayOfString("x")), null);
        log.info("SUPPLIER json is: "+result);
        Assert.assertFalse(result.contains("error"), "Shouldn't have had an error, instead got: "+result);
    }

    @Test
    public void testWrappedStreamSerialization() throws Exception {
        String result = checkSerializesAs(BrooklynTaskTags.tagForStream("TEST", Streams.byteArrayOfString("x")), null);
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

    static class SpecHolder {
        String label;
        WrappedValue<EntitySpec> specT;
        WrappedValue<Object> specU;
    }
    @Test
    public void testEntitySpecSerialization() throws JsonProcessingException {
        ObjectMapper mapperY = BeanWithTypeUtils.newYamlMapper(null, true, null, true);
        ObjectMapper mapperJ = BeanWithTypeUtils.newMapper(null, true, null, true);

        SpecHolder in = mapperY.readerFor(SpecHolder.class).readValue(Strings.lines(
                "label: foo",
                "specT:",
                "  $brooklyn:entitySpec:",
                "    type: "+ TestEntity.class.getName()));
        // above creates a map because the DSL isn't recognised
        Asserts.assertInstanceOf(in.specT.get(), Map.class);

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
        Asserts.assertStringContains(out, EntitySpec.class.getName());
        in2 = mapperJ.readerFor(SpecHolder.class).readValue(out);
        // and we get a map
        Map map;
        map = (Map) in2.specU.get();
        Asserts.assertEquals( TypeCoercions.coerce(map, EntitySpec.class).getType(), TestEntity.class);
        // and same with yaml
        out = mapperY.writerFor(Object.class).writeValueAsString(in);
        Asserts.assertStringContains(out, EntitySpec.class.getName());
        in2 = mapperY.readerFor(SpecHolder.class).readValue(out);
        map = (Map) in2.specU.get();
        Asserts.assertEquals( TypeCoercions.coerce(map, EntitySpec.class).getType(), TestEntity.class);


        ManagementContext mgmt = LocalManagementContextForTests.newInstance();
        try {
            in.specT = WrappedValue.of( EntitySpec.create(TestEntity.class) );
            // and in a list, our deep conversion also works
            List<SpecHolder> inL = BeanWithTypeUtils.convert(mgmt, Arrays.asList(in), new TypeToken<List<SpecHolder>>() {}, true, null, true);
            // TODO would be nice to avoid the serialization cycle in the above

            in2 = inL.iterator().next();
            Asserts.assertEquals(in2.specT.get().getType(), TestEntity.class);
            // management mappers don't have the map artifice
            Asserts.assertEquals( ((EntitySpec) in2.specU.get()).getType(), TestEntity.class);
        } finally {
            Entities.destroyAll(mgmt);
        }
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

}
