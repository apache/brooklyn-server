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

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.workflow.WorkflowSensor;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

public class BrooklynRegisteredTypeJacksonSerializationTest extends BrooklynMgmtUnitTestSupport implements MapperTestFixture {

    // public because of use of JavaClassNameTypePlanTransformer
    public static class SampleBean {
        public String x;
        String y;
        String z;
        SampleBean bean;
    }

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(mgmt(), true, null, true);
    }

    @Test
    public void testSerializeSampleBean() throws Exception {
        SampleBean a = new SampleBean();
        a.x = "hello";
        Assert.assertEquals(ser(a), "{\"type\":\""+SampleBean.class.getName()+"\",\"x\":\"hello\"}");
    }

    @Test
    public void testDeserializeSampleBean() throws Exception {
        Object impl = deser("{\"type\":\""+SampleBean.class.getName()+"\",\"x\":\"hello\"}");
        Asserts.assertInstanceOf(impl, SampleBean.class);
        Asserts.assertEquals(((SampleBean)impl).x, "hello");
    }

    @Test
    public void testDeserializeUnknownTypeFails() throws JsonProcessingException {
        try {
            Object x = BeanWithTypeUtils.newYamlMapper(mgmt, true, null, true).readValue("type: DeliberatelyMissing", Object.class);
            if (AsPropertyIfAmbiguous.THROW_ON_OBJECT_EXPECTED_AND_INVALID_TYPE_KEY_SUPPLIED) {
                Asserts.shouldHaveFailedPreviously("Should have failed due to unknown type; instead got " + x);
            } else {
                Asserts.assertInstanceOf(x, Map.class);
                Asserts.assertSize((Map)x, 1);
            }
        } catch (Exception e) {
            Asserts.expectedFailureContains(e, "DeliberatelyMissing");
        }
    }

    @Test
    public void testDeserializeAlias() throws Exception {
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt,"sample", "1", SampleBean.class);

        Object impl = deser("{\"type\":\"sample\",\"x\":\"hello\"}");
        Asserts.assertInstanceOf(impl, SampleBean.class);
        Asserts.assertEquals(((SampleBean)impl).x, "hello");
    }

    @Test
    public void testSimpleBeanRegisteredType() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "sample-hello", "1",
                new BasicTypeImplementationPlan(BeanWithTypeUtils.FORMAT,
                        "type: " + SampleBean.class.getName() + "\n" +
                                "x: hello\n" +
                                "y: hi"
                ));
        Object impl = mgmt().getTypeRegistry().create(rt, null, null);
        Asserts.assertInstanceOf(impl, SampleBean.class);
        Asserts.assertEquals(((SampleBean)impl).x, "hello");
        Asserts.assertEquals(((SampleBean)impl).y, "hi");

        impl = mgmt().getTypeRegistry().createBeanFromPlan(BeanWithTypePlanTransformer.FORMAT,
                    "type: sample-hello\n"+
                    "y: yo\n"+
                    "z: zzz", null, null);
        Asserts.assertInstanceOf(impl, SampleBean.class);
        Asserts.assertEquals(((SampleBean)impl).x, "hello");
        Asserts.assertEquals(((SampleBean)impl).y, "yo");
        Asserts.assertEquals(((SampleBean)impl).z, "zzz");
    }

    static class ListExtended extends MutableList<String> {
    }

    @Test
    public void testExtendedListBeanRegisteredType() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "list-extended", "1", ListExtended.class);

        Object impl = mgmt().getTypeRegistry().create(rt, null, null);
        Asserts.assertInstanceOf(impl, ListExtended.class);

        impl = mgmt().getTypeRegistry().createBeanFromPlan(BeanWithTypePlanTransformer.FORMAT,
                "type: list-extended"
                , null, null);
        Assert.assertTrue( ((ListExtended)impl).isEmpty() );

        Object impl2 = deser("[]", rt);
        Assert.assertTrue( ((ListExtended)impl2).isEmpty() );

        Object impl3 = deser("[3]", rt);
        Assert.assertEquals( ((ListExtended)impl3).size(), 1 );
    }

    @Test
    public void testDeserializeEntityInitializer() throws Exception {
        Object impl = deser("{\"type\":\""+ StaticSensor.class.getName()+"\""
                +",\"brooklyn.config\":{\"name\":\"mytestsensor\"}"
                +"}");
        Asserts.assertInstanceOf(impl, EntityInitializer.class);
    }

    public static class SampleBeanWithType extends SampleBean {
        @JsonInclude(JsonInclude.Include.NON_NULL)
        public String type;
    }

    @Test
    public void testSerializeAndDesSampleBeanWithType() throws Exception {
        // complications when the class has a field type -- how should the key 'type' be interpreted?
        // prefer (type) also accept @type need this first

        SampleBeanWithType a = new SampleBeanWithType();
        a.x = "hello";
        a.type = "T";
        String deser = "{\"(type)\":\"" + SampleBeanWithType.class.getName() + "\",\"x\":\"hello\",\"type\":\"T\"}";
        Assert.assertEquals(ser(a), deser);

        Object a2 = deser(deser);
        Asserts.assertInstanceOf(a2, SampleBeanWithType.class);
        Asserts.assertEquals(((SampleBeanWithType) a2).x, "hello");
        Asserts.assertEquals(((SampleBeanWithType) a2).type, "T");

        deser = Strings.replaceAllNonRegex(deser, "!", "@");  // @type accepted
        a2 = deser(deser);
        Asserts.assertInstanceOf(a2, SampleBeanWithType.class);
        Asserts.assertEquals(((SampleBeanWithType) a2).x, "hello");
        Asserts.assertEquals(((SampleBeanWithType) a2).type, "T");

        a.type = null;
        deser = "{\"(type)\":\"" + SampleBeanWithType.class.getName() + "\",\"x\":\"hello\"}";
        Assert.assertEquals(ser(a), deser);
        Asserts.assertEquals(((SampleBeanWithType) deser(deser)).x, "hello");
        Asserts.assertNull(((SampleBeanWithType) deser(deser)).type);

        // if type (not (type)) is first it treats it as the type, but warning about ambiguity
        deser = Strings.replaceAllNonRegex(deser, "!", "");
        Asserts.assertEquals(((SampleBeanWithType) deser(deser)).x, "hello");
        Asserts.assertNull(((SampleBeanWithType) deser(deser)).type);

        // if type is not first and refers to a class with a field 'type`, it treats it as a plain vanilla object (map)
        deser = "{\"x\":\"hello\",\"type\":\"" + SampleBeanWithType.class.getName() + "\"}";
        Asserts.assertInstanceOf(deser(deser), Map.class);
        // but for a bean without a field 'type', that is _not_ the case (but might change that in future)
        deser = "{\"x\":\"hello\",\"type\":\"" + SampleBean.class.getName() + "\"}";
        Asserts.assertInstanceOf(deser(deser), SampleBean.class);
    }

    @Test
    public void testDeserializeSampleBeanWithTooMuch() throws Exception {
        String deser = "{\"(type)\":\"" + SampleBeanWithType.class.getName() + "\",\"x\":\"hello\",\"xx\":\"not_supported\"}";
        Asserts.assertFailsWith(() -> deser(deser),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "unrecognized field", "xx"));
    }

    public static class OtherBean {
        public String x;
        public String other;
    }

    @Test
    public void testDeserializeSampleBeanWithOtherType() throws Exception {
        String deser = "{\"type\":\"" + OtherBean.class.getName() + "\",\"x\":\"hello\"}";
        Asserts.assertInstanceOf(deser(deser, SampleBeanWithType.class), SampleBeanWithType.class);
        Asserts.assertInstanceOf(deser(deser, OtherBean.class), OtherBean.class);
        Asserts.assertInstanceOf(deser(deser), OtherBean.class);

        String deser2 = "{\"type\":\"" + OtherBean.class.getName() + "\",\"other\":\"hello\"}";
        Asserts.assertFailsWith(()->deser(deser2, SampleBeanWithType.class), e->true);
        Asserts.assertInstanceOf(deser(deser2, OtherBean.class), OtherBean.class);
        Asserts.assertInstanceOf(deser(deser2), OtherBean.class);

        String deser3 = "{\"type\":\"" + OtherBean.class.getName() + "\",\"y\":\"hello\"}";
        Asserts.assertFailsWith(()->deser(deser3, OtherBean.class), e->true);  // expected as y doesn't exist on OtherBean
        Asserts.assertInstanceOf(deser(deser3, SampleBeanWithType.class), SampleBeanWithType.class);
        Asserts.assertInstanceOf(deser(deser3), Map.class);
        Asserts.assertEquals( ((Map)deser(deser3)).get("type"), OtherBean.class.getName());

        // we have no choice but to fallback to map deserialization
        // however we should allow further coercion to Map (in case we read as typed something which should have been a map)
        // and also coercion that serializes input if complex type then deserializes to intended type, if the intended type has a field 'type'

        // coercing to a map doesn't mean serializing it, that's too silly
//        Map redeserMap = TypeCoercions.coerce(deser(deser), Map.class);
//        Asserts.assertEquals(redeserMap.get("type"), OtherBean.class.getName());

        SampleBeanWithType redeserObj = TypeCoercions.coerce(deser(deser), SampleBeanWithType.class);
        Asserts.assertEquals(redeserObj.x, "hello");
    }

    static class FancyHolder {
        Map map;
        Object obj;

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            FancyHolder mapHolder = (FancyHolder) o;
            return Objects.equals(map, mapHolder.map) && Objects.equals(obj, mapHolder.obj);
        }
    }

    static class FancyMap extends LinkedHashMap {
    }

    @Test
    public void testSerializeAndDesMapWithTypeEntry() throws Exception {
        // if a map is _expected_ at least we should ignore the field type
        FancyHolder a = new FancyHolder();
        a.map = MutableMap.of("type", "not_a_type");

        {
            String expectedSerialization = "{\"map\":{\"type\":\"not_a_type\"}}";
            Assert.assertEquals(ser(a, FancyHolder.class), expectedSerialization);
            Assert.assertEquals(deser(expectedSerialization, FancyHolder.class), a);
        }

        {
            FancyMap f = new FancyMap();
            f.put("a",1);
            // types ignored when serializing any subclass of map; we could change if we want (but make MutableMap the default)
            Assert.assertEquals(ser(f), "{\"a\":1}");
        }

        {
            String expectedDeserialization = "{\"type\":\""+ BrooklynRegisteredTypeJacksonSerializationTest.FancyHolder.class.getName() +"\"," +
                    "\"map\":{\"type\":\"not_a_type\"}}";
            Assert.assertEquals(ser(a), expectedDeserialization);
            Assert.assertEquals(deser(expectedDeserialization), a);
        }

        a.map = null;
        a.obj = MutableMap.of("type", BrooklynRegisteredTypeJacksonSerializationTest.FancyHolder.class.getName());
        {
            // it is expected that a map containing a type comes back as an instance of that type if possible
            String expectedSerialization = "{\"obj\":{\"type\":\"" + BrooklynRegisteredTypeJacksonSerializationTest.FancyHolder.class.getName() + "\"}}";
            Assert.assertEquals(ser(a, FancyHolder.class), expectedSerialization);
            Asserts.assertThat(deser(expectedSerialization, FancyHolder.class).obj, r -> r instanceof FancyHolder);

            // an unknown field should undo that
            ((Map) a.obj).put("unfield", "should_force_map");
            Asserts.assertThat(deser(ser(a, FancyHolder.class), FancyHolder.class), r -> {
                Asserts.assertInstanceOf(r.obj, Map.class);
                Asserts.assertEquals(((Map) r.obj).get("type"), BrooklynRegisteredTypeJacksonSerializationTest.FancyHolder.class.getName());
                Asserts.assertEquals(((Map) r.obj).get("unfield"), "should_force_map");
                return true;
            });
        }

        if (!AsPropertyIfAmbiguous.THROW_ON_OBJECT_EXPECTED_AND_INVALID_TYPE_KEY_SUPPLIED) {
            // all the below will throw if the above is true;
            // see other tests that reference the constant above

            // and an unknown field also forces a map
            ((Map) a.obj).put("type", "not_a_type");
            Asserts.assertThat(deser(ser(a, FancyHolder.class), FancyHolder.class), r -> {
                Asserts.assertInstanceOf(r.obj, Map.class);
                Asserts.assertEquals(((Map) r.obj).get("type"), "not_a_type");
                Asserts.assertEquals(((Map) r.obj).get("unfield"), "should_force_map");
                return true;
            });
            // also with unambiguous name
            ((Map) a.obj).put("@type", "not_a_type");
            Asserts.assertThat(deser(ser(a, FancyHolder.class), FancyHolder.class), r -> {
                Asserts.assertInstanceOf(r.obj, Map.class);
                Asserts.assertEquals(((Map) r.obj).get("type"), "not_a_type");
                Asserts.assertEquals(((Map) r.obj).get("unfield"), "should_force_map");
                return true;
            });

            // what if it's an Object where we don't know the type? probably we should throw, we always have in the past, but we could allow
            Map b = MutableMap.of("type", "not_a_type");
            Asserts.assertEquals(ser(b), "{\"type\":\"not_a_type\"}");
            Asserts.assertEquals(deser(ser(b)), MutableMap.of("type", "not_a_type"));
        }

    }

    @Test
    public void testDeserializeEntityInitializerWithTypeField() throws Exception {
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        Object impl = deser("{\"type\":\""+ WorkflowSensor.class.getName()+"\""
                +",\"brooklyn.config\":{\"sensor\":{\"name\":\"mytestsensor\",\"type\":\"samplebean-with-type\"}}"
                +"}");
        Asserts.assertInstanceOf(impl, WorkflowSensor.class);
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeFieldInParent() throws Exception {
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean", "1", SampleBean.class);
        Object impl = deser("{\"type\":\""+ WorkflowSensor.class.getName()+"\""
                +",\"brooklyn.config\":{\"x\":\"y\""
                +",\"sensor\":{\"name\":\"mytestsensor\",\"type\":\"samplebean\"}"
                +"}}");
        Asserts.assertInstanceOf(impl, WorkflowSensor.class);
    }

    @Test
    public void testDeserializeEntityInitializerFromCatalogWithTypeField() throws Exception {
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "workflow-sensor", "1", WorkflowSensor.class);
        Object impl = deser("{\"type\":\"workflow-sensor\""
                +",\"brooklyn.config\":{\"sensor\":{\"name\":\"mytestsensor\",\"type\":\"samplebean-with-type\"}}"
                +"}");
        Asserts.assertInstanceOf(impl, WorkflowSensor.class);
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeFieldNeededForDeserialization() throws Exception {
        BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        // 'type' field used to give type because the expected type does not expect 'type'
        Object impl = deser("{\"x\":\"mytestsensor\",\"type\":\"samplebean-with-type\"}", SampleBean.class);
        Asserts.assertInstanceOf(impl, SampleBeanWithType.class);
        Asserts.assertEquals( ((SampleBean)impl).x, "mytestsensor");
        Asserts.assertEquals( ((SampleBeanWithType)impl).type, null);
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeFieldUsedForDeserialization() throws Exception {
        // 'type' field used to give type because it is the very first thing in an object, e.g. in a catalog definition
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        Object impl = deser("{\"type\":\"samplebean-with-type\",\"x\":\"mytestsensor\"}", Object.class);
        Asserts.assertInstanceOf(impl, SampleBeanWithType.class);
        Asserts.assertEquals( ((SampleBean)impl).x, "mytestsensor");
        Asserts.assertEquals( ((SampleBeanWithType)impl).type, null);
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeFieldNotUsedForDeserialization() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        Object impl = deser("{\"x\":\"mytestsensor\",\"type\":\"samplebean-with-type\"}", Object.class);
        // above cases don't apply, we get a map with both set as fields
        Asserts.assertInstanceOf(impl, Map.class);
        Asserts.assertEquals( ((Map)impl).get("x"), "mytestsensor");
        Asserts.assertEquals( ((Map)impl).get("type"), "samplebean-with-type");
    }

    @Test
    public void testDeserializeEntityInitializerWithUnambiguousTypeFieldUsedForDeserialization() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        Object impl = deser("{\"x\":\"mytestsensor\",\"(type)\":\"samplebean-with-type\",\"type\":\"other\"}", Object.class);
        // above cases don't apply, we get a map with both set as fields
        Asserts.assertInstanceOf(impl, SampleBeanWithType.class);
        Asserts.assertEquals( ((SampleBean)impl).x, "mytestsensor");
        Asserts.assertEquals( ((SampleBeanWithType)impl).type, "other");
    }

    @Test
    public void testConfigBagSerialization() throws Exception {
        ConfigBag bag = ConfigBag.newInstance();
        bag.put(ConfigKeys.newConfigKey(String.class, "stringTypedKey"), "foo");
        bag.putStringKey("stringUntypedKey", "bar");
        bag.putStringKey("intUntypedKey", 2);
        bag.getStringKey("stringUntypedKey");

        String out = ser(bag);
        Assert.assertEquals(out, "{\"type\":\"org.apache.brooklyn.util.core.config.ConfigBag\",\"config\":{\"stringTypedKey\":\"foo\",\"stringUntypedKey\":\"bar\",\"intUntypedKey\":2},\"unusedConfig\":{\"stringTypedKey\":\"foo\",\"intUntypedKey\":2},\"live\":false,\"sealed\":false}");

        ConfigBag in = (ConfigBag) deser(out);
        // used and unused is serialized
        Asserts.assertSize(in.getUnusedConfig(), 2);
        Asserts.assertEquals(in.getAllConfig(), bag.getAllConfig());
    }

    @Test
    public void testEmptyDuration() {
        Object impl = mgmt().getTypeRegistry().createBeanFromPlan(BeanWithTypePlanTransformer.FORMAT,
                "type: "+ Duration.class.getName(), null, null);
        Asserts.assertInstanceOf(impl, Duration.class);
    }

}
