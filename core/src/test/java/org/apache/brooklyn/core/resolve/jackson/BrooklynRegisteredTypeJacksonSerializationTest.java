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
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.sensor.StaticSensor;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.core.workflow.WorkflowSensor;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Map;

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
            Asserts.shouldHaveFailedPreviously("Should have failed due to unknown type; instead got "+x);
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
        public String type;
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeField() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
        Object impl = deser("{\"type\":\""+ WorkflowSensor.class.getName()+"\""
                +",\"brooklyn.config\":{\"sensor\":{\"name\":\"mytestsensor\",\"type\":\"samplebean-with-type\"}}"
                +"}");
        Asserts.assertInstanceOf(impl, WorkflowSensor.class);
    }

    @Test
    public void testDeserializeEntityInitializerWithTypeFieldNeededForDeserialization() throws Exception {
        RegisteredType rt = BrooklynAppUnitTestSupport.addRegisteredTypeBean(mgmt, "samplebean-with-type", "1", SampleBeanWithType.class);
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
        Object impl = deser("{\"x\":\"mytestsensor\",\"@type\":\"samplebean-with-type\",\"type\":\"other\"}", Object.class);
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
