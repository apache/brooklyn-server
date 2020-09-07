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
import org.apache.brooklyn.api.typereg.RegisteredType;
import org.apache.brooklyn.core.test.BrooklynMgmtUnitTestSupport;
import org.apache.brooklyn.core.typereg.BasicBrooklynTypeRegistry;
import org.apache.brooklyn.core.typereg.BasicTypeImplementationPlan;
import org.apache.brooklyn.core.typereg.JavaClassNameTypePlanTransformer;
import org.apache.brooklyn.core.typereg.RegisteredTypes;
import org.apache.brooklyn.test.Asserts;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrooklynRegisteredTypeJacksonSerializationTest extends BrooklynMgmtUnitTestSupport implements MapperTestFixture {

    // public because of use of JavaClassNameTypePlanTransformer
    public static class SampleBean {
        String x;
        String y;
        String z;
        SampleBean bean;
    }

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(mgmt());
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
    public void testDeserializeAlias() throws Exception {
        RegisteredType rt = RegisteredTypes.bean("sample", "1",
                new BasicTypeImplementationPlan(JavaClassNameTypePlanTransformer.FORMAT, SampleBean.class.getName()));
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);

        Object impl = deser("{\"type\":\"sample\",\"x\":\"hello\"}");
        Asserts.assertInstanceOf(impl, SampleBean.class);
        Asserts.assertEquals(((SampleBean)impl).x, "hello");
    }

    @Test
    public void testSimpleBeanRegisteredType() throws Exception {
        RegisteredType rt = RegisteredTypes.bean("sample-hello", "1",
                new BasicTypeImplementationPlan(BeanWithTypeUtils.FORMAT,
                        "type: " + SampleBean.class.getName() + "\n" +
                        "x: hello\n" +
                        "y: hi"
        ));
        ((BasicBrooklynTypeRegistry)mgmt().getTypeRegistry()).addToLocalUnpersistedTypeRegistry(rt, false);
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

}
