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
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.reflect.TypeToken;
import java.io.IOException;
import java.util.Map;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

public class BrooklynMiscJacksonSerializationTest implements MapperTestFixture {

    private static final Logger LOG = LoggerFactory.getLogger(BrooklynMiscJacksonSerializationTest.class);

    private ObjectMapper mapper;

    public ObjectMapper mapper() {
        if (mapper==null) mapper = BeanWithTypeUtils.newMapper(null, false, null, true);
        return mapper;
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

}
