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

import com.fasterxml.jackson.annotation.JsonIdentityInfo;
import com.fasterxml.jackson.annotation.ObjectIdGenerator;
import com.fasterxml.jackson.annotation.ObjectIdGenerator.IdKey;
import com.fasterxml.jackson.annotation.ObjectIdGenerators.StringIdGenerator;
import com.fasterxml.jackson.annotation.ObjectIdResolver;
import com.fasterxml.jackson.databind.DeserializationConfig;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.KeyDeserializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationConfig;
import com.fasterxml.jackson.databind.cfg.HandlerInstantiator;
import com.fasterxml.jackson.databind.cfg.MapperConfig;
import com.fasterxml.jackson.databind.introspect.Annotated;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.databind.jsontype.TypeIdResolver;
import com.fasterxml.jackson.databind.jsontype.TypeResolverBuilder;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.common.reflect.TypeToken;
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

    static class ObjWithoutIdentityInfoAnnotation {
        String foo;

        @Override
        public String toString() {
            return "Obj{" +
                    "foo='" + foo + '\'' +
                    "}@"+ System.identityHashCode(this);
        }
    }

    public static class AllBeansIdentityHandler extends HandlerInstantiator {
        @Override
        public JsonDeserializer<?> deserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> deserClass) {
            return null;
        }
        @Override
        public KeyDeserializer keyDeserializerInstance(DeserializationConfig config, Annotated annotated, Class<?> keyDeserClass) {
            return null;
        }
        @Override
        public JsonSerializer<?> serializerInstance(SerializationConfig config, Annotated annotated, Class<?> serClass) {
            return null;
        }
        @Override
        public TypeResolverBuilder<?> typeResolverBuilderInstance(MapperConfig<?> config, Annotated annotated, Class<?> builderClass) {
            return null;
        }
        @Override
        public TypeIdResolver typeIdResolverInstance(MapperConfig<?> config, Annotated annotated, Class<?> resolverClass) {
            return null;
        }

        @Override
        public ObjectIdGenerator<?> objectIdGeneratorInstance(MapperConfig<?> config, Annotated annotated, Class<?> implClass) {
            return new StringIdGenerator();
        }

        @Override
        public ObjectIdResolver resolverIdGeneratorInstance(MapperConfig<?> config, Annotated annotated, Class<?> implClass) {
            return new MapBasedInstanceResolver();
        }
    }

    static class MapBasedInstanceResolver implements ObjectIdResolver {

        Map<IdKey,Object> objectsById = MutableMap.of();

        @Override
        public void bindItem(IdKey id, Object pojo) {
            objectsById.put(id, pojo);
        }

        @Override
        public Object resolveId(IdKey id) {
            Object result = objectsById.get(id);
            if (result!=null) return result;
            // seems to happen for YAMLMapper, it doesn't call bindItem
            LOG.warn("No object recorded for ID "+id+"; returning null during deserialization");
            return null;
        }

        @Override
        public ObjectIdResolver newForDeserialization(Object context) {
            return this;
        }

        @Override
        public boolean canUseFor(ObjectIdResolver resolverType) {
            return true;
        }
    }

    @JsonIdentityInfo(property="@object_id", generator=StringIdGenerator.class, resolver= MapBasedInstanceResolver.class)
    static class ObjWithIdentityInfoAnnotation extends ObjWithoutIdentityInfoAnnotation {}

    @Test
    public void testHowObjectIdAndReferences() throws Exception {
        mapper =
                BeanWithTypeUtils.applyCommonMapperConfig(
                    JsonMapper.builder().build()

                // YAML doesn't seem to call "bindItem" whereas JSON mapper does
//                    YAMLMapper.builder().
////                        configure(YAMLGenerator.Feature.USE_NATIVE_OBJECT_ID, true).
//                        build()

                    , null, false, null, true)
        ;
//        mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        ObjWithIdentityInfoAnnotation f1 = new ObjWithIdentityInfoAnnotation(); f1.foo = "1";
        ObjWithIdentityInfoAnnotation f2 = new ObjWithIdentityInfoAnnotation(); f2.foo = "2";
        String out = ser(MutableMap.of("a", f1, "b", f2, "c", f1));
        LOG.info("Result of "+ JavaClassNames.niceClassAndMethod()+": "+out);

        Map in = deser(out,
//                Map.class
                new TypeToken<Map<String, ObjWithIdentityInfoAnnotation>>() {}
                );
        ObjWithIdentityInfoAnnotation a = (ObjWithIdentityInfoAnnotation)in.get("a");
        ObjWithIdentityInfoAnnotation b = (ObjWithIdentityInfoAnnotation)in.get("b");
        ObjWithIdentityInfoAnnotation c = (ObjWithIdentityInfoAnnotation)in.get("c");
        Asserts.assertTrue(a.foo.equals(c.foo), "expected same foo value for a and c - "+a+" != "+c);
        Asserts.assertTrue(!b.foo.equals(c.foo), "expected different foo value for a and b");
        Asserts.assertTrue(a == c, "expected same instance for a and c - "+a+" != "+c);
        Asserts.assertTrue(a != b, "expected different instance for a and b");
    }

    @Test
    public void testCustomHandlerForReferences() throws Exception {
        mapper = new ObjectReferencingSerialization().useMapper(
                BeanWithTypeUtils.applyCommonMapperConfig(
                    YAMLMapper.builder()
//                        .handlerInstantiator(new AllBeansIdentityHandler())
                        .build()
                , null, false, null, true));

        ObjWithoutIdentityInfoAnnotation f1 = new ObjWithoutIdentityInfoAnnotation(); f1.foo = "1";
        ObjWithoutIdentityInfoAnnotation f2 = new ObjWithoutIdentityInfoAnnotation(); f2.foo = "2";
        String out = ser(MutableMap.of("a", f1, "b", f2, "c", f1));
        LOG.info("Result of "+ JavaClassNames.niceClassAndMethod()+": "+out);

        Map in = deser(out,
//                Map.class
                new TypeToken<Map<String, ObjWithoutIdentityInfoAnnotation>>() {}
        );
        ObjWithoutIdentityInfoAnnotation a = (ObjWithoutIdentityInfoAnnotation)in.get("a");
        ObjWithoutIdentityInfoAnnotation b = (ObjWithoutIdentityInfoAnnotation)in.get("b");
        ObjWithoutIdentityInfoAnnotation c = (ObjWithoutIdentityInfoAnnotation)in.get("c");
        Asserts.assertTrue(a.foo.equals(c.foo), "expected same foo value for a and c - "+a+" != "+c);
        Asserts.assertTrue(!b.foo.equals(c.foo), "expected different foo value for a and b");
        Asserts.assertTrue(a == c, "expected same instance for a and c - "+a+" != "+c);
        Asserts.assertTrue(a != b, "expected different instance for a and b");
    }

}
