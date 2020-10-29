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
import java.util.function.Supplier;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue.WrappedValuesInitialized;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.flags.TypeCoercions;
import org.apache.brooklyn.util.core.flags.TypeCoercions.BrooklynCommonAdaptorTypeCoercions;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.javalang.JavaClassNames;
import org.testng.Assert;
import org.testng.annotations.Test;

public class WrappedValuesSerializationTest implements MapperTestFixture {

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(null, false, null, true);
    }

    // basic serialization / deserialization of wrapped values
    static class ObjectWithWrappedValueString extends WrappedValuesInitialized {
        private WrappedValue<String> x;
    }

    @Test
    public void testDeserializeSimpleWrappedValue() throws Exception {
        Object impl = deser("{\"type\":\""+ ObjectWithWrappedValueString.class.getName()+"\",\"x\":\"hello\"}");
        Asserts.assertEquals(((ObjectWithWrappedValueString)impl).x.get(), "hello");
        impl = deser("{\"type\":\""+ ObjectWithWrappedValueString.class.getName()+"\",\"x\":\"4\"}");
        Asserts.assertEquals(((ObjectWithWrappedValueString)impl).x.get(), "4");
    }

    @Test
    public void testDeserializeSimpleWrappedValueWhenTypeKnown() throws Exception {
        ObjectWithWrappedValueString impl = deser("{\"x\":\"hello\"}", ObjectWithWrappedValueString.class);
        Asserts.assertEquals(impl.x.get(), "hello");
    }

    @Test
    public void testSerializeSimpleWrappedValue() throws Exception {
        ObjectWithWrappedValueString a = new ObjectWithWrappedValueString();
        a.x = WrappedValue.of("hello");
        Assert.assertEquals(a.x.get(), "hello");
        Assert.assertEquals(ser(a),
                "{\"type\":\""+ ObjectWithWrappedValueString.class.getName()+"\",\"x\":\"hello\"}");
    }

    // null values populated as wrapped null and omitted on serialization

    @Test
    public void testDeserializeSetsWrappedNull() throws Exception {
        ObjectWithWrappedValueString impl = deser("{}", ObjectWithWrappedValueString.class);
        Asserts.assertNotNull(impl.x);
        Asserts.assertNull(impl.x.get());
    }

    @Test
    public void testSerializeWrappedNullOmits() throws Exception {
        ObjectWithWrappedValueString a = new ObjectWithWrappedValueString();
        a.x = WrappedValue.of(null);
        Assert.assertEquals(ser(a, ObjectWithWrappedValueString.class), "{}");
    }

    // when supplier is used

    static class HelloSupplier implements Supplier<String> {
        @Override
        public String get() {
            return "hello";
        }
    }

    @Test
    public void testSerializeSupplier() throws Exception {
        ObjectWithWrappedValueString a = new ObjectWithWrappedValueString();
        a.x = WrappedValue.of(new HelloSupplier());
        Assert.assertEquals(a.x.get(), "hello");
        Assert.assertEquals(ser(a),
                "{\"type\":\""+ ObjectWithWrappedValueString.class.getName()+"\",\"x\":" +
                        "{\"type\":\""+HelloSupplier.class.getName()+"\"}" +
                        "}");
    }

    static class ObjectWithWrappedValueObject {
        private WrappedValue<ObjectWithWrappedValueString> x;
    }

    @Test
    public void testWrappedValueObject() throws Exception {
        ObjectWithWrappedValueObject b = new ObjectWithWrappedValueObject();
        b.x = WrappedValue.of(new ObjectWithWrappedValueString());
        b.x.get().x = WrappedValue.of("hello");
        String expected = "{" +
                "\"type\":\"" + ObjectWithWrappedValueObject.class.getName() + "\"," +
                "\"x\":" +
                "{" +
                // "\"type\":\"" + ObjectWithWrappedValueString.class.getName() + "\"," +    // suppressed now because it's implied!
                "\"x\":\"hello\"}" +
                "}";
        Assert.assertEquals(ser(b), expected);
        ObjectWithWrappedValueObject b2 = deser(expected);
        Assert.assertEquals(b2.x.get().x.get(), "hello");
    }

    @Test
    public void testWrappedValueCoercion() throws Exception {
        ObjectWithWrappedValueString a = new ObjectWithWrappedValueString();

        a.x = WrappedValue.of("hello");
        Assert.assertEquals(TypeCoercions.coerce(a.x, String.class), "hello");
        Assert.assertEquals(TypeCoercions.coerce(a.x, Object.class), a.x);
        Assert.assertEquals(TypeCoercions.coerce(a.x, WrappedValue.class), a.x);

        a.x = WrappedValue.ofSupplier((Supplier) () -> "hello");
        Asserts.assertNotPresent(TypeCoercions.tryCoerce(a.x, String.class));
    }

    @Test
    public void testWrappedValueResolution() throws Exception {
        ObjectWithWrappedValueString a = new ObjectWithWrappedValueString();

        a.x = WrappedValue.of("hello");
        Assert.assertEquals(resolve(a.x, String.class).get(), "hello");
        Assert.assertEquals(resolve(a.x, Object.class).get(), "hello");
        Assert.assertEquals(resolve(a.x, WrappedValue.class).get().get(), "hello");

        a.x = WrappedValue.ofSupplier((Supplier) () -> "hello");
        Asserts.assertEquals(resolve(a.x, String.class).get(), "hello");
        Asserts.assertEquals(resolve(a.x, Object.class).get(), "hello");
        Asserts.assertEquals(resolve(a.x, WrappedValue.class).get().get(), "hello");
    }

    protected <T> Maybe<T> resolve(Object o, Class<T> type) {
        BasicExecutionManager execManager = new BasicExecutionManager("test-context-"+ JavaClassNames.niceClassAndMethod());
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);

        return Tasks.resolving(o).as(type).context(execContext).deep().getMaybe();
    }

}
