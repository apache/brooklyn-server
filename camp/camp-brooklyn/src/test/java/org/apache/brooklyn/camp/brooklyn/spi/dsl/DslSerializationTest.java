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
package org.apache.brooklyn.camp.brooklyn.spi.dsl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Optional;
import java.util.Map;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.Assert;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.testng.annotations.Test;

@Test
public class DslSerializationTest extends AbstractYamlTest {

    @Test
    public void testSerializeAttributeWhenReady() throws Exception {
        BrooklynDslDeferredSupplier<?> awr = new DslComponent(Scope.GLOBAL, "entity_id").attributeWhenReady("my_sensor");
        ObjectMapper mapper = BeanWithTypeUtils.newMapper(null, false, null, true);

        String out = mapper.writerFor(Object.class).writeValueAsString(awr);
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object supplier2 = mapper.readValue(out, Object.class);
        Asserts.assertInstanceOf(supplier2, BrooklynDslDeferredSupplier.class);
    }

    @Test
    public void testSerializeDslConfigSupplierInMapObject() throws Exception {
        BrooklynDslDeferredSupplier<?> awr = new DslComponent(Scope.GLOBAL, "entity_id").config("my_config");
        Map<String,Object> stuff = MutableMap.<String,Object>of("stuff", awr);

        ObjectMapper mapper = BeanWithTypeUtils.newMapper(null, false, null, true);

        String out = mapper.writerFor(Object.class).writeValueAsString(stuff);
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object stuff2 = mapper.readValue(out, Object.class);
        Object stuff2I = ((Map<?, ?>) stuff2).get("stuff");
        Asserts.assertInstanceOf(stuff2I, BrooklynDslDeferredSupplier.class);
    }

    @Test
    public void testSerializeDslConfigSupplierInWrappedValue() throws Exception {
        BrooklynDslDeferredSupplier<?> awr =
                (BrooklynDslDeferredSupplier<?>) DslUtils.resolveBrooklynDslValue("$brooklyn:component(\"entity_id\").config(\"my_config\")", null, mgmt(), null).get();
                //new DslComponent(Scope.GLOBAL, "entity_id").config("my_config");
        WrappedValue<Object> stuff = WrappedValue.of(awr);

        ObjectMapper mapper = BeanWithTypeUtils.newMapper(null, false, null, true);

        String out = mapper.writeValueAsString(stuff);
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        WrappedValue<?> stuff2 = mapper.readValue(out, WrappedValue.class);
        Asserts.assertInstanceOf(stuff2.getSupplier(), BrooklynDslDeferredSupplier.class);
    }

    @Test
    public void testSerializeDslLiteral() throws Exception {
        /*
        options:
        - ensure DSL only converted at the last mile
        - ensure DSL read/written as string when reading object
        - ensure DSL readable from type even when reading object <- if has $brooklyn:literal set then it will
         */
        String unwrappedDesiredValue = JavaStringEscapes.wrapJavaString("$brooklyn:literal(" + JavaStringEscapes.wrapJavaString("foo") + ")");
        Optional<Object> l = DslUtils.resolveBrooklynDslValue("$brooklyn:literal(" +
                unwrappedDesiredValue
                + ")", null, mgmt(), null);
        Assert.assertTrue(l.isPresent());
        Map<String,Object> stuff = MutableMap.<String,Object>of("stuff", l.get());

        ObjectMapper mapper = BeanWithTypeUtils.newMapper(mgmt(), false, null, true);

        String out = mapper.writerFor(Object.class).writeValueAsString(stuff);
        Assert.assertTrue(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object stuff2 = mapper.readValue(out, Object.class);
        Object stuff2I = ((Map<?, ?>) stuff2).get("stuff");
        Asserts.assertInstanceOf(stuff2I, BrooklynDslDeferredSupplier.class);
        Assert.assertEquals( ((BrooklynDslDeferredSupplier)stuff2I).get(), unwrappedDesiredValue );
    }
}
