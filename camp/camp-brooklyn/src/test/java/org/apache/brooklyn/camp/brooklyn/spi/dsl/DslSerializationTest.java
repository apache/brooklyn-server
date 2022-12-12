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
import java.util.function.Supplier;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.internal.AbstractBrooklynObjectSpec;
import org.apache.brooklyn.camp.brooklyn.AbstractYamlTest;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.BrooklynDslCommon;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent;
import org.apache.brooklyn.camp.brooklyn.spi.dsl.methods.DslComponent.Scope;
import org.apache.brooklyn.core.resolve.jackson.BeanWithTypeUtils;
import org.apache.brooklyn.core.resolve.jackson.MapperTestFixture;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue;
import org.apache.brooklyn.core.resolve.jackson.WrappedValuesSerializationTest;
import org.apache.brooklyn.core.resolve.jackson.WrappedValuesSerializationTest.ObjectWithWrappedValueString;
import org.apache.brooklyn.core.workflow.WorkflowBasicTest;
import org.apache.brooklyn.entity.stock.BasicStartable;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.task.BasicExecutionContext;
import org.apache.brooklyn.util.core.task.BasicExecutionManager;
import org.apache.brooklyn.util.text.StringEscapes.JavaStringEscapes;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class DslSerializationTest extends AbstractYamlTest implements MapperTestFixture {

    @Test
    public void testSerializeAttributeWhenReadyAtRootWorksWithoutBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr = new DslComponent(Scope.GLOBAL, "entity_id").attributeWhenReady("my_sensor");
        ObjectMapper mapper = newMapper();

        String out = mapper.writerFor(Object.class).writeValueAsString(awr);
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object supplier2 = mapper.readValue(out, Object.class);
        Asserts.assertInstanceOf(supplier2, BrooklynDslDeferredSupplier.class);
    }

    @Test
    public void testSerializeDslConfigSupplierInMapObjectFailsWithoutBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr = new DslComponent(Scope.GLOBAL, "entity_id").config("my_config");
        Map<String,Object> stuff = MutableMap.<String,Object>of("stuff", awr);
        ObjectMapper mapper = newMapper();

        String out = mapper.writerFor(Object.class).writeValueAsString(stuff);
        /* literal not present, and we get a map */
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object stuff2 = mapper.readValue(out, Object.class);
        Object stuff2I = ((Map<?, ?>) stuff2).get("stuff");
        Asserts.assertInstanceOf(stuff2I, Map.class);
    }

    private ObjectMapper newMapper() {
        return mapper();
    }

    public ObjectMapper mapper() {
        return BeanWithTypeUtils.newMapper(mgmt(), true, null, true);
    }

    @Test
    public void testSerializeDslConfigSupplierInMapObjectWorksWithBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr =
                (BrooklynDslDeferredSupplier<?>) DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:component(\"entity_id\").config(\"my_config\")");
        Map<String,Object> stuff = MutableMap.<String,Object>of("stuff", awr);
        ObjectMapper mapper = newMapper();

        String out = mapper.writerFor(Object.class).writeValueAsString(stuff);
        /* literal IS present, and we get the right type */
        Assert.assertTrue(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object stuff2 = mapper.readValue(out, Object.class);
        Object stuff2I = ((Map<?, ?>) stuff2).get("stuff");
        Asserts.assertInstanceOf(stuff2I, BrooklynDslDeferredSupplier.class);
    }

    @Test
    public void testSerializeDslConfigSupplierInWrappedValueWorksWithoutBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr = new DslComponent(Scope.GLOBAL, "entity_id").config("my_config");
        WrappedValue<Object> stuff = WrappedValue.of(awr);
        ObjectMapper mapper = newMapper();

        String out = mapper.writeValueAsString(stuff);
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        WrappedValue<?> stuff2 = mapper.readValue(out, WrappedValue.class);
        Asserts.assertInstanceOf(stuff2.getSupplier(), Supplier.class);
    }

    @Test
    public void testSerializeDslPropertyAccessInWrappedValueWorksWithoutBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr =  new DslDeferredPropertyAccess(
            new DslComponent(Scope.GLOBAL, "entity_id")
            .config("my_config"), "foo");
        WrappedValue<Object> stuff = WrappedValue.of(awr);
        ObjectMapper mapper = newMapper();

        String out = mapper.writeValueAsString(stuff);
        Assert.assertTrue(out.contains("\"index\":\"foo\""));
        Assert.assertFalse(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        WrappedValue<?> stuff2 = mapper.readValue(out, WrappedValue.class);
        Asserts.assertInstanceOf(stuff2.getSupplier(), Supplier.class);
    }

    @Test
    public void testSerializeDslConfigSupplierInWrappedValueWorksWithBrooklynLiteralMarker() throws Exception {
        BrooklynDslDeferredSupplier<?> awr =
                (BrooklynDslDeferredSupplier<?>) DslUtils.parseBrooklynDsl(mgmt(), "$brooklyn:component(\"entity_id\").config(\"my_config\")");
        WrappedValue<Object> stuff = WrappedValue.of(awr);
        ObjectMapper mapper = newMapper();

        String out = mapper.writeValueAsString(stuff);
        Assert.assertTrue(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        WrappedValue<?> stuff2 = mapper.readValue(out, WrappedValue.class);
        Asserts.assertInstanceOf(stuff2.getSupplier(), Supplier.class);
    }

    @Test
    public void testSerializeDslLiteralWorksWithBrooklynLiteralMarker() throws Exception {
        String unwrappedDesiredValue = "$brooklyn:literal(" + JavaStringEscapes.wrapJavaString("foo") + ")";
        Optional<Object> l = DslUtils.resolveBrooklynDslValue("$brooklyn:literal(" +
                JavaStringEscapes.wrapJavaString(unwrappedDesiredValue)
                + ")", null, mgmt(), null);
        Assert.assertTrue(l.isPresent());
        Map<String,Object> stuff = MutableMap.<String,Object>of("stuff", l.get());
        ObjectMapper mapper = newMapper();

        String out = mapper.writerFor(Object.class).writeValueAsString(stuff);
        Assert.assertTrue(out.toLowerCase().contains("literal"), "serialization had wrong text: "+out);
        Assert.assertFalse(out.toLowerCase().contains("absent"), "serialization had wrong text: "+out);

        Object stuff2 = mapper.readValue(out, Object.class);
        Object stuff2I = ((Map<?, ?>) stuff2).get("stuff");
        Asserts.assertInstanceOf(stuff2I, BrooklynDslDeferredSupplier.class);
        BasicExecutionManager execManager = new BasicExecutionManager("mycontextid");
        BasicExecutionContext execContext = new BasicExecutionContext(execManager);

        Assert.assertEquals( execContext.submit(((BrooklynDslDeferredSupplier)stuff2I).newTask()).get(), unwrappedDesiredValue );
    }

    @Test
    public void testDeserializeDsl() throws Exception {
        BrooklynDslCommon.registerSerializationHooks();
        // test in CAMP where DSL is registered
        String dslLiteralFoo = "$brooklyn:literal(\"foo\")";
        WrappedValuesSerializationTest.ObjectWithWrappedValueString impl = deser(json("x: " + dslLiteralFoo), ObjectWithWrappedValueString.class);
        Asserts.assertNotNull(impl.x);
        Asserts.assertEquals(resolve(impl.x, String.class).get(), "foo");
    }

    // also see org.apache.brooklyn.rest.util.json.BrooklynJacksonSerializerTest.SpecHolder
    public static class ObjectWithWrappedValueSpec extends WrappedValue.WrappedValuesInitialized {
        public WrappedValue<EntitySpec<?>> sw;
        public EntitySpec<?> su;

        public WrappedValue<AbstractBrooklynObjectSpec<?,?>> asw;
        public AbstractBrooklynObjectSpec<?,?> asu;
    }

    @Test
    public void testDeserializeSpec() throws Exception {
        // also see org.apache.brooklyn.rest.util.json.BrooklynJacksonSerializerTest.SpecHolder

        BrooklynDslCommon.registerSerializationHooks();

        WorkflowBasicTest.addRegisteredTypeSpec(mgmt(), "basic-startable", BasicStartable.class, Entity.class);
        ObjectWithWrappedValueSpec impl;

        impl = deser(json("sw: { type: basic-startable }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.sw);
        Asserts.assertEquals(impl.sw.get().getType(), BasicStartable.class);

        impl = deser(json("sw: { type: "+BasicStartable.class.getName()+" }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.sw);
        Asserts.assertEquals(impl.sw.get().getType(), BasicStartable.class);

        impl = deser(json("asw: { type: basic-startable }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.asw);
        Asserts.assertEquals(impl.asw.get().getType(), BasicStartable.class);

        impl = deser(json("asw: { type: "+BasicStartable.class.getName()+" }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.asw);
        // with non-concrete type, we used to get a map in wrapped value, but now we try the generic type
//        Asserts.assertInstanceOf(impl.asw.get(), Map.class);
        Asserts.assertEquals(impl.asw.get().getType(), BasicStartable.class);

        impl = deser(json("su: { type: basic-startable }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.su);

        impl = deser(json("su: { type: "+BasicStartable.class.getName()+" }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.su);

        impl = deser(json("asu: { type: basic-startable }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.asu);

        impl = deser(json("asu: { type: "+BasicStartable.class.getName()+" }"), ObjectWithWrappedValueSpec.class);
        Asserts.assertNotNull(impl.asu);
    }
}
