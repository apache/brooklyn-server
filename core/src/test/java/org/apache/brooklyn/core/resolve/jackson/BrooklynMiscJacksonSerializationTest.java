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
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.NestedLoggingDeserializer;
import org.apache.brooklyn.core.resolve.jackson.WrappedValue.WrappedValuesInitialized;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.Assert;
import org.testng.annotations.Test;

public class BrooklynMiscJacksonSerializationTest implements MapperTestFixture {

    public ObjectMapper mapper() {
        ObjectMapper mapper = BeanWithTypeUtils.newMapper(null, false, true);

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


}
