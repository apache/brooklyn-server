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

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.ConfigurableBeanDeserializerModifier;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.JsonDeserializerForCommonBrooklynThings;
import org.apache.brooklyn.core.resolve.jackson.BrooklynJacksonSerializationUtils.NestedLoggingDeserializer;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.time.Duration;
import org.testng.annotations.Test;

public class LoggingSerializationTest implements MapperTestFixture {

    public ObjectMapper mapper() {
        ObjectMapper mapper = BeanWithTypeUtils.newSimpleMapper();
        StringBuilder d1 = new StringBuilder("D1");
        StringBuilder d2 = new StringBuilder("D2");
        mapper = new ConfigurableBeanDeserializerModifier()
                .addDeserializerWrapper(
                        d -> new NestedLoggingDeserializer(d1, d),
                        d -> new JsonDeserializerForCommonBrooklynThings(null, d),
                        d -> new NestedLoggingDeserializer(d2, d)
                ).apply(mapper);
        return mapper;
    }

    static class WrappedDuration {
        @JsonProperty
        Duration x;
    }

    @Test
    public void testSeeErrorLoggedAndCorrected() throws Exception {
        // main point of this test is to inspect it in the console, and experiment with other configurations
        Asserts.assertEquals(deser("{\"x\":\"1m\"}", WrappedDuration.class).x, Duration.minutes(1));
    }

}
