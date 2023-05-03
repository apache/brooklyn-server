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
package org.apache.brooklyn.util.text;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;

public class SecretTest {

    @Test
    public void testJacksonNormallySuppressesWriteAndFailsRead() throws JsonProcessingException {
        JsonMapper mapper = JsonMapper.builder().build();
        Asserts.assertEquals(mapper.writeValueAsString(new Secret("my secret")), StringEscapes.JavaStringEscapes.wrapJavaString("<suppressed> (MD5 hash: 0003D04B)"));
        Asserts.assertFailsWith(() -> mapper.readValue("\"my secret\"", Secret.class), err -> Asserts.expectedFailureContainsIgnoreCase(err, "Secrets", "cannot be deserialized"));
    }

    @Test
    public void testJacksonAllowedInSpecialBlock() throws Exception {
        JsonMapper mapper = JsonMapper.builder().build();
        Secret<String> s0 = new Secret<>("my secret");
        String r1 = Secret.SecretHelper.runWithJacksonSerializationEnabledInThread(() -> mapper.writeValueAsString(s0));
        Asserts.assertEquals(r1, StringEscapes.JavaStringEscapes.wrapJavaString("my secret"));

        Secret r2 = Secret.SecretHelper.runWithJacksonSerializationEnabledInThread(() -> mapper.readValue(r1, Secret.class));
        Asserts.assertEquals(s0.get(), r2.get());
        Asserts.assertEquals(s0, r2);
    }

}
