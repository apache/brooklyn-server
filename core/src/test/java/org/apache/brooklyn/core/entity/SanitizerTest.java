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
package org.apache.brooklyn.core.entity;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.testng.annotations.Test;

import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class SanitizerTest {

    @Test
    public void testSanitize() throws Exception {
        Map<String, Object> map = ImmutableMap.<String, Object>builder()
                .put("PREFIX_password_SUFFIX", "pa55w0rd")
                .put("PREFIX_PASSWORD_SUFFIX", "pa55w0rd")
                .put("PREFIX_passwd_SUFFIX", "pa55w0rd")
                .put("PREFIX_credential_SUFFIX", "pa55w0rd")
                .put("PREFIX_secret_SUFFIX", "pa55w0rd")
                .put("PREFIX_private_SUFFIX", "pa55w0rd")
                .put("PREFIX_access.cert_SUFFIX", "myval")
                .put("PREFIX_access.key_SUFFIX", "myval")
                .put("mykey", "myval")
                .build();
        Map<String, Object> expected = MutableMap.<String, Object>builder()
                .putAll(Maps.transformValues(map, Functions.constant("xxxxxxxx")))
                .put("mykey", "myval")
                .build();
        
        Map<String, Object> sanitized = Sanitizer.sanitize(ConfigBag.newInstance(map));
        assertEquals(sanitized, expected);
        
        Map<String, Object> sanitized2 = Sanitizer.sanitize(map);
        assertEquals(sanitized2, expected);
    }
    
    @Test
    public void testSanitizeWithNullKey() throws Exception {
        MutableMap<?, ?> map = MutableMap.of(null, null);
        Map<?, ?> sanitized = Sanitizer.sanitize(map);
        assertEquals(sanitized, map);
    }
}
