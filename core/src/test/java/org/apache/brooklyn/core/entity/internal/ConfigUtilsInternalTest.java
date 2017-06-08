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
package org.apache.brooklyn.core.entity.internal;

import static org.testng.Assert.assertEquals;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class ConfigUtilsInternalTest extends BrooklynAppUnitTestSupport {

    final ConfigKey<String> key1 = ConfigKeys.builder(String.class, "key1")
            .deprecatedNames("oldKey1", "oldKey1b")
            .build();

    @Test
    public void testSetConfig() throws Exception {
        Map<?,?> remaining = ConfigUtilsInternal.setAllConfigKeys(ImmutableMap.of("key1", "myval"), ImmutableList.of(key1), app);
        assertEquals(app.config().get(key1), "myval");
        assertEquals(remaining, ImmutableMap.of());
    }
    
    @Test
    public void testSetConfigUsingDeprecatedValue() throws Exception {
        Map<?,?> remaining = ConfigUtilsInternal.setAllConfigKeys(ImmutableMap.of("oldKey1", "myval"), ImmutableList.of(key1), app);
        assertEquals(app.config().get(key1), "myval");
        assertEquals(remaining, ImmutableMap.of());
    }
    
    @Test
    public void testSetConfigPrefersNonDeprecated() throws Exception {
        Map<?,?> remaining = ConfigUtilsInternal.setAllConfigKeys(
                ImmutableMap.of("key1", "myval", "oldKey1", "myOldVal1", "oldKey1b", "myOldVal1b"), 
                ImmutableList.of(key1), 
                app);
        assertEquals(app.config().get(key1), "myval");
        
        // Should remove deprecated value as well (and warn about it)
        assertEquals(remaining, ImmutableMap.of());
    }
    
    @Test
    public void testReturnsUnmatched() throws Exception {
        Map<?,?> remaining = ConfigUtilsInternal.setAllConfigKeys(ImmutableMap.of("wrong", "myval"), ImmutableList.of(key1), app);
        assertEquals(app.config().get(key1), null);
        assertEquals(remaining, ImmutableMap.of("wrong", "myval"));
    }
}
