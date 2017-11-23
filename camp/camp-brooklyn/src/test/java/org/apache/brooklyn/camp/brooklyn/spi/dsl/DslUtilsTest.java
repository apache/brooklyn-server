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

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class DslUtilsTest {

    @Test
    public void testResolved() throws Exception {
        DeferredSupplier<String> deferredVal = new DeferredSupplier<String>() {
            public String get() {return "myval";};
        };
        
        assertTrue(DslUtils.resolved());
        assertTrue(DslUtils.resolved((Object)null));
        assertTrue(DslUtils.resolved((List<?>)null));
        assertTrue(DslUtils.resolved(ImmutableList.of()));
        assertTrue(DslUtils.resolved(MutableList.of(null)));
        assertTrue(DslUtils.resolved(1, "a", ImmutableList.of("b"), ImmutableMap.of("c", "d")));
        
        assertFalse(DslUtils.resolved(deferredVal));
        assertFalse(DslUtils.resolved(ImmutableList.of(deferredVal)));
        assertFalse(DslUtils.resolved(ImmutableList.of(ImmutableList.of(deferredVal))));
        assertFalse(DslUtils.resolved(ImmutableList.of(ImmutableMap.of(deferredVal, "myval"))));
        assertFalse(DslUtils.resolved(ImmutableList.of(ImmutableMap.of("mykey", deferredVal))));
    }
}
