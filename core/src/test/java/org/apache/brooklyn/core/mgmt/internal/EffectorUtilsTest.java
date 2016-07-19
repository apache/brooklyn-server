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
package org.apache.brooklyn.core.mgmt.internal;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.effector.Effector;
import org.apache.brooklyn.core.effector.EffectorBody;
import org.apache.brooklyn.core.effector.Effectors;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.guava.Maybe;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class EffectorUtilsTest {

    private Effector<Void> effector = Effectors.effector(Void.class, "myEffector")
            .parameter(byte.class, "byteParam")
            .parameter(char.class, "charParam")
            .parameter(short.class, "shortParam")
            .parameter(int.class, "intParam")
            .parameter(long.class, "longParam")
            .parameter(float.class, "floatParam")
            .parameter(double.class, "doubleParam")
            .parameter(boolean.class, "boolParam")
            .parameter(String.class, "stringParam")
            .impl(new EffectorBody<Void>() {
                    @Override public Void call(ConfigBag parameters) {
                        return null;
                    }})
            .build();

    private Map<Object, Object> argsMap = ImmutableMap.builder()
            .put("byteParam", (byte)1)
            .put("charParam", (char)'2')
            .put("shortParam", (short)3)
            .put("intParam", (int)4)
            .put("longParam", (long)5)
            .put("floatParam", (float)6.0)
            .put("doubleParam", (double)7.0)
            .put("boolParam", true)
            .put("stringParam", "mystring")
            .build();

    private Map<Object, Object> argsMapRequiringCoercion = ImmutableMap.builder()
            .put("byteParam", "1")
            .put("charParam", "2")
            .put("shortParam", "3")
            .put("intParam", "4")
            .put("longParam", "5")
            .put("floatParam", "6.0")
            .put("doubleParam", "7.0")
            .put("boolParam", "true")
            .put("stringParam", "mystring")
            .build();

    private Object[] argsArray = new Object[] {(byte)1, (char)'2', (short)3, (int)4, (long)5, (float)6.0, (double)7.0, true, "mystring"};

    private Object[] expectedArgs = argsArray;

    @Test
    public void testPrepareArgsFromMap() throws Exception {
        assertEquals(EffectorUtils.prepareArgsForEffector(effector, argsMap), expectedArgs);
        assertEquals(EffectorUtils.prepareArgsForEffector(effector, argsMapRequiringCoercion), expectedArgs);
    }
    
    @Test
    public void testPrepareArgsFromArray() throws Exception {
        assertEquals(EffectorUtils.prepareArgsForEffector(effector, argsArray), expectedArgs);
    }
    
    @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp=".*Invalid arguments.*missing argument.*stringParam.*")
    public void testPrepareArgsFromMapThrowsIfMissing() throws Exception {
        MutableMap<Object, Object> mapMissingArg = MutableMap.builder()
                .putAll(argsMap)
                .remove("stringParam")
                .build();
        EffectorUtils.prepareArgsForEffector(effector, mapMissingArg);
    }
    
    @Test(expectedExceptions=IllegalArgumentException.class, expectedExceptionsMessageRegExp=".*Invalid arguments.*count mismatch.*")
    public void testPrepareArgsFromArrayThrowsIfMissing() throws Exception {
        Object[] arrayMissingArg = Arrays.asList(argsArray).subList(0, argsArray.length - 1).toArray();
        EffectorUtils.prepareArgsForEffector(effector, arrayMissingArg);
    }

    @Test
    public void testFindEffector() {
        Maybe<Effector<?>> found = EffectorUtils.findEffector(ImmutableList.of(effector), "myEffector");
        assertEquals(found.get(), effector);
        
        Maybe<Effector<?>> notFound = EffectorUtils.findEffector(ImmutableList.of(effector), "wrongEffectorName");
        assertFalse(notFound.isPresent(), notFound.toString());
    }
}
