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
package org.apache.brooklyn.util.collections;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.apache.brooklyn.util.collections.CollectionHelpers.mergeMapsAndTheirInnerMapValues;
import static org.testng.Assert.assertEquals;

public class CollectionHelpersTest {
    @Test
    public void testMergeMapsEmpty() {
        Map<String, Object> targetMap = MutableMap.of();
        Map<String, Object> overrideWithNew = MutableMap.of();

        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew), MutableMap.of());
    }

    @Test
    public void testMergeMapsNonMapValues() {
        Map<String, Object> targetMap = ImmutableMap.<String, Object>of("testKeyTarget", "testValTarget", "testKeyTarget1", "testVal1");
        Map<String, Object> overrideWithNew = ImmutableMap.<String, Object>of("testKeyNew1", "testValNew1", "testKeyTarget", "testValNew2");

        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew),
                ImmutableMap.<String, Object>of(
                        "testKeyTarget1", "testVal1",
                        "testKeyNew1", "testValNew1",
                        "testKeyTarget", "testValNew2"));
    }

    /**
     * Always override with the new value
     */
    @Test
    public void testMergeMapsOverridingMapAndNonMapValues() {
        Map<String, Object> targetMap = ImmutableMap.<String, Object>of("testMapValue", ImmutableMap.of(), "testKeyTarget1", "testVal1");
        Map<String, Object> overrideWithNew = ImmutableMap.<String, Object>of("testKeyNew1", "testValNew1", "testMapValue", "testValNew2");
        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew),
                ImmutableMap.<String, Object>of(
                        "testMapValue", "testValNew2",
                        "testKeyTarget1", "testVal1",
                        "testKeyNew1", "testValNew1"));

        targetMap = ImmutableMap.<String, Object>of("testMapValue", "testValNew2", "testKeyTarget1", "testVal1", "testKeyTarget2", "testVal2");
        overrideWithNew = MutableMap.<String, Object>of("testKeyNew1", "testValNew1", "testMapValue", ImmutableMap.of());
        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew),
                ImmutableMap.<String, Object>of(
                        "testMapValue", ImmutableMap.of(),
                        "testKeyTarget1", "testVal1",
                        "testKeyNew1", "testValNew1",
                        "testKeyTarget2", "testVal2"));
    }

    @Test
    public void testMergeMapsOverridingMapValues() {
        Map<String, Object> targetMap = ImmutableMap.<String, Object>of("testTargetKey", "testTargetValue",
                "testTargetKey1", "testTargetValue1",
                "templateOptions", ImmutableMap.of("locKey1", "locVal1", "locKey2", "locVal2", "locKey3", "locVal3"));

        Map<String, Object> overrideWithNew = ImmutableMap.<String, Object>of("testKeyNew1", "testValNew1",
                "templateOptions", ImmutableMap.of("locKey1", "locValNew1", "locKey2", "locValNew2"),
                "testTargetKey", "testNewValue");

        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew),
                ImmutableMap.<String, Object>of(
                        "testTargetKey1", "testTargetValue1",
                        "templateOptions", ImmutableMap.of("locKey1", "locValNew1", "locKey3", "locVal3", "locKey2", "locValNew2"),
                        "testKeyNew1", "testValNew1",
                        "testTargetKey", "testNewValue"));
    }

    @Test
    public void testMergeMapsOverridingMapValuesNested() {
        Map<String, Object> targetMap = ImmutableMap.<String, Object>of(
                "testTargetKey", "testTargetValue",
                "testTargetKey1", "testTargetValue1",
                "templateOptions", ImmutableMap.of(
                        "locKey2", ImmutableMap.of("locKeyNestedTarget", "locKey2UniqVal"),
                        "locKey1", "locVal1",
                        "locKey3", "locVal3"));

        Map<String, Object> overrideWithNew = ImmutableMap.<String, Object>of("testKeyNew1", "testValNew1",
                "templateOptions", ImmutableMap.of(
                        "locKey1", "locValNew1",
                        "locKey2", ImmutableMap.of("locKey21", "locKeyVal21")),
                "testTargetKey", "testNewValue");

        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew),
                ImmutableMap.<String, Object>of(
                        "testTargetKey1", "testTargetValue1",
                        "templateOptions", ImmutableMap.of(
                                "locKey1", "locValNew1",
                                "locKey3", "locVal3",
                                "locKey2", ImmutableMap.of("locKey21", "locKeyVal21")), // It will override Maps which are one level deeper
                        "testKeyNew1", "testValNew1",
                        "testTargetKey", "testNewValue"));
    }

    @Test
    public void testMergeMapsOverridingMapValuesRestrictedToAKey() {
        Map<String, Object> targetMap = ImmutableMap.<String, Object>of("testTargetKey", "testTargetValue",
                "testTargetKey1", "testTargetValue1",
                "keyToReplace", ImmutableMap.of("locKey1", "locValNew1", "locKey3", "locVal3", "locKey2", "locValNew2"),
                "keyToMerge",   ImmutableMap.of("locKey1", "locValNew1", "locKey3", "locVal3", "locKey2", "locValNew2"));

        Map<String, Object> overrideWithNew = ImmutableMap.<String, Object>of("testKeyNew1", "testValNew1",
                "keyToReplace", ImmutableMap.of("mlocKey1", "newLocValNew1", "locKey3", "newLocVal3", "locKey2", "newLocValNew2"),
                "keyToMerge",   ImmutableMap.of("mlocKey1", "newLocValNew1", "locKey3", "newLocVal3", "locKey2", "newLocValNew2"),
                "testTargetKey", "testNewValue");

        assertEquals(mergeMapsAndTheirInnerMapValues(targetMap, overrideWithNew, "keyToMerge"),
                ImmutableMap.<String, Object>of(
                        "testTargetKey1", "testTargetValue1",
                        "keyToReplace", ImmutableMap.of("mlocKey1", "newLocValNew1", "locKey3", "newLocVal3", "locKey2", "newLocValNew2"),
                        "keyToMerge", ImmutableMap.of("locKey1", "locValNew1",
                                "mlocKey1", "newLocValNew1", "locKey3", "newLocVal3", "locKey2", "newLocValNew2"),
                        "testKeyNew1", "testValNew1",
                        "testTargetKey", "testNewValue"));
    }
}
