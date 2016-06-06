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

import static org.testng.Assert.assertEquals;

import java.io.StringReader;
import java.util.Map;

import org.apache.brooklyn.test.Asserts;
import org.testng.annotations.Test;
import org.yaml.snakeyaml.Yaml;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class CollectionMergerTest {
    
    @Test
    public void testMergeMapsEmpty() {
        Map<String, String> val1 = ImmutableMap.of();
        Map<String, Object> val2 = ImmutableMap.of();
        Map<?, ?> result = CollectionMerger.builder().build().merge(val1, val2);

        assertEquals(result, ImmutableMap.of());
    }

    @Test
    public void testMergeMapsSimple() {
        Map<?, ?> val1 = ImmutableMap.of("key1", "val1a", "key2", "val2a");
        Map<?, ?> val2 = ImmutableMap.of("key1", "val1b", "key3", "val3b");
        Map<?, ?> resultDeep = CollectionMerger.builder().build().merge(val1, val2);
        Map<?, ?> resultShallow = CollectionMerger.builder().deep(false).build().merge(val1, val2);
        
        assertEquals(resultDeep, ImmutableMap.of("key1", "val1a", "key2", "val2a", "key3", "val3b"));
        assertEquals(resultShallow, ImmutableMap.of("key1", "val1a", "key2", "val2a", "key3", "val3b"));
    }

    @Test
    public void testAvoidInfiniteLoop() {
        {
            Map<Object, Object> val1 = MutableMap.<Object, Object>of("key1", "val1a");
            val1.put("key2", val1);
            Map<Object, Object> val2 = MutableMap.<Object, Object>of("key3", "val3a");
            try {
                CollectionMerger.builder().build().merge(val1, val2);
                Asserts.shouldHaveFailedPreviously();
            } catch (IllegalStateException e) {
                Asserts.expectedFailureContains(e, "Recursive self-reference");
            }
        }
        
        {
            Map<Object, Object> val1 = MutableMap.<Object, Object>of("key1", "val1a");
            Map<Object, Object> val2 = MutableMap.<Object, Object>of("key3", "val3a");
            val1.put("key4", val2);
            try {
                CollectionMerger.builder().build().merge(val1, val2);
                Asserts.shouldHaveFailedPreviously();
            } catch (IllegalStateException e) {
                Asserts.expectedFailureContains(e, "Recursive self-reference");
            }
        }
    }

    @Test
    public void testMergeMapsWithDeepSubMaps() {
        String yaml1 = Joiner.on("\n").join(
                "key1: val1",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a");
        String yaml2 = Joiner.on("\n").join(
                "key1: override-ignored",
                "key1b: val1b",
                "key2:",
                "  key2.1: override-ignored",
                "  key2.1b: val2.1b",
                "  key2.2:", 
                "    key2.2.1: override-ignored",
                "    key2.2.1b: val2.2.1b",
                "    key2.2.2:",
                "      key2.2.2.1: override-ignored",
                "      key2.2.2.1b: val2.2.2.1b",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: override-ignored",
                "        key2.2.2.2.1b: val2.2.2.2.1b");
        Map<?, ?> val1 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml1));
        Map<?, ?> val2 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml2));
        
        Map<?, ?> resultDepth1 = CollectionMerger.builder().depth(1).build().merge(val1, val2);
        Map<?, ?> resultDepth2 = CollectionMerger.builder().depth(2).build().merge(val1, val2);
        Map<?, ?> resultDepth3 = CollectionMerger.builder().depth(3).build().merge(val1, val2);
        Map<?, ?> resultDepth4 = CollectionMerger.builder().depth(4).build().merge(val1, val2);
        Map<?, ?> resultDepth5 = CollectionMerger.builder().depth(5).build().merge(val1, val2);
        Map<?, ?> resultShallow = CollectionMerger.builder().deep(false).build().merge(val1, val2);
        Map<?, ?> resultDeep = CollectionMerger.builder().build().merge(val1, val2);
        
        assertEquals(resultDepth1, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a"))));
        assertEquals(resultDepth2, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.1b: val2.1b",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a"))));
        assertEquals(resultDepth3, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.1b: val2.1b",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.1b: val2.2.1b",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a"))));
        assertEquals(resultDepth4, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.1b: val2.1b",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.1b: val2.2.1b",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.1b: val2.2.2.1b",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a"))));
        assertEquals(resultDepth5, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1: val2.1a",
                "  key2.1b: val2.1b",
                "  key2.2:", 
                "    key2.2.1: val2.2.1a",
                "    key2.2.1b: val2.2.1b",
                "    key2.2.2:",
                "      key2.2.2.1: val2.2.2.1a",
                "      key2.2.2.1b: val2.2.2.1b",
                "      key2.2.2.2:",
                "        key2.2.2.2.1: val2.2.2.2.1a",
                "        key2.2.2.2.1b: val2.2.2.2.1b"))));
        assertEquals(resultDeep, resultDepth5);
        assertEquals(resultShallow, resultDepth1);
    }

    @Test
    public void testMergeMapsWithNullOverridesOther() {
        // Expect "key2:" to have a null value (rather than just empty).
        String yaml1 = Joiner.on("\n").join(
                "key1: val1",
                "key2:");
        String yaml2 = Joiner.on("\n").join(
                "key1: override-ignored",
                "key1b: val1b",
                "key2:",
                "  key2.1b: val2.1b");
        Map<?, ?> val1 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml1));
        Map<?, ?> val2 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml2));
        
        Map<?, ?> resultDepth1 = CollectionMerger.builder().depth(1).build().merge(val1, val2);
        Map<?, ?> resultDepth2 = CollectionMerger.builder().depth(2).build().merge(val1, val2);
        
        assertEquals(resultDepth1, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:"))));
        assertEquals(resultDepth2, resultDepth1);
    }
    
    @Test
    public void testMergeMapsWithEmptyIsMerged() {
        // Expect "key2:" to have a null value (rather than just empty).
        String yaml1 = Joiner.on("\n").join(
                "key1: val1",
                "key2: {}");
        String yaml2 = Joiner.on("\n").join(
                "key1: override-ignored",
                "key1b: val1b",
                "key2:",
                "  key2.1b: val2.1b");
        Map<?, ?> val1 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml1));
        Map<?, ?> val2 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml2));
        
        Map<?, ?> resultDepth1 = CollectionMerger.builder().depth(1).build().merge(val1, val2);
        Map<?, ?> resultDepth2 = CollectionMerger.builder().depth(2).build().merge(val1, val2);
        
        assertEquals(resultDepth1, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2: {}"))));
        assertEquals(resultDepth2, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1: val1",
                "key1b: val1b",
                "key2:",
                "  key2.1b: val2.1b"))));
    }

    @Test
    public void testMergeMapsDefaultsToOverridingSubLists() {
        Map<?, ?> val1 = ImmutableMap.of("key1", ImmutableList.of("val1a"));
        Map<?, ?> val2 = ImmutableMap.of("key1", ImmutableList.of("val1b"));
        
        Map<?, ?> resultDepth1 = CollectionMerger.builder().depth(1).build().merge(val1, val2);
        Map<?, ?> resultDepth2 = CollectionMerger.builder().depth(2).build().merge(val1, val2);
        
        assertEquals(resultDepth1, ImmutableMap.of("key1", ImmutableList.of("val1a")));
        assertEquals(resultDepth2, resultDepth1);
    }

    @Test
    public void testMergeMapsWithMergingSubListsRespectsTypes() {
        Map<?, ?> val1 = ImmutableMap.of("key1", ImmutableList.of("val1a"));
        Map<?, ?> val2 = ImmutableMap.of("key1", ImmutableList.of("val1b"));
        Map<?, ?> result = CollectionMerger.builder().mergeNestedLists(true).build().merge(val1, val2);
        
        assertEquals(result, ImmutableMap.of("key1", ImmutableList.of("val1a", "val1b")));
    }

    @Test
    public void testMergeMapsWithMergingSubSetsRespectsTypes() {
        Map<?, ?> val1 = ImmutableMap.of("key1", ImmutableSet.of("val1a"));
        Map<?, ?> val2 = ImmutableMap.of("key1", ImmutableSet.of("val1b"));
        Map<?, ?> result = CollectionMerger.builder().mergeNestedLists(true).build().merge(val1, val2);
        
        assertEquals(result, ImmutableMap.of("key1", ImmutableSet.of("val1a", "val1b")));
    }

    @Test
    public void testMergeMapsWithMergingSubLists() {
        String yaml1 = Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1");
        String yaml2 = Joiner.on("\n").join(
                "key1:",
                "- key1.1b",
                "key2:",
                "  key2.1:",
                "  - key2.1.1b",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1b",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1b");
        Map<?, ?> val1 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml1));
        Map<?, ?> val2 = (Map<?, ?>) Iterables.getOnlyElement(parseYaml(yaml2));
        
        Map<?, ?> resultDepth1 = CollectionMerger.builder().mergeNestedLists(true).depth(1).build().merge(val1, val2);
        Map<?, ?> resultDepth2 = CollectionMerger.builder().mergeNestedLists(true).depth(2).build().merge(val1, val2);
        Map<?, ?> resultDepth3 = CollectionMerger.builder().mergeNestedLists(true).depth(3).build().merge(val1, val2);
        Map<?, ?> resultDepth4 = CollectionMerger.builder().mergeNestedLists(true).depth(4).build().merge(val1, val2);
        Map<?, ?> resultDepth5 = CollectionMerger.builder().mergeNestedLists(true).depth(5).build().merge(val1, val2);
        Map<?, ?> resultShallow = CollectionMerger.builder().mergeNestedLists(true).deep(false).build().merge(val1, val2);
        Map<?, ?> resultDeep = CollectionMerger.builder().mergeNestedLists(true).build().merge(val1, val2);
        
        assertEquals(resultDepth1, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1"))));
        assertEquals(resultDepth2, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "- key1.1b",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1"))));
        assertEquals(resultDepth3, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "- key1.1b",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "  - key2.1.1b",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1"))));
        assertEquals(resultDepth4, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "- key1.1b",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "  - key2.1.1b",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "    - key3.1.1.1b",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1"))));
        assertEquals(resultDepth5, Iterables.getOnlyElement(parseYaml(Joiner.on("\n").join(
                "key1:",
                "- key1.1",
                "- key1.1b",
                "key2:",
                "  key2.1:",
                "  - key2.1.1",
                "  - key2.1.1b",
                "key3:",
                "  key3.1:",
                "    key3.1.1:",
                "    - key3.1.1.1",
                "    - key3.1.1.1b",
                "key4:",
                "  key4.1:",
                "    key4.1.1:",
                "      key4.1.1.1:",
                "      - key4.1.1.1.1",
                "      - key4.1.1.1.1b"))));
        assertEquals(resultDeep, resultDepth5);
        assertEquals(resultShallow, resultDepth1);
    }

    protected Iterable<?> parseYaml(String yaml) {
        return new Yaml().loadAll(new StringReader(yaml));
    }
}
