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
package org.apache.brooklyn.util.guava;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import java.util.Map;
import java.util.Set;
import javax.annotation.Nullable;

@Deprecated
/** @deprecated since introduced; use ImmutableBiMap#of(). here for deserialization compatibility. */
public class EmptyImmutableBiMap<K,V> implements BiMap<K,V> {

    public final static ImmutableBiMap<Object,Object> INSTANCE = ImmutableBiMap.of();

    @Override
    public @Nullable V put(@Nullable K key, @Nullable V value) {
        return null;
    }

    @Override
    public @Nullable V forcePut(@Nullable K key, @Nullable V value) {
        return null;
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> map) {

    }

    @Override
    public Set<V> values() {
        return null;
    }

    @Override
    public ImmutableBiMap<V,K> inverse() {
        return (ImmutableBiMap) INSTANCE;
    }

    @Override
    public boolean isEmpty() {
        return INSTANCE.isEmpty();
    }

    @Override
    public boolean containsKey(@Nullable Object key) {
        return INSTANCE.containsKey(key);
    }

    @Override
    public boolean containsValue(@Nullable Object value) {
        return INSTANCE.containsValue(value);
    }

    @Override
    public V get(@Nullable Object key) {
        return null;
    }

    public Object getOrDefault(@Nullable Object key, @Nullable Object defaultValue) {
        return INSTANCE.getOrDefault(key, defaultValue);
    }

    @Override
    public ImmutableSet<Entry<K, V>> entrySet() {
        return (ImmutableSet) INSTANCE.entrySet();
    }

    @Override
    public ImmutableSet<K> keySet() {
        return (ImmutableSet) INSTANCE.keySet();
    }

    public ImmutableSetMultimap<Object, Object> asMultimap() {
        return INSTANCE.asMultimap();
    }

    @Override
    public boolean equals(@Nullable Object object) {
        return INSTANCE.equals(object);
    }

    @Override
    public int hashCode() {
        return INSTANCE.hashCode();
    }

    @Override
    public String toString() {
        return INSTANCE.toString();
    }

    @Override
    public int size() {
        return INSTANCE.size();
    }

    @Override
    public V remove(Object key) {
        return null;
    }

    @Deprecated
    @Override
    public void clear() {
        INSTANCE.clear();
    }

    @Override
    public boolean remove(Object key, Object value) {
        return INSTANCE.remove(key, value);
    }
}
