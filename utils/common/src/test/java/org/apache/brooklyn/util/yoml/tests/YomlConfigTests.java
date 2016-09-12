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
package org.apache.brooklyn.util.yoml.tests;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.YomlAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YomlConfigTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(YomlConfigTests.class);
    
    static class MockConfigKey<T> implements ConfigKey<T> {
        String name;
        Class<T> type;
        T defaultValue;

        public MockConfigKey(Class<T> type, String name) {
            this.name = name;
            this.type = type;
        }
        
        @Override public String getDescription() { return null; }
        @Override public String getName() { return name; }
        @Override public Collection<String> getNameParts() { return MutableList.of(name); }
        @Override public TypeToken<T> getTypeToken() { return TypeToken.of(type); }
        @Override public Class<? super T> getType() { return type; }

        @Override public String getTypeName() { throw new UnsupportedOperationException(); }
        @Override public T getDefaultValue() { return defaultValue; }
        @Override public boolean hasDefaultValue() { return defaultValue!=null; }
        @Override public boolean isReconfigurable() { return false; }
        @Override public ConfigInheritance getTypeInheritance() { return null; }
        @Override public ConfigInheritance getParentInheritance() { return null; }
        @Override public ConfigInheritance getInheritance() { return null; }
        @Override public Predicate<? super T> getConstraint() { return null; }
        @Override public boolean isValueValid(T value) { return true; }
    }
    
    static class S1 {
        Map<String,String> keys = MutableMap.of();
        static ConfigKey<String> K1 = new MockConfigKey<String>(String.class, "k1");
        S1(Map<String,String> keys) { this.keys.putAll(keys); }
    }
    static class S2 {
        ConfigKey<String> K2;
    }
        
    @Test
    public void testRead() {
        YomlTestFixture y = YomlTestFixture.newInstance();
        
        Set<YomlSerializer> serializers = YomlAnnotations.findSerializerAnnotations(S1.class, "keys", "config");
        y.tr.put("s1", S1.class, serializers);
        // TODO autodetect above, then do this
//        y.addType("s1", S1.class);
        
        y.read("{ type: s1, k1: foo }", null);

        Asserts.assertInstanceOf(y.lastReadResult, S1.class);
        Asserts.assertEquals(((S1)y.lastReadResult).keys.get("k1"), "foo");
    }

    @Test
    public void testWrite() {
        YomlTestFixture y = YomlTestFixture.newInstance();
        
        Set<YomlSerializer> serializers = YomlAnnotations.findSerializerAnnotations(S1.class, "keys", "config");
        y.tr.put("s1", S1.class, serializers);
        // TODO autodetect above, then do this
//        y.addType("s1", S1.class);

        S1 s1 = new S1(MutableMap.of("k1", "foo"));
        y.write(s1);
        YomlTestFixture.assertEqualsIgnoringQuotes(Jsonya.newInstance().add(y.lastWriteResult).toString(), "{ type: s1, k1: foo }", "wrong serialization");
    }


}
