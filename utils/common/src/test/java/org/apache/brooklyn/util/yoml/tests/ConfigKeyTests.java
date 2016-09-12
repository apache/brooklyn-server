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

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.YomlConstructorConfigMap;
import org.apache.brooklyn.util.yoml.internal.YomlConfig;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class ConfigKeyTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ConfigKeyTests.class);
    
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
        static ConfigKey<String> K1 = new MockConfigKey<String>(String.class, "k1");
        Map<String,Object> keys = MutableMap.of();
        S1(Map<String,?> keys) { this.keys.putAll(keys); }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof S1) && ((S1)obj).keys.equals(keys);
        }
        @Override
        public int hashCode() {
            return keys.hashCode();
        }
        @Override
        public String toString() {
            return super.toString()+keys;
        }
    }
        
    @Test
    public void testRead() {
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("s1", S1.class, MutableMap.of("keys", "config"));
        
        y.read("{ type: s1, k1: foo }", null);

        Asserts.assertInstanceOf(y.lastReadResult, S1.class);
        Asserts.assertEquals(((S1)y.lastReadResult).keys.get("k1"), "foo");
    }

    @Test
    public void testWrite() {
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("s1", S1.class, MutableMap.of("keys", "config"));

        S1 s1 = new S1(MutableMap.of("k1", "foo"));
        y.write(s1);
        YomlTestFixture.assertEqualsIgnoringQuotes(Jsonya.newInstance().add(y.lastWriteResult).toString(), "{ type: s1, k1: foo }", "wrong serialization");
    }

    @Test
    public void testReadWrite() {
        YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("s1", S1.class, MutableMap.of("keys", "config"))
        .reading("{ type: s1, k1: foo }").writing(new S1(MutableMap.of("k1", "foo")))
        .doReadWriteAssertingJsonMatch();
    }

    static class S2 extends S1 {
        S2(Map<String,?> keys) { super(keys); }
        static ConfigKey<String> K2 = new MockConfigKey<String>(String.class, "k2");
        static ConfigKey<S1> KS = new MockConfigKey<S1>(S1.class, "ks");
    }

    @Test
    public void testReadWriteInherited() {
        YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("s2", S2.class, MutableMap.of("keys", "config"))
        .reading("{ type: s2, k1: foo, k2: bar }").writing(new S2(MutableMap.of("k1", "foo", "k2", "bar")))
        .doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testReadWriteNested() {
        YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("s1", S1.class, MutableMap.of("keys", "config"))
        .addTypeWithAnnotationsAndConfig("s2", S2.class, MutableMap.of("keys", "config"))
        .reading("{ type: s2, ks: { k1: foo } }").writing(new S2(MutableMap.of("ks", new S1(MutableMap.of("k1", "foo")))))
        .doReadWriteAssertingJsonMatch();
    }
    @Test
    public void testReadWriteNestedGlobalConfigKeySupport() {
        YomlTestFixture y = YomlTestFixture.newInstance(YomlConfig.Builder.builder()
            .serializersPostAdd(new InstantiateTypeFromRegistryUsingConfigMap.Factory().newConfigKeyClassScanningSerializers("keys", "config", true))
            .serializersPostAddDefaults().build());
        y.addTypeWithAnnotations("s1", S1.class)
        .addTypeWithAnnotations("s2", S2.class)
        .reading("{ type: s2, ks: { k1: foo } }").writing(new S2(MutableMap.of("ks", new S1(MutableMap.of("k1", "foo")))))
        .doReadWriteAssertingJsonMatch();
    }
    
    @YomlConstructorConfigMap(value="keys", writeAsKey="extraConfig")
    static class S3 extends S1 {
        S3(Map<String,?> keys) { super(keys); }
        static ConfigKey<String> K2 = new MockConfigKey<String>(String.class, "k2");
        static ConfigKey<S1> KS1 = new MockConfigKey<S1>(S1.class, "ks1");
        static ConfigKey<S3> KS3 = new MockConfigKey<S3>(S3.class, "ks3");
    }
    
    @Test
    public void testReadWriteAnnotation() {
        YomlTestFixture.newInstance()
            .addTypeWithAnnotations("s3", S3.class)
            .reading("{ type: s3, ks3: { k1: foo } }").writing(new S3(MutableMap.of("ks3", new S3(MutableMap.of("k1", "foo")))))
            .doReadWriteAssertingJsonMatch();
    }
    
    @Test
    public void testReadWriteAnnotationTypeInfoNeeded() {
        YomlTestFixture.newInstance()
            .addTypeWithAnnotations("s1", S1.class)
            .addTypeWithAnnotations("s3", S3.class)
            .reading("{ type: s3, ks1: { type: s3, k1: foo } }").writing(new S3(MutableMap.of("ks1", new S3(MutableMap.of("k1", "foo")))))
            .doReadWriteAssertingJsonMatch();
    }
    
    @Test
    public void testReadWriteExtraField() {
        YomlTestFixture.newInstance()
            .addTypeWithAnnotations("s3", S3.class)
            .reading("{ type: s3, k1: foo, extraConfig: { k0: { type: string, value: bar } } }").writing(new S3(MutableMap.of("k1", "foo", "k0", "bar")))
            .doReadWriteAssertingJsonMatch();
    }

}
