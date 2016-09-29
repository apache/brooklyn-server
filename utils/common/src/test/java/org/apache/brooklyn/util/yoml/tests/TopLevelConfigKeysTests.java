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
import java.util.Objects;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.config.ConfigInheritance.ConfigInheritanceContext;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.annotations.Alias;
import org.apache.brooklyn.util.yoml.annotations.YomlAllFieldsTopLevel;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.reflect.TypeToken;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class TopLevelConfigKeysTests {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TopLevelConfigKeysTests.class);
    
    static class MockConfigKey<T> implements ConfigKey<T> {
        String name;
        Class<T> type;
        TypeToken<T> typeToken;
        T defaultValue;
        
        public MockConfigKey(Class<T> type, String name) {
            this.name = name;
            this.type = type;
            this.typeToken = TypeToken.of(type);
        }

        public MockConfigKey(Class<T> type, String name, T defaultValue) {
            this.name = name;
            this.type = type;
            this.typeToken = TypeToken.of(type);
            this.defaultValue = defaultValue;
        }

        @SuppressWarnings("unchecked")
        public MockConfigKey(TypeToken<T> typeToken, String name) {
            this.name = name;
            this.typeToken = typeToken;
            this.type = (Class<T>)typeToken.getRawType();
        }

        @Override public String getDescription() { return null; }
        @Override public String getName() { return name; }
        @Override public Collection<String> getNameParts() { return MutableList.of(name); }
        @Override public TypeToken<T> getTypeToken() { return typeToken; }
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
        @Override public ConfigInheritance getInheritanceByContext(ConfigInheritanceContext context) { return null; }
        @Override public Map<ConfigInheritanceContext, ConfigInheritance> getInheritanceByContext() { return MutableMap.of(); }
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
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s1", S1.class, MutableMap.of("keys", "config"));
        
        y.read("{ type: s1, k1: foo }", null);

        Asserts.assertInstanceOf(y.lastReadResult, S1.class);
        Asserts.assertEquals(((S1)y.lastReadResult).keys.get("k1"), "foo");
    }
    
    @Test
    public void testReadExpectingType() {
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s1", S1.class, MutableMap.of("keys", "config"));
        
        y.read("{ k1: foo }", "s1");

        Asserts.assertInstanceOf(y.lastReadResult, S1.class);
        Asserts.assertEquals(((S1)y.lastReadResult).keys.get("k1"), "foo");
    }

    @Test
    public void testWrite() {
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s1", S1.class, MutableMap.of("keys", "config"));

        S1 s1 = new S1(MutableMap.of("k1", "foo"));
        y.write(s1);
        YomlTestFixture.assertEqualish(Jsonya.newInstance().add(y.lastWriteResult).toString(), "{ type: s1, k1: foo }", "wrong serialization");
    }

    @Test
    public void testReadWrite() {
        YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s1", S1.class, MutableMap.of("keys", "config"))
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
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s2", S2.class, MutableMap.of("keys", "config"))
        .reading("{ type: s2, k1: foo, k2: bar }").writing(new S2(MutableMap.of("k1", "foo", "k2", "bar")))
        .doReadWriteAssertingJsonMatch();
    }

    @Test
    public void testReadWriteNested() {
        YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s1", S1.class, MutableMap.of("keys", "config"))
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("s2", S2.class, MutableMap.of("keys", "config"))
        .reading("{ type: s2, ks: { k1: foo } }").writing(new S2(MutableMap.of("ks", new S1(MutableMap.of("k1", "foo")))))
        .doReadWriteAssertingJsonMatch();
    }
    @Test
    public void testReadWriteNestedGlobalConfigKeySupport() {
        YomlTestFixture y = YomlTestFixture.newInstance(YomlConfig.Builder.builder()
            .serializersPostAdd(InstantiateTypeFromRegistryUsingConfigMap.newFactoryIgnoringInheritance().newConfigKeyClassScanningSerializers("keys", "config", true))
            .serializersPostAddDefaults().build());
        y.addTypeWithAnnotations("s1", S1.class)
        .addTypeWithAnnotations("s2", S2.class)
        .reading("{ type: s2, ks: { k1: foo } }").writing(new S2(MutableMap.of("ks", new S1(MutableMap.of("k1", "foo")))))
        .doReadWriteAssertingJsonMatch();
    }
    
    @YomlConfigMapConstructor(value="keys", writeAsKey="extraConfig")
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

    @YomlConfigMapConstructor("")
    @YomlAllFieldsTopLevel
    static class KeyAsField {
        @Alias("k")
        static ConfigKey<String> K1 = new MockConfigKey<String>(String.class, "key1");
        final String key1Field;
        transient final Map<String,?> keysSuppliedToConstructorForTestAssertions;
        KeyAsField(Map<String,?> keys) {
            key1Field = (String) keys.get(K1.getName());
            keysSuppliedToConstructorForTestAssertions = keys;
        }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof KeyAsField) && Objects.equals( ((KeyAsField)obj).key1Field, key1Field);
        }
        @Override
        public int hashCode() {
            return Objects.hash(key1Field);
        }
        @Override
        public String toString() {
            return super.toString()+":"+key1Field;
        }
    }
    
    final KeyAsField KF_FOO = new KeyAsField(MutableMap.of("key1", "foo"));
    
    @Test
    public void testNoConfigMapFieldCanReadKeyToMapConstructor() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf", KeyAsField.class);
        y.read("{ key1: foo }", "kf").assertResult(KF_FOO);
        Assert.assertEquals(((KeyAsField)y.lastReadResult).keysSuppliedToConstructorForTestAssertions, KF_FOO.keysSuppliedToConstructorForTestAssertions);
    }
    @Test
    public void testNoConfigMapFieldCanReadKeyAliasToMapConstructor() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf", KeyAsField.class);
        y.read("{ k: foo }", "kf").assertResult(KF_FOO);
        Assert.assertEquals(((KeyAsField)y.lastReadResult).keysSuppliedToConstructorForTestAssertions, KF_FOO.keysSuppliedToConstructorForTestAssertions);
    }
    @Test
    public void testStaticFieldNameNotRelevant() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf", KeyAsField.class);
        try {
            y.read("{ k1: foo }", "kf");
            Asserts.shouldHaveFailedPreviously("Got "+y.lastReadResult);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "k1", "foo");
        }
    }
    
    @Test
    public void testNoConfigMapFieldCanWriteAndReadToFieldDirectly() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf", KeyAsField.class);
        // writing must write to the field directly because it is not defined how to reverse map to the config key
        y.writing(KF_FOO).reading("{ type: kf, key1Field: foo }").doReadWriteAssertingJsonMatch();
        // nothing passed to constructor, but constructor is invoked
        Assert.assertEquals(((KeyAsField)y.lastReadResult).keysSuppliedToConstructorForTestAssertions, MutableMap.of(),
            "Constructor given unexpectedly non-empty map: "+((KeyAsField)y.lastReadResult).keysSuppliedToConstructorForTestAssertions);
    }

    static class KfA2C extends KeyAsField {
        KfA2C(Map<String, ?> keys) { super(keys); }

        @Alias("key1Field")  // alias same name as field means it *is* passed to constructor
        static ConfigKey<String> K1 = new MockConfigKey<String>(String.class, "key1");
    }
    
    private static final KfA2C KF_A2C_FOO = new KfA2C(MutableMap.of("key1", "foo"));

    @Test
    public void testConfigKeyTopLevelInherited() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf-a2c", KfA2C.class);
        y.read("{ key1: foo }", "kf-a2c").assertResult(KF_A2C_FOO);
    }
    
    @Test
    public void testConfigKeyOverrideHidesParentAlias() {
        // this could be weakened to be allowed (but aliases at types must not be, for obvious reasons!) 
        
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf-a2c", KfA2C.class);
        try {
            y.read("{ k: foo }", "kf-a2c");
            Asserts.shouldHaveFailedPreviously("Got "+y.lastReadResult);
        } catch (Exception e) {
            Asserts.expectedFailureContainsIgnoreCase(e, "k", "foo");
        }
    }

    @Test
    public void testNoConfigMapFieldWillPreferConstructorIfKeyForFieldCanBeFound() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("kf-a2c", KfA2C.class);
        // writing must write to the field because it is not defined how to reverse map to the config key
        y.writing(KF_A2C_FOO).reading("{ type: kf-a2c, key1Field: foo }").doReadWriteAssertingJsonMatch();
        // is passed to constructor
        Assert.assertEquals(((KeyAsField)y.lastReadResult).keysSuppliedToConstructorForTestAssertions, KF_FOO.keysSuppliedToConstructorForTestAssertions);
    }

    
    static class SDefault extends S3 {
        transient final Map<String,?> keysSuppliedToConstructorForTestAssertions;
            
        SDefault(Map<String, ?> keys) { 
            super(keys); 
            keysSuppliedToConstructorForTestAssertions = keys;
        }

        static ConfigKey<String> KD = new MockConfigKey<String>(String.class, "keyD", "default");
    }

    @Test
    public void testConfigKeyDefaultsReadButNotWritten() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("s-default", SDefault.class);
        y.reading("{ }", "s-default").writing(new SDefault(MutableMap.<String,Object>of()), "s-default")
        .doReadWriteAssertingJsonMatch();
        
        Asserts.assertSize( ((SDefault)y.lastReadResult).keysSuppliedToConstructorForTestAssertions.keySet(), 0 );
    }

    @Test
    public void testConfigKeyDefaultsReadButNotWrittenWithAtLeastOneConfigValueSupplied() {
        // behaviour is different in this case
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("s-default", SDefault.class);
        y.reading("{ k2: x }", "s-default").writing(new SDefault(MutableMap.<String,Object>of("k2", "x")), "s-default")
        .doReadWriteAssertingJsonMatch();
        
        Asserts.assertSize( ((SDefault)y.lastReadResult).keysSuppliedToConstructorForTestAssertions.keySet(), 1 );
    }

}
