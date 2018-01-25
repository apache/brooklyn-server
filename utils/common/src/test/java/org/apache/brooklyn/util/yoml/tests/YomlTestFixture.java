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
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yoml.Yoml;
import org.apache.brooklyn.util.yoml.YomlConfig;
import org.apache.brooklyn.util.yoml.YomlSerializer;
import org.apache.brooklyn.util.yoml.annotations.YomlAnnotations;
import org.apache.brooklyn.util.yoml.serializers.InstantiateTypeFromRegistryUsingConfigMap;
import org.testng.Assert;

public class YomlTestFixture {
    
    public static YomlTestFixture newInstance() { return new YomlTestFixture(); }
    public static YomlTestFixture newInstance(YomlConfig config) { return new YomlTestFixture(config); }
    
    final MockYomlTypeRegistry tr;
    final Yoml y;
    
    public YomlTestFixture() {
        this(YomlConfig.Builder.builder().serializersPostAddDefaults().build());
    }
    public YomlTestFixture(YomlConfig config) {
        if (config.getTypeRegistry()==null) {
            tr = new MockYomlTypeRegistry();
            config = YomlConfig.Builder.builder(config).typeRegistry(tr).build();
        } else {
            tr = null;
        }
        y = Yoml.newInstance(config);
    }
    
    Object writeObject;
    String writeObjectExpectedType;
    String lastWriteExpectedType;
    Object lastWriteResult;
    String readObject;
    String readObjectExpectedType;
    Object lastReadResult;
    String lastReadExpectedType;
    Object lastResult;

    public YomlTestFixture writing(Object objectToWrite) {
        return writing(objectToWrite, null);
    }
    public YomlTestFixture writing(Object objectToWrite, String expectedType) {
        writeObject = objectToWrite;
        writeObjectExpectedType = expectedType;
        return this;
    }
    public YomlTestFixture reading(String stringToRead) {
        return reading(stringToRead, null);
    }
    public YomlTestFixture reading(String stringToRead, String expectedType) {
        readObject = stringToRead;
        readObjectExpectedType = expectedType;
        return this;
    }

    public YomlTestFixture write(Object objectToWrite) {
        return write(objectToWrite, null);
    }
    public YomlTestFixture write(Object objectToWrite, String expectedType) {
        writing(objectToWrite, expectedType);
        lastWriteExpectedType = expectedType;
        lastWriteResult = y.write(objectToWrite, expectedType);
        lastResult = lastWriteResult;
        return this;
    }
    public YomlTestFixture read(String objectToRead, String expectedType) {
        reading(objectToRead, expectedType);
        lastReadExpectedType = expectedType;
        lastReadResult = y.read(objectToRead, expectedType);
        lastResult = lastReadResult;
        return this;
    }
    public YomlTestFixture writeLastRead() {
        write(lastReadResult, lastReadExpectedType);
        return this;
    }
    public YomlTestFixture readLastWrite() {
        read(asJson(lastWriteResult), lastWriteExpectedType);
        return this;
    }
    
    public YomlTestFixture assertResult(Object expectation) {
        if (expectation instanceof String) {
            if (lastResult instanceof Map || lastResult instanceof Collection) {
                assertEqualish(asJson(lastResult), expectation, "Result as JSON string does not match expectation");
            } else {
                assertEqualish(Strings.toString(lastResult), expectation, "Result toString does not match expectation");
            }
        } else {
            Assert.assertEquals(lastResult, expectation);
        }
        return this;
    }
    
    static String asJson(Object o) {
        return Jsonya.newInstance().add(o).toString();
    }
    
    public YomlTestFixture doReadWriteAssertingJsonMatch() {
        read(readObject, readObjectExpectedType);
        write(writeObject, writeObjectExpectedType);
        return assertLastsMatch();
    }
    
    public YomlTestFixture assertLastsMatch() {
        assertEqualish(asJson(lastWriteResult), readObject, "Write output should match read input");
        assertEqualish(lastReadResult, writeObject, "Read output should match write input");
        return this;
    }
    
    private static String removeGuff(String input) {
        return Strings.replaceAll(input, MutableMap.of("\"", "", "\'", "")
            .add("=", ": ").add(":  ", ": ").add(" :", ":")
            .add(" ,", ",").add(", ", ",") 
            .add("{ ", "{").add(" {", "{")
            .add(" }", "}").add("} ", "}")
            );
    }
    
    static void assertEqualish(Object s1, Object s2, String message) {
        if (s1 instanceof String) s1 = removeGuff((String)s1);
        if (s2 instanceof String) s2 = removeGuff((String)s2);
        Assert.assertEquals(s1, s2, message);
    }
    
    public void assertLastWriteIgnoringQuotes(String expected, String message) {
        assertEqualish(Jsonya.newInstance().add(getLastWriteResult()).toString(), expected, message);
    }
    public void assertLastWriteIgnoringQuotes(String expected) {
        assertEqualish(Jsonya.newInstance().add(getLastWriteResult()).toString(), expected, "mismatch on last write");
    }

    // methods below require using the default registry, will NPE otherwise
    
    public YomlTestFixture addType(String name, Class<?> type) { tr.put(name, type); return this; }
    public YomlTestFixture addType(String name, Class<?> type, List<? extends YomlSerializer> serializers) { tr.put(name, type, serializers); return this; }
    public YomlTestFixture addType(String name, String yamlDefinition) { tr.put(name, yamlDefinition); return this; }
    public YomlTestFixture addType(String name, String yamlDefinition, List<? extends YomlSerializer> serializers) { tr.put(name, yamlDefinition, serializers); return this; }
    
    public YomlTestFixture addTypeWithAnnotations(Class<?> type) {
        return addTypeWithAnnotations(null, type);
    }
    public YomlTestFixture addTypeWithAnnotations(String optionalName, Class<?> type) {
        Set<YomlSerializer> serializers = annotationsProvider().findSerializerAnnotations(type, false);
        for (String n: new YomlAnnotations().findTypeNamesFromAnnotations(type, optionalName, false)) {
            tr.put(n, type, serializers);
        }
        return this; 
    }
    public YomlTestFixture addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance(String optionalName, Class<?> type, 
            Map<String, String> configFieldsToKeys) {
        Set<YomlSerializer> serializers = annotationsProvider().findSerializerAnnotations(type, false);
        for (Map.Entry<String,String> entry: configFieldsToKeys.entrySet()) {
            serializers.addAll( InstantiateTypeFromRegistryUsingConfigMap.newFactoryIgnoringInheritance().newConfigKeyClassScanningSerializers(
                entry.getKey(), entry.getValue(), true) );
        }
        for (String n: new YomlAnnotations().findTypeNamesFromAnnotations(type, optionalName, false)) {
            tr.put(n, type, serializers);
        }
        return this;
    }
    protected YomlAnnotations annotationsProvider() {
        return new YomlAnnotations();
    }

    public Object getLastReadResult() {
        return lastReadResult;
    }
    public Object getLastWriteResult() {
        return lastWriteResult;
    }
}
