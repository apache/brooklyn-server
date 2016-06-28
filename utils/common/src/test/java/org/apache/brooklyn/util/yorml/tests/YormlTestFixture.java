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
package org.apache.brooklyn.util.yorml.tests;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.yorml.Yorml;
import org.apache.brooklyn.util.yorml.YormlSerializer;
import org.testng.Assert;

public class YormlTestFixture {
    
    public static YormlTestFixture newInstance() { return new YormlTestFixture(); }
    
    MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
    Yorml y = Yorml.newInstance(tr);
    
    Object writeObject;
    String writeObjectExpectedType;
    Object lastWriteResult;
    String readObject;
    String readObjectExpectedType;
    Object lastReadResult;
    Object lastResult;

    public YormlTestFixture writing(Object objectToWrite) {
        return writing(objectToWrite, null);
    }
    public YormlTestFixture writing(Object objectToWrite, String expectedType) {
        writeObject = objectToWrite;
        writeObjectExpectedType = expectedType;
        return this;
    }
    public YormlTestFixture reading(String stringToRead) {
        return reading(stringToRead, null);
    }
    public YormlTestFixture reading(String stringToRead, String expectedType) {
        readObject = stringToRead;
        readObjectExpectedType = expectedType;
        return this;
    }

    public YormlTestFixture write(Object objectToWrite) {
        return write(objectToWrite, null);
    }
    public YormlTestFixture write(Object objectToWrite, String expectedType) {
        writing(objectToWrite, expectedType);
        lastWriteResult = y.write(objectToWrite, expectedType);
        lastResult = lastWriteResult;
        return this;
    }
    public YormlTestFixture read(String objectToRead, String expectedType) {
        reading(objectToRead, expectedType);
        lastReadResult = y.read(objectToRead, expectedType);
        lastResult = lastReadResult;
        return this;
    }
    
    public YormlTestFixture assertResult(Object expectation) {
        if (expectation instanceof String) {
            if (lastResult instanceof Map || lastResult instanceof Collection) {
                assertEqualsIgnoringQuotes(Jsonya.newInstance().add(lastResult).toString(), expectation, "Result as JSON string does not match expectation");
            } else {
                assertEqualsIgnoringQuotes(Strings.toString(lastResult), expectation, "Result toString does not match expectation");
            }
        } else {
            Assert.assertEquals(lastResult, expectation);
        }
        return this;
    }
    public YormlTestFixture doReadWriteAssertingJsonMatch() {
        read(readObject, readObjectExpectedType);
        write(writeObject, writeObjectExpectedType);
        assertEqualsIgnoringQuotes(Jsonya.newInstance().add(lastWriteResult).toString(), readObject, "Write output should read input");
        assertEqualsIgnoringQuotes(lastReadResult, writeObject, "Read output should match write input");
        return this;
    }
    
    static void assertEqualsIgnoringQuotes(Object s1, Object s2, String message) {
        if (s1 instanceof String) s1 = Strings.replaceAllNonRegex((String)s1, "\"", "");
        if (s2 instanceof String) s2 = Strings.replaceAllNonRegex((String)s2, "\"", "");
        Assert.assertEquals(s1, s2, message);
    }
    
    public YormlTestFixture addType(String name, Class<?> type) { tr.put(name, type); return this; }
    public YormlTestFixture addType(String name, Class<?> type, List<YormlSerializer> serializers) { tr.put(name, type, serializers); return this; }
    public YormlTestFixture addType(String name, String yamlDefinition) { tr.put(name, yamlDefinition); return this; }
    public YormlTestFixture addType(String name, String yamlDefinition, List<YormlSerializer> serializers) { tr.put(name, yamlDefinition, serializers); return this; }
}