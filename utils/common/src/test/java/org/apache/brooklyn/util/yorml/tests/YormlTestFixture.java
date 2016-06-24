package org.apache.brooklyn.util.yorml.tests;

import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.yorml.Yorml;
import org.testng.Assert;

public class YormlTestFixture {
    
    public static YormlTestFixture newInstance() { return new YormlTestFixture(); }
    
    MockYormlTypeRegistry tr = new MockYormlTypeRegistry();
    Yorml y = Yorml.newInstance(tr);
    
    Object writeObject;
    Object lastWriteResult;
    String readObject;
    String readObjectExpectedType;
    Object lastReadResult;
    Object lastResult;

    public YormlTestFixture writing(Object objectToWrite) {
        writeObject = objectToWrite;
        return this;
    }
    public YormlTestFixture reading(String stringToRead, String expectedType) {
        readObject = stringToRead;
        readObjectExpectedType = expectedType;
        return this;
    }
    public YormlTestFixture reading(String stringToRead) {
        readObject = stringToRead;
        return this;
    }

    public YormlTestFixture write(Object objectToWrite) {
        writing(objectToWrite);
        lastWriteResult = y.write(objectToWrite);
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
        Assert.assertEquals(lastResult, expectation);
        return this;
    }
    public YormlTestFixture doReadWriteAssertingJsonMatch() {
        read(readObject, readObjectExpectedType);
        write(writeObject);
        Assert.assertEquals(Jsonya.newInstance().add(lastWriteResult).toString(), readObject, "Write output matches read input.");
        Assert.assertEquals(lastReadResult, writeObject, "Read output matches write input.");
        return this;
    }
    
    public YormlTestFixture addType(String name, Class<?> type) {
        tr.put(name, type);
        return this;
    }
}