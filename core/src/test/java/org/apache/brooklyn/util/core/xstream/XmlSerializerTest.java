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
package org.apache.brooklyn.util.core.xstream;

import com.google.common.base.Objects;
import com.thoughtworks.xstream.annotations.XStreamAlias;
import com.thoughtworks.xstream.annotations.XStreamConverter;
import com.thoughtworks.xstream.converters.basic.BooleanConverter;
import com.thoughtworks.xstream.converters.extended.ToAttributedValueConverter;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.xstream.LambdaPreventionMapper.LambdaPersistenceMode;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.testng.Assert.assertEquals;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

public class XmlSerializerTest {

    private static final Logger LOG = LoggerFactory.getLogger(XmlSerializerTest.class);

    protected XmlSerializer<Object> serializer;
    
    @BeforeMethod(alwaysRun=true)
    private void setUp() {
        serializer = new XmlSerializer<Object>();
    }
    
    @Test
    public void testSimple() throws Exception {
        assertSerializeAndDeserialize("abc");
        assertSerializeAndDeserialize(1);
        assertSerializeAndDeserialize(true);
        assertSerializeAndDeserialize(new ObjectHolder(null));
        assertSerializeAndDeserialize(new StringHolder("abc"));
        assertSerializeAndDeserialize(new ObjectHolder("abc"));
        assertSerializeAndDeserialize(new ObjectHolder(Arrays.asList("val1", "val2")));
        assertSerializeAndDeserialize(new ObjectHolder(123));
        assertSerializeAndDeserialize(new IntHolder(123));
        assertSerializeAndDeserialize(new IntegerHolder(123));
    }

    @Test
    public void testXmlOutput() throws Exception {
        Asserts.assertEquals(serializer.toString(MutableMap.of("a", 1)),
                "<MutableMap>\n" +
                "  <a type=\"int\">1</a>\n" +
                "</MutableMap>");

        Asserts.assertEquals(serializer.toString(MutableSet.of(1)),
                "<MutableSet>\n" +
                "  <int>1</int>\n" +
                "</MutableSet>");

        // no nice serializer for this yet
        Asserts.assertEquals(serializer.toString(MutableList.of(1)),
                "<MutableList>\n" +
                "  <int>1</int>\n" +
                "</MutableList>"
                // old (also accepted as input)
//                "<MutableList serialization=\"custom\">\n" +
//                "  <unserializable-parents/>\n" +
//                "  <list>\n" +
//                "    <default>\n" +
//                "      <size>1</size>\n" +
//                "    </default>\n" +
//                "    <int>1</int>\n" +
//                "    <int>1</int>\n" +
//                "  </list>\n" +
//                "</MutableList>"
                );
    }
    
    @Test
    public void testAnnotationsExample() throws Exception {
        assertSerializeAndDeserialize(new RendezvousMessage(123, true, "mycontent"));
    }

    @Test
    public void testObjectFieldDeserializingStringWhenNoClassSpecified() throws Exception {
        String tag = "org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-ObjectHolder";
        String xml = "<"+tag+"><val>myval</val></"+tag+">";
        Object result = serializer.fromString(xml);
        assertEquals(((ObjectHolder)result).val, "myval");
    }
    
    @Test
    public void testIntFieldDeserializingStringWhenNoClassSpecified() throws Exception {
        String tag = "org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntHolder";
        String xml = "<"+tag+"><val>123</val></"+tag+">";
        Object result = serializer.fromString(xml);
        assertEquals(((IntHolder)result).val, 123);
    }
    
    @Test
    public void testIntegerFieldDeserializingStringWhenNoClassSpecified() throws Exception {
        String tag = "org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntegerHolder";
        String xml = "<"+tag+"><val>123</val></"+tag+">";
        Object result = serializer.fromString(xml);
        assertEquals(((IntegerHolder)result).val, Integer.valueOf(123));
    }
    
    /**
     * See https://issues.apache.org/jira/browse/BROOKLYN-305 and http://x-stream.github.io/faq.html#XML_control_char
     */
    @Test
    public void testIllegalXmlCharacter() throws Exception {
        String val = "PREFIX_\u001b\u0000_SUFFIX";
        assertEquals(val.charAt(7), 27); // expect that to give us unicode character 27
        assertEquals(val.charAt(8), 0); // expect that to give us unicode character 0
        assertSerializeAndDeserialize(val);
        assertSerializeAndDeserialize(new StringHolder(val));
    }


    // lambdas

    private void serializeExpectingNull(Supplier<String> s) {
        String xml = serializer.toString(s);
        Object s2 = serializer.fromString(xml);
        Asserts.assertStringContainsIgnoreCase(xml, "null");
        Asserts.assertNull(s2);
    }
    private void serializeExpectingFailure(Supplier<String> s) {
        serializeExpectingFailure(s, null);
    }
    private void serializeExpectingFailure(Object s, Consumer<Throwable> ...otherChecks) {
        Asserts.assertFailsWith(()->serializer.toString(s),
                error -> {
                    Asserts.expectedFailureContainsIgnoreCase(error, "lambda");
                    if (otherChecks!=null) Arrays.asList(otherChecks).forEach(c -> c.accept(error));
                    return true;
                });
    }
    private void serializeExpectingHelloSupplied(Supplier<String> s) {
        String xml = serializer.toString(s);
        Object s2 = serializer.fromString(xml);
        Asserts.assertInstanceOf(s2, Supplier.class);
        Asserts.assertEquals( ((Supplier<String>)s2).get(), s.get() );
    }

    interface SerializableSupplier<T> extends Supplier<T>, Serializable {}

    @Test
    public void testLambdaXstreamDefaultNotSerializable() throws Exception {
        serializeExpectingNull( () -> "hello" );
    }

    @Test
    public void testLambdaXstreamDefaultSerializable() throws Exception {
        serializeExpectingHelloSupplied( (SerializableSupplier<String>) () -> "hello" );
    }

    @Test
    public void testLambdaXstreamFailingAllSerializable() throws Exception {
        serializer = new XmlSerializer<Object>(null, null, LambdaPreventionMapper.factory(ConfigBag.newInstance().configure(LambdaPreventionMapper.LAMBDA_PERSISTENCE, LambdaPersistenceMode.FAIL)));
        serializeExpectingFailure(() -> "hello");
        serializeExpectingFailure((SerializableSupplier<String>) () -> "hello");
        serializeExpectingFailure(MutableMap.of("key", (Supplier) () -> "hello"), e -> Asserts.expectedFailureContainsIgnoreCase(e, "MutableMap/key"));
    }

    @Test(groups="WIP")
    public void testLambdaXstreamFailingWithNonSerializableException() throws Exception {
        SafeThrowableConverter.TODO++;

        // this test passes but the fact it comes back as object is problematic; if the field needs a supplier or something else, it won't deserialize.

        serializer = new XmlSerializer<Object>(null, null, LambdaPreventionMapper.factory(ConfigBag.newInstance().configure(LambdaPreventionMapper.LAMBDA_PERSISTENCE, LambdaPersistenceMode.FAIL)));
        String safelyTidiedException = serializer.toString(MutableMap.of("key", new RuntimeException("some exception",
                new TestExceptionWithContext("wrapped exception", null, (Supplier) () -> "hello"))));
        Object tidiedException = serializer.fromString(safelyTidiedException);
        Throwable e1 = (Throwable) ((Map)tidiedException).get("key");
        Throwable e2 = e1.getCause();
        Asserts.assertEquals("wrapped exception", e2.getMessage());
        Asserts.assertThat(((TestExceptionWithContext)e2).context, x -> x==null || x.getClass().equals(Object.class));
    }

    public static class TestExceptionWithContext extends Exception {
        final Object context;
        public TestExceptionWithContext(String msg, Throwable cause, Object context) {
            super(msg, cause);
            this.context = context;
        }
    }

    @Test
    public void testLambdaXstreamFailingNonSerializable() throws Exception {
        serializer = new XmlSerializer<Object>(null, null, LambdaPreventionMapper.factory(ConfigBag.newInstance()));
        serializeExpectingFailure( () -> "hello" );
        serializeExpectingHelloSupplied( (SerializableSupplier<String>) () -> "hello" );
    }


    protected void assertSerializeAndDeserialize(Object val) throws Exception {
        String xml = serializer.toString(val);
        Object result = serializer.fromString(xml);
        LOG.debug("val="+val+"'; xml="+xml+"; result="+result);
        assertEquals(result, val);
    }

    public static class StringHolder {
        public String val;
        
        StringHolder(String val) {
            this.val = val;
        }
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof StringHolder) && val.equals(((StringHolder)obj).val);
        }
        @Override
        public int hashCode() {
            return val.hashCode();
        }
    }
    
    private static class ObjectHolder {
        @XStreamConverter(ObjectWithDefaultStringImplConverter.class)
        private final Object val;
        
        ObjectHolder(Object val) {
            this.val = val;
        }
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof ObjectHolder) && Objects.equal(val, ((ObjectHolder)obj).val);
        }
        @Override
        public int hashCode() {
            return (val == null) ? 259238 : val.hashCode();
        }
        @Override
        public String toString() {
            try {
                return "ObjectHolder(val="+val+", type="+(val == null ? null : val.getClass())+")";
            } catch (NullPointerException e) {
                System.out.println("val="+val);
                e.printStackTrace();
                return "what?";
            }
        }
    }

    public static class IntHolder {
        public int val;
        
        IntHolder(int val) {
            this.val = val;
        }
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IntHolder) && val == ((IntHolder)obj).val;
        }
        @Override
        public int hashCode() {
            return val;
        }
    }
    
    public static class IntegerHolder {
        public Integer val;
        
        IntegerHolder(int val) {
            this.val = val;
        }
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof IntegerHolder) && val.equals(((IntegerHolder)obj).val);
        }
        @Override
        public int hashCode() {
            return (val == null) ? 259238 : val.hashCode();
        }
    }
    
    @XStreamAlias("message")
    @XStreamConverter(value=ToAttributedValueConverter.class, strings={"content"})
    static class RendezvousMessage {

        @XStreamAlias("type")
        private int messageType;

        private String content;
        
        @XStreamConverter(value=BooleanConverter.class, booleans={false}, strings={"yes", "no"})
        private boolean important;

        public RendezvousMessage(int messageType, boolean important, String content) {
            this.messageType = messageType;
            this.important = important;
            this.content = content;
        }

        @Override
        public int hashCode() {
            return messageType;
        }
        
        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof RendezvousMessage)) return false;
            RendezvousMessage o = (RendezvousMessage) obj;
            return (messageType == o.messageType) && important == o.important && content.equals(o.content);
        }
    }
}
