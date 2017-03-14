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

import static org.testng.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
        assertSerializeAndDeserialize(new StringHolder("abc"));
        assertSerializeAndDeserialize(new ObjectHolder("abc"));
    }

    @Test
    public void testObjectFieldDeserializingWhenNoClassSpecified() throws Exception {
        String tag = "org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-ObjectHolder";
        String xml = "<"+tag+"><val>myval</val></"+tag+">";
        Object result = serializer.fromString(xml);
        assertEquals(((ObjectHolder)result).val, "myval");
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
    
    public static class ObjectHolder {
        public Object val;
        
        ObjectHolder(Object val) {
            this.val = val;
        }
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof ObjectHolder) && val.equals(((ObjectHolder)obj).val);
        }
        @Override
        public int hashCode() {
            return (val == null) ? 259238 : val.hashCode();
        }
    }
}
