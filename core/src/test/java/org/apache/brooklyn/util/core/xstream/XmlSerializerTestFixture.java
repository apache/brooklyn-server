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
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;

public class XmlSerializerTestFixture {

    private static final Logger LOG = LoggerFactory.getLogger(XmlSerializerTestFixture.class);

    protected XmlSerializer<Object> serializer;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() {
        serializer = new XmlSerializer<Object>();
    }
    
    protected Object assertDeserialize(String fmt, Object obj) {
        Object out = serializer.fromString(fmt);
        Assert.assertEquals(out, obj);
        return out;
    }

    protected Object assertSerializedFormat(Object obj, String fmt) {
        String s1 = serializer.toString(obj);
        Assert.assertEquals(s1, fmt);
        Object out = serializer.fromString(s1);
        Assert.assertEquals(out, obj);
        return out;
    }

    protected void assertSerializeAndDeserialize(Object val) throws Exception {
        String xml = serializer.toString(val);
        Object result = serializer.fromString(xml);
        LOG.debug("val="+val+"'; xml="+xml+"; result="+result);
        assertEquals(result, val);
    }
}
