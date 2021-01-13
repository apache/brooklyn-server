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

import java.net.Inet4Address;
import java.net.UnknownHostException;

import org.testng.annotations.Test;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;

@Test
public class HashMultimapXmlSerializerTest extends XmlSerializerTestFixture {

    // TODO: This is not the serialized form, but would be much nicer if it was! 
    @Test(groups="WIP")
    public void testHashMultimapEmpty() throws UnknownHostException {
        assertSerializedFormat(HashMultimap.create(), "<HashMultimap/>");
    }

    // TODO: This is not the serialized form, but would be much nicer if it was! 
    @Test(groups="WIP")
    public void testHashMultimap() throws UnknownHostException {
        HashMultimap<Object, Object> obj = HashMultimap.create();
        obj.put("mystring", "myval1");
        obj.put("mystring", "myval2");
        obj.put("myintholder", new XmlSerializerTest.IntegerHolder(123));
        obj.put("myInet4Address", Inet4Address.getByName("1.1.1.1")); 

        String expected = Joiner.on("\n").join(
                "<HashMultimap>",
                "  <mystring>myval1</mystring>",
                "  <mystring>myval2</mystring>",
                "  <myintholder>",
                "    <java.net.Inet4Address>one.one.one.one/1.1.1.1</java.net.Inet4Address>",
                "  </myintholder>",
                "  <myInet4Address>",
                "    <org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntegerHolder>",
                "      <val>123</val>",
                "    </org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntegerHolder>",
                "  </myInet4Address>",
                "</HashMultimap>");

        assertSerializedFormat(obj, expected);
    }

    @Test
    public void testLegacyHashMultimapEmpty() throws UnknownHostException {
        String fmt = Joiner.on("\n").join(
                "<com.google.common.collect.HashMultimap serialization=\"custom\">",
                "  <unserializable-parents/>",
                "  <com.google.common.collect.HashMultimap>",
                "    <default/>",
                "    <int>2</int>",
                "    <int>0</int>",
                "  </com.google.common.collect.HashMultimap>",
                "</com.google.common.collect.HashMultimap>");
        
        assertDeserialize(fmt, HashMultimap.create());
    }

    @Test
    public void testLegacyHashMultimap() throws UnknownHostException {
        HashMultimap<Object, Object> obj = HashMultimap.create();
        obj.put("mystring", "myval1");
        obj.put("mystring", "myval2");
        obj.put("myintholder", new XmlSerializerTest.IntegerHolder(123));
        obj.put("myInet4Address", Inet4Address.getByName("1.1.1.1")); 
        
        String fmt = Joiner.on("\n").join(
                "<com.google.common.collect.HashMultimap serialization=\"custom\">",
                "  <unserializable-parents/>",
                "  <com.google.common.collect.HashMultimap>",
                "    <default/>",
                "    <int>2</int>",
                "    <int>3</int>",
                "    <string>myInet4Address</string>",
                "    <int>1</int>",
                "    <java.net.Inet4Address>one.one.one.one/1.1.1.1</java.net.Inet4Address>",
                "    <string>mystring</string>",
                "    <int>2</int>",
                "    <string>myval1</string>",
                "    <string>myval2</string>",
                "    <string>myintholder</string>",
                "    <int>1</int>",
                "    <org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntegerHolder>",
                "      <val>123</val>",
                "    </org.apache.brooklyn.util.core.xstream.XmlSerializerTest_-IntegerHolder>",
                "  </com.google.common.collect.HashMultimap>",
                "</com.google.common.collect.HashMultimap>");

        assertDeserialize(fmt, obj);
    }
}
