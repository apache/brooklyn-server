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

import java.util.List;

import org.apache.brooklyn.util.collections.MutableList;
import org.apache.brooklyn.util.yoml.internal.YomlUtils;
import org.apache.brooklyn.util.yoml.internal.YomlUtils.GenericsParse;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

public class YomlUtilsTest {

    @Test
    public void testGenericsParse() {
        GenericsParse gp = new YomlUtils.GenericsParse("foo<bar>");
        Assert.assertEquals(gp.baseType, "foo");
        Assert.assertEquals(gp.subTypes, MutableList.of("bar"));
        Assert.assertTrue(gp.isGeneric);
    }
    
    @SuppressWarnings("serial")
    @Test
    public void testGenericsWrite() {
        MockYomlTypeRegistry tr = new MockYomlTypeRegistry();
        tr.put("lst", List.class);
        tr.put("str", String.class);
        Assert.assertEquals(YomlUtils.getTypeNameWithGenerics(new TypeToken<List<String>>() {}, tr),
            "lst<str>");
    }
    
}
