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

import java.util.Map;

import org.apache.brooklyn.config.ConfigInheritance;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;
import org.apache.brooklyn.util.yoml.tests.TopLevelConfigKeysTests.MockConfigKey;
import org.apache.brooklyn.util.yoml.tests.YomlConfigKeyGenericsTests.M0;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YomlConfigKeyInheritanceTests {

    private static final Logger log = LoggerFactory.getLogger(YomlConfigKeyInheritanceTests.class);

    @YomlConfigMapConstructor("conf")
    @SuppressWarnings({ "deprecation" })
    static class M1 extends M0 {
        @SuppressWarnings("serial")
        static MockConfigKey<Map<String,Integer>> KM = new MockConfigKey<Map<String,Integer>>(new TypeToken<Map<String,Integer>>() {}, "km");
        static { KM.inheritance = ConfigInheritance.DEEP_MERGE; }

        @SuppressWarnings("serial")
        static MockConfigKey<Map<String,Integer>> KO = new MockConfigKey<Map<String,Integer>>(new TypeToken<Map<String,Integer>>() {}, "ko");
        static { KO.inheritance = ConfigInheritance.ALWAYS; }

        M1(Map<String, ?> keys) { super(keys); }
    }
        
    @Test
    public void testReadMergedMap() {
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotations("m1", M1.class)
        .addType("m1a", "{ type: m1, km: { a: 1, b: 1 }, ko: { a: 1, b: 1 } }")
        .addType("m1b", "{ type: m1a, km: { b: 2 }, ko: { b: 2 } }");
        
        y.read("{ type: m1b }", null);

        M1 m1b = (M1)y.lastReadResult;
        Asserts.assertEquals(m1b.conf.get(M1.KM.getName()), MutableMap.of("a", 1, "b", 2));
        // MERGE is the default, OVERWRITE not respected:
        Asserts.assertEquals(m1b.conf.get(M1.KO.getName()), MutableMap.of("b", 2));
    }

    @Test
    public void testWrite() {
        // the write is not smart enough to look at default/inherited KV pairs
        // (this would be nice to change, but a lot of work and not really worth it)
        
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfig("m1", M1.class, MutableMap.of("keys", "config"));

        M1 m1b = new M1(MutableMap.of("km", MutableMap.of("a", 1, "b", 2), "ko", MutableMap.of("a", 1, "b", 2)));
        y.write(m1b);
        log.info("written as "+y.lastWriteResult);
        YomlTestFixture.assertEqualsIgnoringQuotes(Jsonya.newInstance().add(y.lastWriteResult).toString(), 
            "{ type=m1, km:{ a: 1, b: 2}, ko: { a: 1, b: 2 } }", "wrong serialization");
        YomlTestFixture.assertEqualsIgnoringQuotes(y.lastWriteResult.toString(), 
            "{ type=m1, km:{ a: 1, b: 2}, ko: { a: 1, b: 2 } }", "wrong serialization");
    }

}
