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
package org.apache.brooklyn.camp.yoml;

import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigInheritance;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;
import org.apache.brooklyn.util.yoml.tests.YomlTestFixture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

/** Tests that config key inheritance strategies are obeyed when reading with supertypes. 
 */
public class YomlConfigKeyInheritanceTests {

    private static final Logger log = LoggerFactory.getLogger(YomlConfigKeyInheritanceTests.class);

    @YomlConfigMapConstructor("conf")
    static class M0 {
        Map<String,Object> conf = MutableMap.of();
        M0(Map<String,?> keys) { this.conf.putAll(keys); }
        
        @Override
        public boolean equals(Object obj) {
            return (obj instanceof M0) && ((M0)obj).conf.equals(conf);
        }
        @Override
        public int hashCode() {
            return conf.hashCode();
        }
        @Override
        public String toString() {
            return super.toString()+conf;
        }
    }
    
    @YomlConfigMapConstructor("conf")
    static class M1 extends M0 {
        @SuppressWarnings("serial")
        static ConfigKey<Map<String,Integer>> KM = ConfigKeys.builder(new TypeToken<Map<String,Integer>>() {}, "km")
            .typeInheritance(BasicConfigInheritance.DEEP_MERGE).build();

        @SuppressWarnings("serial")
        static ConfigKey<Map<String,Integer>> KO = ConfigKeys.builder(new TypeToken<Map<String,Integer>>() {}, "ko")
            .typeInheritance(BasicConfigInheritance.OVERWRITE).build();

        M1(Map<String, ?> keys) { super(keys); }
    }
        
    @Test
    public void testReadMergedMap() {
        YomlTestFixture y = BrooklynYomlTestFixture.newInstance().addTypeWithAnnotations("m1", M1.class)
        .addType("m1a", "{ type: m1, km: { a: 1, b: 1 }, ko: { a: 1, b: 1 } }")
        .addType("m1b", "{ type: m1a, km: { b: 2 }, ko: { b: 2 } }");
        
        y.read("{ type: m1b }", null);

        M1 m1b = (M1)y.getLastReadResult();
        Asserts.assertEquals(m1b.conf.get(M1.KM.getName()), MutableMap.of("a", 1, "b", 2));
        Asserts.assertEquals(m1b.conf.get(M1.KO.getName()), MutableMap.of("b", 2));
    }

    @Test
    public void testWrite() {
        // the write is not smart enough to look at default/inherited KV pairs
        // (this would be nice to change, but a lot of work and not really worth it)
        
        YomlTestFixture y = YomlTestFixture.newInstance()
        .addTypeWithAnnotationsAndConfigFieldsIgnoringInheritance("m1", M1.class, MutableMap.of("keys", "config"));

        M1 m1b = new M1(MutableMap.of("km", MutableMap.of("a", 1, "b", 2), "ko", MutableMap.of("a", 1, "b", 2)));
        y.write(m1b);
        log.info("written as "+y.getLastWriteResult());
        y.assertLastWriteIgnoringQuotes( 
            "{ type=m1, km:{ a: 1, b: 2 }, ko: { a: 1, b: 2 }}", "wrong serialization");
    }

}
