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

import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.collections.Jsonya;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.yoml.annotations.YomlConfigMapConstructor;
import org.apache.brooklyn.util.yoml.tests.TopLevelConfigKeysTests.MockConfigKey;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.reflect.TypeToken;

/** Tests that the default serializers can read/write types and fields. 
 * <p>
 * And shows how to use them at a low level.
 */
public class YomlConfigKeyGenericsTests {

    private static final Logger log = LoggerFactory.getLogger(YomlConfigKeyGenericsTests.class);

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
    @SuppressWarnings({ "rawtypes" })
    static class MG extends M0 {
        static MockConfigKey<Map> KR = new MockConfigKey<Map>(Map.class, "kr");
        @SuppressWarnings("serial")
        static MockConfigKey<Map<String,Integer>> KG = new MockConfigKey<Map<String,Integer>>(new TypeToken<Map<String,Integer>>() {}, "kg");

        MG(Map<String, ?> keys) { super(keys); }
    }
    
    static final Map<String, ?> CONF_MAP = MutableMap.of("x", 1);
    final static String RAW_OUT = "{ type: mg, kr: { type: 'map<string,json>', value: { x: 1 } } }";
    static final MG RAW_IN = new MG(MutableMap.of("kr", CONF_MAP));
    
    @Test
    public void testWriteRaw() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("mg", MG.class);

        y.write(RAW_IN);
        log.info("M1B written as: "+y.lastWriteResult);
        YomlTestFixture.assertEqualsIgnoringQuotes(Jsonya.newInstance().add(y.lastWriteResult).toString(), 
            RAW_OUT, "wrong serialization");
    }
    
    @Test
    public void testReadRaw() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("mg", MG.class);
        
        y.read(RAW_OUT, null);

        MG mg = (MG)y.lastReadResult;
        
        Asserts.assertEquals(mg.conf.get(MG.KR.getName()), CONF_MAP);
        Asserts.assertEquals(mg, RAW_IN);
    }

    final static String GEN_OUT = "{ type: mg, kg: { x: 1 } }";
    static final MG GEN_IN = new MG(MutableMap.of("kg", CONF_MAP));

    @Test
    public void testWriteGen() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("mg", MG.class);

        y.write(GEN_IN);
        log.info("M1B written as: "+y.lastWriteResult);
        YomlTestFixture.assertEqualsIgnoringQuotes(Jsonya.newInstance().add(y.lastWriteResult).toString(), 
            GEN_OUT, "wrong serialization");
    }
    
    @Test
    public void testReadGen() {
        YomlTestFixture y = YomlTestFixture.newInstance().addTypeWithAnnotations("mg", MG.class);
        
        y.read(GEN_OUT, null);

        MG mg = (MG)y.lastReadResult;
        
        Asserts.assertEquals(mg.conf.get(MG.KG.getName()), CONF_MAP);
        Asserts.assertEquals(mg, GEN_IN);
    }

}
