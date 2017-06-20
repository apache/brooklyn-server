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
package org.apache.brooklyn.core.policy.basic;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;

import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.policy.basic.BasicPolicyTest.MyPolicy;
import org.apache.brooklyn.core.sensor.DependentConfiguration;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.core.test.entity.TestEntity;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.testng.annotations.Test;

import com.google.common.util.concurrent.Callables;

/**
 * Test that configuration properties are usable and inherited correctly.
 */
public class PolicyConfigTest extends BrooklynAppUnitTestSupport {
    private static final int EARLY_RETURN_GRACE = 10;

    private BasicConfigKey<String> differentKey = new BasicConfigKey<String>(String.class, "differentkey", "diffval");

    @Test
    public void testConfigFlagsPassedInAtConstructionIsAvailable() throws Exception {
        MyPolicy policy = new MyPolicy(MutableMap.builder()
                .put("strKey", "aval")
                .put("intKey", 2)
                .build());
        app.policies().add(policy);
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "aval");
        assertEquals(policy.getConfig(MyPolicy.INT_KEY), (Integer)2);
        // this is set, because key name matches annotation on STR_KEY
        assertEquals(policy.getConfig(MyPolicy.STR_KEY_WITH_DEFAULT), "aval");
    }
    
    @Test
    public void testUnknownConfigPassedInAtConstructionIsWarnedAndIgnored() throws Exception {
        // TODO Also assert it's warned
        MyPolicy policy = new MyPolicy(MutableMap.builder()
                .put(differentKey, "aval")
                .build());
        app.policies().add(policy);
        
        assertEquals(policy.getConfig(differentKey), null);
        assertEquals(policy.getPolicyType().getConfigKey(differentKey.getName()), null);
    }
    
    @Test
    public void testConfigPassedInAtConstructionIsAvailable() throws Exception {
        MyPolicy policy = new MyPolicy(MutableMap.builder()
                .put(MyPolicy.STR_KEY, "aval")
                .put(MyPolicy.INT_KEY, 2)
                .build());
        app.policies().add(policy);
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "aval");
        assertEquals(policy.getConfig(MyPolicy.INT_KEY), (Integer)2);
        // this is not set (contrast with above)
        assertEquals(policy.getConfig(MyPolicy.STR_KEY_WITH_DEFAULT), MyPolicy.STR_KEY_WITH_DEFAULT.getDefaultValue());
    }
    
    @Test
    public void testConfigSetToGroovyTruthFalseIsAvailable() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.INT_KEY_WITH_DEFAULT, 0));
        MyPolicy policy2 = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.INT_KEY_WITH_DEFAULT.getName(), 0));
        
        assertEquals(policy.getConfig(MyPolicy.INT_KEY_WITH_DEFAULT), (Integer)0);
        assertEquals(policy2.getConfig(MyPolicy.INT_KEY_WITH_DEFAULT), (Integer)0);
    }
    
    @Test
    public void testConfigSetToNullIsAvailable() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.STR_KEY_WITH_DEFAULT, (String)null));
        MyPolicy policy2 = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.STR_KEY_WITH_DEFAULT.getName(), null));
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY_WITH_DEFAULT), null);
        assertEquals(policy2.getConfig(MyPolicy.STR_KEY_WITH_DEFAULT), null);
    }
    
    @Test
    public void testConfigCanBeSetOnPolicyBeforeAddedToEntity() throws Exception {
        MyPolicy policy = new MyPolicy();
        policy.config().set(MyPolicy.STR_KEY, "aval");
        policy.config().set(MyPolicy.INT_KEY, 2);
        app.policies().add(policy);
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "aval");
        assertEquals(policy.getConfig(MyPolicy.INT_KEY), (Integer)2);
    }
    
    @Test
    public void testConfigSetterOverridesConstructorValue() throws Exception {
        MyPolicy policy = new MyPolicy(MutableMap.builder()
                .put(MyPolicy.STR_KEY, "aval")
                .build());
        policy.config().set(MyPolicy.STR_KEY, "diffval");
        app.policies().add(policy);
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "diffval");
    }

    @Test
    public void testConfigCannotBeSetAfterApplicationIsStarted() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.STR_KEY, "origval"));
        
        try {
            policy.config().set(MyPolicy.STR_KEY, "newval");
            fail();
        } catch (UnsupportedOperationException e) {
            // success
        }
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "origval");
    }
    
    @Test
    public void testConfigSetterForReconfigurableKey() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(MyPolicy.RECONFIGURABLE_KEY, "origval"));
        policy.config().set(MyPolicy.RECONFIGURABLE_KEY, "diffval");
        
        assertEquals(policy.getConfig(MyPolicy.RECONFIGURABLE_KEY), "diffval");
    }

    @Test
    public void testConfigReturnsDefaultValueIfNotSet() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class));
        
        assertEquals(policy.getConfig(MyPolicy.STR_KEY_WITH_DEFAULT), "str key default");
    }
    
    @Test
    public void testGetFutureConfigWhenReady() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(TestEntity.CONF_NAME, DependentConfiguration.whenDone(Callables.returning("aval"))));
        
        assertEquals(policy.config().get(TestEntity.CONF_NAME), "aval");
    }
    
    @Test
    public void testGetFutureConfigBlocksUntilReady() throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
                .configure(TestEntity.CONF_NAME, DependentConfiguration.whenDone(new Callable<String>() {
                        @Override
                        public String call() {
                            try {
                                latch.await(); return "aval";
                            } catch (InterruptedException e) {
                                throw Exceptions.propagate(e);
                            }
                        }})));
        
        Thread t = new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        Thread.sleep(10+EARLY_RETURN_GRACE);
                        latch.countDown();
                    } catch (InterruptedException e) {
                        throw Exceptions.propagate(e);
                    }
                }});
        try {
            long starttime = System.currentTimeMillis();
            t.start();
            assertEquals(policy.config().get(TestEntity.CONF_NAME), "aval");
            long endtime = System.currentTimeMillis();
            
            assertTrue((endtime - starttime) >= 10, "starttime="+starttime+"; endtime="+endtime);
            
        } finally {
            t.interrupt();
        }
    }
}
