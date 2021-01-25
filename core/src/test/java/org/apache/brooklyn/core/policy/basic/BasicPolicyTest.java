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

import java.util.concurrent.atomic.AtomicInteger;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.core.entity.EntityConfigTest.MyEntityWithDuration;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.task.DeferredSupplier;
import org.apache.brooklyn.util.time.Duration;
import org.apache.brooklyn.util.time.DurationPredicates;
import org.apache.brooklyn.util.time.Time;
import org.testng.Assert;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.util.Map;

import org.apache.brooklyn.api.objs.HighlightTuple;
import org.apache.brooklyn.api.policy.PolicySpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.BasicConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.policy.AbstractPolicy;
import org.apache.brooklyn.core.test.BrooklynAppUnitTestSupport;
import org.apache.brooklyn.util.collections.MutableSet;
import org.apache.brooklyn.util.core.flags.SetFromFlag;
import org.testng.annotations.Test;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicates;
import com.google.common.collect.Iterables;

/**
 * Test that policy can be created and accessed, by construction and by spec
 */
public class BasicPolicyTest extends BrooklynAppUnitTestSupport {
    
    public static class MyPolicy extends AbstractPolicy {
        @SetFromFlag("intKey")
        public static final BasicConfigKey<Integer> INT_KEY = new BasicConfigKey<Integer>(Integer.class, "bkey", "b key");
        
        @SetFromFlag("strKey")
        public static final ConfigKey<String> STR_KEY = new BasicConfigKey<String>(String.class, "akey", "a key");
        public static final ConfigKey<Integer> INT_KEY_WITH_DEFAULT = new BasicConfigKey<Integer>(Integer.class, "ckey", "c key", 1);
        public static final ConfigKey<String> STR_KEY_WITH_DEFAULT = new BasicConfigKey<String>(String.class, "strKeyWithDefault", "str key", "strKeyDefaultVal");
        public static final ConfigKey<String> RECONFIGURABLE_KEY = ConfigKeys.builder(String.class, "reconfigurableKey")
                .reconfigurable(true)
                .build();

        MyPolicy(Map<?,?> flags) {
            super(flags);
        }
        
        public MyPolicy() {
            super();
        }
        
        @Override
        protected <T> void doReconfigureConfig(ConfigKey<T> key, T val) {
            if (key.equals(RECONFIGURABLE_KEY)) {
                // allowed
            } else {
                super.doReconfigureConfig(key, val);
            }
        }

        @VisibleForTesting
        @Override
        protected void setHighlight(String name, HighlightTuple tuple) {
            super.setHighlight(name, tuple);
        }
    }
    
    @Test
    public void testAddInstance() throws Exception {
        MyPolicy policy = new MyPolicy();
        policy.setDisplayName("Bob");
        policy.config().set(MyPolicy.STR_KEY, "aval");
        policy.config().set(MyPolicy.INT_KEY, 2);
        
        app.policies().add(policy);
        assertTrue(Iterables.tryFind(app.policies(), Predicates.equalTo(policy)).isPresent());
        
        assertEquals(policy.getDisplayName(), "Bob");
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "aval");
        assertEquals(policy.getConfig(MyPolicy.INT_KEY), (Integer)2);
    }
    
    @Test
    public void testAddSpec() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class)
            .displayName("Bob")
            .configure(MyPolicy.STR_KEY, "aval").configure(MyPolicy.INT_KEY, 2));
        
        assertEquals(policy.getDisplayName(), "Bob");
        assertEquals(policy.getConfig(MyPolicy.STR_KEY), "aval");
        assertEquals(policy.getConfig(MyPolicy.INT_KEY), (Integer)2);
    }
        
    @Test
    public void testTagsFromSpec() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicy.class).tag(99).uniqueTag("x"));

        assertEquals(policy.tags().getTags(), MutableSet.of("x", 99));
        assertEquals(policy.getUniqueTag(), "x");
    }

    @Test
    public void testHighlights() throws Exception {
        MyPolicy policy = new MyPolicy();

        HighlightTuple highlight = new HighlightTuple("TEST_DESCRIPTION", 123L, "456");
        policy.setHighlight("testHighlightName", highlight);

        Map<String, HighlightTuple> highlights = policy.getHighlights();

        assertEquals(1, highlights.size());
        assertEquals(highlight, highlights.get("testHighlightName"));
    }

    public static class MyPolicyWithDuration extends MyPolicy {
        public static final ConfigKey<Duration> DURATION_POSITIVE = MyEntityWithDuration.DURATION_POSITIVE;
    }

    @Test
    public void testDurationConstraintValidAddInstance() throws Exception {
        MyPolicyWithDuration policy = new MyPolicyWithDuration();
        policy.config().set(MyPolicyWithDuration.DURATION_POSITIVE, Duration.ONE_MINUTE);

        app.policies().add(policy);
        assertTrue(Iterables.tryFind(app.policies(), Predicates.equalTo(policy)).isPresent());

        assertEquals(policy.getConfig(MyPolicyWithDuration.DURATION_POSITIVE), Duration.ONE_MINUTE);
    }

    @Test
    public void testDurationConstraintValidAddSpec() throws Exception {
        MyPolicy policy = app.policies().add(PolicySpec.create(MyPolicyWithDuration.class)
                .configure(MyPolicyWithDuration.DURATION_POSITIVE, Duration.ONE_MINUTE));

        assertEquals(policy.getConfig(MyPolicyWithDuration.DURATION_POSITIVE), Duration.ONE_MINUTE);
    }

    @Test
    public void testDurationConstraintInvalidSetLive() throws Exception {
        MyPolicyWithDuration policy = new MyPolicyWithDuration();
        Asserts.assertFailsWith(() -> policy.config().set(MyPolicyWithDuration.DURATION_POSITIVE, Duration.minutes(-42)),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidAddSpec() throws Exception {
        PolicySpec<MyPolicyWithDuration> spec = PolicySpec.create(MyPolicyWithDuration.class)
                .configure(MyPolicyWithDuration.DURATION_POSITIVE, Duration.minutes(-42));

        Asserts.assertFailsWith(() -> app.policies().add(spec),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidSetLiveByNameCoerced() throws Exception {
        MyPolicyWithDuration policy = new MyPolicyWithDuration();

        // 2021-01 validation done even for untyped set config
        ConfigKey<Object> objKey = ConfigKeys.newConfigKey(Object.class, MyEntityWithDuration.DURATION_POSITIVE.getName());
        Asserts.assertFailsWith(() -> policy.config().set(objKey, "-42m"),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidAddSpecByNameCoerced() throws Exception {
        PolicySpec<MyPolicyWithDuration> spec = PolicySpec.create(MyPolicyWithDuration.class)
                .configure(MyPolicyWithDuration.DURATION_POSITIVE.getName(), "-42m");

        Asserts.assertFailsWith(() -> app.policies().add(spec),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));
    }

    @Test
    public void testDurationConstraintInvalidDeferredSupplier() throws Exception {
        AtomicInteger invocationCount = new AtomicInteger(0);
        DeferredSupplier<Duration> deferred = new DeferredSupplier<Duration>() {
            @Override
            public Duration get() {
                Time.sleep(Duration.millis(10));
                invocationCount.incrementAndGet();
                return Duration.minutes(-42);
            }
        };
        PolicySpec<MyPolicyWithDuration> spec = PolicySpec.create(MyPolicyWithDuration.class)
                .configure(MyPolicyWithDuration.DURATION_POSITIVE.getName(), deferred);
        MyPolicyWithDuration policy = app.policies().add(spec);
        // shouldn't be accessible on setup due to get immediate semantics
        Assert.assertEquals(invocationCount.get(), 0);

        Asserts.assertFailsWith(() -> policy.getConfig(MyPolicyWithDuration.DURATION_POSITIVE),
                e -> Asserts.expectedFailureContainsIgnoreCase(e, "-42m", "positive"));

        Asserts.assertThat(invocationCount.get(), t -> t>0, "invocation should have happened at least once");
    }

}
