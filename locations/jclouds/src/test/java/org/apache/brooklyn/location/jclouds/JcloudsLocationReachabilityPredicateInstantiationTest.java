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
package org.apache.brooklyn.location.jclouds;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.testng.Assert;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

public class JcloudsLocationReachabilityPredicateInstantiationTest {
    public static final HostAndPort ALLOWED_HOST_AND_PORT = HostAndPort.fromParts("localhost", 223);
    private BailOutJcloudsLocation jcloudsLocation;

    private final NodeMetadata node = new NodeMetadataBuilder()
            .id("ID")
            .loginPort(ALLOWED_HOST_AND_PORT.getPort())
            .status(NodeMetadata.Status.RUNNING)
            .privateAddresses(ImmutableList.of(ALLOWED_HOST_AND_PORT.getHostText()))
            .build();

    @BeforeMethod
    public void setUp() {
        jcloudsLocation = new BailOutJcloudsLocation();
    }

    @Test
    public void testConfigLocationWithReachabilityPredicate() {
        List<String> hostsMatchedHolder = new ArrayList<>();
        Predicate<HostAndPort> hostAndPortPredicate = new RecordingReachabilityCheckMapAndFlagsConstructor(ImmutableMap.<String, Object>of("hostsMatchedHolder", hostsMatchedHolder));
        ConfigBag predicateConfig = new ConfigBag();
        predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, hostAndPortPredicate);
        jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        Assert.assertTrue(hostsMatchedHolder.contains(ALLOWED_HOST_AND_PORT.getHostText()));
    }

    protected static final AtomicInteger TEST_INIT_DEFAULT_CONSTRUCTOR_COUNTER = new AtomicInteger(0);
    @Test
    public void testInitDefaultConstructor() throws Exception {
        Assert.assertEquals(TEST_INIT_DEFAULT_CONSTRUCTOR_COUNTER.get(), 0);
        ConfigBag predicateConfig = new ConfigBag();
        predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE, RecordingReachabilityCheck.class);
        jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        Assert.assertEquals(TEST_INIT_DEFAULT_CONSTRUCTOR_COUNTER.get(), 1);
    }

    protected static final AtomicInteger TEST_INIT_MAP_CONSTRUCTOR_COUNTER = new AtomicInteger(0);
    @Test
    public void testInitMapConstructor() {
        Assert.assertEquals(TEST_INIT_MAP_CONSTRUCTOR_COUNTER.get(), 0);
        ConfigBag predicateConfig = new ConfigBag();
        predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE, RecordingReachabilityCheckMapConstructor.class);
        predicateConfig.putStringKey(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ".key1", "val1");
        predicateConfig.putStringKey(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ".key2", "val2");
        jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        Assert.assertEquals(TEST_INIT_MAP_CONSTRUCTOR_COUNTER.get(), 1);
    }

    @Test
    public void testInitMapConstructorWithFlags() {
        ConfigBag predicateConfig = new ConfigBag();
        predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE, RecordingReachabilityCheckMapAndFlagsConstructor.class);
        List<String> hostsMatchedHolder = new ArrayList<>();
        predicateConfig.putStringKey(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ".hostsMatchedHolder", hostsMatchedHolder);
        jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        Assert.assertTrue(hostsMatchedHolder.contains(ALLOWED_HOST_AND_PORT.getHostText()));
    }

    protected static final AtomicInteger TEST_INIT_MAP_AND_EMPTY_CONSTRUCTOR_COUNTER = new AtomicInteger(0);
    @Test
    public void testInitEmptyConstructor() {
        Assert.assertEquals(TEST_INIT_MAP_AND_EMPTY_CONSTRUCTOR_COUNTER.get(), 0);
        ConfigBag predicateConfig = new ConfigBag();
        predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE, RecordingReachabilityCheckProtectedMapConstructor.class);
        jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        Assert.assertEquals(TEST_INIT_MAP_AND_EMPTY_CONSTRUCTOR_COUNTER.get(), 1);
    }

    @Test
    public void testNoSuitableConstructorFound() {
        ConfigBag predicateConfig = new ConfigBag();

        try {
            predicateConfig.put(CloudLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE_TYPE, NoSuitableConstructorPredicate.class);
            jcloudsLocation.getFirstReachableAddress(node, predicateConfig);
        } catch (RuntimeException e) {
            Assert.assertTrue(InstantiationException.class.isAssignableFrom(e.getCause().getClass()));
        }
    }

    static class NoSuitableConstructorPredicate implements Predicate<HostAndPort> {
        public NoSuitableConstructorPredicate(NoSuitableConstructorPredicate a) {}

        @Override
        public boolean apply(@Nullable HostAndPort input) {
            return false;
        }
    }

    // Needs to be static in order to be instantiated dynamically
    public static class RecordingReachabilityCheck implements Predicate<HostAndPort> {
        @Override
        public boolean apply(@Nullable HostAndPort input) {
            TEST_INIT_DEFAULT_CONSTRUCTOR_COUNTER.incrementAndGet();
            return true;
        }
    }

    public static class RecordingReachabilityCheckMapConstructor implements Predicate<HostAndPort> {
        private Map<String, Object> flags;

        public RecordingReachabilityCheckMapConstructor(Map<String, Object> flags) {
            this.flags = flags;
        }

        @Override
        public boolean apply(@Nullable HostAndPort input) {
            // TODO bad practice to assert in thread which doesn't pass Assert exceptions
            Assert.assertEquals(flags.get("key1"), "val1");
            Assert.assertEquals(flags.get("key2"), "val2");
            TEST_INIT_MAP_CONSTRUCTOR_COUNTER.getAndIncrement();
            return true;
        }
    }

    public static class RecordingReachabilityCheckMapAndFlagsConstructor implements Predicate<HostAndPort> {
        private Map<String, Object> flags;

        public RecordingReachabilityCheckMapAndFlagsConstructor(Map<String, Object> flags) {
            this.flags = flags;
        }

        @Override
        public boolean apply(@Nullable HostAndPort input) {
            ((List<String>)flags.get("hostsMatchedHolder")).add(input.getHostText());
            return true;
        }
    }

    public static class RecordingReachabilityCheckProtectedMapConstructor implements Predicate<HostAndPort> {
        public RecordingReachabilityCheckProtectedMapConstructor() {
            TEST_INIT_MAP_AND_EMPTY_CONSTRUCTOR_COUNTER.getAndIncrement();
        }

        protected RecordingReachabilityCheckProtectedMapConstructor(Map<String, Object> flags) {}

        @Override
        public boolean apply(@Nullable HostAndPort input) {
            return true;
        }
    }

    public static class BailOutJcloudsLocation extends JcloudsLocation {
        public BailOutJcloudsLocation() {
            super();
        }

        @Override
        public String getFirstReachableAddress(NodeMetadata nodeMetada, ConfigBag setup) {
            return super.getFirstReachableAddress(nodeMetada, setup);
        }
    }
}
