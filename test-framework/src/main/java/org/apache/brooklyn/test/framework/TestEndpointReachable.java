/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.test.framework;

import org.apache.brooklyn.api.entity.ImplementedBy;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

/**
 * Entity that checks if a TCP endpoint is reachable.
 * 
 * For example:
 * <pre>
 * {@code
 * services:
 * - type: com.acme.MyEntityUnderTest
 *   id: entity-under-test
 * - type: org.apache.brooklyn.test.framework.TestCase
 *   name: Tests
 *   brooklyn.children:
 *   - type: org.apache.brooklyn.test.framework.TestEndpointReachable
 *     name: Endpoint reachable
 *     brooklyn.config:
 *       targetId: entity-under-test
 *       timeout: 2m
 *       endpointSensor: datastore.url
 * }
 * </pre>
 * 
 * The sensor's value can be in a number of different formats: a string in the form of {@code ip:port}
 * or URI format; or a {@link com.google.common.net.HostAndPort}; or a {@link java.net.URI}; or a 
 * {@link java.net.URL} instance.
 * 
 * Alternatively an explicit endpoint can be used (e.g. constructed from other sensors of
 * the target entity):
 * <pre>
 * {@code
 * services:
 * - type: MyEntityUnderTest
 *   id: entity-under-test
 * - type: org.apache.brooklyn.test.framework.TestCase
 *   name: Tests
 *   brooklyn.children:
 *   - type: org.apache.brooklyn.test.framework.TestEndpointReachable
 *     name: Endpoint reachable
 *     brooklyn.config:
 *       targetId: entity-under-test
 *       timeout: 2m
 *       endpoint:
 *         $brooklyn:formatString:
 *         - %s:%s"
 *         - $brooklyn:entity("entity-under-test").attributeWhenReady("host.name")
 *         - $brooklyn:entity("entity-under-test").attributeWhenReady("https.port")
 * }
 * </pre>
 */
@ImplementedBy(value = TestEndpointReachableImpl.class)
public interface TestEndpointReachable extends BaseTest {

    ConfigKey<String> ENDPOINT = ConfigKeys.newStringConfigKey(
            "endpoint", 
            "Endpoint (be it URL or host:port) to test, for tcp-reachability; mutually exclusive with 'endpointSensor'");

    ConfigKey<Object> ENDPOINT_SENSOR = ConfigKeys.newConfigKey(
            Object.class,
            "endpointSensor", 
            "Sensor (or name of sensor) on target that advertises the endpoint (to test for tcp-reachability); mutually exclusive with 'endpoint'");
}
