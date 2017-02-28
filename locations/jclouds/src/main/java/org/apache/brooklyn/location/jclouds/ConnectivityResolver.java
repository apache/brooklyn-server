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

import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.core.sensor.Sensors;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.compute.domain.NodeMetadata;

import com.google.common.annotations.Beta;
import com.google.common.reflect.TypeToken;

/**
 * LocationNetworkInfoCustomizers are used by {@link JcloudsLocation} to determine the host and port
 * on which connections to locations should be made and the credentials that should be used.
 *
 * @see JcloudsLocationConfig#CONNECTIVITY_RESOLVER
 */
@Beta
public interface ConnectivityResolver {

    @SuppressWarnings("serial")
    AttributeSensor<Iterable<String>> PUBLIC_ADDRESSES = Sensors.newSensor(new TypeToken<Iterable<String>>() {},
            "host.addresses.public", "Public addresses on an instance");

    @SuppressWarnings("serial")
    AttributeSensor<Iterable<String>> PRIVATE_ADDRESSES = Sensors.newSensor(new TypeToken<Iterable<String>>() {},
            "host.addresses.private", "Private addresses on an instance");

    /**
     * @param location       The caller
     * @param node           The node the caller has created
     * @param config         The configuration the caller used to create the node
     * @param resolveOptions Additional options the caller has chosen when creating the node
     * @return The HostAndPort and LoginCredentials to use when connecting to the node.
     */
    ManagementAddressResolveResult resolve(
            JcloudsLocation location, NodeMetadata node, ConfigBag config, ConnectivityResolverOptions resolveOptions);

}
