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
package org.apache.brooklyn.core.network;

import org.apache.brooklyn.api.catalog.Catalog;
import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.util.text.Strings;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.net.HostAndPort;

/**
 * TODO This is a temporary measure while we discuss and implement the proposal 
 * "Working with Multiple Networks": 
 *     https://docs.google.com/document/d/1IrWLWunWSl_ScwY3MRICped8eJMjQEH1FbWZJcoK0Iw/edit#heading=h.gwaayi613qqk
 * 
 * Can be added to an entity so that it advertises its subnet ports (by concatenating the subnet.address
 * and the port number).
 * 
 * For example, to configure MySQL to publish "datastore.url.mapped.subnet":
 * <pre>
 * {@code
 * services:
 * - type: org.apache.brooklyn.entity.database.mysql.MySqlNode
 *   brooklyn.enrichers:
 *   - type: org.apache.brooklyn.core.network.OnSubnetNetworkEnricher
 *     brooklyn.config:
 *       sensor: datastore.url
 * }
 * </pre>
 */
@Beta
@Catalog(name = "Subnet Network Advertiser", description = "Advertises entity's subnet mapped ports. This can be used with sensors of type URI, HostAndPort or plain integer port values")
public class OnSubnetNetworkEnricher extends AbstractOnNetworkEnricher {

    // TODO This is a temporary measure while we discuss and implement 
    // the proposal "Working with Multiple Networks": 
    // https://docs.google.com/document/d/1IrWLWunWSl_ScwY3MRICped8eJMjQEH1FbWZJcoK0Iw/edit#heading=h.gwaayi613qqk

    public static ConfigKey<Function<? super String, String>> SENSOR_NAME_CONVERTER = ConfigKeys.newConfigKeyWithDefault(
            AbstractOnNetworkEnricher.SENSOR_NAME_CONVERTER,
            new SensorNameConverter("subnet"));

    @Override
    protected Optional<HostAndPort> getMappedEndpoint(Entity source, MachineLocation machine, int port) {
        String address = source.sensors().get(Attributes.SUBNET_ADDRESS);
        if (Strings.isNonBlank(address)) {
            return Optional.of(HostAndPort.fromParts(address, port));
        } else {
            return Optional.absent();
        }
    }
}
