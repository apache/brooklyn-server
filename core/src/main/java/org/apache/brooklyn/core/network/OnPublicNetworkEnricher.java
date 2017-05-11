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
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.sensor.AttributeSensor;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.access.PortForwardManager;
import org.apache.brooklyn.core.location.access.PortForwardManagerLocationResolver;
import org.apache.brooklyn.util.guava.Maybe;
import org.apache.brooklyn.util.text.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.net.HostAndPort;
import com.google.common.reflect.TypeToken;

/**
 * Can be added to an entity so that it advertises its mapped ports (according to the port-mappings
 * recorded in the {@link PortForwardManager}). This can be used with sensors of type URI, HostAndPort
 * or plain integer port values. The port-mappings is retrieved by looking up the entity's machine
 * and the private port, in the {@link PortForwardManager}'s recorded port-mappings.
 * 
 * For example, to configure each Tomcat node to publish its mapped uri, and to use that sensor
 * in Nginx for the target servers:
 * <pre>
 * {@code
 * services:
 * - type: cluster
 *   id: cluster
 *   brooklyn.config:
 *    memberSpec:
 *      $brooklyn:entitySpec:
 *        type: org.apache.brooklyn.entity.webapp.tomcat.TomcatServer
 *        brooklyn.enrichers:
 *        - type: org.apache.brooklyn.core.network.OnPublicNetworkEnricher
 *          brooklyn.config:
 *            sensor: main.uri
 * - type: org.apache.brooklyn.entity.proxy.nginx.NginxController
 *   brooklyn.config:
 *     member.sensor.hostandport: $brooklyn:sensor("main.uri.mapped.public")
 *     serverPool: cluster
 * }
 * </pre>
 */
@Beta
@Catalog(name = "Public Network Advertiser", description = "Advertises entity's public mapped ports. This can be used with sensors of type URI, HostAndPort or plain integer port values")
public class OnPublicNetworkEnricher extends AbstractOnNetworkEnricher {

    // TODO Need more logging, particularly for when the value has *not* been transformed.
    //
    // TODO What if the sensor has an unrelated hostname - we will currently still transform this!
    // That seems acceptable: if the user configures it to look at the sensor, then we can make
    // assumptions that the sensor's value will need translated.
    //
    // TODO If there is no port-mapping, should we advertise the original sensor value?
    // That would allow the enricher to be used for an entity in a private network, and for
    // it to be a no-op in a public cloud (so the same blueprint can be used in both). 
    // However I don't think we should publish the original value: it could be the association
    // just hasn't been created yet. If we publish the wrong (i.e. untransformed) value, that
    // will cause other entity's using attributeWhenReady to immediately trigger.

    private static final Logger LOG = LoggerFactory.getLogger(OnPublicNetworkEnricher.class);

    public static ConfigKey<Function<? super String, String>> SENSOR_NAME_CONVERTER = ConfigKeys.newConfigKeyWithDefault(
            AbstractOnNetworkEnricher.SENSOR_NAME_CONVERTER,
            new SensorNameConverter("public"));

    public static final ConfigKey<PortForwardManager> PORT_FORWARD_MANAGER = ConfigKeys.newConfigKey(
            PortForwardManager.class, 
            "portForwardManager",
            "The PortForwardManager storing the port-mappings; if null, the global instance will be used");
    
    @SuppressWarnings("serial")
    public static final ConfigKey<AttributeSensor<String>> ADDRESS_SENSOR = ConfigKeys.newConfigKey(
            new TypeToken<AttributeSensor<String>>() {}, 
            "addressSensor",
            "The sensor to use to retrieve the entity's public address; if null (default), then use the PortForwardManager instead");
    
    protected PortForwardManager.AssociationListener pfmListener;
    
    @Override
    public void setEntity(final EntityLocal entity) {
        super.setEntity(entity);
        
        /*
         * To find the transformed sensor value we need several things to be set. Therefore 
         * subscribe to all of them, and re-compute whenever any of the change. These are:
         *  - A port-mapping to exist for the relevant machine + private port.
         *  - The entity to have a machine location (so we can lookup the mapped port association).
         *  - The relevant sensors to have a value, which includes the private port.
         */
        pfmListener = new PortForwardManager.AssociationListener() {
            @Override
            public void onAssociationCreated(PortForwardManager.AssociationMetadata metadata) {
                Maybe<MachineLocation> machine = getMachine();
                if (!(machine.isPresent() && machine.get().equals(metadata.getLocation()))) {
                    // not related to this entity's machine; ignoring
                    return;
                }
                
                LOG.debug("{} attempting transformations, triggered by port-association {}, with machine {} of entity {}", 
                        new Object[] {OnPublicNetworkEnricher.this, metadata, machine.get(), entity});
                tryTransformAll();
            }
            @Override
            public void onAssociationDeleted(PortForwardManager.AssociationMetadata metadata) {
                // no-op
            }
        };
        getPortForwardManager().addAssociationListener(pfmListener, Predicates.alwaysTrue());
    }

    @Override
    public void destroy() {
        try {
            if (pfmListener != null) {
                getPortForwardManager().removeAssociationListener(pfmListener);
            }
        } finally {
            super.destroy();
        }
    }

    @Override
    protected Optional<HostAndPort> getMappedEndpoint(Entity source, MachineLocation machine, int port) {
        AttributeSensor<String> sensor = config().get(ADDRESS_SENSOR);
        if (sensor == null) {
            HostAndPort publicTarget = getPortForwardManager().lookup(machine, port);
            return Optional.fromNullable(publicTarget);
        } else {
            String address = source.sensors().get(sensor);
            if (Strings.isNonBlank(address)) {
                return Optional.of(HostAndPort.fromParts(address, port));
            } else {
                return Optional.absent();
            }
        }
    }
    
    protected PortForwardManager getPortForwardManager() {
        PortForwardManager portForwardManager = config().get(PORT_FORWARD_MANAGER);
        if (portForwardManager == null) {
            portForwardManager = (PortForwardManager) getManagementContext().getLocationRegistry().getLocationManaged(PortForwardManagerLocationResolver.PFM_GLOBAL_SPEC);
        }
        return portForwardManager;
    }
}
