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
package org.apache.brooklyn.core.location.access;

import java.util.Collection;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;

import com.google.common.annotations.Beta;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.net.HostAndPort;

/**
 * Acts as a registry for existing port mappings (e.g. the public endpoints for accessing specific
 * ports on private VMs). This could be using DNAT, or iptables port-forwarding, or Docker port-mapping 
 * via the host, or any other port mapping approach.
 * 
 * Also controls the allocation of ports via {@link #acquirePublicPort(String)}
 * (e.g. for port-mapping with DNAT, then which port to use for the public side).
 * 
 * Implementations typically will not know anything about what the firewall/IP actually is, they just 
 * handle a unique identifier for it.
 * 
 * To use, see {@link PortForwardManagerLocationResolver}, with code such as 
 * {@code managementContext.getLocationRegistry().resolve("portForwardManager(scope=global)")}.
 * 
 * @see PortForwardManagerImpl for implementation notes and considerations.
 */
@Beta
public interface PortForwardManager extends Location {

    @Beta
    class AssociationMetadata {
        private final String publicIpId;
        private final HostAndPort publicEndpoint;
        private final Location location;
        private final int privatePort;

        /**
         * Users are discouraged from calling this constructor; the signature may change in future releases.
         * Instead, instances will be created automatically by Brooklyn to be passed to the
         * {@link AssociationListener#onAssociationCreated(AssociationMetadata)} method.
         */
        public AssociationMetadata(String publicIpId, HostAndPort publicEndpoint, Location location, int privatePort) {
            this.publicIpId = publicIpId;
            this.publicEndpoint = publicEndpoint;
            this.location = location;
            this.privatePort = privatePort;
        }

        public String getPublicIpId() {
            return publicIpId;
        }

        public HostAndPort getPublicEndpoint() {
            return publicEndpoint;
        }

        public Location getLocation() {
            return location;
        }

        public int getPrivatePort() {
            return privatePort;
        }

        public String toString() {
            return Objects.toStringHelper(this)
                    .add("publicIpId", publicIpId)
                    .add("publicEndpoint", publicEndpoint)
                    .add("location", location)
                    .add("privatePort", privatePort)
                    .toString();
        }
    }

    @Beta
    interface AssociationListener {
        void onAssociationCreated(AssociationMetadata metadata);
        void onAssociationDeleted(AssociationMetadata metadata);
    }

    /**
     * The intention is that there is one PortForwardManager instance per "scope". If you 
     * use global, then it will be a shared instance (for that management context). If you 
     * pass in your own name (e.g. "docker-fjie3") then it will shared with just any other
     * places that use that same location spec (e.g. {@code portForwardManager(scope=docker-fjie3)}).
     */
    // TODO Note: using name "scope" rather than "brooklyn.portForwardManager.scope" so that location spec 
    // "portForwardManager(scope=global)" works, rather than having to do 
    // portForwardManager(brooklyn.portForwardManager.scope=global).
    // The config being read by the PortForwardManagerLocationResolver doesn't respect @SetFromFlag("scope").
    public static final ConfigKey<String> SCOPE = ConfigKeys.newStringConfigKey(
            "scope",
            "The scope that this applies to, defaulting to global",
            "global");

    @Beta
    public static final ConfigKey<Integer> PORT_FORWARD_MANAGER_STARTING_PORT = ConfigKeys.newIntegerConfigKey(
            "brooklyn.portForwardManager.startingPort",
            "The starting port for assigning port numbers, such as for DNAT",
            11000);

    public String getScope();

    /**
     * Reserves a unique public port on the given publicIpId.
     * <p>
     * Often followed by {@link #associate(String, HostAndPort, int)} or {@link #associate(String, HostAndPort, Location, int)}
     * to enable {@link #lookup(String, int)} or {@link #lookup(Location, int)} respectively.
     */
    public int acquirePublicPort(String publicIpId);

    /**
     * Records a location and private port against a public endpoint (ip and port),
     * to support {@link #lookup(Location, int)}.
     */
    public void associate(String publicIpId, HostAndPort publicEndpoint, Location l, int privatePort);

    /**
     * Records a mapping for publicIpId:privatePort to a public endpoint, such that it can
     * subsequently be looked up using {@link #lookup(String, int)}.
     */
    public void associate(String publicIpId, HostAndPort publicEndpoint, int privatePort);

    /**
     * Registers a listener, which will be notified each time a new port mapping is associated. See {@link #associate(String, HostAndPort, int)}
     * and {@link #associate(String, HostAndPort, Location, int)}.
     */
    @Beta
    public void addAssociationListener(AssociationListener listener, Predicate<? super AssociationMetadata> filter);

    @Beta
    public void removeAssociationListener(AssociationListener listener);
    
    /**
     * Returns the public ip hostname and public port for use contacting the given endpoint.
     * <p>
     * Will return null if:
     * <ul>
     * <li>No publicPort is associated with this location and private port.
     * <li>No publicIpId is associated with this location and private port.
     * <li>No publicIpHostname is recorded against the associated publicIpId.
     * </ul>
     * Conceivably this may have to be access-location specific.
     *
     * @see #associate(String, HostAndPort, int)
     */
    public HostAndPort lookup(Location l, int privatePort);

    /**
     * Returns the public endpoint (host and port) for use contacting the given endpoint.
     * 
     * Expects a previous call to {@link #associate(String, HostAndPort, int)}, to register
     * the endpoint.
     * 
     * Will return null if there has not been a public endpoint associated with this pairing.
     */
    public HostAndPort lookup(String publicIpId, int privatePort);

    /** 
     * Clears the given port mapping, returning true if there was a match.
     */
    public boolean forgetPortMapping(String publicIpId, int publicPort);
    
    /** 
     * Clears the port mappings associated with the given location, returning true if there were any matches.
     */
    public boolean forgetPortMappings(Location location);
    
    /** 
     * Clears the port mappings associated with the given publicIpId, returning true if there were any matches.
     */
    public boolean forgetPortMappings(String publicIpId);
    
    public String toVerboseString();

    /**
     * Returns the subset of port mappings associated with a given public IP ID.
     *
     * @deprecated since 0.7.0; this method will be internal only
     */
    // TODO: Should this be deprecated? It's used by JcloudsLocation and has no obvious replacement.
    @Deprecated
    public Collection<PortMapping> getPortMappingWithPublicIpId(String publicIpId);

     /**
      * Returns the subset of port mappings associated with a given location.
      *
      * @deprecated since 0.7.0; this method will be internal only
      */
     // TODO: Should this be deprecated? It's used by JcloudsLocation and has no obvious replacement.
     @Deprecated
     public Collection<PortMapping> getLocationPublicIpIds(Location l);

}
