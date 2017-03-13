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

import java.util.Iterator;
import java.util.Map;
import javax.annotation.Nullable;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.entity.EntityInitializer;
import org.apache.brooklyn.api.entity.EntityLocal;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.entity.Attributes;
import org.apache.brooklyn.core.entity.BrooklynConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.objs.BasicConfigurableObject;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.task.Tasks;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.Beta;
import com.google.common.base.MoreObjects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Supplier;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.net.HostAndPort;

/**
 * DefaultConnectivityResolver provides the default implementation of
 * {@link ConnectivityResolver}. It exposes options to have JcloudsLocation
 * prefer to contact VMs on private addresses and can be injected on a
 * per-entity basis. For example:
 * <pre>
 * services:
 * - type: server
 *   location: the-same-private-network-as-brooklyn
 *   brooklyn.initializers:
 *   - type: org.apache.brooklyn.location.jclouds.DefaultConnectivityResolver
 *     brooklyn.config:
 *       mode: ONLY_PRIVATE
 * - type: server
 *   location: another-cloud
 *   # implicit use of PREFER_PUBLIC.
 * </pre>
 * Would result in the first entity being managed on the instance's private address (and deployment
 * failing if this was not possible) and the second being managed on its public address. Graceful
 * fallback is possible by replacing ONLY_PRIVATE with PREFER_PRIVATE. There are PUBLIC variants of
 * each of these.
 * <p>
 * DefaultConnectivityResolver is the default location network info customizer used by
 * {@link JcloudsLocation} when {@link JcloudsLocationConfig#CONNECTIVITY_RESOLVER}
 * is unset.
 * <p>
 * When used as an {@link EntityInitializer} the instance inserts itself into the entity's
 * provisioning properties under the {@link JcloudsLocationConfig#CONNECTIVITY_RESOLVER}
 * subkey.
 * <p>
 * This class is annotated @Beta and is likely to change in the future.
 */
@Beta
public class DefaultConnectivityResolver extends BasicConfigurableObject implements ConnectivityResolver, EntityInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultConnectivityResolver.class);

    public enum NetworkMode {
        /**
         * Check each node's {@link NodeMetadata#getPublicAddresses() public addresses}
         * for reachability before its {@link NodeMetadata#getPrivateAddresses() private addresses}.
         */
        PREFER_PUBLIC,
        /**
         * Check each node's {@link NodeMetadata#getPrivateAddresses() private addresses}
         * for reachability before its {@link NodeMetadata#getPublicAddresses() public addresses}.
         */
        PREFER_PRIVATE,
        /**
         * Check only a node's {@link NodeMetadata#getPublicAddresses() public addresses} for reachability.
         */
        ONLY_PUBLIC,
        /**
         * Check only a node's {@link NodeMetadata#getPrivateAddresses()}  private addresses} for reachability.
         */
        ONLY_PRIVATE
    }

    public static final ConfigKey<NetworkMode> NETWORK_MODE = ConfigKeys.newConfigKey(NetworkMode.class,
            "mode", "Operation mode: PREFER_PUBLIC, PREFER_PRIVATE, ONLY_PUBLIC or ONLY_PRIVATE");

    @Beta
    public static final ConfigKey<Boolean> CHECK_CREDENTIALS = ConfigKeys.newBooleanConfigKey(
            "checkCredentials",
            "Indicates that credentials should be tested when determining endpoint reachability.",
            Boolean.TRUE);

    public static final ConfigKey<Boolean> PUBLISH_NETWORKS = ConfigKeys.newBooleanConfigKey(
            "publishNetworks",
            "Indicates that the customizer should publish addresses as sensors on each entity",
            Boolean.TRUE);

    // --------------------------------------------------------------------------------------

    public DefaultConnectivityResolver() {
        this(ImmutableMap.of());
    }

    public DefaultConnectivityResolver(Map<?, ?> params) {
        this(ConfigBag.newInstance(params));
    }

    public DefaultConnectivityResolver(final ConfigBag params) {
        for (Map.Entry<String, Object> entry : params.getAllConfig().entrySet()) {
            config().set(ConfigKeys.newConfigKey(Object.class, entry.getKey()), entry.getValue());
        }
    }

    // --------------------------------------------------------------------------------------

    /**
     * Sets the instance as the value of {@link JcloudsLocationConfig#CONNECTIVITY_RESOLVER}
     * in entity's provisioning properties.
     */
    @Override
    public void apply(EntityLocal entity) {
        final String sensorName = JcloudsLocationConfig.CONNECTIVITY_RESOLVER.getName();
        ConfigKey<Object> subkey = BrooklynConfigKeys.PROVISIONING_PROPERTIES.subKey(sensorName);
        entity.config().set(subkey, this);
        LOG.debug("{} set itself as the {} on {}", new Object[]{this, sensorName, entity});
    }

    // --------------------------------------------------------------------------------------

    /**
     * Combines the given resolve options with the customiser's configuration to determine the
     * best address and credential pair for management. In particular, if the resolve options
     * allow it will check that the credential is actually valid for the address.
     */
    @Override
    public ManagementAddressResolveResult resolve(
            JcloudsLocation location, NodeMetadata node, ConfigBag config, ConnectivityResolverOptions options) {
        LOG.debug("{} resolving management parameters for {}, node={}, config={}, options={}",
                new Object[]{this, location, node, config, options});
        final Stopwatch timer = Stopwatch.createStarted();
        // Should only be null in tests.
        final Entity contextEntity = getContextEntity(config);
        if (shouldPublishNetworks() && !options.isRebinding() && contextEntity != null) {
            publishNetworks(node, contextEntity);
        }
        HostAndPort hapChoice = null;
        LoginCredentials credChoice = null;

        final Iterable<HostAndPort> managementCandidates = getManagementCandidates(location, node, config, options);
        final Iterable<LoginCredentials> credentialCandidates = getCredentialCandidates(location, node, options, config);

        // Try each pair of address and credential until one succeeds.
        if (shouldCheckCredentials() && options.pollForReachableAddresses()) {
            for (HostAndPort hap : managementCandidates) {
                for (LoginCredentials cred : credentialCandidates) {
                    LOG.trace("Testing host={} with credential={}", hap, cred);
                    if (checkCredential(location, hap, cred, config, options.isWindows())) {
                        hapChoice = hap;
                        credChoice = cred;
                        break;
                    }
                }
                if (hapChoice != null) break;
            }
        } else if (shouldCheckCredentials()) {
            LOG.debug("{} set on {} but pollForFirstReachableAddress={}",
                    new Object[]{CHECK_CREDENTIALS.getName(), this, options.pollForReachableAddresses()});
        }

        if (hapChoice == null) {
            LOG.trace("Choosing first management candidate given node={} and mode={}", node, getNetworkMode());
            hapChoice = Iterables.getFirst(managementCandidates, null);
        }
        if (hapChoice == null) {
            LOG.trace("Choosing first address of node={} in mode={}", node, getNetworkMode());
            final Iterator<String> hit = getResolvableAddressesWithMode(node).iterator();
            if (hit.hasNext()) HostAndPort.fromHost(hit.next());
        }

        if (hapChoice == null) {
            throw new IllegalStateException("jclouds did not return any IP addresses matching " + getNetworkMode() + " in " + location);
        }
        if (credChoice == null) {
            credChoice = Iterables.getFirst(credentialCandidates, null);
            if (credChoice == null) {
                throw new IllegalStateException("No credentials configured for " + location);
            }
        }

        if (contextEntity != null) {
            contextEntity.sensors().set(Attributes.ADDRESS, hapChoice.getHostText());
        }

        // Treat AWS as a special case because the DNS fully qualified hostname in AWS is
        // (normally?!) a good way to refer to the VM from both inside and outside of the region.
        if (!isNetworkModeSet() && !options.isWindows()) {
            final boolean lookupAwsHostname = Boolean.TRUE.equals(config.get(JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME));
            String provider = config.get(JcloudsLocationConfig.CLOUD_PROVIDER);
            if (provider == null) {
                provider = location.getProvider();
            }
            if (options.waitForConnectable() && "aws-ec2".equals(provider) && lookupAwsHostname) {
                // getHostnameAws sshes to the machine and curls 169.254.169.254/latest/meta-data/public-hostname.
                try {
                    LOG.debug("Resolving AWS hostname of {}", location);
                    String result = location.getHostnameAws(hapChoice, credChoice, config);
                    hapChoice = HostAndPort.fromParts(result, hapChoice.getPort());
                    LOG.debug("Resolved AWS hostname of {}: {}", location, result);
                } catch (Exception e) {
                    LOG.debug("Failed to resolve AWS hostname of " + location, e);
                }
            }
        }

        ManagementAddressResolveResult result = new ManagementAddressResolveResult(hapChoice, credChoice);
        LOG.debug("{} resolved management parameters for {} in {}: {}",
                new Object[]{this, location, Duration.of(timer), result});
        return result;
    }

    private boolean shouldPublishNetworks() {
        return Boolean.TRUE.equals(config().get(PUBLISH_NETWORKS));
    }

    void publishNetworks(NodeMetadata node, Entity entity) {
        if (entity.sensors().get(PRIVATE_ADDRESSES) == null) {
            entity.sensors().set(PRIVATE_ADDRESSES, ImmutableSet.copyOf(node.getPrivateAddresses()));
        }
        if (entity.sensors().get(PUBLIC_ADDRESSES) == null) {
            entity.sensors().set(PUBLIC_ADDRESSES, ImmutableSet.copyOf(node.getPublicAddresses()));
        }
    }

    // --------------------------------------------------------------------------------------

    /**
     * Returns the hosts and ports that should be considered when determining the address
     * to use when connecting to the location by assessing the following criteria:
     * <ol>
     *     <li>Use the hostAndPortOverride set in options.</li>
     *     <li>If the machine is connectable, user credentials are given and the machine is provisioned
     *     in AWS then use {@link JcloudsLocation#getHostnameAws(NodeMetadata, Optional, Supplier, ConfigBag)}.</li>
     *     <li>If the machine is connectable and pollForFirstReachableAddress is set in options then use all
     *     {@link #getReachableAddresses reachable} addresses.</li>
     *     <li>Use the first address that is resolvable with {@link #isAddressResolvable}.</li>
     *     <li>Use the first address in the node's public then private addresses.</li>
     * </ol>
     */
    protected Iterable<HostAndPort> getManagementCandidates(
            JcloudsLocation location, NodeMetadata node, ConfigBag config, ConnectivityResolverOptions options) {
        final Optional<HostAndPort> portForwardSshOverride = options.portForwardSshOverride();

        if (portForwardSshOverride.isPresent()) {
            // Don't try to resolve it; just use it
            int port = portForwardSshOverride.get().hasPort()
                       ? portForwardSshOverride.get().getPort()
                       : options.defaultLoginPort();
            final HostAndPort override = HostAndPort.fromParts(portForwardSshOverride.get().getHostText(), port);
            switch (getNetworkMode()) {
            case ONLY_PRIVATE:
                LOG.info("Ignoring mode {} in favour of port forwarding override for management candidates of {}: {}",
                        new Object[]{NetworkMode.ONLY_PRIVATE.name(), location, override});
                break;
            default:
                LOG.debug("Using host and port override for management candidates of {}: {}", location, override);
            }
            return ImmutableList.of(override);
        }

        if (options.pollForReachableAddresses() && options.reachableAddressPredicate() != null) {
            LOG.debug("Using reachable addresses for management candidates of {}", location);
            try {
                final Predicate<? super HostAndPort> predicate = options.reachableAddressPredicate();
                return getReachableAddresses(node, predicate, options.reachableAddressTimeout());
            } catch (RuntimeException e) {
                if (options.propagatePollForReachableFailure()) {
                    throw Exceptions.propagate(e);
                } else {
                    LOG.warn("No reachable address ({}/{}); falling back to any advertised address; may cause future failures",
                            location.getCreationString(config), node);
                }
            }
        } else if (options.pollForReachableAddresses()) {
            throw new IllegalStateException(this + " was configured to expect " + node + " to be reachable " +
                    "and to poll for its reachable addresses but the predicate to determine reachability was null");
        }

        Iterable<String> addresses = getResolvableAddressesWithMode(node);
        LOG.debug("Using first resolvable address in {} for management candidates of {}", Iterables.toString(addresses), location);
        for (String address : addresses) {
            if (isAddressResolvable(address)) {
                return ImmutableList.of(HostAndPort.fromParts(address, options.defaultLoginPort()));
            } else {
                LOG.debug("Unresolvable address: " + address);
            }
        }

        LOG.warn("No resolvable address in {} ({}/{}); using first; may cause future failures",
                new Object[]{addresses, location.getCreationString(config), node});
        String host = Iterables.getFirst(addresses, null);
        if (host != null) {
            return ImmutableList.of(HostAndPort.fromParts(host, options.defaultLoginPort()));
        } else {
            return ImmutableList.of();
        }
    }

    /**
     * Returns all reachable addresses according to reachablePredicate.
     * Iterators are ordered according to the configured {@link #getNetworkMode() mode}.
     */
    protected Iterable<HostAndPort> getReachableAddresses(NodeMetadata node, Predicate<? super HostAndPort> reachablePredicate, Duration timeout) {
        if (timeout == null) timeout = Duration.FIVE_MINUTES;
        Iterable<String> candidates = getResolvableAddressesWithMode(node);
        return JcloudsUtil.getReachableAddresses(candidates, node.getLoginPort(), timeout, reachablePredicate);
    }

    protected Iterable<String> getResolvableAddressesWithMode(NodeMetadata node) {
        Iterable<String> base;
        switch (getNetworkMode()) {
        case ONLY_PRIVATE:
            base = node.getPrivateAddresses();
            break;
        case ONLY_PUBLIC:
            base = node.getPublicAddresses();
            break;
        case PREFER_PRIVATE:
            base = Iterables.concat(node.getPrivateAddresses(), node.getPublicAddresses());
            break;
        case PREFER_PUBLIC:
        default:
            base = Iterables.concat(node.getPublicAddresses(), node.getPrivateAddresses());
        }
        return FluentIterable.from(base)
                .filter(new AddressResolvable());
    }

    protected static boolean isAddressResolvable(String addr) {
        try {
            Networking.getInetAddressWithFixedName(addr);
            return true; // fine, it resolves
        } catch (RuntimeException e) {
            Exceptions.propagateIfFatal(e);
            return false;
        }
    }

    private static class AddressResolvable implements Predicate<String> {
        @Override
        public boolean apply(@Nullable String input) {
            return isAddressResolvable(input);
        }
    }

    // --------------------------------------------------------------------------------------

    protected boolean shouldCheckCredentials() {
        return Boolean.TRUE.equals(config().get(CHECK_CREDENTIALS));
    }

    protected boolean checkCredential(
            JcloudsLocation location, HostAndPort hostAndPort, LoginCredentials credentials,
            ConfigBag config, boolean isWindows) {
        try {
            if (isWindows) {
                location.waitForWinRmAvailable(credentials, hostAndPort, config);
            } else {
                location.waitForSshable(hostAndPort, ImmutableList.of(credentials), config);
            }
            return true;
        } catch (IllegalStateException e) {
            return false;
        }
    }

    protected Iterable<LoginCredentials> getCredentialCandidates(
            JcloudsLocation location, NodeMetadata node, ConnectivityResolverOptions options, ConfigBag setup) {
        LoginCredentials userCredentials = null;
        // Figure out which login credentials to use. We only make a connection with
        // initialCredentials when jclouds didn't do any sshing and wait for connectable is true.
        // 0. if jclouds didn't do anything and we should wait for the machine then initial credentials is
        //    whatever waitForSshable determines and then create the user ourselves.
        if (options.skipJcloudsSshing() && options.waitForConnectable()) {
            if (options.isWindows() && options.initialCredentials().isPresent()) {
                return ImmutableList.of(options.initialCredentials().get());
            } else {
                return location.generateCredentials(node.getCredentials(), setup.get(JcloudsLocationConfig.LOGIN_USER));
            }
        }

        // 1. Were they configured by the user?
        LoginCredentials customCredentials = setup.get(JcloudsLocationConfig.CUSTOM_CREDENTIALS);
        if (customCredentials != null) {
            userCredentials = customCredentials;
            //set userName and other data, from these credentials
            Object oldUsername = setup.put(JcloudsLocationConfig.USER, customCredentials.getUser());
            LOG.debug("Using username {}, from custom credentials, on node {}. User was previously {}",
                    new Object[]{customCredentials.getUser(), node, oldUsername});
            if (customCredentials.getOptionalPassword().isPresent()) {
                setup.put(JcloudsLocationConfig.PASSWORD, customCredentials.getOptionalPassword().get());
            }
            if (customCredentials.getOptionalPrivateKey().isPresent()) {
                setup.put(JcloudsLocationConfig.PRIVATE_KEY_DATA, customCredentials.getOptionalPrivateKey().get());
            }
        }
        // 2. Can they be extracted from the setup+node?
        if ((userCredentials == null ||
                (!userCredentials.getOptionalPassword().isPresent() && !userCredentials.getOptionalPrivateKey().isPresent())) &&
                options.initialCredentials().isPresent()) {
            // We either don't have any userCredentials, or it is missing both a password/key.
            if (userCredentials != null) {
                LOG.debug("Custom credential from {} is missing both password and private key; " +
                        "extracting them from the VM: {}", JcloudsLocationConfig.CUSTOM_CREDENTIALS.getName(), userCredentials);
            }
            // TODO See waitForSshable, which now handles if the node.getLoginCredentials has both a password+key
            userCredentials = location.extractVmCredentials(setup, node, options.initialCredentials().get());
        }
        if (userCredentials == null) {
            // only happens if something broke above...
            userCredentials = node.getCredentials();
        }

        return userCredentials != null? ImmutableList.of(userCredentials) : ImmutableList.<LoginCredentials>of();
    }

    // --------------------------------------------------------------------------------------

    protected Entity getContextEntity(ConfigBag configBag) {
        Object context = configBag.get(LocationConfigKeys.CALLER_CONTEXT);
        if (context instanceof Entity) {
            return (Entity) context;
        } else {
            Entity taskContext = BrooklynTaskTags.getContextEntity(Tasks.current());
            if (taskContext != null) {
                return taskContext;
            }
        }
        LOG.warn("No context entity found in config or current task");
        return null;
    }

    protected NetworkMode getNetworkMode() {
        NetworkMode networkMode = config().get(NETWORK_MODE);
        return networkMode != null ? networkMode : NetworkMode.PREFER_PUBLIC;
    }

    private boolean isNetworkModeSet() {
        return config().get(NETWORK_MODE) != null;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("mode", getNetworkMode())
                .toString();
    }

}
