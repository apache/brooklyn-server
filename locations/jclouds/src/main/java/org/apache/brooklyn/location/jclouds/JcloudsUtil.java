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

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.brooklyn.util.net.Networking;
import org.apache.brooklyn.util.net.ReachableSocketFinder;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.aws.ec2.AWSEC2Api;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.docker.DockerApi;
import org.jclouds.docker.domain.Container;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.ec2.compute.domain.PasswordDataAndPrivateKey;
import org.jclouds.ec2.compute.functions.WindowsLoginCredentialsFromEncryptedData;
import org.jclouds.ec2.domain.PasswordData;
import org.jclouds.ec2.features.WindowsApi;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.jclouds.util.Predicates2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.google.inject.Module;

public class JcloudsUtil {

    // TODO Review what utility methods are needed, and what is now supported in jclouds 1.1

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsUtil.class);

    private JcloudsUtil() {}

    /**
     * Uses {@link Networking#isReachablePredicate()} to determine reachability.
     * @see #getReachableAddresses(NodeMetadata, Duration, Predicate)
     */
    public static String getFirstReachableAddress(NodeMetadata node, Duration timeout) {
        return getFirstReachableAddress(node, timeout, Networking.isReachablePredicate());
    }

    /** @see #getReachableAddresses(NodeMetadata, Duration, Predicate) */
    public static String getFirstReachableAddress(NodeMetadata node, Duration timeout, Predicate<? super HostAndPort> socketTester) {
        Iterable<HostAndPort> addresses = getReachableAddresses(node, timeout, socketTester);
        HostAndPort address = Iterables.getFirst(addresses, null);
        if (address != null) {
            return address.getHostText();
        } else {
            throw new IllegalStateException("No reachable IPs for " + node + "; check whether the node is " +
                    "reachable and whether it meets the requirements of the HostAndPort tester: " + socketTester);
        }
    }

    /**
     * @return The public and private addresses of node that are reachable.
     * @see #getReachableAddresses(Iterable, Duration, Predicate)
     */
    public static Iterable<HostAndPort> getReachableAddresses(NodeMetadata node, Duration timeout, Predicate<? super HostAndPort> socketTester) {
        final int port = node.getLoginPort();
        return getReachableAddresses(Iterables.concat(node.getPublicAddresses(), node.getPrivateAddresses()), port, timeout, socketTester);
    }

    /** @see #getReachableAddresses(Iterable, Duration, Predicate) */
    public static Iterable<HostAndPort> getReachableAddresses(Iterable<String> hosts, final int port, Duration timeout, Predicate<? super HostAndPort> socketTester) {
        FluentIterable<HostAndPort> sockets = FluentIterable
                .from(hosts)
                .transform(new Function<String, HostAndPort>() {
                        @Override public HostAndPort apply(String input) {
                            return HostAndPort.fromParts(input, port);
                        }});
        return getReachableAddresses(sockets, timeout, socketTester);
    }

    /**
     * Uses {@link ReachableSocketFinder} to determine which sockets are reachable. Iterators
     * are unmodifiable and are lazily evaluated.
     * @param sockets The host-and-ports to test
     * @param timeout Max time to try to connect to the ip:port
     * @param socketTester A predicate determining reachability.
     */
    public static Iterable<HostAndPort> getReachableAddresses(Iterable<HostAndPort> sockets, Duration timeout, Predicate<? super HostAndPort> socketTester) {
        ReachableSocketFinder finder = new ReachableSocketFinder(socketTester);
        return finder.findOpenSocketsOnNode(sockets, timeout);
    }

    // Suggest at least 15 minutes for timeout
    public static String waitForPasswordOnAws(ComputeService computeService, final NodeMetadata node, long timeout, TimeUnit timeUnit) throws TimeoutException {
        ComputeServiceContext computeServiceContext = computeService.getContext();
        AWSEC2Api ec2Client = computeServiceContext.unwrapApi(AWSEC2Api.class);
        final WindowsApi client = ec2Client.getWindowsApi().get();
        final String region = node.getLocation().getParent().getId();

        // The Administrator password will take some time before it is ready - Amazon says sometimes 15 minutes.
        // So we create a predicate that tests if the password is ready, and wrap it in a retryable predicate.
        Predicate<String> passwordReady = new Predicate<String>() {
            @Override public boolean apply(String s) {
                if (Strings.isNullOrEmpty(s)) return false;
                PasswordData data = client.getPasswordDataInRegion(region, s);
                if (data == null) return false;
                return !Strings.isNullOrEmpty(data.getPasswordData());
            }
        };

        LOG.info("Waiting for password, for "+node.getProviderId()+":"+node.getId());
        Predicate<String> passwordReadyRetryable = Predicates2.retry(passwordReady, timeUnit.toMillis(timeout), 10*1000, TimeUnit.MILLISECONDS);
        boolean ready = passwordReadyRetryable.apply(node.getProviderId());
        if (!ready) throw new TimeoutException("Password not available for "+node+" in region "+region+" after "+timeout+" "+timeUnit.name());

        // Now pull together Amazon's encrypted password blob, and the private key that jclouds generated
        PasswordDataAndPrivateKey dataAndKey = new PasswordDataAndPrivateKey(
                client.getPasswordDataInRegion(region, node.getProviderId()),
                node.getCredentials().getPrivateKey());

        // And apply it to the decryption function
        WindowsLoginCredentialsFromEncryptedData f = computeServiceContext.utils().injector().getInstance(WindowsLoginCredentialsFromEncryptedData.class);
        LoginCredentials credentials = f.apply(dataAndKey);

        return credentials.getPassword();
    }

    public static Map<Integer, Integer> dockerPortMappingsFor(JcloudsLocation docker, String containerId) {
        ComputeServiceContext context = null;
        try {
            Properties properties = new Properties();
            properties.setProperty(Constants.PROPERTY_TRUST_ALL_CERTS, Boolean.toString(true));
            properties.setProperty(Constants.PROPERTY_RELAX_HOSTNAME, Boolean.toString(true));
            context = ContextBuilder.newBuilder("docker")
                    .endpoint(docker.getEndpoint())
                    .credentials(docker.getIdentity(), docker.getCredential())
                    .overrides(properties)
                    .modules(ImmutableSet.<Module>of(new SLF4JLoggingModule(), new SshjSshClientModule()))
                    .build(ComputeServiceContext.class);
            DockerApi api = context.unwrapApi(DockerApi.class);
            Container container = api.getContainerApi().inspectContainer(containerId);
            Map<Integer, Integer> portMappings = Maps.newLinkedHashMap();
            Map<String, List<Map<String, String>>> ports = container.networkSettings().ports();
            if (ports == null) ports = ImmutableMap.<String, List<Map<String,String>>>of();

            LOG.debug("Docker will forward these ports {}", ports);
            for (Map.Entry<String, List<Map<String, String>>> entrySet : ports.entrySet()) {
                String containerPort = Iterables.get(Splitter.on("/").split(entrySet.getKey()), 0);
                String hostPort = Iterables.getOnlyElement(Iterables.transform(entrySet.getValue(),
                        new Function<Map<String, String>, String>() {
                            @Override
                            public String apply(Map<String, String> hostIpAndPort) {
                                return hostIpAndPort.get("HostPort");
                            }
                        }));
                portMappings.put(Integer.parseInt(containerPort), Integer.parseInt(hostPort));
            }
            return portMappings;
        } finally {
            if (context != null) {
                context.close();
            }
        }
    }
}
