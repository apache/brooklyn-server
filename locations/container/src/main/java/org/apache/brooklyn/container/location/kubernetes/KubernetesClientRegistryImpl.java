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
package org.apache.brooklyn.container.location.kubernetes;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.google.common.io.BaseEncoding;

import io.fabric8.kubernetes.api.model.AuthInfo;
import io.fabric8.kubernetes.api.model.Cluster;
import io.fabric8.kubernetes.api.model.Config;
import io.fabric8.kubernetes.api.model.Context;
import io.fabric8.kubernetes.api.model.NamedAuthInfo;
import io.fabric8.kubernetes.api.model.NamedCluster;
import io.fabric8.kubernetes.api.model.NamedContext;
import io.fabric8.kubernetes.client.ConfigBuilder;
import io.fabric8.kubernetes.client.DefaultKubernetesClient;
import io.fabric8.kubernetes.client.KubernetesClient;
import io.fabric8.kubernetes.client.internal.KubeConfigUtils;

public class KubernetesClientRegistryImpl implements KubernetesClientRegistry {

    private static final Logger LOG = LoggerFactory.getLogger(KubernetesClientRegistryImpl.class);

    public static final KubernetesClientRegistryImpl INSTANCE = new KubernetesClientRegistryImpl();

    @Override
    public KubernetesClient getKubernetesClient(ConfigBag conf) {
        ConfigBuilder configBuilder = new ConfigBuilder();

        String configFile = conf.get(KubernetesLocationConfig.KUBECONFIG);
        if (Strings.isNonBlank(configFile)) {
            try {
                Path configPath = Paths.get(configFile);
                Path configFolder = configPath.normalize().getParent();
                Config kubeconfig = KubeConfigUtils.parseConfig(configPath.toFile());
                String currentContext = kubeconfig.getCurrentContext();
                Optional<NamedContext> foundContext = Iterables.tryFind(kubeconfig.getContexts(), c -> c.getName().equals(currentContext));
                if (!foundContext.isPresent()) {
                    throw new IllegalStateException(String.format("Context %s not found", currentContext));
                }
                Context context = foundContext.get().getContext();
                LOG.debug("Context {} additional properties: {}", currentContext, context.getAdditionalProperties());
                configBuilder.withNamespace(context.getNamespace());

                String user = context.getUser();
                Optional<NamedAuthInfo> foundAuthInfo = Iterables.tryFind(kubeconfig.getUsers(), u -> u.getName().equals(user));
                if (!foundAuthInfo.isPresent()) {
                    throw new IllegalStateException(String.format("Auth info %s not found", user));
                }
                AuthInfo auth = foundAuthInfo.get().getUser();
                LOG.debug("Auth info {} additional properties: {}", user, auth.getAdditionalProperties());
                configBuilder.withUsername(auth.getUsername());
                configBuilder.withPassword(auth.getPassword());
                if (auth.getToken() == null) {
                    if (auth.getAuthProvider() != null) {
                        configBuilder.withOauthToken(auth.getAuthProvider().getConfig().get("id-token"));
                    }
                } else {
                    configBuilder.withOauthToken(auth.getToken());
                }
                configBuilder.withClientCertFile(getRelativeFile(auth.getClientCertificate(), configFolder));
                configBuilder.withClientCertData(auth.getClientCertificateData());
                configBuilder.withClientKeyFile(getRelativeFile(auth.getClientKey(), configFolder));
                configBuilder.withClientKeyData(auth.getClientKeyData());

                String clusterName = context.getCluster();
                Optional<NamedCluster> foundCluster = Iterables.tryFind(kubeconfig.getClusters(), c -> c.getName().equals(clusterName));
                if (!foundCluster.isPresent()) {
                    throw new IllegalStateException(String.format("Cluster %s not found", clusterName));
                }
                Cluster cluster = foundCluster.get().getCluster();
                configBuilder.withMasterUrl(cluster.getServer());
                configBuilder.withCaCertFile(getRelativeFile(cluster.getCertificateAuthority(), configFolder));
                configBuilder.withCaCertData(cluster.getCertificateAuthorityData());
                configBuilder.withApiVersion(Optional.fromNullable(cluster.getApiVersion()).or("v1"));
                configBuilder.withTrustCerts(Boolean.TRUE.equals(cluster.getInsecureSkipTlsVerify()));
                LOG.debug("Cluster {} server: {}", clusterName, cluster.getServer());
                LOG.debug("Cluster {} additional properties: {}", clusterName, cluster.getAdditionalProperties());
            } catch (IOException e) {
                Exceptions.propagate(e);
            }
        } else {
            String masterUrl = checkNotNull(conf.get(KubernetesLocationConfig.MASTER_URL), "master url must not be null");

            URL url;
            try {
                url = new URL(masterUrl);
            } catch (MalformedURLException e) {
                throw Throwables.propagate(e);
            }

            configBuilder.withMasterUrl(masterUrl)
                         .withTrustCerts(false);

            if (url.getProtocol().equals("https")) {
                KubernetesCerts certs = new KubernetesCerts(conf);
                if (certs.caCertData.isPresent()) configBuilder.withCaCertData(toBase64Encoding(certs.caCertData.get()));
                if (certs.clientCertData.isPresent()) configBuilder.withClientCertData(toBase64Encoding(certs.clientCertData.get()));
                if (certs.clientKeyData.isPresent()) configBuilder.withClientKeyData(toBase64Encoding(certs.clientKeyData.get()));
                if (certs.clientKeyAlgo.isPresent()) configBuilder.withClientKeyAlgo(certs.clientKeyAlgo.get());
                if (certs.clientKeyPassphrase.isPresent()) configBuilder.withClientKeyPassphrase(certs.clientKeyPassphrase.get());
                // TODO Should we also set configBuilder.withTrustCerts(true) here?
            }

            String username = conf.get(KubernetesLocationConfig.ACCESS_IDENTITY);
            if (Strings.isNonBlank(username)) configBuilder.withUsername(username);

            String password = conf.get(KubernetesLocationConfig.ACCESS_CREDENTIAL);
            if (Strings.isNonBlank(password)) configBuilder.withPassword(password);

            String token = conf.get(KubernetesLocationConfig.OAUTH_TOKEN);
            if (Strings.isNonBlank(token)) configBuilder.withOauthToken(token);
        }

        Duration clientTimeout = conf.get(KubernetesLocationConfig.CLIENT_TIMEOUT);
        if (clientTimeout.isPositive()) {
            configBuilder.withConnectionTimeout((int) clientTimeout.toMilliseconds());
            configBuilder.withRequestTimeout((int) clientTimeout.toMilliseconds());
        } else {
            throw new IllegalArgumentException("Kubernetes client timeout should be a positive duration: " + clientTimeout.toString());
        }
        Duration actionTimeout = conf.get(KubernetesLocationConfig.ACTION_TIMEOUT);
        if (actionTimeout.isPositive()) {
            configBuilder.withRollingTimeout(actionTimeout.toMilliseconds());
            configBuilder.withScaleTimeout(actionTimeout.toMilliseconds());
        } else {
            throw new IllegalArgumentException("Kubernetes action timeout should be a positive duration: " + actionTimeout.toString());
        }

        return new DefaultKubernetesClient(configBuilder.build());
    }

    protected String toBase64Encoding(String val) {
        return BaseEncoding.base64().encode(val.getBytes());
    }

    protected String getRelativeFile(String file, Path folder) {
        if (Strings.isBlank(file)) {
            return null;
        }
        Path path = Paths.get(file);
        if (!Files.exists(path)) {
            path = folder.resolve(file);
        }
        return path.toString();
    }
}
