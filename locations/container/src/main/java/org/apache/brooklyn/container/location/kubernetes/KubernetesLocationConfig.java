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

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.config.ConfigKeys;
import org.apache.brooklyn.core.location.LocationConfigKeys;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.util.time.Duration;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.reflect.TypeToken;

public interface KubernetesLocationConfig extends CloudLocationConfig {

    ConfigKey<String> MASTER_URL = LocationConfigKeys.CLOUD_ENDPOINT;

    ConfigKey<String> KUBECONFIG = ConfigKeys.builder(String.class)
            .name("kubeconfig")
            .description("Kubernetes .kubeconfig file to use instead of individual Location configuration keys")
            .constraint(file -> Files.isReadable(Paths.get(file)))
            .build();

    ConfigKey<String> CA_CERT_DATA = ConfigKeys.builder(String.class)
            .name("caCertData")
            .description("Data for CA certificate")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CA_CERT_FILE = ConfigKeys.builder(String.class)
            .name("caCertFile")
            .description("URL of resource containing CA certificate data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_CERT_DATA = ConfigKeys.builder(String.class)
            .name("clientCertData")
            .description("Data for client certificate")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_CERT_FILE = ConfigKeys.builder(String.class)
            .name("clientCertFile")
            .description("URL of resource containing client certificate data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_DATA = ConfigKeys.builder(String.class)
            .name("clientKeyData")
            .description("Data for client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_FILE = ConfigKeys.builder(String.class)
            .name("clientKeyFile")
            .description("URL of resource containing client key data")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_ALGO = ConfigKeys.builder(String.class)
            .name("clientKeyAlgo")
            .description("Algorithm used for the client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> CLIENT_KEY_PASSPHRASE = ConfigKeys.builder(String.class)
            .name("clientKeyPassphrase")
            .description("Passphrase used for the client key")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> OAUTH_TOKEN = ConfigKeys.builder(String.class)
            .name("oauthToken")
            .description("The OAuth token data for the current user")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<Duration> CLIENT_TIMEOUT = ConfigKeys.builder(Duration.class)
            .name("timeout")
            .description("The timeout for the client")
            .defaultValue(Duration.seconds(10))
            .constraint(Predicates.<Duration>notNull())
            .build();

    ConfigKey<Duration> ACTION_TIMEOUT = ConfigKeys.builder(Duration.class)
            .name("actionTimeout")
            .description("The timeout for Kubernetes actions")
            .defaultValue(Duration.ONE_MINUTE)
            .constraint(Predicates.<Duration>notNull())
            .build();

    ConfigKey<Boolean> CREATE_NAMESPACE = ConfigKeys.builder(Boolean.class)
            .name("namespace.create")
            .description("Whether to create the namespace if it does not exist")
            .defaultValue(true)
            .constraint(Predicates.<Boolean>notNull())
            .build();

    ConfigKey<Boolean> DELETE_EMPTY_NAMESPACE = ConfigKeys.builder(Boolean.class)
            .name("namespace.deleteEmpty")
            .description("Whether to delete an empty namespace when releasing resources")
            .defaultValue(false)
            .constraint(Predicates.<Boolean>notNull())
            .build();

    ConfigKey<String> NAMESPACE = ConfigKeys.builder(String.class)
            .name("namespace")
            .description("Namespace where resources will live; the default is 'brooklyn'")
            .defaultValue("brooklyn")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<Boolean> PRIVILEGED = ConfigKeys.builder(Boolean.class)
            .name("privileged")
            .description("Whether the pods use privileged containers")
            .defaultValue(false)
            .build();

    @SuppressWarnings("serial")
    ConfigKey<Map<String, ?>> ENV = ConfigKeys.builder(new TypeToken<Map<String, ?>>() { })
            .name("env")
            .description("Environment variables to inject when starting the container")
            .defaultValue(ImmutableMap.<String, Object>of())
            .constraint(Predicates.<Map<String, ?>>notNull())
            .build();

    ConfigKey<String> IMAGE = ConfigKeys.builder(String.class)
            .name("image")
            .description("Docker image to be deployed into the pod")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> OS_FAMILY = ConfigKeys.builder(String.class)
            .name("osFamily")
            .description("OS family, e.g. CentOS, Ubuntu")
            .build();

    ConfigKey<String> OS_VERSION_REGEX = ConfigKeys.builder(String.class)
            .name("osVersionRegex")
            .description("Regular expression for the OS version to load")
            .build();

    ConfigKey<KubernetesClientRegistry> KUBERNETES_CLIENT_REGISTRY = ConfigKeys.builder(KubernetesClientRegistry.class)
            .name("kubernetesClientRegistry")
            .description("Registry/Factory for creating Kubernetes client; default is almost always fine, "
                    + "except where tests want to customize behaviour")
            .defaultValue(KubernetesClientRegistryImpl.INSTANCE)
            .build();

    ConfigKey<String> LOGIN_USER = ConfigKeys.builder(String.class)
            .name("loginUser")
            .description("Override the user who logs in initially to perform setup")
            .defaultValue("root")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<String> LOGIN_USER_PASSWORD = ConfigKeys.builder(String.class)
            .name("loginUser.password")
            .description("Custom password for the user who logs in initially")
            .constraint(Predicates.<String>notNull())
            .build();

    ConfigKey<Boolean> INJECT_LOGIN_CREDENTIAL = ConfigKeys.builder(Boolean.class)
            .name("injectLoginCredential")
            .description("Whether to inject login credentials (if null, will infer from image choice); ignored if explicit 'loginUser.password' supplied")
            .build();

}

