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

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.azurecompute.arm.config.AzureComputeRateLimitModule;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.domain.Credentials;
import org.jclouds.encryption.bouncycastle.config.BouncyCastleCryptoModule;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;

import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Module;

public abstract class AbstractComputeServiceRegistry implements ComputeServiceRegistry, JcloudsLocationConfig {

    private final Map<Map<?, ?>, ComputeService> cachedComputeServices = new ConcurrentHashMap<>();

    @Override
    public ComputeService findComputeService(ConfigBag conf, boolean allowReuse) {
        JCloudsPropertiesBuilder propertiesBuilder = new JCloudsPropertiesBuilder(conf)
                .setCommonJcloudsProperties();

        Iterable<Module> modules = getCommonModules();

        // Enable aws-ec2 lazy image fetching, if given a specific imageId; otherwise customize for specific owners; or all as a last resort
        // See https://issues.apache.org/jira/browse/WHIRR-416
        String provider = getProviderFromConfig(conf);
        if ("aws-ec2".equals(provider)) {
            propertiesBuilder.setAWSEC2Properties();
        } else if ("azurecompute-arm".equals(provider)) {
            propertiesBuilder.setAzureComputeArmProperties();
            // jclouds 2.0.0 does not include the rate limit module for Azure ARM. This quick fix enables this which will
            // avoid provisioning to fail due to rate limit exceeded
            // See https://issues.apache.org/jira/browse/JCLOUDS-1229
            modules = ImmutableSet.<Module>builder()
                    .addAll(modules)
                    .add(new AzureComputeRateLimitModule())
                    .build();
        }

        Properties properties = propertiesBuilder
                .setCustomJcloudsProperties()
                .setEndpointProperty()
                .build();

        Supplier<ComputeService> computeServiceSupplier =  new ComputeServiceSupplier(conf, modules, properties);
        if (allowReuse) {
            return cachedComputeServices.computeIfAbsent(makeCacheKey(conf, properties), key -> computeServiceSupplier.get());
        }
        return computeServiceSupplier.get();
    }

    private Map<?, ?> makeCacheKey(ConfigBag conf, Properties properties) {
        String provider = getProviderFromConfig(conf);
        String identity = checkNotNull(conf.get(CloudLocationConfig.ACCESS_IDENTITY), "identity must not be null");
        String credential = checkNotNull(conf.get(CloudLocationConfig.ACCESS_CREDENTIAL), "credential must not be null");
        String endpoint = properties.getProperty(Constants.PROPERTY_ENDPOINT);
        return MutableMap.builder()
                .putAll(properties)
                .put("provider", provider)
                .put("identity", identity)
                .put("credential", credential)
                .putIfNotNull("endpoint", endpoint)
                .build()
                .asUnmodifiable();
    }

    public class ComputeServiceSupplier implements Supplier<ComputeService> {

        private final String provider;
        private final ConfigBag conf;
        private final Iterable<? extends Module> modules;
        private final Properties properties;

        private final Object createComputeServicesMutex = new Object();

        public ComputeServiceSupplier(ConfigBag conf, Iterable<? extends Module> modules, Properties properties) {
            this.provider = getProviderFromConfig(conf);
            this.conf = conf;
            this.modules = modules;
            this.properties = properties;
        }

        public ComputeService get() {
            // Synchronizing to avoid deadlock from sun.reflect.annotation.AnnotationType.
            // See https://github.com/brooklyncentral/brooklyn/issues/974
            synchronized (createComputeServicesMutex) {
                ComputeServiceContext computeServiceContext = ContextBuilder.newBuilder(provider)
                        .modules(modules)
                        .credentialsSupplier(AbstractComputeServiceRegistry.this.makeCredentials(conf))
                        .overrides(properties)
                        .build(ComputeServiceContext.class);
                return computeServiceContext.getComputeService();
            }
        }
    }

    protected String getProviderFromConfig(ConfigBag conf) {
        String rawProvider = checkNotNull(conf.get(CLOUD_PROVIDER), "provider must not be null");
        return DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(rawProvider);
    }

    protected abstract Supplier<Credentials> makeCredentials(ConfigBag conf);

    /**
     * returns the jclouds modules we typically install
     */
    protected ImmutableSet<Module> getCommonModules() {
        return ImmutableSet.<Module>of(
                new SshjSshClientModule(),
                new SLF4JLoggingModule(),
                new BouncyCastleCryptoModule());
    }

    @Override
    public String toString() {
        return getClass().getName();
    }

}
