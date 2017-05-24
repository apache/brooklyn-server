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
import static org.apache.brooklyn.util.JavaGroovyEquivalents.groovyTruth;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_AMI_QUERY;
import static org.jclouds.aws.ec2.reference.AWSEC2Constants.PROPERTY_EC2_CC_AMI_QUERY;

import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.apache.brooklyn.util.collections.MutableMap;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.Constants;
import org.jclouds.ContextBuilder;
import org.jclouds.azurecompute.arm.config.AzureComputeRateLimitModule;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.ComputeServiceContext;
import org.jclouds.domain.Credentials;
import org.jclouds.ec2.reference.EC2Constants;
import org.jclouds.encryption.bouncycastle.config.BouncyCastleCryptoModule;
import org.jclouds.location.reference.LocationConstants;
import org.jclouds.logging.slf4j.config.SLF4JLoggingModule;
import org.jclouds.sshj.config.SshjSshClientModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.base.Supplier;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.inject.Module;

public abstract class AbstractComputeServiceRegistry implements ComputeServiceRegistry, JcloudsLocationConfig {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractComputeServiceRegistry.class);

    private final Map<Map<?, ?>, ComputeService> cachedComputeServices = new ConcurrentHashMap<>();

    @Override
    public ComputeService findComputeService(ConfigBag conf, boolean allowReuse) {
        Properties properties = new Properties();
        setCommonProperties(conf, properties);

        Iterable<Module> modules = getCommonModules();

        // Enable aws-ec2 lazy image fetching, if given a specific imageId; otherwise customize for specific owners; or all as a last resort
        // See https://issues.apache.org/jira/browse/WHIRR-416
        String provider = getProviderFromConfig(conf);
        if ("aws-ec2".equals(provider)) {
            setAWSEC2Properties(conf, properties);
        } else if ("azurecompute-arm".equals(provider)) {
            setAzureComputeArmProperties(conf, properties);
            // jclouds 2.0.0 does not include the rate limit module for Azure ARM. This quick fix enables this which will
            // avoid provisioning to fail due to rate limit exceeded
            // See https://issues.apache.org/jira/browse/JCLOUDS-1229
            modules = ImmutableSet.<Module>builder()
                    .addAll(modules)
                    .add(new AzureComputeRateLimitModule())
                    .build();
        }

        addJCloudsProperties(conf, properties);
        addEndpointProperty(conf, properties);

        Supplier<ComputeService> computeServiceSupplier = allowReuse
                ? new ReusableComputeServiceSupplier(conf, modules, properties)
                : new ComputeServiceSupplierImpl(conf, modules, properties);

        return computeServiceSupplier.get();
    }

    public abstract class ComputeServiceSupplier implements Supplier<ComputeService> {

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

        protected ConfigBag getConf() {
            return conf;
        }

        protected Properties getProperties() {
            return properties;
        }
    }

    public class ComputeServiceSupplierImpl extends ComputeServiceSupplier {

        public ComputeServiceSupplierImpl(ConfigBag conf, Iterable<? extends Module> modules, Properties properties) {
            super(conf, modules, properties);
        }
    }

    public class ReusableComputeServiceSupplier extends ComputeServiceSupplier {

        private Map<?, ?> cacheKey;

        public ReusableComputeServiceSupplier(ConfigBag conf, Iterable<? extends Module> modules, Properties properties) {
            super(conf, modules, properties);
            this.cacheKey = makeCacheKey();
        }

        @Override
        public ComputeService get() {
            ComputeService result = cachedComputeServices.get(cacheKey);
            if (result != null) {
                LOG.trace("jclouds ComputeService cache hit for compute service, for " + Sanitizer.sanitize(getProperties()));
                return result;
            }
            LOG.debug("jclouds ComputeService cache miss for compute service, creating, for " + Sanitizer.sanitize(getProperties()));
            final ComputeService computeService = super.get();
            synchronized (cachedComputeServices) {
                result = cachedComputeServices.get(cacheKey);
                if (result != null) {
                    LOG.debug("jclouds ComputeService cache recovery for compute service, for " + Sanitizer.sanitize(cacheKey));
                    //keep the old one, discard the new one
                    computeService.getContext().close();
                    return result;
                }
                LOG.debug("jclouds ComputeService created " + computeService + ", adding to cache, for " + Sanitizer.sanitize(getProperties()));
                cachedComputeServices.put(cacheKey, computeService);
            }
            return result;
        }

        private Map<?, ?> makeCacheKey() {
            String provider = getProviderFromConfig(getConf());
            String identity = checkNotNull(getConf().get(CloudLocationConfig.ACCESS_IDENTITY), "identity must not be null");
            String credential = checkNotNull(getConf().get(CloudLocationConfig.ACCESS_CREDENTIAL), "credential must not be null");
            String endpoint = getProperties().getProperty(Constants.PROPERTY_ENDPOINT);
            return MutableMap.builder()
                    .putAll(getProperties())
                    .put("provider", provider)
                    .put("identity", identity)
                    .put("credential", credential)
                    .putIfNotNull("endpoint", endpoint)
                    .build()
                    .asUnmodifiable();
        }
    }

    protected String getProviderFromConfig(ConfigBag conf) {
        String rawProvider = checkNotNull(conf.get(CLOUD_PROVIDER), "provider must not be null");
        return DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(rawProvider);
    }

    private String addEndpointProperty(ConfigBag conf, Properties properties) {
        String endpoint = conf.get(CloudLocationConfig.CLOUD_ENDPOINT);
        if (!groovyTruth(endpoint)) endpoint = getDeprecatedProperty(conf, Constants.PROPERTY_ENDPOINT);
        if (groovyTruth(endpoint)) properties.setProperty(Constants.PROPERTY_ENDPOINT, endpoint);
        return endpoint;
    }

    private void setCommonProperties(ConfigBag conf, Properties properties) {
        properties.setProperty(Constants.PROPERTY_TRUST_ALL_CERTS, Boolean.toString(true));
        properties.setProperty(Constants.PROPERTY_RELAX_HOSTNAME, Boolean.toString(true));
        properties.setProperty("jclouds.ssh.max-retries", conf.getStringKey("jclouds.ssh.max-retries") != null ?
                conf.getStringKey("jclouds.ssh.max-retries").toString() : "50");

        if (conf.get(OAUTH_ENDPOINT) != null)
            properties.setProperty(OAUTH_ENDPOINT.getName(), conf.get(OAUTH_ENDPOINT));

        // See https://issues.apache.org/jira/browse/BROOKLYN-394
        // For retries, the backoff times are:
        //   Math.min(2^failureCount * retryDelayStart, retryDelayStart * 10) + random(10%)
        // Therefore the backoff times will be: 500ms, 1s, 2s, 4s, 5s, 5s.
        // The defaults (if not overridden here) are 50ms and 5 retires. This gives backoff
        // times of 50ms, 100ms, 200ms, 400ms, 500ms (so a total backoff time of 1.25s),
        // which is not long when you're being rate-limited and there are multiple thread all
        // retrying their API calls.
        properties.setProperty(Constants.PROPERTY_RETRY_DELAY_START, "500");
        properties.setProperty(Constants.PROPERTY_MAX_RETRIES, "6");
    }

    private void addJCloudsProperties(ConfigBag conf, Properties properties) {
        // Add extra jclouds-specific configuration
        Map<String, Object> extra = Maps.filterKeys(conf.getAllConfig(), Predicates.containsPattern("^jclouds\\."));
        if (extra.size() > 0) {
            String provider = getProviderFromConfig(conf);
            LOG.debug("Configuring custom jclouds property overrides for {}: {}", provider, Sanitizer.sanitize(extra));
        }
        properties.putAll(Maps.filterValues(extra, Predicates.notNull()));
    }

    private void setAzureComputeArmProperties(ConfigBag conf, Properties properties) {
        String region = conf.get(CLOUD_REGION_ID);
        if (Strings.isNonBlank(region)) {
            properties.setProperty(LocationConstants.PROPERTY_REGIONS, region);
        }
    }

    private void setAWSEC2Properties(ConfigBag conf, Properties properties) {
        // TODO convert AWS-only flags to config keys
        if (groovyTruth(conf.get(IMAGE_ID))) {
            properties.setProperty(PROPERTY_EC2_AMI_QUERY, "");
            properties.setProperty(PROPERTY_EC2_CC_AMI_QUERY, "");
        } else if (groovyTruth(conf.getStringKey("imageOwner"))) {
            properties.setProperty(PROPERTY_EC2_AMI_QUERY, "owner-id=" + conf.getStringKey("imageOwner") + ";state=available;image-type=machine");
        } else if (groovyTruth(conf.getStringKey("anyOwner"))) {
            // set `anyOwner: true` to override the default query (which is restricted to certain owners as per below),
            // allowing the AMI query to bind to any machine
            // (note however, we sometimes pick defaults in JcloudsLocationFactory);
            // (and be careful, this can give a LOT of data back, taking several minutes,
            // and requiring extra memory allocated on the command-line)
            properties.setProperty(PROPERTY_EC2_AMI_QUERY, "state=available;image-type=machine");
                /*
                 * by default the following filters are applied:
                 * Filter.1.Name=owner-id&Filter.1.Value.1=137112412989&
                 * Filter.1.Value.2=063491364108&
                 * Filter.1.Value.3=099720109477&
                 * Filter.1.Value.4=411009282317&
                 * Filter.2.Name=state&Filter.2.Value.1=available&
                 * Filter.3.Name=image-type&Filter.3.Value.1=machine&
                 */
        }

        // See https://issues.apache.org/jira/browse/BROOKLYN-399
        String region = conf.get(CLOUD_REGION_ID);
        if (Strings.isNonBlank(region)) {
                /*
                 * Drop availability zone suffixes. Without this deployments to regions like us-east-1b fail
                 * because jclouds throws an IllegalStateException complaining that: location id us-east-1b
                 * not found in: [{scope=PROVIDER, id=aws-ec2, description=https://ec2.us-east-1.amazonaws.com,
                 * iso3166Codes=[US-VA, US-CA, US-OR, BR-SP, IE, DE-HE, SG, AU-NSW, JP-13]}]. The exception is
                 * thrown by org.jclouds.compute.domain.internal.TemplateBuilderImpl#locationId(String).
                 */
            if (Character.isLetter(region.charAt(region.length() - 1))) {
                region = region.substring(0, region.length() - 1);
            }
            properties.setProperty(LocationConstants.PROPERTY_REGIONS, region);
        }

        // occasionally can get com.google.common.util.concurrent.UncheckedExecutionException: java.lang.RuntimeException:
        //     security group eu-central-1/jclouds#brooklyn-bxza-alex-eu-central-shoul-u2jy-nginx-ielm is not available after creating
        // the default timeout was 500ms so let's raise it in case that helps
        properties.setProperty(EC2Constants.PROPERTY_EC2_TIMEOUT_SECURITYGROUP_PRESENT, "" + Duration.seconds(30).toMilliseconds());
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

    protected String getDeprecatedProperty(ConfigBag conf, String key) {
        if (conf.containsKey(key)) {
            LOG.warn("Jclouds using deprecated brooklyn-jclouds property " + key + ": " + Sanitizer.sanitize(conf.getAllConfig()));
            return (String) conf.getStringKey(key);
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return getClass().getName();
    }

}
