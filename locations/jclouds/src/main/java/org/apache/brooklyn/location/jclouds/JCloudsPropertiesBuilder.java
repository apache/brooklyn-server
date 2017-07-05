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

import org.apache.brooklyn.core.config.Sanitizer;
import org.apache.brooklyn.core.location.cloud.CloudLocationConfig;
import org.apache.brooklyn.core.mgmt.persist.DeserializingJcloudsRenamesProvider;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.text.Strings;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.Constants;
import org.jclouds.ec2.reference.EC2Constants;
import org.jclouds.location.reference.LocationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Predicates;
import com.google.common.collect.Maps;

public class JCloudsPropertiesBuilder implements JcloudsLocationConfig{
    private ConfigBag conf;
    private Properties properties = new Properties();

    private static final Logger LOG = LoggerFactory.getLogger(JCloudsPropertiesBuilder.class);

    public JCloudsPropertiesBuilder(ConfigBag conf) {
        this.conf = conf;
    }

    public JCloudsPropertiesBuilder setCommonJcloudsProperties() {
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
        return this;
    }

    public JCloudsPropertiesBuilder setAWSEC2Properties() {
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
        return this;
    }

    public JCloudsPropertiesBuilder setAzureComputeArmProperties() {
        String region = conf.get(CloudLocationConfig.CLOUD_REGION_ID);
        if (Strings.isNonBlank(region)) {
            properties.setProperty(LocationConstants.PROPERTY_REGIONS, region);
        }
        return this;
    }

    public JCloudsPropertiesBuilder setCustomJcloudsProperties() {
        Map<String, Object> extra = Maps.filterKeys(conf.getAllConfig(), Predicates.containsPattern("^jclouds\\."));
        if (extra.size() > 0) {
            String provider = getProviderFromConfig(conf);
            LOG.debug("Configuring custom jclouds property overrides for {}: {}", provider, Sanitizer.sanitize(extra));
        }
        properties.putAll(Maps.filterValues(extra, Predicates.notNull()));
        return this;
    }

    public JCloudsPropertiesBuilder setEndpointProperty() {
        String endpoint = conf.get(CloudLocationConfig.CLOUD_ENDPOINT);
        if (!groovyTruth(endpoint)) endpoint = getDeprecatedProperty(conf, Constants.PROPERTY_ENDPOINT);
        if (groovyTruth(endpoint)) properties.setProperty(Constants.PROPERTY_ENDPOINT, endpoint);
        return this;
    }

    public Properties build() {
        return properties;
    }

    private String getDeprecatedProperty(ConfigBag conf, String key) {
        if (conf.containsKey(key)) {
            LOG.warn("Jclouds using deprecated brooklyn-jclouds property " + key + ": " + Sanitizer.sanitize(conf.getAllConfig()));
            return (String) conf.getStringKey(key);
        } else {
            return null;
        }
    }

    private String getProviderFromConfig(ConfigBag conf) {
        String rawProvider = checkNotNull(conf.get(CLOUD_PROVIDER), "provider must not be null");
        return DeserializingJcloudsRenamesProvider.INSTANCE.applyJcloudsRenames(rawProvider);
    }
}
