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

import java.io.IOException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.exceptions.Exceptions;
import org.jclouds.aws.domain.SessionCredentials;
import org.jclouds.compute.ComputeService;
import org.jclouds.domain.Credentials;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Supplier;

/**
 * A ComputeServiceRegistry that can infer credentials from the AWS EC2 instance profile.
 *
 * This only works if Brooklyn is running on an EC2 instance that has an IAM Role attached with e.g the AmazonEC2FullAccess
 * policy set.
 *
 * usage:
 *
 * {@code
 * jclouds.computeServiceRegistry:
 *   $brooklyn:object:
 *     type: org.apache.brooklyn.location.jclouds.AwsEc2SessionAwareComputeServiceRegistry
 *  }
 */
public class AwsEc2SessionAwareComputeServiceRegistry extends AbstractComputeServiceRegistry implements ComputeServiceRegistry, AwsEc2SessionAwareLocationConfig {

    public static final String ACCESS_KEY_ID = "AccessKeyId";
    public static final String SECRET_ACCESS_KEY = "SecretAccessKey";
    public static final String TOKEN = "Token";
    public static final String EXPIRATION = "Expiration";
    public static final String AWS_SECURITY_CREDENTIAL_URL = "http://169.254.169.254/latest/meta-data/iam/security-credentials";
    public static final String AWS_EXPIRATION_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss'Z'";

    public AwsEc2SessionAwareComputeServiceRegistry(){
        // empty constructor required
    }

    @Override
    public ComputeService findComputeService(ConfigBag conf, boolean allowReuse) {
        return super.findComputeService(conf, false); // do not allow caching
    }

    @Override
    protected Supplier<Credentials> makeCredentials(ConfigBag conf) {
        Credentials credentials;
        String identity = null, credential = null, token = null;
        Date expiration = null;
        String provider = getProviderFromConfig(conf);
        String iamRoleName = getIamRoleNameFromConfig(conf);
        if ("aws-ec2".equals(provider)) {
            try {
                String instanceProfileUrl = AWS_SECURITY_CREDENTIAL_URL;
                JsonNode node = new ObjectMapper().readTree(new URL(instanceProfileUrl + "/" + iamRoleName));
                identity = node.path(ACCESS_KEY_ID).asText();
                credential = node.path(SECRET_ACCESS_KEY).asText();
                token = node.path(TOKEN).asText();
                expiration = new SimpleDateFormat(AWS_EXPIRATION_DATE_FORMAT).parse(node.path(EXPIRATION).asText());
            } catch (IOException | ParseException e) {
                Exceptions.propagate(e);
            }
        } else {
            throw new IllegalArgumentException("Provider " + provider + " does not support session credentials");
        }

        identity = checkNotNull(identity, "identity must not be null");
        credential = checkNotNull(credential, "credential must not be null");
        token = checkNotNull(token, "token must not be null");

        credentials = SessionCredentials.builder()
                .accessKeyId(identity)
                .credential(credential)
                .sessionToken(token)
                .expiration(expiration)
                .build();
        return () -> credentials;
    }

    private String getIamRoleNameFromConfig(ConfigBag conf) {
        return checkNotNull(conf.get(IAM_ROLE_NAME), "IAM role must not be null");
    }

    @Override
    public String toString(){
        return getClass().getName();
    }

}
