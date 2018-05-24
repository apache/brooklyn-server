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
package org.apache.brooklyn.location.jclouds.provider;

import java.util.Map;

import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.ec2.EC2Api;
import org.jclouds.ec2.domain.KeyPair;
import org.jclouds.ssh.SshKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.Assert;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

/**
 * Tests basic provisioning to aws-ec2.
 * 
 * Requires AWS credentials be set up in {@code ~/.brooklyn/brooklyn.properties}.
 */
public class AwsEc2LocationLiveTest extends AbstractJcloudsLocationTest {

    private static final Logger log = LoggerFactory.getLogger(AwsEc2LocationLiveTest.class);
    
    private static final String PROVIDER = "aws-ec2";
    private static final String EUWEST_REGION_NAME = "eu-west-1";
    private static final String USEAST_REGION_NAME = "us-east-1";
    private static final String EUWEST_IMAGE_ID = EUWEST_REGION_NAME+"/"+"ami-69841c1e"; // RightImage_CentOS_7.0_x64_v14.2.1_HVM_EBS
    private static final String USEAST_IMAGE_ID = USEAST_REGION_NAME+"/"+"ami-5492ba3c"; // RightImage_CentOS_7.0_x64_v14.2.1_HVM_EBS
    private static final String IMAGE_OWNER = "411009282317";
    private static final String IMAGE_PATTERN = "RightImage_CentOS_7.0_x64_v14.2.1.*";

    public AwsEc2LocationLiveTest() {
        super(PROVIDER);
    }

    @Override
    @DataProvider(name = "fromImageId")
    public Object[][] cloudAndImageIds() {
        return new Object[][] {
                new Object[] { EUWEST_REGION_NAME, EUWEST_IMAGE_ID, IMAGE_OWNER },
                new Object[] { USEAST_REGION_NAME, USEAST_IMAGE_ID, IMAGE_OWNER }
            };
    }

    @Override
    @DataProvider(name = "fromImageDescriptionPattern")
    public Object[][] cloudAndImageDescriptionPatterns() {
        return new Object[][] {
                new Object[] { EUWEST_REGION_NAME, IMAGE_PATTERN, IMAGE_OWNER },
                new Object[] { USEAST_REGION_NAME, IMAGE_PATTERN, IMAGE_OWNER }
            };
    }

    @Override
    @DataProvider(name = "fromImageNamePattern")
    public Object[][] cloudAndImageNamePatterns() {
        return new Object[][] {
                new Object[] { USEAST_REGION_NAME, IMAGE_PATTERN, IMAGE_OWNER }
            };
    }

    @Test(enabled = false)
    public void noop() { } /* just exists to let testNG IDE run the test */
    
    @Test(groups = "Live")
    public void testProvisionVmAndAuthPublicKey() {
        String regionName = USEAST_REGION_NAME;
        loc = (JcloudsLocation) mgmt().getLocationRegistry().getLocationManaged(provider + (regionName == null ? "" : ":" + regionName));

        EC2Api api = loc.getComputeService().getContext().unwrapApi(EC2Api.class);
        String primaryKeyName = "brooklyn-keypair-" + System.currentTimeMillis();
        try {
            Map<String, String> extra = SshKeys.generate();
            KeyPair primary = api.getKeyPairApiForRegion(regionName).get().createKeyPairInRegion(regionName, primaryKeyName);
            SshMachineLocation machine = obtainMachine(MutableMap.of(
                    JcloudsLocationConfig.KEY_PAIR, primary.getKeyName(),
                    JcloudsLocationConfig.LOGIN_USER_PRIVATE_KEY_DATA, primary.getKeyMaterial(),
                    JcloudsLocationConfig.EXTRA_PUBLIC_KEY_DATA_TO_AUTH, extra.get("public"),
                    JcloudsLocationConfig.IMAGE_ID, "us-east-1/ami-5492ba3c"
            ));

            log.info("Provisioned {} vm {}; checking if ssh'able; extra private key below\n{}", provider, machine, extra.get("private"));
            Assert.assertTrue(machine.isSshable());
        } finally {
            api.getKeyPairApiForRegion(USEAST_REGION_NAME).get().deleteKeyPairInRegion(USEAST_REGION_NAME, primaryKeyName);
        }
    }
    
}
