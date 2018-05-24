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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
    
    // outline of test which can be used to assert combos of keys etc
    // currently uses my (alex's) hardcoded keys and preexisting pair;
    // should refactor to generate keys and the keypair and make a real test,
    // but for now keeping it commented out as a template for people to do manual testing
//    @Test(groups = "Live")
//    public void testProvisionVmAndAuthPublicKey() {
//        String regionName = USEAST_REGION_NAME;
//        loc = (JcloudsLocation) mgmt().getLocationRegistry().getLocationManaged(provider + (regionName == null ? "" : ":" + regionName));
//        SshMachineLocation machine = obtainMachine(MutableMap.of(
//            JcloudsLocationConfig.KEY_PAIR, "alex-aws-2",
//            JcloudsLocationConfig.LOGIN_USER_PRIVATE_KEY_FILE, "~/.ssh/alex-aws-2-id_rsa",
//            
//            JcloudsLocationConfig.PRIVATE_KEY_FILE, "~/.ssh/aws-cloudera_rsa",
//            JcloudsLocationConfig.PUBLIC_KEY_FILE, "~/.ssh/aws-cloudera_rsa.pub",
//            JcloudsLocationConfig.EXTRA_PUBLIC_KEY_DATA_TO_AUTH,
//                aws-whirr
//                "ssh-rsa AAAAB3NzaC1yc2EAAAABIwAAAQEAw9R7FG0pOpZ7MR+KYty+UzHerEtW9NVBJj+NmznT4zg57klqDAxitahe6cJpj8Nbt0Rmp9G4TZQzAWuoSH5ZUJpFpcVP75tMYWOP6ZymH7MZ5hXLjLHTicNyQ/EB9H2eXSK2xLnq/8oDnzKgnhIXL7tbcC7hOY9Yzu25UrO+xQDbzM3nuwlr38JJwo1fLsIiEVI3uutZW9ANZgcZg0USlFFvxGcAA2KZ322tqtQtP3YYE0IogYUjTSFj5xexFDzIcN5V2Z2tHYKW+Jl/jR98EAsq4By1L+whoX142NJGZsB1GKm4zZTh3vjfzpeGNmxrrHDGp2TOCGFJjk2seHqyyw== alex@almac.rocklynn.cognetics.org",
//            JcloudsLocationConfig.IMAGE_ID, "us-east-1/ami-5492ba3c"
//        ));
//        
//        log.info("Provisioned {} vm {}; checking if ssh'able", provider, machine);
//        assertTrue(machine.isSshable());
//    }

}
