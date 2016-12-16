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

import org.testng.annotations.DataProvider;

/**
 * Tests basic provisioning to Softlayer.
 * 
 * Requires AWS credentials be set up in {@code ~/.brooklyn/brooklyn.properties}.
 */
public class SoftlayerLocationLiveTest extends AbstractJcloudsLocationTest {

    private static final String PROVIDER = "softlayer";
    private static final String IMAGE_ID = "CENTOS_6_64"; // Image: {id=CENTOS_6_64, providerId=CENTOS_6_64, os={family=centos, version=6.5, description=CentOS / CentOS / 6.5-64 LAMP for Bare Metal, is64Bit=true}, description=CENTOS_6_64, status=AVAILABLE, loginUser=root}
    private static final String IMAGE_PATTERN = "CENTOS_6_64.*";
    private static final String REGION_NAME = null;//FIXME "ams06";
    private static final String IMAGE_OWNER = null;
    
//    public static final int MAX_TAG_LENGTH = 20;
//    public static final int MAX_VM_NAME_LENGTH = 30;

    public SoftlayerLocationLiveTest() {
        super(PROVIDER);
    }

    @Override
    @DataProvider(name = "fromImageId")
    public Object[][] cloudAndImageIds() {
        return new Object[][] {
                new Object[] { REGION_NAME, IMAGE_ID, IMAGE_OWNER }
            };
    }

    @Override
    @DataProvider(name = "fromImageDescriptionPattern")
    public Object[][] cloudAndImageDescriptionPatterns() {
        return new Object[][] {
                new Object[] { REGION_NAME, IMAGE_PATTERN, IMAGE_OWNER }
            };
    }

    // For Softlayer, use "imageDescriptionPattern" instead
    @Override
    @DataProvider(name = "fromImageDescriptionPattern")
    public Object[][] cloudAndImageNamePatterns() {
        return new Object[][] {};
    }
}
