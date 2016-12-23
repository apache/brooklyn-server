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

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.testng.annotations.DataProvider;

/**
 * Tests basic provisioning to GCE.
 * 
 * Requires AWS credentials be set up in {@code ~/.brooklyn/brooklyn.properties}.
 */
public class GceLocationLiveTest extends AbstractJcloudsLocationTest {

    // TODO Would be nice to support short-form of imageId and hardwareId!
    
    private static final String PROVIDER = "google-compute-engine";
    private static final String IMAGE_ID = "https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20161129";
    private static final String IMAGE_PATTERN = "centos-6-.*";
    private static final String REGION_NAME = null;
    private static final String IMAGE_OWNER = null;
    
    protected BrooklynProperties brooklynProperties;
    protected ManagementContext ctx;
    
    protected TestApplication app;
    protected Location jcloudsLocation;
    
    public GceLocationLiveTest() {
        super(PROVIDER);
    }

    @Override
    @DataProvider(name = "fromImageId")
    public Object[][] cloudAndImageIds() {
        return new Object[][] {
                new Object[] { REGION_NAME, IMAGE_ID, IMAGE_OWNER }
            };
    }

    // For GCE, use "imageNamePattern" instead
    @Override
    @DataProvider(name = "fromImageDescriptionPattern")
    public Object[][] cloudAndImageDescriptionPatterns() {
        return new Object[][] {};
    }

    @Override
    @DataProvider(name = "fromImageNamePattern")
    public Object[][] cloudAndImageNamePatterns() {
        return new Object[][] {
                new Object[] { REGION_NAME, IMAGE_PATTERN, IMAGE_OWNER }
            };
    }
}
