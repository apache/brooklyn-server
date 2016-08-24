/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.brooklyn.entity.software.base.location;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class NetworkEffectorsEc2LiveTests extends NetworkingEffectorsLiveTests {
    public static final String PROVIDER = "aws-ec2";
    public static final String REGION_NAME = "us-east-1";
    public static final String LOCATION_SPEC = PROVIDER + (REGION_NAME == null ? "" : ":" + REGION_NAME);

    @Test(groups = "Live")
    public void testPassSecurityGroupParameters() {
        super.testPassSecurityGroupParameters();
    }

    @Override
    public String getLocationSpec() {
        return LOCATION_SPEC;
    }

    @Override
    public Map<String, Object> getLocationProperties() {
        return ImmutableMap.<String, Object>of("imageId", "us-east-1/ami-a96b01c0", "hardwareId", "t1.micro");
    }
}
