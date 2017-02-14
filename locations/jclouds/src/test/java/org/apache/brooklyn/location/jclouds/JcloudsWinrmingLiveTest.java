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

import java.util.Map;

import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.compute.domain.OsFamily;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Tests the initial WinRM command execution, for a VM provisioned through jclouds.
 */
public class JcloudsWinrmingLiveTest extends AbstractJcloudsLiveTest {

    // TODO GCE fails with the default user ("Administrator").
    //  - https://issues.apache.org/jira/browse/BROOKLYN-415 (worked around with `loginUser: myname`)
    
    // TODO Softlayer provisioning fails (therefore not included):
    //     new Object[] { SOFTLAYER_LOCATION_SPEC, null, ImmutableMap.of(JcloudsLocation.IMAGE_ID.getName(), SOFTLAYER_IMAGE_ID, JcloudsLocation.VM_NAME_MAX_LENGTH.getName(), 15) }
    //
    // - https://issues.apache.org/jira/browse/BROOKLYN-414 (worked around with `vmNameMaxLength: 15`)
    // - https://issues.apache.org/jira/browse/BROOKLYN-413
    // - https://issues.apache.org/jira/browse/BROOKLYN-411

    public static final String AWS_EC2_LOCATION_SPEC = "jclouds:" + AWS_EC2_PROVIDER + ":" + AWS_EC2_USEAST_REGION_NAME;
    public static final String AWS_EC2_IMAGE_NAME_REGEX = "Windows_Server-2012-R2_RTM-English-64Bit-Base-.*";

    public static final String GCE_LOCATION_SPEC = "jclouds:" + GCE_PROVIDER + ":" + GCE_USCENTRAL_REGION_NAME;
    public static final String GCE_IMAGE_NAME_REGEX = "windows-server-2012-r2-.*";

    public static final String SOFTLAYER_LOCATION_SPEC = "jclouds:" + SOFTLAYER_PROVIDER;
    public static final String SOFTLAYER_IMAGE_ID = "WIN_2012-STD-R2_64";

    @DataProvider(name = "cloudAndImageNames")
    public Object[][] cloudAndImageNames() {
        return new Object[][] {
                new Object[] { AWS_EC2_LOCATION_SPEC, AWS_EC2_IMAGE_NAME_REGEX, ImmutableMap.of() },
                new Object[] { GCE_LOCATION_SPEC, GCE_IMAGE_NAME_REGEX, ImmutableMap.of(JcloudsLocation.LOGIN_USER.getName(), "myname") },
            };
    }

    @Test(groups = "Live", dataProvider="cloudAndImageNames")
    public void testCreatesWindowsVm(String locationSpec, String imageNameRegex, Map<String, ?> additionalConfig) throws Exception {
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(locationSpec);

        JcloudsWinRmMachineLocation machine = obtainWinrmMachine(MutableMap.<String,Object>builder()
                .putIfAbsent("inboundPorts", ImmutableList.of(5986, 5985, 3389))
                .put(JcloudsLocation.IMAGE_NAME_REGEX.getName(), imageNameRegex)
                .put(JcloudsLocation.USE_JCLOUDS_SSH_INIT.getName(), false)
                .put(JcloudsLocation.OS_FAMILY_OVERRIDE.getName(), OsFamily.WINDOWS)
                .putAll(additionalConfig != null ? additionalConfig : ImmutableMap.<String, Object>of())
                .build());
        assertWinrmable(machine);
    }
    
}
