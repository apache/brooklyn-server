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

import org.apache.brooklyn.util.collections.MutableMap;
import org.jclouds.compute.domain.OsFamily;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;

/**
 * Tests the initial WinRM command execution, for a VM provisioned through jclouds.
 */
public class JcloudsWinrmingLiveTest extends AbstractJcloudsLiveTest {

    public static final String AWS_EC2_LOCATION_SPEC = "jclouds:" + AWS_EC2_PROVIDER + ":" + AWS_EC2_EUWEST_REGION_NAME;
    public static final String AWS_EC2_IMAGE_NAME_REGEX = "Windows_Server-2012-R2_RTM-English-64Bit-Base-.*";

    @Test(groups = {"Live"})
    public void testCreatesWindowsVm() throws Exception {
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(AWS_EC2_LOCATION_SPEC);
        
        JcloudsWinRmMachineLocation machine = obtainWinrmMachine(MutableMap.<String,Object>builder()
                .putIfAbsent("inboundPorts", ImmutableList.of(5986, 5985, 3389))
                .put(JcloudsLocation.IMAGE_NAME_REGEX.getName(), AWS_EC2_IMAGE_NAME_REGEX)
                .put(JcloudsLocation.USE_JCLOUDS_SSH_INIT.getName(), false)
                .put(JcloudsLocation.OS_FAMILY_OVERRIDE.getName(), OsFamily.WINDOWS)
                .build());
        assertWinrmable(machine);
    }
}
