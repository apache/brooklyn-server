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
package org.apache.brooklyn.entity;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Runs a test on Openstack with many different distros and versions.
 * Relies on the something like the following being in brooklyn.properties:

brooklyn.location.jclouds.openstack-nova.endpoint = https://your.endpoint.here:5000/v2.0
brooklyn.location.jclouds.openstack-nova.identity = you:you
brooklyn.location.jclouds.openstack-nova.credential = yourPa55w0rd
brooklyn.location.jclouds.openstack-nova.jclouds.keystone.credential-type = passwordCredentials
brooklyn.location.jclouds.openstack-nova.jclouds.openstack-nova.auto-create-floating-ips = false
brooklyn.location.jclouds.openstack-nova.jclouds.openstack-nova.auto-generate-keypairs = false
brooklyn.location.jclouds.openstack-nova.loginUser = centos
brooklyn.location.jclouds.openstack-nova.loginUser.privateKeyFile = ~/.ssh/openstack.pem
brooklyn.location.jclouds.openstack-nova.generate.hostname = true
brooklyn.location.jclouds.openstack-nova.securityGroups = VPN_local
brooklyn.location.jclouds.openstack-nova.templateOptions={"networks":["abcdef12-1234-abcd-5678-00000000000"], "keyPairName": "openstack"}

 */
public abstract class AbstractOpenstackLiveTest extends AbstractMultiDistroLiveTest {
    
    private static final Logger LOG = LoggerFactory.getLogger(AbstractOpenstackLiveTest.class);

    @Override
    public String getProvider() {
        return PROVIDER;
    }

    @Override
    public String getLocationSpec() {
        return LOCATION_SPEC;
    }

    public static final String PROVIDER = "openstack-nova";
    public static final String REGION_NAME = "RegionOne";
    public static final String LOCATION_SPEC = PROVIDER + (REGION_NAME == null ? "" : ":" + REGION_NAME);

    @Test(groups = {"Live"})
    public void test_Centos_6() throws Exception {
        // There are two images named "CentOS 6"; we need the newest so using the explicit imageId
        runTest(ImmutableMap.of(
            "imageId", "RegionOne/55e1fcb5-5a74-461c-b4fc-5b14c575b188",
            "loginUser", "centos",
            "minRam", "2000"));
    }

    @Test(groups = {"Live"})
    public void test_Centos_7() throws Exception {
        // release codename "squeeze"
        runTest(ImmutableMap.of(
            "imageNameRegex", "CentOS 7",
            "loginUser", "centos",
            "minRam", "2000"));
    }

}
