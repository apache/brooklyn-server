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
package org.apache.brooklyn.entity.software.base;

import com.google.common.base.CaseFormat;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.test.BrooklynAppLiveTestSupport;
import org.apache.brooklyn.util.collections.MutableMap;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

/**
 * Runs a test with many different distros and versions.
 */
public abstract class AbstractDockerLiveTest extends BrooklynAppLiveTestSupport {
    
    public static final String PROVIDER = "docker";

    protected Location jcloudsLocation;
    
    @Override
    protected BrooklynProperties getBrooklynProperties() {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        List<String> propsToRemove = ImmutableList.of("imageDescriptionRegex", "imageNameRegex", "inboundPorts",
                "hardwareId", "minRam");

        BrooklynProperties result = BrooklynProperties.Factory.newDefault();
        for (String propToRemove : propsToRemove) {
            for (String propVariant : ImmutableList.of(propToRemove, CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_HYPHEN, propToRemove))) {
                result.remove("brooklyn.locations.jclouds."+PROVIDER+"."+propVariant);
                result.remove("brooklyn.locations."+propVariant);
                result.remove("brooklyn.jclouds."+PROVIDER+"."+propVariant);
                result.remove("brooklyn.jclouds."+propVariant);
            }
        }

        // Also removes scriptHeader (e.g. if doing `. ~/.bashrc` and `. ~/.profile`, then that can cause "stdin: is not a tty")
        result.remove("brooklyn.ssh.config.scriptHeader");

        return result;
    }
    
    @Test(groups={"Live", "WIP"})
    public void test_Ubuntu_13_10() throws Exception {
          runTest(ImmutableMap.of("imageId", "7fe2ec2ff748c411cf0d6833120741778c00e1b07a83c4104296b6258b5331c4",
              "loginUser", "root",
              "loginUser.password", "password"));
     }

    protected void runTest(Map<String,?> flags) throws Exception {
        String tag = getClass().getSimpleName().toLowerCase();
        Map<String,?> allFlags = MutableMap.<String,Object>builder()
                .put("tags", ImmutableList.of(tag))
                .putAll(flags)
                .build();
        jcloudsLocation = mgmt.getLocationRegistry().getLocationManaged(PROVIDER, allFlags);
        doTest(jcloudsLocation);
    }

    protected abstract void doTest(Location loc) throws Exception;
}
