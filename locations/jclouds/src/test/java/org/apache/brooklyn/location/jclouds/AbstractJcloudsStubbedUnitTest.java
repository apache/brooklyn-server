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

import com.google.common.collect.ImmutableMap;
import org.apache.brooklyn.api.location.Location;
import org.apache.brooklyn.api.location.LocationSpec;
import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.BasicNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import java.util.Map;

/**
 * Stubs out all comms with the cloud provider.
 * <p>
 * Expects sub-classes to call {@link #initStubbedJcloudsLocation(Map)} before
 * the test methods are called.
 */
public abstract class AbstractJcloudsStubbedUnitTest extends AbstractJcloudsLiveTest {

    private static final Logger LOG = LoggerFactory.getLogger(AbstractJcloudsStubbedUnitTest.class);

    // TODO These values are hard-coded into the JcloudsStubTemplateBuilder, so best not to mess!
    public static final String LOCATION_SPEC = "jclouds:stub";
    public static final String PUBLIC_IP_ADDRESS = "144.175.1.1";
    public static final String PRIVATE_IP_ADDRESS = "10.1.1.1";

    protected NodeCreator nodeCreator;
    protected ComputeServiceRegistry computeServiceRegistry;

    @BeforeMethod(alwaysRun = true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
        RecordingWinRmTool.clear();
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            RecordingSshTool.clear();
            RecordingWinRmTool.clear();
        }
    }

    @Override
    protected LocalManagementContext newManagementContext() {
        return LocalManagementContextForTests.builder(true).useAdditionalProperties(customBrooklynProperties()).build();
    }

    /**
     * For overriding.
     */
    protected Map<String, ?> customBrooklynProperties() {
        return ImmutableMap.of();
    }

    /**
     * Expect sub-classes to call this - either in their {@link BeforeMethod} or at the very
     * start of the test method (to allow custom config per test).
     */
    protected JcloudsLocation initStubbedJcloudsLocation(Map<?, ?> jcloudsLocationConfig) throws Exception {
        final Map<ConfigKey<?>, Object> defaults = ImmutableMap.<ConfigKey<?>, Object>builder()
                .put(JcloudsLocationConfig.ACCESS_IDENTITY, "stub-identity")
                .put(JcloudsLocationConfig.ACCESS_CREDENTIAL, "stub-credential")
                .put(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .put(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName())
                .put(JcloudsLocation.WAIT_FOR_SSHABLE, Boolean.FALSE)
                .put(JcloudsLocationConfig.USE_JCLOUDS_SSH_INIT, Boolean.FALSE)
                .put(JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS, Boolean.FALSE)
                .put(JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME, Boolean.FALSE)
                .build();
        final ImmutableMap.Builder<Object, Object> flags = ImmutableMap.builder()
                .putAll(jcloudsLocationConfig);
        for (Map.Entry<ConfigKey<?>, Object> entry : defaults.entrySet()) {
            ConfigKey<?> key = entry.getKey();
            if (!jcloudsLocationConfig.containsKey(key) && !jcloudsLocationConfig.containsKey(key.getName())) {
                flags.put(key, entry.getValue());
            } else {
                Object overrideVal = jcloudsLocationConfig.get(key);
                if (overrideVal == null) overrideVal = jcloudsLocationConfig.get(key.getName());
                LOG.debug("Overridden default value for {} with: {}", new Object[]{key, overrideVal});
            }
        }
        return (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(getLocationSpec(), flags.build());
    }

    /**
     * For overriding.
     */
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }

    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator();
    }

    protected String getProvider() {
        LocationSpec<?> spec = mgmt().getLocationRegistry().getLocationSpec(getLocationSpec()).get();
        return getRequiredConfig(spec, JcloudsLocation.CLOUD_PROVIDER);
    }

    protected String getRegion() {
        LocationSpec<? extends Location> spec = mgmt().getLocationRegistry().getLocationSpec(getLocationSpec()).get();
        return getRequiredConfig(spec, JcloudsLocation.CLOUD_REGION_ID);
    }

    protected String getRequiredConfig(LocationSpec<?> spec, ConfigKey<String> key) {
        String result = (String) spec.getConfig().get(key);
        if (result != null) {
            return result;
        }
        result = (String) spec.getFlags().get(key.getName());
        return result;
    }
}
