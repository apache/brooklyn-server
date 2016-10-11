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
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.test.entity.LocalManagementContextForTests;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Stubs out all comms with the cloud provider.
 * 
 * Expects sub-classes to call {@link #initNodeCreatorAndJcloudsLocation(NodeCreator, Map)} before
 * the test methods are called.
 */
public abstract class AbstractJcloudsStubbedUnitTest extends AbstractJcloudsLiveTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJcloudsStubbedUnitTest.class);

    // TODO These values are hard-coded into the JcloudsStubTemplateBuilder, so best not to mess!
    public static final String LOCATION_SPEC = "jclouds:aws-ec2:us-east-1";
    
    protected NodeCreator nodeCreator;
    protected ComputeServiceRegistry computeServiceRegistry;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
        RecordingWinRmTool.clear();
    }
    
    @AfterMethod(alwaysRun=true)
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
        return LocalManagementContextForTests.builder(true).build();
    }
    
    /**
     * Expect sub-classes to call this - either in their {@link BeforeMethod} or at the very 
     * start of the test method (to allow custom config per test).
     */
    protected void initNodeCreatorAndJcloudsLocation(NodeCreator nodeCreator, Map<?, ?> jcloudsLocationConfig) throws Exception {
        this.nodeCreator = nodeCreator;
        this.computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator, false);

        this.jcloudsLocation = (JcloudsLocation)managementContext.getLocationRegistry().getLocationManaged(
                getLocationSpec(),
                ImmutableMap.builder()
                        .put(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                        .put(JcloudsLocationConfig.TEMPLATE_BUILDER, JcloudsStubTemplateBuilder.create())
                        .put(JcloudsLocationConfig.ACCESS_IDENTITY, "stub-identity")
                        .put(JcloudsLocationConfig.ACCESS_CREDENTIAL, "stub-credential")
                        .put(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                        .put(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName())
                        .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, Predicates.alwaysTrue())
                        .putAll(jcloudsLocationConfig)
                        .build());
    }

    /**
     * For overriding.
     */
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }
    
    protected NodeCreator newVanillaNodeCreator() {
        return new AbstractNodeCreator() {
            private final AtomicInteger counter = new AtomicInteger(1);
            @Override
            protected NodeMetadata newNode(String group, Template template) {
                int suffix = counter.getAndIncrement();
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("mynodeid"+suffix)
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.of("173.194.32."+suffix))
                        .privateAddresses(ImmutableList.of("172.168.10."+suffix))
                        .build();
                return result;
            }
        };
    }
}
