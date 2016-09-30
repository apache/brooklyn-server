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
package org.apache.brooklyn.location.jclouds.networking;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.brooklyn.config.ConfigKey;
import org.apache.brooklyn.location.jclouds.AbstractJcloudsStubbedLiveTest;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsLocationConfig;
import org.apache.brooklyn.location.jclouds.JcloudsSshMachineLocation;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.test.Asserts;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponse;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.CustomResponseGenerator;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool.ExecParams;
import org.apache.brooklyn.util.core.internal.ssh.SshTool;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.net.HostAndPort;

/**
 * The VM creation is stubbed out, but it still requires live access (i.e. real account credentials)
 * to generate the template etc.
 * 
 * Simulates the creation of a VM that has multiple IPs. Checks that we choose the right address.
 * 
 */
public class JcloudsReachableAddressStubbedLiveTest extends AbstractJcloudsStubbedLiveTest {

    // TODO Aim is to test the various situations/permutations, where we pass in different config.
    // More tests still need to be added.

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(JcloudsReachableAddressStubbedLiveTest.class);

    @Override
    protected AbstractNodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            int nextIpSuffix = 2;
            @Override
            protected NodeMetadata newNode(String group, Template template) {
                int ipSuffix = nextIpSuffix++;
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myid-"+ipSuffix)
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.<String>of())
                        .privateAddresses(ImmutableList.of("1.1.1.1", "1.1.1.2", "1.1.1.2"))
                        .build();
                return result;
            }
        };
    }

    protected AbstractNodeCreator getNodeCreator() {
        return (AbstractNodeCreator) nodeCreator;
    }

    // With waitForSshable=true; pollForFirstReachableAddress=true; and custom reachable-predicate
    @Test(groups = {"Live", "Live-sanity"})
    protected void testMachineUsesChosenAddress() throws Exception {
        final String desiredIp = "1.1.1.2"; 
        final AtomicBoolean chooserCalled = new AtomicBoolean(false);
        
        Predicate<HostAndPort> addressChooser = new Predicate<HostAndPort>() {
            @Override public boolean apply(HostAndPort input) {
                chooserCalled.set(true);
                return desiredIp.equals(input.getHostText());
            }
        };
        
        RecordingSshTool.setCustomResponse(".*", new CustomResponseGenerator() {
            @Override public CustomResponse generate(ExecParams execParams) throws Exception {
                System.out.println("ssh call: "+execParams);
                Object host = execParams.props.get(SshTool.PROP_HOST);
                if (desiredIp.equals(host)) {
                    return new CustomResponse(0, "", "");
                } else {
                    throw new IOException("Simulate VM not reachable for host '"+host+"'");
                }
            }});
        
        JcloudsSshMachineLocation machine = obtainMachine(ImmutableMap.<ConfigKey<?>,Object>builder()
                .put(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                .put(JcloudsLocationConfig.WAIT_FOR_SSHABLE, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS, Asserts.DEFAULT_LONG_TIMEOUT.toString())
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, addressChooser)
                .build());
        
        assertTrue(chooserCalled.get());
        assertEquals(machine.getAddress().getHostAddress(), desiredIp);
    }
}
