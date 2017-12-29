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

import static org.testng.Assert.assertEquals;

import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.jclouds.compute.ComputeService;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.options.TemplateOptions;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.googlecomputeengine.compute.options.GoogleComputeEngineTemplateOptions;
import org.jclouds.googlecomputeengine.domain.Instance.ServiceAccount;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class JcloudsTemplateOptionsStubbedTest extends AbstractJcloudsStubbedUnitTest {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(JcloudsImageChoiceStubbedLiveTest.class);
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        jcloudsLocation = initStubbedJcloudsLocation(ImmutableMap.of());
    }
    
    /**
     * For overriding.
     */
    protected String getLocationSpec() {
        return "jclouds:google-compute-engine:us-central1-a";
    }
    

    @Override
    protected NodeCreator newNodeCreator() {
        return new AbstractNodeCreator() {
            @Override protected NodeMetadata newNode(String group, Template template) {
                NodeMetadata result = new NodeMetadataBuilder()
                        .id("myid")
                        .credentials(LoginCredentials.builder().identity("myuser").credential("mypassword").build())
                        .loginPort(22)
                        .status(Status.RUNNING)
                        .publicAddresses(ImmutableList.of("173.194.32.123"))
                        .privateAddresses(ImmutableList.of("172.168.10.11"))
                        .build();
                return result;
            }
        };
    }

    @Test
    public void testTemplateOption() throws Exception {
        ServiceAccount serviceAccount = ServiceAccount.create("myemail", ImmutableList.of("myscope1"));
        
        RecordingLocationCustomizer customizer = new RecordingLocationCustomizer();
        obtainMachine(ImmutableMap.builder()
                .put(JcloudsLocation.JCLOUDS_LOCATION_CUSTOMIZERS, ImmutableList.of(customizer))
                .put(JcloudsLocation.TEMPLATE_OPTIONS, ImmutableMap.of(
                        "serviceAccounts", ImmutableList.of(serviceAccount)))
                .build());
        
        GoogleComputeEngineTemplateOptions options = (GoogleComputeEngineTemplateOptions) customizer.templateOptions;
        assertEquals(options.serviceAccounts(), ImmutableList.of(serviceAccount));
    }

    private static class RecordingLocationCustomizer extends BasicJcloudsLocationCustomizer {
        TemplateOptions templateOptions;
        
        @Override
        public void customize(JcloudsLocation location, ComputeService computeService, TemplateOptions templateOptions) {
            this.templateOptions = templateOptions;
        }
    }
}
