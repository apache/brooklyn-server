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

import org.apache.brooklyn.core.location.Locations;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.collect.ImmutableMap;

/**
 * The VM creation is stubbed out, but it still requires live access (i.e. real account credentials)
 * to generate the template etc.
 * 
 * We supply a ComputeServiceRegistry that delegates to the real instance for everything except
 * VM creation and deletion. For those operations, it delegates to a NodeCreator that 
 * returns a dummy NodeMetadata, recording all calls made to it.
 */
public abstract class AbstractJcloudsStubbedLiveTest extends AbstractJcloudsLiveTest {

    @SuppressWarnings("unused")
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJcloudsStubbedLiveTest.class);

    public static final String LOCATION_SPEC = "jclouds:" + SOFTLAYER_PROVIDER + ":" + SOFTLAYER_AMS01_REGION_NAME;
    
    protected NodeCreator nodeCreator;
    protected ComputeServiceRegistry computeServiceRegistry;
    
    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        nodeCreator = newNodeCreator();
        computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator);

        jcloudsLocation = replaceJcloudsLocation(getLocationSpec());
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            nodeCreator = null;
            computeServiceRegistry = null;
            jcloudsLocation = null;
        }
    }
    
    protected JcloudsLocation replaceJcloudsLocation(String locationSpec) {
        if (machines != null && machines.size() > 0) {
            throw new IllegalStateException("Cannot replace jcloudsLocation after provisioning machine with old one");
        }
        if (jcloudsLocation != null) {
            Locations.unmanage(jcloudsLocation);
        }
        jcloudsLocation = (JcloudsLocation) managementContext.getLocationRegistry().getLocationManaged(
                locationSpec,
                jcloudsLocationConfig(ImmutableMap.<Object, Object>of(
                        JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry,
                        JcloudsLocationConfig.WAIT_FOR_SSHABLE, "false",
                        JcloudsLocationConfig.POLL_FOR_FIRST_REACHABLE_ADDRESS, "false")));
        return jcloudsLocation;
    }
    
    /**
     * For overriding.
     */
    protected Map<Object, Object> jcloudsLocationConfig(Map<Object, Object> defaults) {
        return defaults;
    }

    // For overriding
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }
    
    protected abstract NodeCreator newNodeCreator();
    
    protected NodeCreator getNodeCreator() {
        return nodeCreator;
    }
}
