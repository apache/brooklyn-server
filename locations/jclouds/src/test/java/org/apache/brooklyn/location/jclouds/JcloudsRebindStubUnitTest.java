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
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

import java.io.File;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.BasicNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.ResourceUtils;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.apache.commons.io.FileUtils;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.Template;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Tests rebind (i.e. restarting Brooklyn server) when there are JcloudsSshMachineLocation object(s).
 * 
 * It is just a unit test, because it uses the StubbedComputeServiceRegistry.
 */
public class JcloudsRebindStubUnitTest extends RebindTestFixtureWithApp {

    // TODO Duplication of JcloudsRebindStubTest, and AbstractJcloudsStubbedUnitTest
    
    // TODO The ByonComputeServiceRegistry extends ComputeServiceRegistryImpl, which means when it  
    // is serialized it will try to serialize the cachedComputeServices. That will try to serialize 
    // threads and all sorts!

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsRebindStubUnitTest.class);

    public static final String PROVIDER = AbstractJcloudsLiveTest.AWS_EC2_PROVIDER;
    public static final String REGION = "us-east-1";
    public static final String LOCATION_SPEC = "jclouds:" + PROVIDER + ":" + REGION;
    public static final String IMAGE_ID = REGION + "/bogus-image"; // matches name in JcloudsStubTemplateBuilder
    
    protected List<ManagementContext> mgmts;
    protected Multimap<ManagementContext, JcloudsSshMachineLocation> machines;
    protected BrooklynProperties brooklynProperties;

    // TODO These values are hard-coded into the JcloudsStubTemplateBuilder, so best not to mess!
    
    protected NodeCreator nodeCreator;
    protected ComputeServiceRegistry computeServiceRegistry;

    @Override
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        RecordingSshTool.clear();
        RecordingWinRmTool.clear();
        mgmts = Lists.newCopyOnWriteArrayList(ImmutableList.<ManagementContext>of(origManagementContext));
        machines = Multimaps.synchronizedMultimap(ArrayListMultimap.<ManagementContext, JcloudsSshMachineLocation>create());
    }

    @Override
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            List<Exception> exceptions = Lists.newArrayList();
            for (ManagementContext mgmt : mgmts) {
                try {
                    if (mgmt.isRunning()) Entities.destroyAll(mgmt);
                } catch (Exception e) {
                    LOG.warn("Error destroying management context", e);
                    exceptions.add(e);
                }
            }
            
            super.tearDown();
            
            if (exceptions.size() > 0) {
                throw new CompoundRuntimeException("Error in tearDown of "+getClass(), exceptions);
            }
        } finally {
            mgmts.clear();
            origManagementContext = null;
            newManagementContext = null;
            origApp = null;
            newApp = null;
            RecordingSshTool.clear();
            RecordingWinRmTool.clear();
        }
    }


    @Override
    protected BrooklynProperties createBrooklynProperties() {
        // Don't let any defaults from brooklyn.properties (except credentials) interfere with test
        BrooklynProperties result = super.createBrooklynProperties();
        AbstractJcloudsLiveTest.stripBrooklynProperties(result);
        return result;
    }
    
    @Override
    protected boolean useLiveManagementContext() {
        return false;
    }

    @Override
    protected TestApplication rebind() throws Exception {
        TestApplication result = super.rebind();
        mgmts.add(newManagementContext);
        return result;
    }

    @Test
    public void testRebind() throws Exception {
        this.nodeCreator = newNodeCreator();
        this.computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator, false);

        JcloudsLocation origJcloudsLoc = newJcloudsLocation(computeServiceRegistry);
    
        JcloudsSshMachineLocation origMachine = (JcloudsSshMachineLocation) obtainMachine(origJcloudsLoc, ImmutableMap.of("imageId", IMAGE_ID));
        
        String origHostname = origMachine.getHostname();
        NodeMetadata origNode = origMachine.getNode();

        rebind();
        
        // Check the machine is as before.
        // Call to getOptionalNode() will cause it to try to resolve this node in Softlayer; but it won't find it.
        JcloudsSshMachineLocation newMachine = (JcloudsSshMachineLocation) mgmt().getLocationManager().getLocation(origMachine.getId());
        JcloudsLocation newJcloudsLoc = newMachine.getParent();
        String newHostname = newMachine.getHostname();
        String newNodeId = newMachine.getJcloudsId();
        Optional<NodeMetadata> newNode = newMachine.getOptionalNode();
        Optional<Template> newTemplate = newMachine.getOptionalTemplate();
        
        assertEquals(newHostname, origHostname);
        assertEquals(origNode.getId(), newNodeId);
        assertTrue(newNode.isPresent(), "newNode="+newNode);
        assertEquals(newNode.get(), origNode);
        assertFalse(newTemplate.isPresent(), "newTemplate="+newTemplate);
        
        assertEquals(newJcloudsLoc.getProvider(), origJcloudsLoc.getProvider());

        // Release the machine
        newJcloudsLoc.release(newMachine);
    }
    
    // See https://issues.apache.org/jira/browse/BROOKLYN-554, and fix in JcloudsLocation.rebind()
    @Test
    public void testHistoricLocationWithoutSemaphoresStops() throws Exception {
        ResourceUtils resourceUtils = ResourceUtils.create(this);
        FileUtils.write(
                new File(mementoDir, "locations/afy79330h5"),
                resourceUtils.getResourceAsString("classpath://org/apache/brooklyn/location/jclouds/persisted-no-semaphores-stubbed-parent-afy79330h5"));
        FileUtils.write(
                new File(mementoDir, "locations/l27nwbyisk"),
                resourceUtils.getResourceAsString("classpath://org/apache/brooklyn/location/jclouds/persisted-no-semaphores-stubbed-machine-l27nwbyisk"));

        rebind();
        
        JcloudsLocation jcloudsLoc = (JcloudsLocation) mgmt().getLocationManager().getLocation("afy79330h5");
        JcloudsSshMachineLocation machine = (JcloudsSshMachineLocation) mgmt().getLocationManager().getLocation("l27nwbyisk");
        
        jcloudsLoc.release(machine);
    }
    
    protected JcloudsMachineLocation obtainMachine(JcloudsLocation jcloudsLoc, Map<?,?> props) throws Exception {
        return (JcloudsMachineLocation) jcloudsLoc.obtain(ImmutableMap.of("imageId", IMAGE_ID));
    }

    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry) throws Exception {
        return newJcloudsLocation(computeServiceRegistry, ImmutableMap.of());
    }
    
    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry, Map<?, ?> jcloudsLocationConfig) throws Exception {
        return (JcloudsLocation) mgmt().getLocationRegistry().getLocationManaged(
                getLocationSpec(),
                ImmutableMap.builder()
                        .put(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                        .put(JcloudsLocationConfig.TEMPLATE_BUILDER, JcloudsStubTemplateBuilder.create())
                        .put(JcloudsLocationConfig.ACCESS_IDENTITY, "stub-identity")
                        .put(JcloudsLocationConfig.ACCESS_CREDENTIAL, "stub-credential")
                        .put(SshMachineLocation.SSH_TOOL_CLASS, RecordingSshTool.class.getName())
                        .put(WinRmMachineLocation.WINRM_TOOL_CLASS, RecordingWinRmTool.class.getName())
                        .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, Predicates.alwaysTrue())
                        .put(JcloudsLocationConfig.LOOKUP_AWS_HOSTNAME, Boolean.FALSE)
                        .putAll(jcloudsLocationConfig)
                        .build());
    }
    
    /**
     * For overriding.
     */
    protected NodeCreator newNodeCreator() {
        return new BasicNodeCreator();
    }
    
    /**
     * For overriding.
     */
    protected String getLocationSpec() {
        return LOCATION_SPEC;
    }
}
