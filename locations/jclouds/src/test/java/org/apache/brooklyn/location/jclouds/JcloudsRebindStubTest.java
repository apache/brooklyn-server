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

import java.net.URI;
import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestFixtureWithApp;
import org.apache.brooklyn.core.test.entity.TestApplication;
import org.apache.brooklyn.util.exceptions.CompoundRuntimeException;
import org.jclouds.compute.domain.Image;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.OperatingSystem;
import org.jclouds.compute.domain.OsFamily;
import org.jclouds.compute.domain.Processor;
import org.jclouds.compute.domain.Template;
import org.jclouds.compute.domain.Volume;
import org.jclouds.compute.domain.internal.HardwareImpl;
import org.jclouds.compute.domain.internal.NodeMetadataImpl;
import org.jclouds.domain.LocationScope;
import org.jclouds.domain.LoginCredentials;
import org.jclouds.domain.internal.LocationImpl;
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
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

/**
 * Tests rebind (i.e. restarting Brooklyn server) when there are live JcloudsSshMachineLocation object(s).
 * 
 * It is still a live test because it connects to the Softlayer API for finding images, etc.
 * But it does not provision any VMs, so is much faster/cheaper.
 */
public class JcloudsRebindStubTest extends RebindTestFixtureWithApp {

    // TODO Duplication of AbstractJcloudsLiveTest, because we're subclassing RebindTestFixture instead.
    
    // TODO The ByonComputeServiceRegistry extends ComputeServiceRegistryImpl, which means when it  
    // is serialized it will try to serialize the cachedComputeServices. That will try to serialize 
    // threads and all sorts!

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsRebindStubTest.class);

    public static final String PROVIDER = AbstractJcloudsLiveTest.SOFTLAYER_PROVIDER;
    public static final String LOCATION_SPEC = "jclouds:" + PROVIDER;
    public static final String IMAGE_ID = "UBUNTU_14_64";
    
    protected List<ManagementContext> mgmts;
    protected Multimap<ManagementContext, JcloudsSshMachineLocation> machines;
    protected BrooklynProperties brooklynProperties;
    
    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        super.setUp();
        mgmts = Lists.newCopyOnWriteArrayList(ImmutableList.<ManagementContext>of(origManagementContext));
        machines = Multimaps.synchronizedMultimap(ArrayListMultimap.<ManagementContext, JcloudsSshMachineLocation>create());
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        List<Exception> exceptions = Lists.newArrayList();
        for (ManagementContext mgmt : mgmts) {
            try {
                if (mgmt.isRunning()) Entities.destroyAll(mgmt);
            } catch (Exception e) {
                LOG.warn("Error destroying management context", e);
                exceptions.add(e);
            }
        }
        mgmts.clear();
        origManagementContext = null;
        newManagementContext = null;
        origApp = null;
        newApp = null;
        
        super.tearDown();
        
        if (exceptions.size() > 0) {
            throw new CompoundRuntimeException("Error in tearDown of "+getClass(), exceptions);
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
        return true;
    }

    @Override
    protected TestApplication rebind() throws Exception {
        TestApplication result = super.rebind();
        mgmts.add(newManagementContext);
        return result;
    }

    @Test(groups={"Live", "Live-sanity"})
    public void testRebind() throws Exception {
        LocationImpl locImpl = new LocationImpl(
                LocationScope.REGION, 
                "myLocId", 
                "myLocDescription", 
                null, 
                ImmutableList.<String>of(), // iso3166Codes 
                ImmutableMap.<String,Object>of()); // metadata
        
        NodeMetadata node = new NodeMetadataImpl(
                "softlayer", 
                "myname", 
                "123", // ids in SoftLayer are numeric
                locImpl,
                URI.create("http://myuri.com"), 
                ImmutableMap.<String, String>of(), // userMetadata 
                ImmutableSet.<String>of(), // tags
                "mygroup",
                new HardwareImpl(
                        "myHardwareProviderId", 
                        "myHardwareName", 
                        "myHardwareId", 
                        locImpl, 
                        URI.create("http://myuri.com"), 
                        ImmutableMap.<String, String>of(), // userMetadata 
                        ImmutableSet.<String>of(), // tags
                        ImmutableList.<Processor>of(), 
                        1024, 
                        ImmutableList.<Volume>of(), 
                        Predicates.<Image>alwaysTrue(), // supportsImage, 
                        (String)null, // hypervisor
                        false),
                IMAGE_ID,
                new OperatingSystem(
                        OsFamily.CENTOS, 
                        "myOsName", 
                        "myOsVersion", 
                        "myOsArch", 
                        "myDescription", 
                        true), // is64Bit
                Status.RUNNING,
                "myBackendStatus",
                22, // login-port
                ImmutableList.of("1.2.3.4"), // publicAddresses, 
                ImmutableList.of("10.2.3.4"), // privateAddresses, 
                LoginCredentials.builder().identity("myidentity").password("mypassword").build(), 
                "myHostname");
        
        StubbedComputeServiceRegistry computeServiceRegistry = new StubbedComputeServiceRegistry(node);

        JcloudsLocation origJcloudsLoc = newJcloudsLocation(computeServiceRegistry);
    
        JcloudsSshMachineLocation origMachine = (JcloudsSshMachineLocation) obtainMachine(origJcloudsLoc, ImmutableMap.of("imageId", IMAGE_ID));
        
        String origHostname = origMachine.getHostname();
        NodeMetadata origNode = origMachine.getNode();
        Template origTemplate = origMachine.getTemplate();

        rebind();
        
        // Check the machine is as before.
        // Call to getOptionalNode() will cause it to try to resolve this node in Softlayer; but it won't find it.
        JcloudsSshMachineLocation newMachine = (JcloudsSshMachineLocation) newManagementContext.getLocationManager().getLocation(origMachine.getId());
        JcloudsLocation newJcloudsLoc = newMachine.getParent();
        String newHostname = newMachine.getHostname();
        String newNodeId = newMachine.getJcloudsId();
        Optional<NodeMetadata> newNode = newMachine.getOptionalNode();
        Optional<Template> newTemplate = newMachine.getOptionalTemplate();
        
        assertEquals(newHostname, origHostname);
        assertEquals(origNode.getId(), newNodeId);
        assertFalse(newNode.isPresent(), "newNode="+newNode);
        assertFalse(newTemplate.isPresent(), "newTemplate="+newTemplate);
        
        assertEquals(newJcloudsLoc.getProvider(), origJcloudsLoc.getProvider());
    }
    
    protected JcloudsMachineLocation obtainMachine(JcloudsLocation jcloudsLoc, Map<?,?> props) throws Exception {
        return (JcloudsMachineLocation) jcloudsLoc.obtain(ImmutableMap.of("imageId", IMAGE_ID));
    }
    
    protected JcloudsLocation newJcloudsLocation(ComputeServiceRegistry computeServiceRegistry) throws Exception {
        return (JcloudsLocation) mgmt().getLocationRegistry().getLocationManaged("jclouds:softlayer", ImmutableMap.of(
                JcloudsLocation.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry, 
                JcloudsLocation.WAIT_FOR_SSHABLE, false,
                JcloudsLocation.USE_JCLOUDS_SSH_INIT, false));
    }
}
