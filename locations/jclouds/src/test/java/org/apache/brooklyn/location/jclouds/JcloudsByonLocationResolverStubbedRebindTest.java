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
import static org.testng.Assert.assertNotNull;

import java.io.File;
import java.util.Map;
import java.util.Set;

import org.apache.brooklyn.api.entity.Application;
import org.apache.brooklyn.api.entity.EntitySpec;
import org.apache.brooklyn.api.location.MachineLocation;
import org.apache.brooklyn.api.mgmt.ManagementContext;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.core.internal.BrooklynProperties;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.core.mgmt.persist.FileBasedObjectStore;
import org.apache.brooklyn.core.mgmt.rebind.RebindOptions;
import org.apache.brooklyn.core.mgmt.rebind.RebindTestUtils;
import org.apache.brooklyn.entity.stock.BasicApplication;
import org.apache.brooklyn.location.byon.FixedListMachineProvisioningLocation;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.AbstractNodeCreator;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.util.os.Os;
import org.apache.brooklyn.util.text.Identifiers;
import org.apache.brooklyn.util.time.Duration;
import org.jclouds.compute.domain.NodeMetadata;
import org.jclouds.compute.domain.NodeMetadata.Status;
import org.jclouds.compute.domain.NodeMetadataBuilder;
import org.jclouds.compute.domain.Template;
import org.jclouds.domain.LoginCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;

public class JcloudsByonLocationResolverStubbedRebindTest extends AbstractJcloudsStubbedUnitTest {

    private static final Logger LOG = LoggerFactory.getLogger(JcloudsByonLocationResolverStubbedRebindTest.class);
    
    // Using static so that the static NodeCreator class can access it.
    // We are using the static NodeCreator to ensure it is serializable, without it trying to serialize JcloudsByonLocationResolverStubbedRebindLiveTest.
    private static final String nodeId = "mynodeid";
    private static final String nodePublicAddress = "173.194.32.123";
    private static final String nodePrivateAddress = "172.168.10.11";

    protected static final Duration TIMEOUT_MS = Duration.TEN_SECONDS;

    protected ClassLoader classLoader = getClass().getClassLoader();
    protected LocalManagementContext origManagementContext;
    protected File mementoDir;
    protected File mementoDirBackup;
    
    protected Application origApp;
    protected Application newApp;
    protected ManagementContext newManagementContext;

    @BeforeMethod(alwaysRun=true)
    public void setUp() throws Exception {
        mementoDir = Os.newTempDir(getClass());
        File mementoDirParent = mementoDir.getParentFile();
        mementoDirBackup = new File(mementoDirParent, mementoDir.getName()+"."+Identifiers.makeRandomId(4)+".bak");

        origManagementContext = createOrigManagementContext();
        origApp = origManagementContext.getEntityManager().createEntity(EntitySpec.create(BasicApplication.class));
        LOG.info("Test "+getClass()+" persisting to "+mementoDir);
        
        super.setUp();
        
        initNodeCreatorAndJcloudsLocation(newNodeCreator(), ImmutableMap.of());
    }

    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        super.tearDown();
        if (origApp != null) Entities.destroyAll(origApp.getManagementContext());
        if (newApp != null) Entities.destroyAll(newApp.getManagementContext());
        if (newManagementContext != null) Entities.destroyAll(newManagementContext);
        origApp = null;
        newApp = null;
        newManagementContext = null;

        if (origManagementContext != null) Entities.destroyAll(origManagementContext);
        if (mementoDir != null) FileBasedObjectStore.deleteCompletely(mementoDir);
        if (mementoDirBackup != null) FileBasedObjectStore.deleteCompletely(mementoDir);
        origManagementContext = null;
    }

    protected NodeCreator newNodeCreator() {
        return new NodeCreatorForRebinding();
    }
    public static class NodeCreatorForRebinding extends AbstractNodeCreator {
        @Override
        public Set<? extends NodeMetadata> listNodesDetailsMatching(Predicate<? super NodeMetadata> filter) {
            NodeMetadata result = new NodeMetadataBuilder()
                    .id(nodeId)
                    .credentials(LoginCredentials.builder().identity("dummy").credential("dummy").build())
                    .loginPort(22)
                    .status(Status.RUNNING)
                    .publicAddresses(ImmutableList.of(nodePublicAddress))
                    .privateAddresses(ImmutableList.of(nodePrivateAddress))
                    .build();
            return ImmutableSet.copyOf(Iterables.filter(ImmutableList.of(result), filter));
        }
        @Override
        protected NodeMetadata newNode(String group, Template template) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    public void testRebind() throws Exception {
        String spec = "jcloudsByon:(provider=\""+SOFTLAYER_PROVIDER+"\",region=\""+SOFTLAYER_AMS01_REGION_NAME+"\",user=\"myuser\",password=\"mypassword\",hosts=\""+nodeId+"\")";
        Map<?,?> specFlags = ImmutableMap.builder()
                .put(JcloudsLocationConfig.COMPUTE_SERVICE_REGISTRY, computeServiceRegistry)
                .put(JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE, Predicates.alwaysTrue())
                .build();

        FixedListMachineProvisioningLocation<MachineLocation> location = getLocationManaged(spec, specFlags);
        JcloudsSshMachineLocation machine = (JcloudsSshMachineLocation) Iterables.getOnlyElement(location.getAllMachines());
        
        rebind();

        FixedListMachineProvisioningLocation<?> newLocation = (FixedListMachineProvisioningLocation<?>) newManagementContext.getLocationManager().getLocation(location.getId());
        JcloudsSshMachineLocation newMachine = (JcloudsSshMachineLocation) newManagementContext.getLocationManager().getLocation(machine.getId());
        assertNotNull(newLocation);
        assertEquals(newMachine.getJcloudsId(), nodeId);
    }

    @SuppressWarnings("unchecked")
    private FixedListMachineProvisioningLocation<MachineLocation> getLocationManaged(String val, Map<?,?> specFlags) {
        return (FixedListMachineProvisioningLocation<MachineLocation>) managementContext.getLocationRegistry().getLocationManaged(val, specFlags);
    }


    /////////////////////////////////////////////////////////////////////////////////////////////
    // Everything below this point is copied from RebindTestFixture (or is a tweak required to // 
    // make it work like that). We need this because we are extending                          //
    // AbstractJcloudsStubbedLiveTest. In comparison, most rebind tests extend                 //
    // RebindTestFixtureWithApp.                                                               //
    /////////////////////////////////////////////////////////////////////////////////////////////

    // called by super.setUp
    @Override
    protected LocalManagementContext newManagementContext() {
        return origManagementContext;
    }

    protected Application rebind() throws Exception {
        return rebind(RebindOptions.create());
    }
    
    protected Application rebind(RebindOptions options) throws Exception {
        if (newApp != null || newManagementContext != null) {
            throw new IllegalStateException("already rebound - use switchOriginalToNewManagementContext() if you are trying to rebind multiple times");
        }
        
        options = RebindOptions.create(options);
        if (options.classLoader == null) options.classLoader(classLoader);
        if (options.mementoDir == null) options.mementoDir(mementoDir);
        if (options.origManagementContext == null) options.origManagementContext(origManagementContext);
        if (options.newManagementContext == null) options.newManagementContext(createNewManagementContext(options.mementoDir));
        
        RebindTestUtils.stopPersistence(origApp);
        
        newManagementContext = options.newManagementContext;
        newApp = RebindTestUtils.rebind(options);
        return newApp;
    }
    
    /** @return A started management context */
    protected LocalManagementContext createOrigManagementContext() {
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .persistPeriodMillis(getPersistPeriodMillis())
                .forLive(useLiveManagementContext())
                .emptyCatalog(useEmptyCatalog())
                .properties(createBrooklynProperties())
                .buildStarted();
    }

    /** As {@link #createNewManagementContext(File)} using the default memento dir */
    protected LocalManagementContext createNewManagementContext() {
        return createNewManagementContext(mementoDir);
    }
    
    /** @return An unstarted management context using the specified mementoDir (or default if null) */
    protected LocalManagementContext createNewManagementContext(File mementoDir) {
        if (mementoDir==null) mementoDir = this.mementoDir;
        return RebindTestUtils.managementContextBuilder(mementoDir, classLoader)
                .forLive(useLiveManagementContext())
                .emptyCatalog(useEmptyCatalog())
                .properties(createBrooklynProperties())
                .buildUnstarted();
    }
    
    private BrooklynProperties createBrooklynProperties() {
        // This really is stubbed; no live access to the cloud
        BrooklynProperties brooklynProperties = BrooklynProperties.Factory.newEmpty();
        brooklynProperties.put("brooklyn.location.jclouds."+SOFTLAYER_PROVIDER+".identity", "myidentity");
        brooklynProperties.put("brooklyn.location.jclouds."+SOFTLAYER_PROVIDER+".credential", "mycredential");
        return brooklynProperties;
    }

    protected boolean useLiveManagementContext() {
        return false;
    }

    protected boolean useEmptyCatalog() {
        return true;
    }

    protected int getPersistPeriodMillis() {
        return 1;
    }
}
