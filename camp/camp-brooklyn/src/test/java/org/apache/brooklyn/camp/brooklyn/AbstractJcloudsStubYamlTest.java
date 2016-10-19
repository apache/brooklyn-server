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
package org.apache.brooklyn.camp.brooklyn;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Reader;
import java.util.Set;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.api.mgmt.Task;
import org.apache.brooklyn.camp.brooklyn.AbstractJcloudsStubYamlTest.ByonComputeServiceStaticRef;
import org.apache.brooklyn.camp.spi.Assembly;
import org.apache.brooklyn.camp.spi.AssemblyTemplate;
import org.apache.brooklyn.core.mgmt.BrooklynTaskTags;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.location.jclouds.AbstractJcloudsStubbedUnitTest;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsStubTemplateBuilder;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.StubbedComputeServiceRegistry.NodeCreator;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.config.ConfigBag;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * This is a combination of the jclouds tests and the yaml tests. As such, it extends
 * {@link AbstractJcloudsStubbedUnitTest} and duplicates some of the utility methods in
 * {@link AbstractYamlTest}.
 */
public abstract class AbstractJcloudsStubYamlTest extends AbstractJcloudsStubbedUnitTest {

    // TODO Some duplication compared to AbstractJcloudsRebindStubYamlTest
    
    private static final Logger LOG = LoggerFactory.getLogger(AbstractJcloudsStubYamlTest.class);

    public static final String LOCATION_CATALOG_ID = "stubbed-aws-ec2";
    
    protected BrooklynCampPlatformLauncherNoServer launcher;
    protected BrooklynCampPlatform platform;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        
        NodeCreator nodeCreator = newNodeCreator();
        StubbedComputeServiceRegistry computeServiceRegistry = new StubbedComputeServiceRegistry(nodeCreator, false);
        ByonComputeServiceStaticRef.setInstance(computeServiceRegistry);
        
        addStubbedLocationToCatalog();
    }
    
    @AfterMethod(alwaysRun=true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            ByonComputeServiceStaticRef.clearInstance();
            if (launcher != null) launcher.stopServers();
        }
    }
    
    @Override
    protected LocalManagementContext newManagementContext() {
        launcher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return AbstractJcloudsStubYamlTest.super.newManagementContext();
            }
        };
        launcher.launch();
        platform = launcher.getCampPlatform();
        LocalManagementContext mgmt = (LocalManagementContext) launcher.getBrooklynMgmt();
        return mgmt;
    }

    protected void addStubbedLocationToCatalog() {
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + LOCATION_CATALOG_ID,
                "  version: 1.0.0",
                "  itemType: location",
                "  item:",
                "    type: " + LOCATION_SPEC,
                "    brooklyn.config:",
                "      identity: myidentity",
                "      credential: mycredential",
                "      jclouds.computeServiceRegistry:",
                "        $brooklyn:object:",
                "          type: " + ByonComputeServiceStaticRef.class.getName(),
                "      " + SshMachineLocation.SSH_TOOL_CLASS.getName() + ": " + RecordingSshTool.class.getName(),
                "      templateBuilder:",
                "        $brooklyn:object:",
                "          type: "+JcloudsStubTemplateBuilder.class.getName(),
                "          factoryMethod.name: create",
                "      " + JcloudsLocation.POLL_FOR_FIRST_REACHABLE_ADDRESS_PREDICATE.getName() + ":",
                "        $brooklyn:object:",
                "          type: "+Predicates.class.getName(),
                "          factoryMethod.name: alwaysTrue",
                "      " + SshMachineLocation.SSH_TOOL_CLASS.getName() + ": " + RecordingSshTool.class.getName(),
                "      " + WinRmMachineLocation.WINRM_TOOL_CLASS.getName() + ": " + RecordingWinRmTool.class.getName());
    }

    protected void addCatalogItems(String... catalogYaml) {
        addCatalogItems(Joiner.on("\n").join(catalogYaml));
    }
    
    protected void addCatalogItems(String catalogYaml) {
        mgmt().getCatalog().addItems(catalogYaml, false);
    }
    
    protected Entity createAndStartApplication(Reader input) throws Exception {
        AssemblyTemplate at = platform.pdp().registerDeploymentPlan(input);
        Assembly assembly;
        try {
            assembly = at.getInstantiator().newInstance().instantiate(at, platform);
        } catch (Exception e) {
            LOG.warn("Unable to instantiate " + at + " (rethrowing): " + e);
            throw e;
        }
        LOG.info("Test - created " + assembly);
        final Entity app = mgmt().getEntityManager().getEntity(assembly.getId());
        LOG.info("App - " + app);
        
        // wait for app to have started
        Set<Task<?>> tasks = mgmt().getExecutionManager().getTasksWithAllTags(ImmutableList.of(
                BrooklynTaskTags.EFFECTOR_TAG, 
                BrooklynTaskTags.tagForContextEntity(app), 
                BrooklynTaskTags.tagForEffectorCall(app, "start", ConfigBag.newInstance(ImmutableMap.of("locations", ImmutableMap.of())))));
        Iterables.getOnlyElement(tasks).get();
        
        return app;
    }

    public static class ByonComputeServiceStaticRef {
        private static volatile ComputeServiceRegistry instance;

        public ComputeServiceRegistry asComputeServiceRegistry() {
            return checkNotNull(instance, "instance");
        }
        static void setInstance(ComputeServiceRegistry val) {
            instance = val;
        }
        static void clearInstance() {
            instance = null;
        }
    }
}
