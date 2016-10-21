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

import java.io.File;

import org.apache.brooklyn.api.mgmt.ha.HighAvailabilityMode;
import org.apache.brooklyn.camp.brooklyn.AbstractJcloudsStubYamlTest.ByonComputeServiceStaticRef;
import org.apache.brooklyn.core.mgmt.internal.LocalManagementContext;
import org.apache.brooklyn.location.jclouds.ComputeServiceRegistry;
import org.apache.brooklyn.location.jclouds.JcloudsLocation;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubTest;
import org.apache.brooklyn.location.jclouds.JcloudsRebindStubUnitTest;
import org.apache.brooklyn.location.jclouds.JcloudsStubTemplateBuilder;
import org.apache.brooklyn.location.ssh.SshMachineLocation;
import org.apache.brooklyn.location.winrm.WinRmMachineLocation;
import org.apache.brooklyn.util.core.internal.ssh.RecordingSshTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;

import com.google.common.base.Joiner;
import com.google.common.base.Predicates;

/**
 * Implementation notes. This relies on the test {@link JcloudsRebindStubTest#testRebind()}.
 * It changes the setup for the test in the following ways:
 * <ul>
 *   <li>Location is defined in YAML (added to the catalog, to make it easier for sub-classes to use).
 *   <li>When creating management context, it also creates {@link BrooklynCampPlatformLauncherNoServer}.
 *   <li>It uses {@link JcloudsRebindStubYamlTest#ByonComputeServiceStaticRef} to allow
 *       the test's {@link ComputeServiceRegistry} to be wired up via YAML. This is important so that
 *       we can serialize/deserialize in the rebind test.
 * </ul>
 */
public abstract class AbstractJcloudsRebindStubYamlTest extends JcloudsRebindStubUnitTest {

    // TODO Some duplication compared to AbstractJcloudsStubYamlTest

    public static final String LOCATION_CATALOG_ID = "stubbed-aws-ec2";

    protected BrooklynCampPlatformLauncherNoServer origLauncher;
    protected BrooklynCampPlatformLauncherNoServer newLauncher;

    @BeforeMethod(alwaysRun=true)
    @Override
    public void setUp() throws Exception {
        super.setUp();
        addStubbedLocationToCatalog();
    }
    
    @Override
    @AfterMethod(alwaysRun=true)
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } finally {
            ByonComputeServiceStaticRef.clearInstance();
            if (origLauncher != null) origLauncher.stopServers();
            if (newLauncher != null) newLauncher.stopServers();
        }
    }
    
    @Override
    protected LocalManagementContext createOrigManagementContext() {
        origLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return AbstractJcloudsRebindStubYamlTest.super.createOrigManagementContext();
            }
        };
        origLauncher.launch();
        LocalManagementContext mgmt = (LocalManagementContext) origLauncher.getBrooklynMgmt();
        return mgmt;
    }

    @Override
    protected LocalManagementContext createNewManagementContext(final File mementoDir, final HighAvailabilityMode haMode) {
        newLauncher = new BrooklynCampPlatformLauncherNoServer() {
            @Override
            protected LocalManagementContext newMgmtContext() {
                return AbstractJcloudsRebindStubYamlTest.super.createNewManagementContext(mementoDir, haMode);
            }
        };
        newLauncher.launch();
        return (LocalManagementContext) newLauncher.getBrooklynMgmt();
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
}
