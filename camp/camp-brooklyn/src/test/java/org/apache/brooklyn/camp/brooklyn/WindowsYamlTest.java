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

import static org.testng.Assert.assertEquals;

import java.util.List;
import java.util.Map;

import org.apache.brooklyn.api.entity.Entity;
import org.apache.brooklyn.core.entity.Entities;
import org.apache.brooklyn.entity.software.base.VanillaWindowsProcess;
import org.apache.brooklyn.util.core.internal.winrm.ExecCmdAsserts;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool;
import org.apache.brooklyn.util.core.internal.winrm.RecordingWinRmTool.CustomResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

/**
 * Tests Windows YAML blueprint features.
 */
@Test
public class WindowsYamlTest extends AbstractWindowsYamlTest {
    
    private static final Logger log = LoggerFactory.getLogger(WindowsYamlTest.class);

    private static final String LOCATION_CATALOG_ID = "byonWindowsLoc";
    
    protected Entity app;
    protected VanillaWindowsProcess entity;
    
    protected boolean useDefaultProperties() {
        return true;
    }
    
    @BeforeMethod(alwaysRun = true)
    public void setUpClass() throws Exception {
        super.setUp();
        
        RecordingWinRmTool.clear();
        
        addCatalogItems(
                "brooklyn.catalog:",
                "  id: " + LOCATION_CATALOG_ID,
                "  version: 1.0.0",
                "  itemType: location",
                "  item:",
                "    type: byon",
                "    brooklyn.config:",
                "      hosts:",
                "      - winrm: 1.2.3.4",
                "        user: admin",
                "        brooklyn.winrm.config.winrmToolClass: "+RecordingWinRmTool.class.getName(),
                "        password: pa55w0rd",
                "        osFamily: windows");
    }

    @AfterMethod(alwaysRun = true)
    @Override
    public void tearDown() throws Exception {
        try {
            super.tearDown();
        } catch (Throwable t) {
            RecordingWinRmTool.clear();
        }
    }

    @Test
    public void testPassesThroughUseHttps() throws Exception {
        super.createAndStartApplication(
                "location: "+LOCATION_CATALOG_ID,
                "services:",
                "- type: "+VanillaWindowsProcess.class.getName(),
                "  brooklyn.config:",
                "    launch.powershell.command: myLaunch",
                "    checkRunning.powershell.command: myCheckRunning",
                "    provisioning.properties:",
                "      winrm.useHttps: true");

        Map<?, ?> constructorProps = RecordingWinRmTool.getLastConstructorProps();
        assertEquals(constructorProps.get("winrm.useHttps"), Boolean.TRUE, "props="+constructorProps);
    }
    
    @Test
    public void testPowershellMinimal() throws Exception {
        createAndStartApplication(
                "location: "+LOCATION_CATALOG_ID,
                "services:",
                "- type: "+VanillaWindowsProcess.class.getName(),
                "  brooklyn.config:",
                "    launch.powershell.command: myLaunch",
                "    checkRunning.powershell.command: myCheckRunning");
        
        RecordingWinRmTool.getExecs();
        ExecCmdAsserts.assertExecHasOnlyOnce(RecordingWinRmTool.getExecs(), "myLaunch");
        ExecCmdAsserts.assertExecContains(RecordingWinRmTool.getLastExec(), "myCheckRunning");
    }

    @Test
    public void testPowershell() throws Exception {
        RecordingWinRmTool.setCustomResponse("myPreInstall", new CustomResponse(0, "myPreInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myInstall", new CustomResponse(0, "myInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPostInstall", new CustomResponse(0, "myPostInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myCustomize", new CustomResponse(0, "myCustomizeStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPreLaunch", new CustomResponse(0, "myPreLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myLaunch", new CustomResponse(0, "myLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPostLaunch", new CustomResponse(0, "myPostLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myStop", new CustomResponse(0, "myStopStdout", ""));
        
        app = createAndStartApplication(
                "location: "+LOCATION_CATALOG_ID,
                "services:",
                "- type: "+VanillaWindowsProcess.class.getName(),
                "  brooklyn.config:",
                "    pre.install.powershell.command: myPreInstall", 
                "    install.powershell.command: myInstall",
                "    post.install.powershell.command: myPostInstall",
                "    customize.powershell.command: myCustomize",
                "    pre.launch.powershell.command: myPreLaunch",
                "    launch.powershell.command: myLaunch",
                "    post.launch.powershell.command: myPostLaunch",
                "    checkRunning.powershell.command: myCheckRunning",
                "    stop.powershell.command: myStop");
        entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, VanillaWindowsProcess.class));
        
        Map<String, List<String>> stdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: pre-install.*", ImmutableList.of("myPreInstallStdout"))
                .put("winrm: install.*", ImmutableList.of("myInstallStdout"))
                .put("winrm: post-install.*", ImmutableList.of("myPostInstallStdout"))
                .put("winrm: customize.*", ImmutableList.of("myCustomizeStdout"))
                .put("winrm: pre-launch.*", ImmutableList.of("myPreLaunchStdout"))
                .put("winrm: launch.*", ImmutableList.of("myLaunchStdout"))
                .put("winrm: post-launch.*", ImmutableList.of("myPostLaunchStdout"))
                .build();

        assertStreams(entity, stdouts);

        entity.stop();

        Map<String, List<String>> stopStdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: stop.*", ImmutableList.of("myStopStdout"))
                .build();

        assertStreams(entity, stopStdouts);
    }

    @Test
    public void testCommands() throws Exception {
        RecordingWinRmTool.setCustomResponse("myPreInstall", new CustomResponse(0, "myPreInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myInstall", new CustomResponse(0, "myInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPostInstall", new CustomResponse(0, "myPostInstallStdout", ""));
        RecordingWinRmTool.setCustomResponse("myCustomize", new CustomResponse(0, "myCustomizeStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPreLaunch", new CustomResponse(0, "myPreLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myLaunch", new CustomResponse(0, "myLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myPostLaunch", new CustomResponse(0, "myPostLaunchStdout", ""));
        RecordingWinRmTool.setCustomResponse("myStop", new CustomResponse(0, "myStopStdout", ""));
        
        app = createAndStartApplication(
                "location: "+LOCATION_CATALOG_ID,
                "services:",
                "- type: "+VanillaWindowsProcess.class.getName(),
                "  brooklyn.config:",
                "    pre.install.command: myPreInstall", 
                "    install.command: myInstall",
                "    post.install.command: myPostInstall",
                "    customize.command: myCustomize",
                "    pre.launch.command: myPreLaunch",
                "    launch.command: myLaunch",
                "    post.launch.command: myPostLaunch",
                "    checkRunning.command: myCheckRunning",
                "    stop.command: myStop");
        entity = Iterables.getOnlyElement(Entities.descendantsAndSelf(app, VanillaWindowsProcess.class));
        
        Map<String, List<String>> stdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: pre-install.*", ImmutableList.of("myPreInstallStdout"))
                .put("winrm: install.*", ImmutableList.of("myInstallStdout"))
                .put("winrm: post-install.*", ImmutableList.of("myPostInstallStdout"))
                .put("winrm: customize.*", ImmutableList.of("myCustomizeStdout"))
                .put("winrm: pre-launch.*", ImmutableList.of("myPreLaunchStdout"))
                .put("winrm: launch.*", ImmutableList.of("myLaunchStdout"))
                .put("winrm: post-launch.*", ImmutableList.of("myPostLaunchStdout"))
                .build();

        assertStreams(entity, stdouts);

        entity.stop();

        Map<String, List<String>> stopStdouts = ImmutableMap.<String, List<String>>builder()
                .put("winrm: stop.*", ImmutableList.of("myStopStdout"))
                .build();

        assertStreams(entity, stopStdouts);
    }

    @Override
    protected Logger getLogger() {
        return log;
    }
}
